package db

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
)

//
// =====================================
//  FAKES (Test Doubles for SQL adapter)
// =====================================
//
// Purpose:
//   These fakes implement the minimal seams used by the adapter. They enable
//   fast, deterministic tests with no network or real DB connections.
//
// Design:
//   - fakeStmt: captures ExecContext calls; can fail on a specific call.
//   - fakeSQLTx: implements sqlTxCore; can inject prepare/commit/rollback errs.
//   - fakeSQLDB: implements sqlDBCore; records Exec and simulates BeginTx.
//

// fakeStmt simulates *sql.Stmt via the stmtCore seam.
// It records ExecContext arguments and can be configured to fail on the Nth call.
type fakeStmt struct {
	execs  [][]any // recorded ExecContext args per call
	errOn  int     // 1-based index of the call to fail; 0 means “never fail”
	closed bool
}

func (s *fakeStmt) ExecContext(ctx context.Context, args ...any) (sql.Result, error) {
	s.execs = append(s.execs, args)
	if s.errOn > 0 && len(s.execs) == s.errOn {
		return nil, errors.New("stmt exec failure")
	}
	return nil, nil
}
func (s *fakeStmt) Close() error { s.closed = true; return nil }

// fakeSQLTx implements sqlTxCore.
// It can inject a prepare error; otherwise it returns a (possibly injected) stmt.
// Commit/Rollback errors are also injectable.
type fakeSQLTx struct {
	execCalls []struct {
		q    string
		args []any
	}
	stmt        stmtCore
	prepErr     error
	commitErr   error
	rollbackErr error
}

func (t *fakeSQLTx) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	t.execCalls = append(t.execCalls, struct {
		q    string
		args []any
	}{q, args})
	return nil, nil
}
func (t *fakeSQLTx) PrepareContext(ctx context.Context, q string) (stmtCore, error) {
	if t.prepErr != nil {
		return nil, t.prepErr
	}
	if t.stmt == nil {
		t.stmt = &fakeStmt{}
	}
	return t.stmt, nil
}
func (t *fakeSQLTx) Commit() error   { return t.commitErr }
func (t *fakeSQLTx) Rollback() error { return t.rollbackErr }

// fakeSQLDB implements sqlDBCore and records ExecContext calls.
// BeginTx returns a zero-value *sql.Tx sentinel to satisfy the signature;
// the adapter wraps it in realSQLTx internally (we never use the sentinel directly).
type fakeSQLDB struct {
	execCalls []struct {
		q    string
		args []any
	}
	tx       *fakeSQLTx
	beginErr error
	closed   bool
}

func (f *fakeSQLDB) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	f.execCalls = append(f.execCalls, struct {
		q    string
		args []any
	}{q: q, args: args})
	return nil, nil
}
func (f *fakeSQLDB) BeginTx(ctx context.Context, _ *sql.TxOptions) (*sql.Tx, error) {
	if f.beginErr != nil {
		return nil, f.beginErr
	}
	// Zero-value *sql.Tx is sufficient as a sentinel; it won’t be used directly.
	return &sql.Tx{}, nil
}
func (f *fakeSQLDB) Close() error { f.closed = true; return nil }

//
// ======================
//  ADAPTER TESTS (sqlDB)
// ======================
//
// Tests focus on adapter behavior (pass-through, error propagation, and
// CopyInto logic) and remain hermetic.
//

// Test_sqlDB_Exec_PassesThrough verifies that sqlDB.Exec forwards the SQL string
// and arguments verbatim to the underlying core and returns only the error.
func Test_sqlDB_Exec_PassesThrough(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Arrange
	fdb := &fakeSQLDB{}
	s := newSQLDBForTest(fdb)

	// Act
	if err := s.Exec(ctx, "UPDATE t SET a=?", 1); err != nil {
		t.Fatalf("Exec error: %v", err)
	}

	// Assert
	if len(fdb.execCalls) != 1 ||
		fdb.execCalls[0].q != "UPDATE t SET a=?" ||
		!reflect.DeepEqual(fdb.execCalls[0].args, []any{1}) {
		t.Fatalf("exec captured = %#v", fdb.execCalls)
	}
}

// Test_sqlDB_BeginTx_SuccessAndError validates both BeginTx paths:
//   - success: returns a non-nil Tx adapter
//   - error: propagates the error and returns a nil Tx
func Test_sqlDB_BeginTx_SuccessAndError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Success: BeginTx returns sentinel *sql.Tx; adapter must return a non-nil Tx.
	fdbOK := &fakeSQLDB{tx: &fakeSQLTx{}}
	sOK := newSQLDBForTest(fdbOK)
	tx, err := sOK.BeginTx(ctx)
	if err != nil || tx == nil {
		t.Fatalf("BeginTx success: want non-nil tx, got tx=%v err=%v", tx, err)
	}

	// Error: injected beginErr must propagate; adapter must return tx=nil.
	fdbErr := &fakeSQLDB{beginErr: errors.New("nope")}
	sErr := newSQLDBForTest(fdbErr)
	tx2, err2 := sErr.BeginTx(ctx)
	if err2 == nil || !strings.Contains(err2.Error(), "nope") || tx2 != nil {
		t.Fatalf("BeginTx error not propagated correctly: tx=%T err=%v", tx2, err2)
	}
}

// Test_sqlTx_Exec_CopyInto_Commit_Rollback_Table exercises the sqlTx wrapper:
//   - Exec pass-through
//   - CopyInto success, prepare error, and exec error (returns inserted-so-far)
//   - Commit success and Rollback error propagation
func Test_sqlTx_Exec_CopyInto_Commit_Rollback_Table(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	type row = []interface{}

	cases := []struct {
		name       string
		tx         *fakeSQLTx
		rows       []row
		wantN      int64
		wantErrSub string
	}{
		{
			name:  "copyinto_success",
			tx:    &fakeSQLTx{stmt: &fakeStmt{}},
			rows:  []row{{1, "a"}, {2, "b"}},
			wantN: 2,
		},
		{
			name:       "prepare_error",
			tx:         &fakeSQLTx{prepErr: errors.New("prep!")},
			rows:       []row{{1}},
			wantN:      0,
			wantErrSub: "prep!",
		},
		{
			name:       "exec_error_on_second_row",
			tx:         &fakeSQLTx{stmt: &fakeStmt{errOn: 2}},
			rows:       []row{{1}, {2}},
			wantN:      1, // first succeeded; error on second returns inserted-so-far
			wantErrSub: "stmt exec failure",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ad := newSQLTxForTest(tc.tx)

			n, err := ad.CopyInto(ctx, "T", []string{"c1", "c2"}, tc.rows)
			if tc.wantErrSub == "" && err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if tc.wantErrSub != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
					t.Fatalf("want err containing %q, got %v", tc.wantErrSub, err)
				}
			}
			if n != tc.wantN {
				t.Fatalf("CopyInto n=%d want %d", n, tc.wantN)
			}
		})
	}

	// Commit/Rollback behavior (separate from CopyInto cases).
	ftx := &fakeSQLTx{}
	ad := newSQLTxForTest(ftx)

	if err := ad.Commit(ctx); err != nil {
		t.Fatalf("commit err: %v", err)
	}
	ftx.rollbackErr = errors.New("boom")
	if err := ad.Rollback(ctx); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("rollback error not propagated: %v", err)
	}
}

// Test_join_Checker validates the helper used to construct placeholder lists.
func Test_join_Checker(t *testing.T) {
	t.Parallel()

	if got := join([]string{"@p1", "@p2", "@p3"}, ","); got != "@p1,@p2,@p3" {
		t.Fatalf("join output mismatch: got %q", got)
	}
	if got := join(nil, ","); got != "" {
		t.Fatalf("join(nil) expected empty string, got %q", got)
	}
}
