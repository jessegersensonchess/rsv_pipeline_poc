package db

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
)

//
// ==============================
//  Small MSSQL adapter test fakes
// ==============================
//
// We keep these fakes narrow and focused on the smallMSSQL seams. They avoid
// any live database dependency, while still letting us verify DDL/Close logic.
//

type fakeSmallStmt struct {
	execs  int
	errOn  int // fail on Nth Exec
	closed bool
}

func (s *fakeSmallStmt) ExecContext(ctx context.Context, args ...any) (sql.Result, error) {
	s.execs++
	if s.errOn > 0 && s.execs == s.errOn {
		return nil, errors.New("stmt failed")
	}
	return nil, nil
}
func (s *fakeSmallStmt) Close() error { s.closed = true; return nil }

type fakeSmallTx struct {
	stmt       stmtCore
	prepErr    error
	commitErr  error
	rolledBack bool
	committed  bool
}

func (t *fakeSmallTx) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	return nil, nil
}
func (t *fakeSmallTx) PrepareContext(ctx context.Context, q string) (stmtCore, error) {
	if t.prepErr != nil {
		return nil, t.prepErr
	}
	if t.stmt == nil {
		t.stmt = &fakeSmallStmt{}
	}
	return t.stmt, nil
}
func (t *fakeSmallTx) Commit() error {
	t.committed = true
	return t.commitErr
}
func (t *fakeSmallTx) Rollback() error {
	t.rolledBack = true
	return nil
}

type fakeSmallDB struct {
	execDDL   []string
	beginErr  error
	tx        *fakeSmallTx
	closed    bool
	execError error
}

func (d *fakeSmallDB) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	d.execDDL = append(d.execDDL, q)
	return nil, d.execError
}
func (d *fakeSmallDB) BeginTx(ctx context.Context, _ *sql.TxOptions) (*sql.Tx, error) {
	// Return a dummy *sql.Tx just to match the signature.
	// smallMSSQL uses *sql.Tx directly; for hermetic tests we avoid exercising
	// the full CopyOwnership path and instead validate DDL + Close behavior.
	return &sql.Tx{}, d.beginErr
}
func (d *fakeSmallDB) Close() error { d.closed = true; return nil }

//
// ============================
//  Constructors / AsSQLDB
// ============================
//
// These tests validate constructor error paths (no network) and that AsSQLDB
// exposes the underlying *sql.DB for both supported core shapes.
//

// TestNewSQLDB_UnknownDriverError ensures NewSQLDB fails fast for an unregistered driver.
func TestNewSQLDB_UnknownDriverError(t *testing.T) {
	t.Parallel()
	if _, err := NewSQLDB("definitely-not-registered", "dsn"); err == nil {
		t.Fatalf("expected error for unknown driver")
	}
}

// TestNewSmallMSSQL_UnknownDriverError ensures NewSmallMSSQL surfaces the driver error.
func TestNewSmallMSSQL_UnknownDriverError(t *testing.T) {
	t.Parallel()
	if _, err := NewSmallMSSQL("sqlserver://user:pass@localhost:1433?database=db"); err == nil {
		t.Fatalf("expected error for unregistered sqlserver driver")
	}
}

// TestAsSQLDB_TrueBranch verifies AsSQLDB returns *sql.DB for both core shapes:
//  1. sqlDB{db: *sql.DB} and 2) sqlDB{db: realSQLDB{db: *sql.DB}}.
func TestAsSQLDB_TrueBranch(t *testing.T) {
	t.Parallel()

	under := &sql.DB{}
	ad1 := &sqlDB{db: under}
	got1, ok1 := AsSQLDB(ad1)
	if !ok1 || got1 != under {
		t.Fatalf("AsSQLDB should expose *sql.DB when core is *sql.DB")
	}

	ad2 := &sqlDB{db: realSQLDB{db: under}}
	got2, ok2 := AsSQLDB(ad2)
	if !ok2 || got2 != under {
		t.Fatalf("AsSQLDB should expose *sql.DB when core is realSQLDB{db:*sql.DB}")
	}
}

// notSQLAdapter is a DB implementation that is not backed by *sql.DB.
// It is used to exercise the false branch in AsSQLDB.
type notSQLAdapter struct{}

func (notSQLAdapter) Exec(ctx context.Context, q string, args ...any) error { return nil }
func (notSQLAdapter) BeginTx(ctx context.Context) (Tx, error)               { return nil, nil }
func (notSQLAdapter) Close(ctx context.Context) error                       { return nil }

// TestAsSQLDB_FalseBranch verifies AsSQLDB returns (nil,false) for non-sql adapters.
func TestAsSQLDB_FalseBranch(t *testing.T) {
	t.Parallel()

	if got, ok := AsSQLDB(notSQLAdapter{}); ok || got != nil {
		t.Fatalf("AsSQLDB should return (nil,false) for non-sql adapters")
	}
}

//
// ============================
//  smallMSSQL: DDL / Close only
// ============================
//
// We keep these tests hermetic. CopyOwnership path in smallMSSQL relies on
// *sql.Tx behaviors and a live driver; the row-insert semantics are already
// validated via sqlTx.CopyInto in adapter tests above.
//

// TestSmallMSSQL_CreateOwnershipTable verifies the DDL is executed and targets
// the expected table name (ownership) using SQL Server semantics.
func TestSmallMSSQL_CreateOwnershipTable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	f := &fakeSmallDB{tx: &fakeSmallTx{}}
	m := &smallMSSQL{db: f}

	if err := m.CreateOwnershipTable(ctx); err != nil {
		t.Fatalf("CreateOwnershipTable err: %v", err)
	}
	if len(f.execDDL) != 1 || !strings.Contains(f.execDDL[0], "CREATE TABLE ownership") {
		t.Fatalf("unexpected DDL: %v", f.execDDL)
	}
}

// TestSmallMSSQL_Close verifies the adapter delegates Close to the underlying core.
func TestSmallMSSQL_Close(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	f := &fakeSmallDB{tx: &fakeSmallTx{}}
	m := &smallMSSQL{db: f}
	if err := m.Close(ctx); err != nil {
		t.Fatalf("Close err: %v", err)
	}
	if !f.closed {
		t.Fatalf("underlying Close not called")
	}
}
