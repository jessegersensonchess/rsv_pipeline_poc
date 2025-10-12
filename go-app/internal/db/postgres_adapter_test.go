package db

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

//
// ==============================
//  FAKES (Test Doubles for pgx)
// ==============================
//
// We provide minimal fakes that satisfy the interfaces used by our adapter.
// Goals:
//  - Avoid network/socket usage (hermetic, fully deterministic tests).
//  - Capture arguments for assertions.
//  - Allow us to simulate success and failure paths.
//  - Keep the fake surface area as small as possible while satisfying pgx v5
//    interfaces used in our code.
//

// fakePgConn implements pgConnLike (the seam around *pgx.Conn).
// It records Exec calls and simulates Begin/Close behavior.
type fakePgConn struct {
	execCalls []struct {
		q    string
		args []any
	}
	beginTx  pgx.Tx // returned when beginErr == nil
	beginErr error  // returned from Begin when non-nil
	closed   bool   // set true when Close is called
}

// Exec records the query and args. We don't simulate rows affected since
// adapter code only checks for error presence.
func (c *fakePgConn) Exec(ctx context.Context, q string, args ...any) (pgconn.CommandTag, error) {
	c.execCalls = append(c.execCalls, struct {
		q    string
		args []any
	}{q: q, args: args})
	return pgconn.CommandTag{}, nil
}

// Begin returns either the injected transaction or an error, enabling tests
// to exercise both success and failure paths deterministically.
func (c *fakePgConn) Begin(ctx context.Context) (pgx.Tx, error) {
	if c.beginErr != nil {
		return nil, c.beginErr
	}
	return c.beginTx, nil
}

// Close marks the fake as closed. We assert this in tests when relevant.
func (c *fakePgConn) Close(ctx context.Context) error { c.closed = true; return nil }

// fakePgTx implements pgx.Tx (v5) with no-ops for methods we don't exercise,
// and with instrumentation for methods we do (Exec, CopyFrom, Commit/Rollback).
// This satisfies the interface shape used by our adapter and lets us assert on
// arguments/behavior without a live DB.
type fakePgTx struct {
	execCalls []struct {
		q    string
		args []any
	}
	copyCount   int64 // number of rows "copied" via CopyFrom
	copyErr     error // if set, CopyFrom returns this error
	commitErr   error // if set, Commit returns this error
	rollbackErr error // if set, Rollback returns this error
}

// Begin exists on pgx.Tx in v5; nested tx isn't used in our tests, so
// returning self is sufficient to satisfy the interface.
func (t *fakePgTx) Begin(ctx context.Context) (pgx.Tx, error) { return t, nil }

// Exec records the query and args so callers can assert correct pass-through.
func (t *fakePgTx) Exec(ctx context.Context, q string, args ...any) (pgconn.CommandTag, error) {
	t.execCalls = append(t.execCalls, struct {
		q    string
		args []any
	}{q, args})
	return pgconn.CommandTag{}, nil
}

// Query is not exercised by this adapter. A minimal stub is sufficient.
func (t *fakePgTx) Query(ctx context.Context, q string, args ...any) (pgx.Rows, error) {
	return nil, nil
}

// QueryRow is not exercised by this adapter. A minimal stub is sufficient.
func (t *fakePgTx) QueryRow(ctx context.Context, q string, args ...any) pgx.Row { return nil }

// CopyFrom drains the provided source and counts rows. This mirrors how pgx
// pulls rows from its CopyFromSource. We support injecting an error to test
// error propagation.
func (t *fakePgTx) CopyFrom(ctx context.Context, table pgx.Identifier, cols []string, src pgx.CopyFromSource) (int64, error) {
	if t.copyErr != nil {
		return 0, t.copyErr
	}
	var n int64
	for src.Next() {
		if _, err := src.Values(); err != nil {
			return n, err
		}
		n++
	}
	if err := src.Err(); err != nil {
		return n, err
	}
	t.copyCount = n
	return n, nil
}

// SendBatch, LargeObjects, Conn: present on pgx.Tx interface (v5). Not used
// by our adapter; minimal stubs maintain interface compliance.
func (t *fakePgTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults { return nil }
func (t *fakePgTx) LargeObjects() pgx.LargeObjects                               { return pgx.LargeObjects{} }
func (t *fakePgTx) Conn() *pgx.Conn                                              { return nil }

// Prepare/Deallocate exist on pgx.Tx in v5. Our code doesn't exercise them;
// no-op implementations keep the fake compliant with the interface.
func (t *fakePgTx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	return nil, nil
}
func (t *fakePgTx) Deallocate(ctx context.Context, name string) error { return nil }

// Commit/Rollback propagate injected errors so the adapter's behavior can be verified.
func (t *fakePgTx) Commit(ctx context.Context) error   { return t.commitErr }
func (t *fakePgTx) Rollback(ctx context.Context) error { return t.rollbackErr }

//
// =====================
//  ADAPTER TESTS (pgx)
// =====================
//
// Test strategy (high level):
//  - Validate pass-through of Exec query/args on the connection.
//  - Validate BeginTx: success returns a non-nil Tx; error propagates and Tx is nil.
//  - Validate Tx behaviors: Exec pass-through, CopyInto row counting + error path,
//    Commit success, and Rollback error propagation.
//
// These tests intentionally avoid touching network or filesystem, making them
// fast and reliable. Each test isolates a single behavior and uses clear
// Arrange/Act/Assert blocks.
//

// Test_pgDB_Exec_PassesThrough verifies that pgDB.Exec forwards the SQL and
// arguments to the underlying connection without mutation and returns any error.
func Test_pgDB_Exec_PassesThrough(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Arrange: a fake connection to capture Exec calls.
	fc := &fakePgConn{}
	p := newPgDBFromConn(fc)

	// Act: call Exec through the adapter.
	if err := p.Exec(ctx, "VACUUM"); err != nil {
		t.Fatalf("Exec err: %v", err)
	}

	// Assert: query and args are recorded exactly once, unmodified.
	if len(fc.execCalls) != 1 || fc.execCalls[0].q != "VACUUM" || len(fc.execCalls[0].args) != 0 {
		t.Fatalf("exec captured = %#v", fc.execCalls)
	}
}

// Test_pgDB_BeginTx_SuccessAndError verifies both the success and error
// paths of BeginTx: on success it returns a non-nil Tx; on error it returns
// a non-nil error and a nil Tx.
func Test_pgDB_BeginTx_SuccessAndError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Success case: fake Begin returns a transaction and no error.
	fcOK := &fakePgConn{beginTx: &fakePgTx{}}
	pOK := newPgDBFromConn(fcOK)
	tx, err := pOK.BeginTx(ctx)
	if err != nil || tx == nil {
		t.Fatalf("BeginTx success expected non-nil tx, got tx=%v err=%v", tx, err)
	}

	// Error case: fake Begin returns an error.
	fcErr := &fakePgConn{beginErr: errors.New("boom")}
	pErr := newPgDBFromConn(fcErr)
	tx2, err2 := pErr.BeginTx(ctx)

	// Require non-nil error and nil tx (adapter should not fabricate a Tx on error).
	if err2 == nil || tx2 != nil {
		t.Fatalf("expected error and nil tx, got tx=%v err=%v", tx2, err2)
	}
}

// Test_pgTx_Exec_CopyInto_Commit_Rollback exercises the pgTx wrapper behaviors:
//   - Exec forwards SQL/args.
//   - CopyInto drains a source and returns the count.
//   - Commit success returns nil.
//   - Rollback error is propagated.
func Test_pgTx_Exec_CopyInto_Commit_Rollback(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Arrange: a fake transaction to capture calls and control outcomes.
	ftx := &fakePgTx{}
	tx := newPgTxForTest(ftx)

	// Act/Assert (Exec): verify pass-through of SQL and args.
	if err := tx.Exec(ctx, "SET search_path=?", "public"); err != nil {
		t.Fatalf("Exec err: %v", err)
	}
	if len(ftx.execCalls) != 1 || ftx.execCalls[0].q != "SET search_path=?" || !reflect.DeepEqual(ftx.execCalls[0].args, []any{"public"}) {
		t.Fatalf("exec captured = %#v", ftx.execCalls)
	}

	// Act/Assert (CopyInto): provide two rows; adapter should return 2.
	rows := [][]interface{}{
		{int64(1), []byte(`{"a":1}`)},
		{int64(2), []byte(`{"b":2}`)},
	}
	n, err := tx.CopyInto(ctx, "vehicle_tech", []string{"pcv", "payload"}, rows)
	if err != nil {
		t.Fatalf("CopyInto err: %v", err)
	}
	if n != int64(len(rows)) || ftx.copyCount != n {
		t.Fatalf("copy count mismatch: n=%d ftx=%d", n, ftx.copyCount)
	}

	// Act/Assert (Commit): default path succeeds.
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit err: %v", err)
	}

	// Act/Assert (Rollback): inject an error and verify propagation.
	ftx.rollbackErr = errors.New("rb")
	if err := tx.Rollback(ctx); err == nil {
		t.Fatalf("rollback should error")
	}
}
