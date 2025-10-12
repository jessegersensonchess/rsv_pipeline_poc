package db

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

//
// ==============================
//  FAKES (focused, no conflicts)
// ==============================
//
// We reuse existing fakes from postgres_adapter_test.go where possible.
// To observe Commit/Rollback and simulate partial CopyFrom results, we
// introduce a *separate* recording fake with a distinct name to avoid
// type redefinitions.
//

// fakeSmallConn implements smallPgConnLike for smallPg tests.
type fakeSmallConn struct {
	execSQL  []string
	beginTx  pgx.Tx
	beginErr error
	closed   bool
	execErr  error
	closeErr error
}

func (c *fakeSmallConn) Exec(ctx context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	c.execSQL = append(c.execSQL, sql)
	return pgconn.CommandTag{}, c.execErr
}
func (c *fakeSmallConn) Begin(ctx context.Context) (pgx.Tx, error) {
	if c.beginErr != nil {
		return nil, c.beginErr
	}
	return c.beginTx, nil
}
func (c *fakeSmallConn) Close(ctx context.Context) error {
	c.closed = true
	return c.closeErr
}

// smallFakeTxRecording is a pgx.Tx-compatible recorder used only here.
// It tracks Commit/Rollback calls and lets tests control CopyFrom outcomes.
type smallFakeTxRecording struct {
	copyErr        error  // if set, CopyFrom returns this error
	commitErr      error  // if set, Commit returns this error
	rollbackErr    error  // if set, Rollback returns this error
	forcedCopied   *int64 // if non-nil, CopyFrom returns this count
	commitCalled   bool
	rollbackCalled bool
}

func (t *smallFakeTxRecording) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}
func (t *smallFakeTxRecording) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, nil
}
func (t *smallFakeTxRecording) QueryRow(context.Context, string, ...any) pgx.Row       { return nil }
func (t *smallFakeTxRecording) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults { return nil }
func (t *smallFakeTxRecording) LargeObjects() pgx.LargeObjects                         { return pgx.LargeObjects{} }
func (t *smallFakeTxRecording) Conn() *pgx.Conn                                        { return nil }
func (t *smallFakeTxRecording) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, nil
}
func (t *smallFakeTxRecording) Deallocate(context.Context, string) error { return nil }
func (t *smallFakeTxRecording) Begin(context.Context) (pgx.Tx, error)    { return t, nil }
func (t *smallFakeTxRecording) Commit(context.Context) error {
	t.commitCalled = true
	return t.commitErr
}
func (t *smallFakeTxRecording) Rollback(context.Context) error {
	t.rollbackCalled = true
	return t.rollbackErr
}
func (t *smallFakeTxRecording) CopyFrom(ctx context.Context, table pgx.Identifier, cols []string, src pgx.CopyFromSource) (int64, error) {
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
	if t.forcedCopied != nil {
		return *t.forcedCopied, nil
	}
	return n, nil
}

//
// ===============================
//  smallPg tests (table-friendly)
// ===============================

// TestSmallPg_CreateOwnershipTable verifies the DDL executes once and targets
// the expected table. We assert the SQL contains "CREATE TABLE IF NOT EXISTS ownership".
func TestSmallPg_CreateOwnershipTable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	fc := &fakeSmallConn{beginTx: &smallFakeTxRecording{}}
	p := newSmallPgFromConn(fc)

	if err := p.CreateOwnershipTable(ctx); err != nil {
		t.Fatalf("CreateOwnershipTable err: %v", err)
	}
	if len(fc.execSQL) != 1 || !strings.Contains(fc.execSQL[0], "CREATE TABLE IF NOT EXISTS ownership") {
		t.Fatalf("unexpected DDL: %v", fc.execSQL)
	}
}

// TestSmallPg_CopyOwnership_Success covers the happy path: Begin -> CopyFrom -> Commit.
// Note: because the code defers Rollback, Rollback is also invoked after Commit;
// we only assert that Commit was called.
func TestSmallPg_CopyOwnership_Success(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tx := &smallFakeTxRecording{}
	fc := &fakeSmallConn{beginTx: tx}
	p := newSmallPgFromConn(fc)

	rows := [][]interface{}{
		{1, 1, 1, true, 123, "ACME", "Addr", nil, nil},
		{2, 1, 1, false, 456, "BETA", "Addr2", nil, nil},
	}
	if err := p.CopyOwnership(ctx, rows); err != nil {
		t.Fatalf("CopyOwnership err: %v", err)
	}
	if !tx.commitCalled {
		t.Fatalf("expected commit to be called")
	}
	// Rollback may also be called due to defer pattern; do not assert on it here.
}

// TestSmallPg_CopyOwnership_PartialInsert exercises the "fewer rows than expected" log path
// by forcing CopyFrom to report fewer rows than provided. We still expect Commit.
func TestSmallPg_CopyOwnership_PartialInsert(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	forced := int64(1) // simulate 1 row inserted out of 2
	tx := &smallFakeTxRecording{forcedCopied: &forced}
	fc := &fakeSmallConn{beginTx: tx}
	p := newSmallPgFromConn(fc)

	rows := [][]interface{}{
		{1, 1, 1, true, 123, "ACME", "Addr", nil, nil},
		{2, 1, 1, false, 456, "BETA", "Addr2", nil, nil},
	}
	if err := p.CopyOwnership(ctx, rows); err != nil {
		t.Fatalf("CopyOwnership err: %v", err)
	}
	if !tx.commitCalled {
		t.Fatalf("expected commit on partial insert")
	}
}

// TestSmallPg_CopyOwnership_CopyError ensures errors from CopyFrom propagate
// and the deferred Rollback is invoked (Commit should not be called).
func TestSmallPg_CopyOwnership_CopyError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tx := &smallFakeTxRecording{copyErr: errors.New("copy failed")}
	fc := &fakeSmallConn{beginTx: tx}
	p := newSmallPgFromConn(fc)

	err := p.CopyOwnership(ctx, [][]interface{}{{1}})
	if err == nil {
		t.Fatalf("expected error from CopyOwnership")
	}
	if !tx.rollbackCalled {
		t.Fatalf("expected rollback on error")
	}
	if tx.commitCalled {
		t.Fatalf("did not expect commit on error")
	}
}

// TestSmallPg_Close verifies Close delegates to the underlying connection.
func TestSmallPg_Close(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	fc := &fakeSmallConn{beginTx: &smallFakeTxRecording{}}
	p := newSmallPgFromConn(fc)

	if err := p.Close(ctx); err != nil {
		t.Fatalf("Close err: %v", err)
	}
	if !fc.closed {
		t.Fatalf("expected underlying connection to be closed")
	}
}

// TestNewSmallPg_InvalidDSN makes sure invalid DSNs fail fast (parse/config level),
// giving coverage to the constructor's error branch without dialing a live DB.
func TestNewSmallPg_InvalidDSN(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	if _, err := NewSmallPg(ctx, "not-a-valid-dsn"); err == nil {
		t.Fatalf("expected error for invalid DSN")
	}
}
