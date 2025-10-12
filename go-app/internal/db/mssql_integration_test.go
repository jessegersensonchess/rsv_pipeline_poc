package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

//
// ==========================================================
//  In-memory "testsql" driver for exercising real wrappers
// ==========================================================
//
// Purpose
//   Drive the real adapter stack (*sql.DB → *sql.Tx → *sql.Stmt) without a
//   networked database, so the following thin wrappers are truly executed:
//
//     • realSQLTx.ExecContext
//     • realSQLTx.PrepareContext   → realStmt.ExecContext + realStmt.Close
//     • realSQLTx.Commit           → sqlTx.Commit path
//     • realSQLTx.Rollback         → sqlTx.Rollback path
//     • sqlTx.Exec                 (delegates to realSQLTx.ExecContext)
//     • sqlDB.Close
//     • smallMSSQL.CopyOwnership   (success path end-to-end)
//
// Approach
//   Implement a minimal sql/driver driver that supports ping, BeginTx,
//   PrepareContext, and ExecContext. The driver records calls but never
//   touches a real DB, keeping tests hermetic.
//

// Register the test driver exactly once.
func init() {
	sql.Register("testsql", &testDriver{})
}

type testDriver struct{}

// Open parses a tiny DSN format (e.g., "execfail=2;tag=abc") and returns a
// connection that supports Ping, BeginTx, PrepareContext, and ExecContext.
func (d *testDriver) Open(name string) (driver.Conn, error) {
	cfg := parseDSN(name)
	return &testConn{
		cfg:    cfg,
		execMu: &sync.Mutex{},
	}, nil
}

// parseDSN extracts key=value pairs from a semi-colon separated DSN.
// Unknown keys are ignored; values are used to toggle behavior in tests.
func parseDSN(dsn string) map[string]string {
	out := map[string]string{}
	for _, part := range strings.Split(dsn, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) == 2 {
			out[strings.ToLower(strings.TrimSpace(kv[0]))] = strings.TrimSpace(kv[1])
		}
	}
	return out
}

// testConn implements the driver.Conn interfaces we need:
//   - driver.Pinger
//   - driver.ConnBeginTx
//   - driver.ConnPrepareContext
//   - driver.ExecerContext
type testConn struct {
	driver.Conn
	driver.Pinger
	driver.ConnBeginTx
	driver.ConnPrepareContext
	driver.ExecerContext

	cfg      map[string]string
	execMu   *sync.Mutex
	lastExec *execRecord
	closed   bool
}

type execRecord struct {
	query string
	args  []driver.NamedValue
}

func (c *testConn) Close() error               { c.closed = true; return nil }
func (c *testConn) Ping(context.Context) error { return nil }
func (c *testConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return &testTx{c: c}, nil
}

// ExecContext records the statement executed directly on the connection.
// (The adapter uses prepared statements for inserts; this is here for completeness.)
func (c *testConn) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	c.execMu.Lock()
	c.lastExec = &execRecord{query: q, args: append([]driver.NamedValue(nil), args...)}
	c.execMu.Unlock()
	return testResult(1), nil
}

// PrepareContext returns a statement that supports ExecContext.
func (c *testConn) PrepareContext(ctx context.Context, q string) (driver.Stmt, error) {
	return &testStmt{c: c, query: q}, nil
}

type testTx struct {
	driver.Tx
	c         *testConn
	committed bool
	rolled    bool
}

func (t *testTx) Commit() error   { t.committed = true; return nil }
func (t *testTx) Rollback() error { t.rolled = true; return nil }

// testStmt implements driver.Stmt + driver.StmtExecContext.
type testStmt struct {
	driver.Stmt
	driver.StmtExecContext

	c       *testConn
	query   string
	execCnt int
	closed  bool
}

func (s *testStmt) Close() error { s.closed = true; return nil }

// NumInput returning -1 means “unknown”, which is acceptable to database/sql.
func (s *testStmt) NumInput() int { return -1 }

// Exec (legacy) is routed to ExecContext for simplicity.
func (s *testStmt) Exec(args []driver.Value) (driver.Result, error) {
	nvs := make([]driver.NamedValue, len(args))
	for i, v := range args {
		nvs[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return s.ExecContext(context.Background(), nvs)
}
func (s *testStmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, errors.New("Query not supported by test driver")
}
func (s *testStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	s.execCnt++
	// Optional failure injection (not used here, but useful if you add cases):
	// fail on Nth exec when DSN contains execfail=N
	if nStr := s.c.cfg["execfail"]; nStr != "" {
		if n, err := parseInt(nStr); err == nil && s.execCnt == n {
			return nil, errors.New("test driver: forced Exec failure")
		}
	}
	s.c.execMu.Lock()
	s.c.lastExec = &execRecord{query: s.query, args: append([]driver.NamedValue(nil), args...)}
	s.c.execMu.Unlock()
	return testResult(1), nil
}

func parseInt(s string) (int, error) {
	var n int
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}

// testResult is a fixed driver.Result implementation.
type testResult int64

func (r testResult) LastInsertId() (int64, error) { return 0, errors.New("not supported") }
func (r testResult) RowsAffected() (int64, error) { return int64(r), nil }

//
// ================================
//  Tests exercising real wrappers
// ================================

// TestRealWrappers_Smoke drives the adapter against the in-memory driver so
// we execute the real wrappers and success-path logic end-to-end.
// This covers (indirectly):
//   - realSQLTx.ExecContext / PrepareContext / Commit / Rollback
//   - realStmt.ExecContext / Close
//   - sqlTx.Exec
//   - sqlDB.Close
//   - smallMSSQL.CopyOwnership (success)
func TestRealWrappers_Smoke(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Arrange: open a *sql.DB using our in-memory "testsql" driver.
	db, err := sql.Open("testsql", "") // empty DSN: no injected failures
	if err != nil {
		t.Fatalf("Open err: %v", err)
	}
	defer db.Close()

	// db.Ping must succeed because NewSQLDB calls Ping under the hood.
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("Ping err: %v", err)
	}

	// Build the sqlDB adapter that wraps real *sql.DB.
	sqlAdapter, err := NewSQLDB("testsql", "")
	if err != nil {
		t.Fatalf("NewSQLDB err: %v", err)
	}
	defer func() {
		// Covers sqlDB.Close.
		if err := sqlAdapter.Close(ctx); err != nil {
			t.Fatalf("Close err: %v", err)
		}
	}()

	// Begin a transaction via the adapter. Internally this yields sqlTx with
	// a realSQLTx (wrapping the *sql.Tx).
	tx, err := sqlAdapter.BeginTx(ctx)
	if err != nil {
		t.Fatalf("BeginTx err: %v", err)
	}

	// 1) sqlTx.Exec → realSQLTx.ExecContext
	if err := tx.Exec(ctx, "UPDATE t SET a=?", 1); err != nil {
		t.Fatalf("tx.Exec err: %v", err)
	}

	// 2) realSQLTx.PrepareContext + realStmt.ExecContext via CopyInto.
	rows := [][]interface{}{
		{int64(1), "a"},
		{int64(2), "b"},
	}
	if _, err := tx.CopyInto(ctx, "T", []string{"c1", "c2"}, rows); err != nil {
		t.Fatalf("CopyInto err: %v", err)
	}

	// 3) realSQLTx.Commit.
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit err: %v", err)
	}

	// 4) realSQLTx.Rollback (via sqlTx) on a new Tx.
	tx2, err := sqlAdapter.BeginTx(ctx)
	if err != nil {
		t.Fatalf("BeginTx(2) err: %v", err)
	}
	if err := tx2.Rollback(ctx); err != nil {
		t.Fatalf("Rollback err: %v", err)
	}

	// 5) smallMSSQL.CopyOwnership success path using the same in-memory DB.
	small := &smallMSSQL{db: realSQLDB{db: db}}
	if err := small.CopyOwnership(ctx, [][]interface{}{
		{1, 1, 1, true, 123, "ACME", "Addr", nil, nil},
		{2, 1, 1, false, 456, "BETA", "Addr2", nil, nil},
	}); err != nil {
		t.Fatalf("smallMSSQL.CopyOwnership err: %v", err)
	}
}
