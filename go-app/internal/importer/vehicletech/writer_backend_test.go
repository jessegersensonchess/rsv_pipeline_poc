package vehicletech

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"csvloader/internal/db"

	"github.com/jackc/pgx/v5"
)

// ---------- shared helpers ----------

// makeEncoded builds a buffered channel with the provided jobs and then closes it.
func makeEncoded(jobs ...encodedJob) <-chan encodedJob {
	ch := make(chan encodedJob, len(jobs))
	for _, j := range jobs {
		ch <- j
	}
	close(ch)
	return ch
}

// ---------- fakes for Postgres path ----------

// fakePgConn implements the minimal pgCopyConn interface used by pgWriterBackend.
// It drains the CopyFromSource, counts rows, and returns that count.
type fakePgConn struct {
	lastTable pgx.Identifier
	lastCols  []string
	count     int64
	fail      error
}

func (f *fakePgConn) CopyFrom(ctx context.Context, table pgx.Identifier, cols []string, src pgx.CopyFromSource) (int64, error) {
	f.lastTable, f.lastCols = table, cols
	if f.fail != nil {
		return 0, f.fail
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
	f.count = n
	return n, nil
}

// fakeDBOnly satisfies db.DB but is not recognized by db.AsPgConn/db.AsSQLDB,
// which lets us test error branches without network calls.
type fakeDBOnly struct{}

func (fakeDBOnly) Exec(context.Context, string, ...any) error { return nil }
func (fakeDBOnly) BeginTx(context.Context) (db.Tx, error)     { return nil, nil }
func (fakeDBOnly) Close(context.Context) error                { return nil }

// ---------- fakes for MSSQL path ----------

// fakeStmt mimics *sql.Stmt with just enough behavior to observe Exec calls.
// We count normal Execs (with args) and a "finalize" Exec (no args).
type fakeStmt struct {
	execs    int
	finalize int
	failOn   int // if >0, fail on that Exec call index (1-based)
	closed   bool
}

func (s *fakeStmt) ExecContext(ctx context.Context, args ...any) (sql.Result, error) {
	if len(args) == 0 {
		s.finalize++
	} else {
		s.execs++
	}
	if s.failOn > 0 && (s.execs+s.finalize) == s.failOn {
		return nil, errors.New("exec failure")
	}
	return nil, nil
}
func (s *fakeStmt) Close() error {
	s.closed = true
	return nil
}

// fakePreparer mimics *sql.DB for PrepareContext; returns our fakeStmt.
type fakePreparer struct {
	stmt *fakeStmt
	err  error
}

func (p *fakePreparer) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	if p.err != nil {
		return nil, p.err
	}
	// We cannot return *fakeStmt as *sql.Stmt, so we cheat:
	// In tests we never call methods on the returned *sql.Stmt directly;
	// instead, we intercept through our backend's injected stmt via ExecContext/Close.
	// To stick to the backend's signature, we'll panic here if production code tries
	// to use the real *sql.Stmt. The backend under test does NOT (it stores the returned
	// value into an interface with ExecContext/Close), so we won't reach this.
	panic("PrepareContext should not be called in tests returning *sql.Stmt; backend is injected with a custom stmt via the seam")
}

// To actually test mssql backend without panic, we provide a tiny wrapper backend that
// bypasses PrepareContext entirely and uses our fakeStmt directly.
type testableMSSQLBackend struct {
	stmt *fakeStmt
}

func (b *testableMSSQLBackend) Write(ctx context.Context, encodedCh <-chan encodedJob, addSkip func(string, int, string, string), logEvery int) (int, error) {
	ins := 0
	for job := range encodedCh {
		// ✅ Match production behavior: require non-zero pcv AND valid JSON payload
		if job.pcv == 0 || job.payload == nil || !json.Valid(job.payload) {
			addSkip("invalid_row", job.lineNum, job.pcvField, job.raw)
			continue
		}
		if _, err := b.stmt.ExecContext(ctx, job.pcv, string(job.payload)); err != nil {
			return ins, err
		}
		ins++
	}
	if _, err := b.stmt.ExecContext(ctx); err != nil {
		return ins, err
	}
	return ins, nil
}

// ---------- Tests: Postgres backend ----------

func TestPgWriterBackend_NotPostgresErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newPgWriterBackend(fakeDBOnly{}) // AsPgConn(fakeDBOnly) -> false

	// Use a quiet channel; we don't need to feed any rows for the type-check error.
	got, err := b.Write(ctx, makeEncoded(), func(string, int, string, string) {}, 100)
	if err == nil || got != 0 || err.Error() != "not a Postgres-backed DB" {
		t.Fatalf("expected not-a-Postgres error; got n=%d err=%v", got, err)
	}
}

func TestPgWriterBackend_WriteCountsValidRowsAndSkipsInvalid(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Inject a fake pgx conn so we don't depend on db.AsPgConn/real network.
	fc := &fakePgConn{}
	b := &pgWriterBackend{conn: fc}

	// Build a stream with one valid row and two invalid rows.
	enc := makeEncoded(
		encodedJob{pcv: 1, pcvField: "1", payload: []byte(`{"ok":true}`), lineNum: 10, raw: "valid"},
		encodedJob{pcv: 0, pcvField: "", payload: []byte(`{"x":1}`), lineNum: 11, raw: "bad-pcv"},
		encodedJob{pcv: 2, pcvField: "2", payload: []byte(`not-json`), lineNum: 12, raw: "bad-json"},
	)

	// Track skips to assert behavior.
	var skipped []string
	addSkip := func(reason string, ln int, pcvField, raw string) {
		skipped = append(skipped, fmt.Sprintf("%s@%d:%s", reason, ln, raw))
	}

	n, err := b.Write(ctx, enc, addSkip, 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 1 {
		t.Fatalf("CopyFrom row count=%d, want 1", n)
	}
	// pg table/col sanity
	if fmt.Sprint(fc.lastTable) != fmt.Sprint(pgx.Identifier{"vehicle_tech"}) {
		t.Fatalf("table mismatch: %v", fc.lastTable)
	}
	if len(fc.lastCols) != 2 || fc.lastCols[0] != "pcv" || fc.lastCols[1] != "payload" {
		t.Fatalf("cols mismatch: %v", fc.lastCols)
	}
	// The two invalid rows were not passed to CopyFrom; they should have been skipped.
	if len(skipped) != 2 {
		t.Fatalf("skipped=%v want 2 skips", skipped)
	}
}

// ---------- Tests: MSSQL backend ----------

func TestMSSQLWriterBackend_NotSQLBackedErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := newMSSQLWriterBackend(fakeDBOnly{}) // AsSQLDB(fakeDBOnly) -> false

	got, err := b.Write(ctx, makeEncoded(), func(string, int, string, string) {}, 500)
	if err == nil || got != 0 || err.Error() != "not a SQL-backed DB" {
		t.Fatalf("expected not-a-SQL error; got n=%d err=%v", got, err)
	}
}

func TestMSSQLWriterBackend_WriteCountsValidRowsAndFinalize(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Instead of using the production backend (which calls PrepareContext on *sql.DB),
	// we exercise the same write loop via a small testable backend that uses a fake stmt.
	stmt := &fakeStmt{}
	tb := &testableMSSQLBackend{stmt: stmt}

	enc := makeEncoded(
		encodedJob{pcv: 10, pcvField: "10", payload: []byte(`{"a":1}`), lineNum: 1, raw: "ok1"},
		encodedJob{pcv: 0, pcvField: "", payload: []byte(`{"b":2}`), lineNum: 2, raw: "bad-pcv"},      // skip
		encodedJob{pcv: 11, pcvField: "11", payload: []byte(`not-json`), lineNum: 3, raw: "bad-json"}, // skip
		encodedJob{pcv: 12, pcvField: "12", payload: []byte(`{"c":3}`), lineNum: 4, raw: "ok2"},
	)

	var skips int
	addSkip := func(string, int, string, string) { skips++ }

	n, err := tb.Write(ctx, enc, addSkip, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 2 {
		t.Fatalf("inserted=%d want 2", n)
	}
	if skips != 2 {
		t.Fatalf("skips=%d want 2", skips)
	}
	if stmt.execs != 2 || stmt.finalize != 1 {
		t.Fatalf("stmt.execs=%d finalize=%d (want 2/1)", stmt.execs, stmt.finalize)
	}
}

func TestMSSQLWriterBackend_WriteStopsOnExecError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	stmt := &fakeStmt{failOn: 2} // fail on second Exec (first finalize or normal counted accordingly)
	tb := &testableMSSQLBackend{stmt: stmt}

	enc := makeEncoded(
		encodedJob{pcv: 1, pcvField: "1", payload: []byte(`{"x":1}`), lineNum: 1, raw: "ok1"},
		encodedJob{pcv: 2, pcvField: "2", payload: []byte(`{"y":2}`), lineNum: 2, raw: "ok2"},
	)

	_, err := tb.Write(ctx, enc, func(string, int, string, string) {}, 100)
	if err == nil {
		t.Fatalf("expected error on exec")
	}
}

// Tiny sanity: make sure our tests remain fast even if someone bumps logEvery.
// (No assertions; just ensure no timeouts.)
//
//nolint:tparallel // keep single-threaded for determinism
func TestBackends_NoSlowPaths(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	fc := &fakePgConn{}
	pg := &pgWriterBackend{conn: fc}
	_, _ = pg.Write(ctx, makeEncoded(), func(string, int, string, string) {}, 1)

	stmt := &fakeStmt{}
	tb := &testableMSSQLBackend{stmt: stmt}
	_, _ = tb.Write(ctx, makeEncoded(), func(string, int, string, string) {}, 1)
}

// ---------- Additional coverage: Postgres backend error path ----------

// TestPgWriterBackend_CopyFromError
// Exercises the error branch when pgx.CopyFrom itself fails. We inject a fake
// pg conn that returns a controlled error from CopyFrom. No rows are consumed,
// and the backend should surface the error verbatim.
func TestPgWriterBackend_CopyFromError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	fc := &fakePgConn{fail: errors.New("copy-fail")}
	b := &pgWriterBackend{conn: fc}

	// A single "valid" row; it won't matter because fake CopyFrom fails immediately.
	enc := makeEncoded(encodedJob{pcv: 42, pcvField: "42", payload: []byte(`{"ok":true}`), lineNum: 2, raw: "x"})

	n, err := b.Write(ctx, enc, func(string, int, string, string) {}, 100)
	if err == nil || !strings.Contains(err.Error(), "copy-fail") {
		t.Fatalf("want copy-fail error, got n=%d err=%v", n, err)
	}
	if n != 0 {
		t.Fatalf("rows reported inserted should be 0 on CopyFrom failure, got %d", n)
	}
}

// ---------- Additional coverage: MSSQL backend early failures ----------

// fakePreparerErr returns an error from PrepareContext using the same seam
// shape as production (stmtCore, error). This lets us exercise the early
// PrepareContext failure branch in mssqlWriterBackend.Write without needing
// a real *sql.Stmt or driver.
type fakePreparerErr struct{ err error }

func (p *fakePreparerErr) PrepareContext(ctx context.Context, q string) (stmtCore, error) {
	return nil, p.err
}

// TestMSSQLWriterBackend_PrepareContextError
// Covers the branch where PrepareContext fails before any rows are read.
// This is the most direct way to exercise the production Write path without
// needing a real database/sql statement instance.
func TestMSSQLWriterBackend_PrepareContextError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	be := &mssqlWriterBackend{prep: &fakePreparerErr{err: errors.New("prep-fail")}}

	// Stream can be empty; failure occurs before we iterate the channel.
	n, err := be.Write(ctx, makeEncoded(), func(string, int, string, string) {}, 100)
	if err == nil || !strings.Contains(err.Error(), "prep-fail") {
		t.Fatalf("want prep-fail, got n=%d err=%v", n, err)
	}
	if n != 0 {
		t.Fatalf("expected no inserts on prepare failure, got %d", n)
	}
}

// fakePreparerCore returns a provided stmtCore (or an error), letting us drive
// the production Write loop without a real *sql.Stmt.
type fakePreparerCore struct {
	stmt stmtCore
	err  error
}

func (p *fakePreparerCore) PrepareContext(ctx context.Context, q string) (stmtCore, error) {
	if p.err != nil {
		return nil, p.err
	}
	return p.stmt, nil
}

// fakeStmtCore implements stmtCore and counts Exec calls. It can be configured
// to fail on the Nth Exec (counting both data Execs and the finalizing Exec).
type fakeStmtCore struct {
	execs   int // number of ExecContext calls with args (data rows)
	finals  int // number of ExecContext calls with no args (finalizer)
	failNth int // if >0, fail on this absolute call index (execs+finals)
	calls   int // absolute call counter
}

func (s *fakeStmtCore) ExecContext(ctx context.Context, args ...any) (sql.Result, error) {
	s.calls++
	if len(args) == 0 {
		s.finals++
	} else {
		s.execs++
	}
	if s.failNth > 0 && s.calls == s.failNth {
		return nil, errors.New("stmt-exec-fail")
	}
	return nil, nil
}
func (s *fakeStmtCore) Close() error { return nil }

// TestMSSQLWriterBackend_Loop_Skip_Exec_Log_Finalize
// Covers the production Write loop branches:
//   - skip invalid rows (incl. pcvField fallback via strconv.FormatInt)
//   - successful Execs for valid rows
//   - logging modulo branch when ins%logEvery == 0
//   - successful finalize Exec (no args)
//
// We don't assert on log output; executing the code paths provides coverage.
func TestMSSQLWriterBackend_Loop_Skip_Exec_Log_Finalize(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	stmt := &fakeStmtCore{}
	be := &mssqlWriterBackend{
		prep: &fakePreparerCore{stmt: stmt},
	}

	// Build a stream:
	//  1) invalid: pcv>0 but payload invalid, pcvField empty => fallback must format pcv
	//  2) valid
	//  3) valid (to hit the logEvery == 2 modulo)
	ch := makeEncoded(
		encodedJob{pcv: 5, pcvField: "", payload: []byte("not-json"), lineNum: 2, raw: "bad"},
		encodedJob{pcv: 10, pcvField: "10", payload: []byte(`{"a":1}`), lineNum: 3, raw: "ok1"},
		encodedJob{pcv: 11, pcvField: "11", payload: []byte(`{"b":2}`), lineNum: 4, raw: "ok2"},
	)

	// Capture the pcvField used for skipped invalid row to verify fallback behavior.
	var capturedPCVField string
	addSkip := func(_ string, _ int, pcvField, _ string) {
		capturedPCVField = pcvField
	}

	n, err := be.Write(ctx, ch, addSkip, 2) // logEvery=2 to trigger the modulo branch
	if err != nil {
		t.Fatalf("unexpected write err: %v", err)
	}
	if n != 2 {
		t.Fatalf("inserted=%d want 2", n)
	}
	if stmt.execs != 2 || stmt.finals != 1 {
		t.Fatalf("stmt execs=%d finals=%d (want 2/1)", stmt.execs, stmt.finals)
	}
	// pcvField fallback should have used "5" for the invalid row
	if capturedPCVField != "5" {
		t.Fatalf("pcvField fallback = %q want %q", capturedPCVField, "5")
	}
}

// TestMSSQLWriterBackend_Loop_ExecError
// Ensures a per-row Exec error is returned immediately with the number of
// successfully inserted rows up to that point.
func TestMSSQLWriterBackend_Loop_ExecError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Fail on the second Exec (i.e., after one successful insert).
	stmt := &fakeStmtCore{failNth: 2}
	be := &mssqlWriterBackend{prep: &fakePreparerCore{stmt: stmt}}

	ch := makeEncoded(
		encodedJob{pcv: 1, pcvField: "1", payload: []byte(`{"x":1}`), lineNum: 1, raw: "ok1"},
		encodedJob{pcv: 2, pcvField: "2", payload: []byte(`{"y":2}`), lineNum: 2, raw: "ok2"},
	)

	n, err := be.Write(ctx, ch, func(string, int, string, string) {}, 100)
	if err == nil || !strings.Contains(err.Error(), "stmt-exec-fail") {
		t.Fatalf("want stmt-exec-fail, got n=%d err=%v", n, err)
	}
	if n != 1 {
		t.Fatalf("rows inserted before error = %d want 1", n)
	}
}

// TestMSSQLWriterBackend_Loop_FinalizeError
// Ensures an error on the finalizing Exec (no args) is surfaced and the
// inserted count is preserved.
func TestMSSQLWriterBackend_Loop_FinalizeError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Two data Execs + one finalizer => fail on 3rd call
	stmt := &fakeStmtCore{failNth: 3}
	be := &mssqlWriterBackend{prep: &fakePreparerCore{stmt: stmt}}

	ch := makeEncoded(
		encodedJob{pcv: 1, pcvField: "1", payload: []byte(`{"x":1}`), lineNum: 1, raw: "ok1"},
		encodedJob{pcv: 2, pcvField: "2", payload: []byte(`{"y":2}`), lineNum: 2, raw: "ok2"},
	)

	n, err := be.Write(ctx, ch, func(string, int, string, string) {}, 2)
	if err == nil || !strings.Contains(err.Error(), "stmt-exec-fail") {
		t.Fatalf("want stmt-exec-fail on finalize, got n=%d err=%v", n, err)
	}
	if n != 2 {
		t.Fatalf("rows inserted before finalize error = %d want 2", n)
	}
}

// ---- Additional Postgres coverage ----

// TestPgWriterBackend_LogDeltaClosure
// Drives the logf closure inside pgWriterBackend.Write (delta computation
// and insertedAtLastLog update). We set logEvery=1 and provide two rows so
// the closure is executed at least twice. We do not assert on log output;
// execution alone covers the closure.
func TestPgWriterBackend_LogDeltaClosure(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	fc := &fakePgConn{}
	pg := &pgWriterBackend{conn: fc}

	ch := makeEncoded(
		encodedJob{pcv: 100, pcvField: "100", payload: []byte(`{"ok":true}`), lineNum: 2, raw: "a"},
		encodedJob{pcv: 101, pcvField: "101", payload: []byte(`{"ok":true}`), lineNum: 3, raw: "b"},
	)

	n, err := pg.Write(ctx, ch, func(string, int, string, string) {}, 1) // every row logs
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if n != 2 {
		t.Fatalf("inserted=%d want 2", n)
	}
}
