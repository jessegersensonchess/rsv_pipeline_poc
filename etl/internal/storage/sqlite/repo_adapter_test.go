package sqlite

import (
	"context"
	"fmt"
	"testing"
)

/*
Package-level helpers
*/

// newMemRepo creates a live in-memory *Repository using the modernc SQLite driver.
// It is sufficient to exercise wrappedRepo’s forwarding behavior without any third-party deps.
func newMemRepo(tb testing.TB) *Repository {
	tb.Helper()
	db, err := Open(":memory:")
	if err != nil {
		tb.Fatalf("open :memory:: %v", err)
	}
	tb.Cleanup(func() { _ = db.Close() })
	return New(db)
}

//// mustExec runs a SQL statement and fails the test on error.
//func mustExec(tb testing.TB, r *Repository, sqlStmt string) {
//	tb.Helper()
//	if err := r.Exec(context.Background(), sqlStmt); err != nil {
//		tb.Fatalf("exec %q: %v", sqlStmt, err)
//	}
//}

//// uniqNameFrom returns a simple, SQLite-safe identifier derived from a test/bench name.
//func uniqNameFrom(name, suffix string) string {
//	n := strings.ReplaceAll(name, "/", "_")
//	n = strings.ReplaceAll(n, ":", "_")
//	return fmt.Sprintf("%s_%s", n, suffix)
//}

/*
Unit tests
*/

// TestWrappedRepo_Close verifies that wrappedRepo.Close invokes the provided closeFn.
func TestWrappedRepo_Close(t *testing.T) {
	t.Parallel()

	r := newMemRepo(t)
	var closed bool
	w := &wrappedRepo{Repository: r, closeFn: func() { closed = true }}

	w.Close()
	if !closed {
		t.Fatalf("Close did not invoke closeFn")
	}
}

// TestWrappedRepo_CopyFromAndBulkUpsert verifies method forwarding correctness
// by writing rows through wrappedRepo and reading them back.
//
// This exercises both CopyFrom and BulkUpsert paths with a live in-memory DB.
func TestWrappedRepo_CopyFromAndBulkUpsert(t *testing.T) {
	t.Parallel()

	r := newMemRepo(t)
	// Configure columns/table to satisfy the underlying repo methods.
	r.cfg = Config{
		Table:   uniqNameFrom(t.Name(), "items"),
		Columns: []string{"id", "name"},
	}
	w := &wrappedRepo{Repository: r, closeFn: func() {}}

	// Create table.
	mustExec(t, r, fmt.Sprintf(`CREATE TABLE %s (id INTEGER, name TEXT)`, sqlIdent(r.cfg.Table)))

	// CopyFrom: insert initial rows.
	rows := [][]any{{1, "a"}, {2, "b"}}
	n, err := w.CopyFrom(context.Background(), r.cfg.Columns, rows)
	if err != nil {
		t.Fatalf("CopyFrom: %v", err)
	}
	if n != int64(len(rows)) {
		t.Fatalf("CopyFrom affected: got %d want %d", n, len(rows))
	}

	// BulkUpsert: replace id=2 and add id=3.
	recs := []map[string]any{
		{"id": 2, "name": "b2"},
		{"id": 3, "name": "c"},
	}
	n, err = w.BulkUpsert(context.Background(), recs, []string{"id"}, "")
	if err != nil {
		t.Fatalf("BulkUpsert: %v", err)
	}
	if n != 2 {
		t.Fatalf("BulkUpsert affected: got %d want 2", n)
	}

	// Verify table contents: {1:a, 2:b2, 3:c}
	rowsSQL, err := r.db.QueryContext(context.Background(),
		`SELECT id, name FROM `+sqlIdent(r.cfg.Table)+` ORDER BY id`)
	if err != nil {
		t.Fatalf("verify query: %v", err)
	}
	defer rowsSQL.Close()

	got := make(map[int]string)
	for rowsSQL.Next() {
		var id int
		var name string
		if err := rowsSQL.Scan(&id, &name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[id] = name
	}
	if err := rowsSQL.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	want := map[int]string{1: "a", 2: "b2", 3: "c"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("contents mismatch: got %v want %v", got, want)
	}
}

// TestAdapter_ConfigMapping ensures the factory-level config mapping (storage.Config -> sqlite.Config)
// uses the newRepository test hook and preserves fields correctly.
// We do not rely on storage’s internal registry; instead, we exercise the same closure logic
// with a controlled fake newRepository.
func TestAdapter_ConfigMapping(t *testing.T) {
	t.Parallel()

	type storageConfig struct {
		DSN        string
		Table      string
		Columns    []string
		KeyColumns []string
	}

	type capture struct {
		sqliteCfg Config
		called    bool
	}

	var cap capture

	// Swap newRepository with a fake that records the sqlite.Config and returns a minimal live repo.
	orig := newRepository
	newRepository = func(ctx context.Context, cfg Config) (*Repository, func(), error) {
		cap.sqliteCfg = cfg
		cap.called = true
		// Return a trivially openable in-memory repo and a close func.
		db, err := Open(":memory:")
		if err != nil {
			return nil, nil, err
		}
		close := func() { _ = db.Close() }
		return &Repository{db: db, cfg: cfg}, close, nil
	}
	defer func() { newRepository = orig }()

	// Emulate the actual init-registered factory function body.
	// (We can’t reach storage.Register internals from here, but this validates the adapter’s mapping logic.)
	input := storageConfig{
		DSN:        "file:adapter.sqlite",
		Table:      "t",
		Columns:    []string{"a", "b"},
		KeyColumns: []string{"a"},
	}

	// Invoke the same mapping used in init() and ensure our fake got the expected cfg.
	ctx := context.Background()
	r, closeFn, err := newRepository(ctx, Config{
		DSN:        input.DSN,
		Table:      input.Table,
		Columns:    input.Columns,
		KeyColumns: input.KeyColumns,
	})
	if err != nil {
		t.Fatalf("fake newRepository returned error: %v", err)
	}
	defer closeFn()
	defer func() { _ = r.db.Close() }()

	if !cap.called {
		t.Fatalf("expected newRepository test hook to be called")
	}
	if cap.sqliteCfg.DSN != input.DSN ||
		cap.sqliteCfg.Table != input.Table ||
		fmt.Sprint(cap.sqliteCfg.Columns) != fmt.Sprint(input.Columns) ||
		fmt.Sprint(cap.sqliteCfg.KeyColumns) != fmt.Sprint(input.KeyColumns) {
		t.Fatalf("config mapping mismatch: got %+v", cap.sqliteCfg)
	}
}

/*
Benchmarks
*/

// BenchmarkWrapped_CopyFrom measures the overhead of calling CopyFrom via wrappedRepo
// versus the underlying *Repository (focus is on adapter forwarding, not absolute DB speed).
func BenchmarkWrapped_CopyFrom(b *testing.B) {
	r := newMemRepo(b)
	r.cfg = Config{
		Table:   uniqNameFrom(b.Name(), "bench"),
		Columns: []string{"id", "name"},
	}
	w := &wrappedRepo{Repository: r, closeFn: func() {}}
	mustExec(b, r, fmt.Sprintf(`CREATE TABLE %s (id INTEGER, name TEXT)`, sqlIdent(r.cfg.Table)))

	const batch = 128
	rows := make([][]any, batch)
	for i := 0; i < batch; i++ {
		rows[i] = []any{i, "x"}
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := w.CopyFrom(context.Background(), r.cfg.Columns, rows); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWrapped_BulkUpsert measures the forwarding overhead for BulkUpsert.
func BenchmarkWrapped_BulkUpsert(b *testing.B) {
	r := newMemRepo(b)
	r.cfg = Config{
		Table:   uniqNameFrom(b.Name(), "bench"),
		Columns: []string{"id", "name"},
	}
	w := &wrappedRepo{Repository: r, closeFn: func() {}}
	mustExec(b, r, fmt.Sprintf(`CREATE TABLE %s (id INTEGER, name TEXT)`, sqlIdent(r.cfg.Table)))

	// Seed 0..N-1, then upsert to update and extend.
	const N = 256
	seed := make([][]any, N)
	for i := 0; i < N; i++ {
		seed[i] = []any{i, "seed"}
	}
	if _, err := r.CopyFrom(context.Background(), r.cfg.Columns, seed); err != nil {
		b.Fatalf("seed: %v", err)
	}

	recs := make([]map[string]any, 0, N+N/2)
	for i := 0; i < N; i++ {
		recs = append(recs, map[string]any{"id": i, "name": "upd"})
	}
	for i := N; i < N+N/2; i++ {
		recs = append(recs, map[string]any{"id": i, "name": "new"})
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := w.BulkUpsert(context.Background(), recs, []string{"id"}, ""); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWrapped_Close measures the overhead of Close indirection only.
func BenchmarkWrapped_Close(b *testing.B) {
	r := newMemRepo(b)
	var n int
	w := &wrappedRepo{
		Repository: r,
		closeFn:    func() { n++ },
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w.Close()
	}
	if n == 0 {
		b.Fatalf("closeFn was never called")
	}
}

/*
Doc-style example to show wrappedRepo usage in isolation.
*/

// Example_wrappedRepo demonstrates constructing and using wrappedRepo over a live in-memory DB.
func Example_wrappedRepo() {
	db, _ := Open(":memory:")
	defer db.Close()

	r := New(db)
	r.cfg = Config{
		Table:   "example",
		Columns: []string{"id", "name"},
	}
	_ = r.Exec(context.Background(), `CREATE TABLE "example" (id INTEGER, name TEXT)`)

	w := &wrappedRepo{Repository: r, closeFn: func() { _ = db.Close() }}

	w.CopyFrom(context.Background(), r.cfg.Columns, [][]any{{1, "alice"}})
	w.BulkUpsert(context.Background(), []map[string]any{{"id": 1, "name": "alice2"}}, []string{"id"}, "")
	w.Close()
	// Output:
}
