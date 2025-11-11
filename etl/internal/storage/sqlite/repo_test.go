package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"runtime"
	"strings"
	"testing"
)

/*
Package-level test helpers (TB-aware)
*/

func newMemDB(tb testing.TB) *sql.DB {
	tb.Helper()
	db, err := Open(":memory:")
	if err != nil {
		tb.Fatalf("open sqlite :memory:: %v", err)
	}
	tb.Cleanup(func() { _ = db.Close() })
	return db
}

func newRepo(tb testing.TB) *Repository {
	tb.Helper()
	return New(newMemDB(tb))
}

func mustExec(tb testing.TB, r *Repository, sqlStmt string) {
	tb.Helper()
	if err := r.Exec(context.Background(), sqlStmt); err != nil {
		tb.Fatalf("exec %q: %v", sqlStmt, err)
	}
}

func uniqNameFrom(name, suffix string) string {
	// Keep identifiers simple and deterministic per test/bench.
	n := strings.ReplaceAll(name, "/", "_")
	n = strings.ReplaceAll(n, ":", "_")
	return fmt.Sprintf("%s_%s", n, suffix)
}

/*
Unit tests
*/

// TestCreateTableAndSchema verifies CreateTable builds the expected schema and
// Schema() can fetch the original DDL text from sqlite_master.
func TestCreateTableAndSchema(t *testing.T) {
	t.Parallel()

	r := newRepo(t)
	ctx := context.Background()

	table := uniqNameFrom(t.Name(), "people")
	cols := []Column{
		{Name: "id", SQLType: "INTEGER", NotNull: true},
		{Name: "name", SQLType: "TEXT", NotNull: true},
		{Name: "age", SQLType: "INTEGER", NotNull: false},
	}
	if err := r.CreateTable(ctx, table, cols, "id"); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	ddl, err := r.Schema(ctx, table)
	if err != nil {
		t.Fatalf("Schema: %v", err)
	}
	// The exact DDL is driver/SQLite-version dependent, so assert key parts (case-insensitive).
	// Compare case-insensitively by uppercasing both sides.
	up := strings.ToUpper(ddl)
	wantParts := []string{
		strings.ToUpper(fmt.Sprintf(`CREATE TABLE "%s"`, table)), // <-- fixed
		`"ID"`, `"NAME"`, `"AGE"`,
		"PRIMARY KEY", `"ID"`,
	}
	for _, w := range wantParts {
		if !strings.Contains(up, w) {
			t.Fatalf("schema %q missing %q", ddl, w)
		}
	}
}

// TestInsertRowsAndSampleField checks InsertRows inserts data and SampleField
// returns values up to the requested limit.
func TestInsertRowsAndSampleField(t *testing.T) {
	t.Parallel()

	r := newRepo(t)
	ctx := context.Background()
	table := uniqNameFrom(t.Name(), "items")

	mustExec(t, r, fmt.Sprintf(`CREATE TABLE %s (id INTEGER, label TEXT)`, sqlIdent(table)))

	rows := [][]any{
		{1, "a"}, {2, "b"}, {3, "c"},
	}
	if err := r.InsertRows(ctx, table, []string{"id", "label"}, rows); err != nil {
		t.Fatalf("InsertRows: %v", err)
	}

	got, err := r.SampleField(ctx, table, "label", 2)
	if err != nil {
		t.Fatalf("SampleField: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("SampleField length: got %d want 2", len(got))
	}
	if got[0] != "a" || got[1] != "b" {
		t.Fatalf("SampleField values: got %#v", got)
	}
}

// TestSearch validates Search behavior with (a) empty columns, (b) empty query,
// and (c) LIKE across multiple columns with a limit.
func TestSearch(t *testing.T) {
	t.Parallel()

	r := newRepo(t)
	ctx := context.Background()
	table := uniqNameFrom(t.Name(), "search")

	mustExec(t, r, fmt.Sprintf(`CREATE TABLE %s (id INTEGER, name TEXT, note TEXT)`, sqlIdent(table)))
	seed := [][]any{
		{1, "Ada", "math genius"},
		{2, "Alan", "turing stuff"},
		{3, "Grace", "compiler pioneer"},
	}
	if err := r.InsertRows(ctx, table, []string{"id", "name", "note"}, seed); err != nil {
		t.Fatalf("InsertRows: %v", err)
	}

	type tc struct {
		name    string
		cols    []string
		q       string
		limit   int
		wantIDs []int64 // order is by table order since no ORDER BY
	}
	cases := []tc{
		{
			name:    "empty_cols_returns_some_rows",
			cols:    nil,
			q:       "ignored",
			limit:   2,
			wantIDs: []int64{1, 2},
		},
		{
			name:    "empty_query_returns_some_rows",
			cols:    []string{"name"},
			q:       "",
			limit:   2,
			wantIDs: []int64{1, 2},
		},
		{
			name:    "like_across_two_columns",
			cols:    []string{"name", "note"},
			q:       "ing", // matches "turing"
			limit:   10,
			wantIDs: []int64{2},
		},
		{
			name:    "limit_applied",
			cols:    []string{"name"},
			q:       "a", // matches Ada, Alan, Grace
			limit:   1,
			wantIDs: []int64{1},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			rows, err := r.Search(ctx, table, c.cols, c.q, c.limit)
			if err != nil {
				t.Fatalf("Search: %v", err)
			}
			defer rows.Close()

			var ids []int64
			for rows.Next() {
				var id int64
				var name, note string
				if err := rows.Scan(&id, &name, &note); err != nil {
					t.Fatalf("scan: %v", err)
				}
				ids = append(ids, id)
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows err: %v", err)
			}
			if len(ids) != len(c.wantIDs) {
				t.Fatalf("length mismatch: got %v want %v", ids, c.wantIDs)
			}
			for i := range ids {
				if ids[i] != c.wantIDs[i] {
					t.Fatalf("ids mismatch at %d: got %v want %v", i, ids, c.wantIDs)
				}
			}
		})
	}
}

// TestNewRepositoryAndCopyFrom checks NewRepository opens a DB and CopyFrom
// inserts rows using the configured table/columns.
func TestNewRepositoryAndCopyFrom(t *testing.T) {
	t.Parallel()

	cfg := Config{DSN: ":memory:", Table: uniqNameFrom(t.Name(), "cf"), Columns: []string{"id", "name"}}
	r, close, err := NewRepository(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewRepository: %v", err)
	}
	defer close()

	// Create the table using Exec to exercise that path too.
	if err := r.Exec(context.Background(), fmt.Sprintf(`CREATE TABLE %s (id INTEGER, name TEXT)`, sqlIdent(cfg.Table))); err != nil {
		t.Fatalf("Exec: %v", err)
	}

	rows := [][]any{{1, "x"}, {2, "y"}, {3, "z"}}
	n, err := r.CopyFrom(context.Background(), cfg.Columns, rows)
	if err != nil {
		t.Fatalf("CopyFrom: %v", err)
	}
	if n != int64(len(rows)) {
		t.Fatalf("CopyFrom affected: got %d want %d", n, len(rows))
	}

	// Verify count back from the DB.
	var count int
	if err := r.db.QueryRow(`SELECT COUNT(*) FROM ` + sqlIdent(cfg.Table)).Scan(&count); err != nil {
		t.Fatalf("verify count: %v", err)
	}
	if count != len(rows) {
		t.Fatalf("row count mismatch: got %d want %d", count, len(rows))
	}
}

// TestBulkUpsert validates delete-then-insert semantics and return count.
func TestBulkUpsert(t *testing.T) {
	t.Parallel()

	r := newRepo(t)
	ctx := context.Background()
	r.cfg = Config{
		Table:   uniqNameFrom(t.Name(), "upsert"),
		Columns: []string{"id", "name"},
	}
	mustExec(t, r, fmt.Sprintf(`CREATE TABLE %s (id INTEGER, name TEXT)`, sqlIdent(r.cfg.Table)))

	// Seed with one row (id=1).
	if _, err := r.CopyFrom(ctx, r.cfg.Columns, [][]any{{1, "before"}}); err != nil {
		t.Fatalf("seed CopyFrom: %v", err)
	}

	// Upsert records: one replaces id=1, one new id=2.
	recs := []map[string]any{
		{"id": 1, "name": "after"},
		{"id": 2, "name": "two"},
	}
	n, err := r.BulkUpsert(ctx, recs, []string{"id"}, "")
	if err != nil {
		t.Fatalf("BulkUpsert: %v", err)
	}
	// Return value is total inserted rows.
	if n != 2 {
		t.Fatalf("BulkUpsert affected: got %d want 2", n)
	}

	// Verify results.
	got := map[int]string{}
	rows, err := r.db.QueryContext(ctx, `SELECT id, name FROM `+sqlIdent(r.cfg.Table)+` ORDER BY id`)
	if err != nil {
		t.Fatalf("query verify: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[id] = name
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	want := map[int]string{1: "after", 2: "two"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("contents mismatch: got %v want %v", got, want)
	}

	// Empty input short-circuits with zero rows affected.
	n, err = r.BulkUpsert(ctx, nil, []string{"id"}, "")
	if err != nil {
		t.Fatalf("BulkUpsert empty: %v", err)
	}
	if n != 0 {
		t.Fatalf("BulkUpsert empty affected: got %d want 0", n)
	}

	// Missing configured columns returns an error.
	r2 := newRepo(t)
	r2.cfg = Config{Table: r.cfg.Table} // no Columns set
	if _, err := r2.BulkUpsert(ctx, recs, []string{"id"}, ""); err == nil || !strings.Contains(err.Error(), "no columns configured") {
		t.Fatalf("BulkUpsert without columns: expected 'no columns configured', got %v", err)
	}
}

/*
Benchmarks
*/

// BenchmarkSqlite_InsertRows measures batched INSERT using a prepared statement.
func BenchmarkSqlite_InsertRows(b *testing.B) {
	r := newRepo(b)
	ctx := context.Background()
	table := uniqNameFrom(b.Name(), "bench")
	mustExec(b, r, fmt.Sprintf(`CREATE TABLE %s (id INTEGER, name TEXT)`, sqlIdent(table)))

	// Prepare a small batch to simulate ETL micro-batches.
	const batch = 128
	rows := make([][]any, batch)
	for i := 0; i < batch; i++ {
		rows[i] = []any{i, "x"}
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := r.InsertRows(ctx, table, []string{"id", "name"}, rows); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSqlite_CopyFrom measures the transaction + prepared statement path.
func BenchmarkSqlite_CopyFrom(b *testing.B) {
	r := newRepo(b)
	ctx := context.Background()
	r.cfg = Config{
		Table:   uniqNameFrom(b.Name(), "bench"),
		Columns: []string{"id", "name"},
	}
	mustExec(b, r, fmt.Sprintf(`CREATE TABLE %s (id INTEGER, name TEXT)`, sqlIdent(r.cfg.Table)))

	const batch = 256
	rows := make([][]any, batch)
	for i := 0; i < batch; i++ {
		rows[i] = []any{i, "y"}
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := r.CopyFrom(ctx, r.cfg.Columns, rows); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSqlite_BulkUpsert measures delete-then-insert with a small key set.
func BenchmarkSqlite_BulkUpsert(b *testing.B) {
	r := newRepo(b)
	ctx := context.Background()
	r.cfg = Config{
		Table:   uniqNameFrom(b.Name(), "bench"),
		Columns: []string{"id", "name"},
	}
	mustExec(b, r, fmt.Sprintf(`CREATE TABLE %s (id INTEGER, name TEXT)`, sqlIdent(r.cfg.Table)))

	// Seed initial rows so deletes have work to do for the upsert path.
	const count = 128
	seed := make([][]any, count)
	for i := 0; i < count; i++ {
		seed[i] = []any{i, "seed"}
	}
	if _, err := r.CopyFrom(ctx, r.cfg.Columns, seed); err != nil {
		b.Fatalf("seed: %v", err)
	}

	// Prepare upsert records (half overlap, half new).
	recs := make([]map[string]any, 0, count+count/2)
	for i := 0; i < count; i++ {
		recs = append(recs, map[string]any{"id": i, "name": "upd"})
	}
	for i := count; i < count+count/2; i++ {
		recs = append(recs, map[string]any{"id": i, "name": "new"})
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := r.BulkUpsert(ctx, recs, []string{"id"}, ""); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSqlite_SearchLike measures a simple LIKE with two columns and a small limit.
func BenchmarkSqlite_SearchLike(b *testing.B) {
	r := newRepo(b)
	ctx := context.Background()
	table := uniqNameFrom(b.Name(), "bench")
	mustExec(b, r, fmt.Sprintf(`CREATE TABLE %s (id INTEGER, name TEXT, note TEXT)`, sqlIdent(table)))

	// Insert some data to search over.
	const n = 1000
	batch := make([][]any, 0, 256)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := r.InsertRows(ctx, table, []string{"id", "name", "note"}, batch); err != nil {
			b.Fatalf("seed insert: %v", err)
		}
		batch = batch[:0]
	}
	for i := 0; i < n; i++ {
		batch = append(batch, []any{i, fmt.Sprintf("name_%d", i), "lorem ipsum dolor sit amet"})
		if len(batch) == cap(batch) {
			flush()
		}
	}
	flush()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rows, err := r.Search(ctx, table, []string{"name", "note"}, "ipsum", 25)
		if err != nil {
			b.Fatal(err)
		}
		// Consume the rows to account for scanning cost.
		var sink int
		for rows.Next() {
			var id int
			var name, note string
			if err := rows.Scan(&id, &name, &note); err != nil {
				b.Fatal(err)
			}
			sink += id
		}
		_ = sink
		if err := rows.Err(); err != nil {
			b.Fatal(err)
		}
		rows.Close()
	}
}

/*
Keep benchmarks stable across platforms by avoiding spillover effects.
*/
func TestMain(m *testing.M) {
	// Modernc SQLite may use many threads; keep the scheduler predictable in CI.
	runtime.GOMAXPROCS(runtime.NumCPU())
	m.Run()
}
