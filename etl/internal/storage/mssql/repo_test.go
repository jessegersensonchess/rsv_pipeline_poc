package mssql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"testing"
)

// // TestBulkUpsertEmptyRecords verifies that BulkUpsert short-circuits and
// // does not require a live database connection when no records are provided.
// func TestBulkUpsertEmptyRecords(t *testing.T) {
// 	t.Parallel()
//
// 	r := &Repository{
// 		db:  nil, // should not be touched for empty input
// 		cfg: Config{Table: "dbo.t", Columns: []string{"id", "name"}},
// 	}
//
// 	got, err := r.BulkUpsert(context.Background(), nil, []string{"id"}, "")
// 	if err != nil {
// 		t.Fatalf("BulkUpsert(nil...) error = %v, want nil", err)
// 	}
// 	if got != 0 {
// 		t.Fatalf("BulkUpsert(nil...) = %d, want 0", got)
// 	}
// }
//
// // TestBulkUpsertNoColumnsConfigured verifies that BulkUpsert validates the
// // configured columns before touching the database.
// func TestBulkUpsertNoColumnsConfigured(t *testing.T) {
// 	t.Parallel()
//
// 	r := &Repository{
// 		db:  nil, // should not be touched for this error case
// 		cfg: Config{Table: "dbo.t", Columns: nil},
// 	}
//
// 	recs := []map[string]any{
// 		{"id": 1, "name": "alice"},
// 	}
//
// 	got, err := r.BulkUpsert(context.Background(), recs, []string{"id"}, "")
// 	if err == nil {
// 		t.Fatalf("BulkUpsert() error = nil, want non-nil when Columns is empty")
// 	}
// 	if got != 0 {
// 		t.Fatalf("BulkUpsert() rows = %d, want 0 on error", got)
// 	}
// 	if !strings.Contains(err.Error(), "no columns configured") {
// 		t.Fatalf("BulkUpsert() error = %q, want it to mention 'no columns configured'", err.Error())
// 	}
// }

// TestCopyFromEmptyRows verifies that CopyFrom short-circuits when no rows
// are provided and does not require a live database connection.
func TestCopyFromEmptyRows(t *testing.T) {
	t.Parallel()

	r := &Repository{
		db:  nil, // must not be used in this path
		cfg: Config{Table: "dbo.t"},
	}

	got, err := r.CopyFrom(context.Background(), []string{"id", "name"}, nil)
	if err != nil {
		t.Fatalf("CopyFrom(nil...) error = %v, want nil", err)
	}
	if got != 0 {
		t.Fatalf("CopyFrom(nil...) = %d, want 0", got)
	}
}

// TestBuildDeleteConditionTwo verifies that buildDeleteCondition builds the
// expected join predicate for various key column sets.
func TestBuildDeleteConditionTwo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		keyColumns []string
		want       string
	}{
		{
			name:       "single key",
			keyColumns: []string{"id"},
			want:       "T.[id] = S.[id]",
		},
		{
			name:       "multiple keys",
			keyColumns: []string{"id", "tenant_id"},
			want:       "T.[id] = S.[id] AND T.[tenant_id] = S.[tenant_id]",
		},
		{
			name:       "no keys",
			keyColumns: nil,
			want:       "",
		},
		{
			name:       "key with special chars",
			keyColumns: []string{"user]id"},
			want:       "T.[user]]id] = S.[user]]id]",
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := buildDeleteCondition(tt.keyColumns)
			if got != tt.want {
				t.Fatalf("buildDeleteCondition(%v) = %q, want %q", tt.keyColumns, got, tt.want)
			}
		})
	}
}

// TestFilterConflictKeysB verifies that filterConflictKeys removes SET parts
// targeting key columns and preserves the original order of non-key parts.
func TestFilterConflictKeysB(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setParts []string
		keys     []string
		want     []string
	}{
		{
			name:     "no keys",
			setParts: []string{"id = EXCLUDED.id", "name = EXCLUDED.name"},
			keys:     nil,
			want:     []string{"id = EXCLUDED.id", "name = EXCLUDED.name"},
		},
		{
			name:     "single key",
			setParts: []string{"id = EXCLUDED.id", "name = EXCLUDED.name"},
			keys:     []string{"id"},
			want:     []string{"name = EXCLUDED.name"},
		},
		{
			name:     "multiple keys",
			setParts: []string{"id = EXCLUDED.id", "tenant_id = EXCLUDED.tenant_id", "name = EXCLUDED.name"},
			keys:     []string{"id", "tenant_id"},
			want:     []string{"name = EXCLUDED.name"},
		},
		{
			name:     "non-matching keys",
			setParts: []string{"id = EXCLUDED.id", "name = EXCLUDED.name"},
			keys:     []string{"created_at"},
			want:     []string{"id = EXCLUDED.id", "name = EXCLUDED.name"},
		},
		{
			name:     "no parts",
			setParts: nil,
			keys:     []string{"id"},
			want:     nil,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := filterConflictKeys(tt.setParts, tt.keys)
			if len(got) != len(tt.want) {
				t.Fatalf("filterConflictKeys(%v, %v) length = %d, want %d",
					tt.setParts, tt.keys, len(got), len(tt.want))
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("filterConflictKeys(%v, %v)[%d] = %q, want %q",
						tt.setParts, tt.keys, i, got[i], tt.want[i])
				}
			}
		})
	}
}

// TestToCopyVal verifies that toCopyVal passes non-nil values through
// unchanged and preserves nil.
func TestToCopyVal(t *testing.T) {
	t.Parallel()

	type custom struct {
		X int
	}

	tests := []struct {
		name string
		in   any
	}{
		{name: "nil", in: nil},
		{name: "string", in: "hello"},
		{name: "int", in: 42},
		{name: "struct", in: custom{X: 1}},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := toCopyVal(tt.in)
			if tt.in == nil {
				if got != nil {
					t.Fatalf("toCopyVal(nil) = %#v, want nil", got)
				}
				return
			}
			// For non-nil values we expect the identical interface value.
			if got != tt.in {
				t.Fatalf("toCopyVal(%#v) = %#v, want same value", tt.in, got)
			}
		})
	}
}

// TestMsIdentB verifies the MSSQL identifier quoting and escaping in msIdent.
func TestMsIdentB(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "simple", in: "id", want: "[id]"},
		{name: "empty", in: "", want: "[]"},
		{name: "with space", in: "user id", want: "[user id]"},
		{name: "escape closing bracket", in: "user]id", want: "[user]]id]"},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := msIdent(tt.in)
			if got != tt.want {
				t.Fatalf("msIdent(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// TestMsFQNB verifies that msFQN correctly handles simple and schema-qualified
// names and applies identifier quoting to each segment.
func TestMsFQNB(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "simple table", in: "Users", want: "[Users]"},
		{name: "schema and table", in: "dbo.Users", want: "[dbo].[Users]"},
		{name: "multi schema", in: "a.b.c", want: "[a].[b].[c]"},
		{name: "with bracket", in: "dbo.user]s", want: "[dbo].[user]]s]"},
		{name: "empty", in: "", want: "[]"}, // msIdent("") -> "[]"
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := msFQN(tt.in)
			if got != tt.want {
				t.Fatalf("msFQN(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// TestMapIdent verifies that mapIdent applies msIdent to each column name
// while preserving order.
func TestMapIdent(t *testing.T) {
	t.Parallel()

	in := []string{"id", "user]id", "name"}
	got := mapIdent(in)

	if len(got) != len(in) {
		t.Fatalf("mapIdent(%v) length = %d, want %d", in, len(got), len(in))
	}
	want := []string{"[id]", "[user]]id]", "[name]"}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("mapIdent(%v)[%d] = %q, want %q", in, i, got[i], want[i])
		}
	}
}

// BenchmarkBuildDeleteCondition measures the cost of building join predicates
// for varying numbers of key columns.
func BenchmarkBuildDeleteCondition(b *testing.B) {
	keyColumns := []string{"id", "tenant_id", "region", "partition_id"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = buildDeleteCondition(keyColumns)
	}
}

// BenchmarkFilterConflictKeys measures the cost of filtering out key columns
// from SET parts for typical UPSERT-like workloads.
func BenchmarkFilterConflictKeys(b *testing.B) {
	setParts := []string{
		"id = EXCLUDED.id",
		"tenant_id = EXCLUDED.tenant_id",
		"name = EXCLUDED.name",
		"updated_at = EXCLUDED.updated_at",
	}
	keys := []string{"id", "tenant_id"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = filterConflictKeys(setParts, keys)
	}
}

// BenchmarkMsIdent measures the cost of quoting single identifiers.
func BenchmarkMsIdent(b *testing.B) {
	ids := []string{"id", "tenant_id", "user]id", "very_long_column_name_with_suffix"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = msIdent(ids[i%len(ids)])
	}
}

// BenchmarkMsFQN measures the cost of quoting fully qualified names.
func BenchmarkMsFQN(b *testing.B) {
	names := []string{
		"dbo.Users",
		"schema.table",
		"multi.segment.name",
		"dbo.user]table",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = msFQN(names[i%len(names)])
	}
}

// BenchmarkMapIdent measures the cost of mapping identifier quoting over
// a list of column names.
func BenchmarkMapIdent(b *testing.B) {
	cols := []string{"id", "name", "tenant_id", "user]id", "created_at", "updated_at"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = mapIdent(cols)
	}
}

// BenchmarkToCopyVal measures the cost of the simple pass-through logic
// in toCopyVal for both nil and non-nil values.
func BenchmarkToCopyVal(b *testing.B) {
	values := []any{nil, "string", 123, 45.67, struct{ X int }{X: 1}}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = toCopyVal(values[i%len(values)])
	}
}

// --- Test driver plumbing for exercising Exec and CopyFrom without a real DB --

type errDriver struct{}

type errConn struct{}

type errTx struct{}

func (d *errDriver) Open(name string) (driver.Conn, error) {
	return &errConn{}, nil
}

// Prepare is not expected to be called in our tests; if it is, fail loudly.
func (c *errConn) Prepare(query string) (driver.Stmt, error) {
	return nil, errors.New("unexpected Prepare call")
}

func (c *errConn) Close() error { return nil }

// Begin is required by driver.Conn; database/sql calls BeginTx when available.
func (c *errConn) Begin() (driver.Tx, error) {
	return nil, errors.New("begin (legacy) should not be called")
}

// BeginTx implements driver.ConnBeginTx and always fails, to exercise the
// error path in Repository.CopyFrom.
func (c *errConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return nil, errors.New("begin failed")
}

// ExecContext implements driver.ExecerContext and always fails, to exercise
// the error path in Repository.Exec.
func (c *errConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return nil, errors.New("exec failed")
}

// We don't expect queries in these tests.
func (c *errConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return nil, errors.New("unexpected QueryContext call")
}

func (t *errTx) Commit() error   { return nil }
func (t *errTx) Rollback() error { return nil }

var (
	testDriverOnce sync.Once
	testDriverName = "mssql_test_err"
)

// openErrDB registers and opens a test driver that fails BeginTx and ExecContext.
func openErrDB(t *testing.T) *sql.DB {
	t.Helper()

	testDriverOnce.Do(func() {
		sql.Register(testDriverName, &errDriver{})
	})
	db, err := sql.Open(testDriverName, "")
	if err != nil {
		t.Fatalf("sql.Open(%q) error = %v", testDriverName, err)
	}
	return db
}

// --- Tests ---

// TestExecPropagatesError verifies that Exec forwards errors from the underlying
// *sql.DB.ExecContext call when the driver returns an error.
func TestExecPropagatesError(t *testing.T) {
	t.Parallel()

	db := openErrDB(t)
	r := &Repository{
		db:  db,
		cfg: Config{Table: "dbo.t"},
	}

	ctx := context.Background()
	err := r.Exec(ctx, "SELECT 1")
	if err == nil {
		t.Fatalf("Exec() error = nil, want non-nil")
	}

	// Ensure the error is the one produced by our test driver.
	if !strings.Contains(err.Error(), "exec failed") {
		t.Fatalf("Exec() error = %q, want it to contain %q", err.Error(), "exec failed")
	}
}

// TestCopyFromBeginTxError verifies that CopyFrom surfaces errors from
// db.BeginTx before any bulk-copy logic runs.
func TestCopyFromBeginTxError(t *testing.T) {
	t.Parallel()

	db := openErrDB(t)
	r := &Repository{
		db:  db,
		cfg: Config{Table: "dbo.t"},
	}

	ctx := context.Background()
	columns := []string{"id", "name"}
	rows := [][]any{
		{1, "alice"},
		{2, "bob"},
	}

	n, err := r.CopyFrom(ctx, columns, rows)
	if err == nil {
		t.Fatalf("CopyFrom() error = nil, want non-nil when BeginTx fails")
	}
	if n != 0 {
		t.Fatalf("CopyFrom() rows = %d, want 0 on error", n)
	}
	if !strings.Contains(err.Error(), "begin tx:") {
		t.Fatalf("CopyFrom() error = %q, want it wrapped with 'begin tx:'", err.Error())
	}
}
