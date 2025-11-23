package ddl

import (
	"strconv"
	"strings"
	"testing"

	gddl "etl/internal/ddl"
)

// TestQuoteIdent verifies that quoteIdent applies SQLite-style double-quoted
// identifier quoting and correctly escapes embedded double quotes.
func TestQuoteIdent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "simple", in: "name", want: `"name"`},
		{name: "empty", in: "", want: `""`},
		{name: "with space", in: "user name", want: `"user name"`},
		{name: "with double quote", in: `weird"name`, want: `"weird""name"`},
		{name: "multiple quotes", in: `"a""b"`, want: `"""a""""b"""`},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := quoteIdent(tt.in)
			if got != tt.want {
				t.Fatalf("quoteIdent(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// TestQuoteFQN verifies that quoteFQN correctly quotes each segment of a
// possibly-qualified table name and ignores empty segments.
func TestQuoteFQN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "simple table", in: "events", want: `"events"`},
		{name: "main schema", in: "main.events", want: `"main"."events"`},
		{name: "multiple segments", in: "a.b.c", want: `"a"."b"."c"`},
		{name: "with spaces and empties", in: " .main..events. ", want: `"main"."events"`},
		{name: "with quotes", in: `main."events"`, want: `"main"."""events"""`},
		{name: "empty", in: "", want: ""},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := quoteFQN(tt.in)
			if got != tt.want {
				t.Fatalf("quoteFQN(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// TestBuildCreateTableSQLErrors validates basic input validation in
// BuildCreateTableSQL.
func TestBuildCreateTableSQLErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		def  gddl.TableDef
	}{
		{
			name: "empty FQN",
			def: gddl.TableDef{
				FQN:     "   ",
				Columns: []gddl.ColumnDef{{Name: "id", SQLType: "INTEGER"}},
			},
		},
		{
			name: "no columns",
			def: gddl.TableDef{
				FQN:     "events",
				Columns: nil,
			},
		},
		{
			name: "column empty name",
			def: gddl.TableDef{
				FQN: "events",
				Columns: []gddl.ColumnDef{
					{Name: "id", SQLType: "INTEGER"},
					{Name: "   ", SQLType: "TEXT"},
				},
			},
		},
		{
			name: "column missing type",
			def: gddl.TableDef{
				FQN: "events",
				Columns: []gddl.ColumnDef{
					{Name: "id", SQLType: ""},
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sql, err := BuildCreateTableSQL(tt.def)
			if err == nil {
				t.Fatalf("BuildCreateTableSQL(%+v) error = nil, want non-nil", tt.def)
			}
			if sql != "" {
				t.Fatalf("BuildCreateTableSQL(%+v) SQL = %q, want empty string on error", tt.def, sql)
			}
		})
	}
}

// TestBuildCreateTableSQLBasic verifies that BuildCreateTableSQL renders a
// simple CREATE TABLE statement with nullable and non-nullable columns and
// a PRIMARY KEY clause.
func TestBuildCreateTableSQLBasic(t *testing.T) {
	t.Parallel()

	def := gddl.TableDef{
		FQN: "events",
		Columns: []gddl.ColumnDef{
			{Name: "id", SQLType: "INTEGER", Nullable: false, PrimaryKey: true},
			{Name: "name", SQLType: "TEXT", Nullable: true, Default: `'unknown'`},
		},
	}

	got, err := BuildCreateTableSQL(def)
	if err != nil {
		t.Fatalf("BuildCreateTableSQL() error = %v", err)
	}

	want := "" +
		`CREATE TABLE IF NOT EXISTS "events" (` + "\n" +
		`  "id" INTEGER NOT NULL,` + "\n" +
		`  "name" TEXT DEFAULT 'unknown',` + "\n" +
		`  PRIMARY KEY ("id")` + "\n" +
		`);`

	if got != want {
		t.Fatalf("BuildCreateTableSQL() =\n%s\nwant:\n%s", got, want)
	}
}

// TestBuildCreateTableSQLPrimaryKey verifies that BuildCreateTableSQL renders
// multi-column primary keys and does not force NOT NULL on primary-key columns
// (unlike some other backends).
func TestBuildCreateTableSQLPrimaryKey(t *testing.T) {
	t.Parallel()

	def := gddl.TableDef{
		FQN: "main.order_items",
		Columns: []gddl.ColumnDef{
			{Name: "order_id", SQLType: "INTEGER", Nullable: true, PrimaryKey: true},
			{Name: "item_id", SQLType: "INTEGER", Nullable: true, PrimaryKey: true},
			{Name: "qty", SQLType: "INTEGER", Nullable: false},
		},
	}

	got, err := BuildCreateTableSQL(def)
	if err != nil {
		t.Fatalf("BuildCreateTableSQL() error = %v", err)
	}

	// For SQLite builder we only add NOT NULL when Nullable=false.
	if !strings.Contains(got, `"qty" INTEGER NOT NULL`) {
		t.Fatalf("SQL does not mark qty as NOT NULL: \n%s", got)
	}
	if strings.Contains(got, `"order_id" INTEGER NOT NULL`) {
		t.Fatalf("SQL unexpectedly marks order_id as NOT NULL: \n%s", got)
	}

	if !strings.Contains(got, `PRIMARY KEY ("order_id", "item_id")`) {
		t.Fatalf("SQL PRIMARY KEY clause missing or incorrect:\n%s", got)
	}
}

// BenchmarkBuildCreateTableSQLSmall measures performance for a small table.
func BenchmarkBuildCreateTableSQLSmall(b *testing.B) {
	def := gddl.TableDef{
		FQN: "events",
		Columns: []gddl.ColumnDef{
			{Name: "id", SQLType: "INTEGER", PrimaryKey: true},
			{Name: "name", SQLType: "TEXT", Nullable: true},
			{Name: "created_at", SQLType: "TEXT", Nullable: false, Default: "CURRENT_TIMESTAMP"},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := BuildCreateTableSQL(def); err != nil {
			b.Fatalf("BuildCreateTableSQL() error = %v", err)
		}
	}
}

// BenchmarkBuildCreateTableSQLWide measures performance for a wide table.
func BenchmarkBuildCreateTableSQLWide(b *testing.B) {
	const numCols = 64

	cols := make([]gddl.ColumnDef, 0, numCols)
	for i := 0; i < numCols; i++ {
		cols = append(cols, gddl.ColumnDef{
			Name:     "col_" + strconv.Itoa(i),
			SQLType:  "TEXT",
			Nullable: i%2 == 0,
		})
	}
	cols[0].PrimaryKey = true

	def := gddl.TableDef{
		FQN:     "wide_table",
		Columns: cols,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := BuildCreateTableSQL(def); err != nil {
			b.Fatalf("BuildCreateTableSQL() error = %v", err)
		}
	}
}
