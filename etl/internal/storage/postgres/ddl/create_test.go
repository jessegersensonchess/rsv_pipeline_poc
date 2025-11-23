package ddl

import (
	"strconv"
	"strings"
	"testing"

	gddl "etl/internal/ddl"
)

// TestQuoteIdent verifies Postgres identifier quoting and escaping for single
// identifier segments in quoteIdent.
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

// TestQuoteFQN verifies quoting and splitting behavior for schema-qualified
// table names in quoteFQN.
func TestQuoteFQN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "simple table", in: "users", want: `"users"`},
		{name: "schema and table", in: "public.users", want: `"public"."users"`},
		{name: "three segments", in: "a.b.c", want: `"a"."b"."c"`},
		{name: "with empty segments", in: ".public..users.", want: `"public"."users"`},
		{name: "with quotes", in: `sch."table"`, want: `"sch"."""table"""`},
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

// TestBuildCreateTableSQLErrors validates error handling and basic input
// validation in BuildCreateTableSQL.
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
				Columns: []gddl.ColumnDef{{Name: "id", SQLType: "BIGINT"}},
			},
		},
		{
			name: "no columns",
			def: gddl.TableDef{
				FQN:     "public.users",
				Columns: nil,
			},
		},
		{
			name: "column empty name",
			def: gddl.TableDef{
				FQN: "public.users",
				Columns: []gddl.ColumnDef{
					{Name: "id", SQLType: "BIGINT"},
					{Name: "   ", SQLType: "TEXT"},
				},
			},
		},
		{
			name: "column missing SQLType",
			def: gddl.TableDef{
				FQN: "public.users",
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

			got, err := BuildCreateTableSQL(tt.def)
			if err == nil {
				t.Fatalf("BuildCreateTableSQL(%+v) error = nil, want non-nil", tt.def)
			}
			if got != "" {
				t.Fatalf("BuildCreateTableSQL(%+v) SQL = %q, want empty string on error", tt.def, got)
			}
		})
	}
}

// TestBuildCreateTableSQLBasic verifies that BuildCreateTableSQL renders a
// simple table with primary key, nullability, and default expression.
func TestBuildCreateTableSQLBasic(t *testing.T) {
	t.Parallel()

	def := gddl.TableDef{
		FQN: "public.users",
		Columns: []gddl.ColumnDef{
			{
				Name:       "id",
				SQLType:    "BIGINT",
				Nullable:   false,
				PrimaryKey: true,
			},
			{
				Name:     "name",
				SQLType:  "TEXT",
				Nullable: true,
				Default:  `'anonymous'`,
			},
		},
	}

	got, err := BuildCreateTableSQL(def)
	if err != nil {
		t.Fatalf("BuildCreateTableSQL() error = %v", err)
	}

	want := "" +
		`CREATE TABLE IF NOT EXISTS "public"."users" (` + "\n" +
		`  "id" BIGINT NOT NULL,` + "\n" +
		`  "name" TEXT DEFAULT 'anonymous',` + "\n" +
		`  PRIMARY KEY ("id")` + "\n" +
		`);`

	if got != want {
		t.Fatalf("BuildCreateTableSQL() =\n%s\nwant:\n%s", got, want)
	}
}

// TestBuildCreateTableSQLPrimaryKeys verifies that primary-key columns are
// forced to NOT NULL and that the PRIMARY KEY clause is sorted alphabetically.
func TestBuildCreateTableSQLPrimaryKeys(t *testing.T) {
	t.Parallel()

	def := gddl.TableDef{
		FQN: "public.order_items",
		Columns: []gddl.ColumnDef{
			{Name: "order_id", SQLType: "BIGINT", PrimaryKey: true, Nullable: true},
			{Name: "item_id", SQLType: "INT", PrimaryKey: true, Nullable: true},
			{Name: "qty", SQLType: "INT", Nullable: false},
		},
	}

	got, err := BuildCreateTableSQL(def)
	if err != nil {
		t.Fatalf("BuildCreateTableSQL() error = %v", err)
	}

	// Primary keys must be NOT NULL even if Nullable=true.
	if !strings.Contains(got, `"order_id" BIGINT NOT NULL`) {
		t.Fatalf("SQL does not mark primary key column order_id as NOT NULL:\n%s", got)
	}
	if !strings.Contains(got, `"item_id" INT NOT NULL`) {
		t.Fatalf("SQL does not mark primary key column item_id as NOT NULL:\n%s", got)
	}

	// PRIMARY KEY clause should have alphabetically sorted identifiers:
	// ("item_id", "order_id")
	if !strings.Contains(got, `PRIMARY KEY ("item_id", "order_id")`) {
		t.Fatalf("SQL PRIMARY KEY clause not sorted as expected:\n%s", got)
	}
}

// BenchmarkBuildCreateTableSQLSmall measures the performance of
// BuildCreateTableSQL for a small table definition.
func BenchmarkBuildCreateTableSQLSmall(b *testing.B) {
	def := gddl.TableDef{
		FQN: "public.users",
		Columns: []gddl.ColumnDef{
			{Name: "id", SQLType: "BIGINT", PrimaryKey: true},
			{Name: "name", SQLType: "TEXT", Nullable: true},
			{Name: "created_at", SQLType: "TIMESTAMPTZ", Nullable: false, Default: "NOW()"},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := BuildCreateTableSQL(def); err != nil {
			b.Fatalf("BuildCreateTableSQL() error = %v", err)
		}
	}
}

// BenchmarkBuildCreateTableSQLWide measures the performance of
// BuildCreateTableSQL for a wide table with many columns.
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
		FQN:     "public.wide_table",
		Columns: cols,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := BuildCreateTableSQL(def); err != nil {
			b.Fatalf("BuildCreateTableSQL() error = %v", err)
		}
	}
}
