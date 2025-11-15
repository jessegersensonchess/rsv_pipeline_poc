package ddl

import (
	"fmt"
	"strings"
	"testing"
)

/*
Unit tests for BuildCreateTableSQL and quoteFQN.

These tests use table-driven cases to validate:
  - error handling for missing table name and empty columns
  - correct fully-qualified name quoting with escaping
  - NOT NULL and DEFAULT handling
  - multi-column formatting and stable whitespace

No third-party dependencies are used.
*/

// TestBuildCreateTableSQL validates the core SQL generation behavior using
// table-driven checks for both happy paths and error paths.
func TestBuildCreateTableSQL(t *testing.T) {
	t.Parallel()

	type td struct {
		name      string
		in        TableDef
		wantSQL   string // full match when non-empty
		wantError string // substring match when non-empty
	}
	cases := []td{
		{
			name:      "error_missing_table_name",
			in:        TableDef{FQN: "", Columns: []ColumnDef{{Name: "id", SQLType: "INTEGER"}}},
			wantError: "missing table name",
		},
		{
			name:      "error_no_columns",
			in:        TableDef{FQN: "t", Columns: nil},
			wantError: "no columns",
		},
		{
			name: "single_column_nullable_with_default",
			in: TableDef{
				FQN: "public.users",
				Columns: []ColumnDef{
					{Name: "name", SQLType: "TEXT", Default: "'anon'", Nullable: true},
				},
			},
			wantSQL: "CREATE TABLE IF NOT EXISTS \"public\".\"users\" (\n" +
				"  \"name\" TEXT DEFAULT 'anon'\n" +
				");",
		},
		{
			name: "single_column_not_null_no_default",
			in: TableDef{
				FQN: "users",
				Columns: []ColumnDef{
					{Name: "id", SQLType: "INTEGER", Nullable: false},
				},
			},
			wantSQL: "CREATE TABLE IF NOT EXISTS \"users\" (\n" +
				"  \"id\" INTEGER NOT NULL\n" +
				");",
		},
		{
			name: "multiple_columns_mixed",
			in: TableDef{
				FQN: "app.events",
				Columns: []ColumnDef{
					{Name: "id", SQLType: "INTEGER", Nullable: false},
					{Name: "ts", SQLType: "TIMESTAMP", Default: "CURRENT_TIMESTAMP", Nullable: false},
					{Name: "payload", SQLType: "TEXT", Nullable: true},
				},
			},
			wantSQL: "CREATE TABLE IF NOT EXISTS \"app\".\"events\" (\n" +
				"  \"id\" INTEGER NOT NULL,\n" +
				"  \"ts\" TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,\n" +
				"  \"payload\" TEXT\n" +
				");",
		},
		{
			name: "fqn_with_quotes_needs_escaping",
			in: TableDef{
				FQN: `sch"ema.tbl"name`,
				Columns: []ColumnDef{
					{Name: "x", SQLType: "TEXT", Nullable: true},
				},
			},
			wantSQL: "CREATE TABLE IF NOT EXISTS \"sch\"\"ema\".\"tbl\"\"name\" (\n" +
				"  \"x\" TEXT\n" +
				");",
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			got, err := BuildCreateTableSQL(c.in)
			if c.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantError) {
					t.Fatalf("expected error containing %q, got %v", c.wantError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.wantSQL {
				t.Fatalf("SQL mismatch:\n--- got ---\n%s\n--- want ---\n%s", got, c.wantSQL)
			}
		})
	}
}

// TestQuoteFQN exercises quoting and embedded double-quote escaping across
// single-, double-, and triple-segment FQNs.
func TestQuoteFQN(t *testing.T) {
	t.Parallel()

	type tc struct {
		in   string
		want string
	}
	cases := []tc{
		{in: "tbl", want: `"tbl"`},
		{in: "sch.tbl", want: `"sch"."tbl"`},
		{in: `s"c"h.t"b"l`, want: `"s""c""h"."t""b""l"`},
		{in: `a.b.c`, want: `"a"."b"."c"`},
		{in: `weird"name`, want: `"weird""name"`},
	}

	for _, c := range cases {
		if got := quoteFQN(c.in); got != c.want {
			t.Fatalf("quoteFQN(%q) = %q; want %q", c.in, got, c.want)
		}
	}
}

/*
Doc examples (godoc-style).
*/

// Example_quoteFQN shows how fully-qualified names are quoted and escaped.
func Example_quoteFQN() {
	fmt.Println(quoteFQN(`public.users`))
	fmt.Println(quoteFQN(`weird"name.tbl`))
	// Output:
	// "public"."users"
	// "weird""name"."tbl"
}

// Example_BuildCreateTableSQL demonstrates generating a simple CREATE TABLE.
func ExampleBuildCreateTableSQL() {
	sql, _ := BuildCreateTableSQL(TableDef{
		FQN: "public.users",
		Columns: []ColumnDef{
			{Name: "id", SQLType: "INTEGER", Nullable: false},
			{Name: "name", SQLType: "TEXT", Default: "'anon'", Nullable: true},
		},
	})
	fmt.Println(sql)
	// Output:
	// CREATE TABLE IF NOT EXISTS "public"."users" (
	//   "id" INTEGER NOT NULL,
	//   "name" TEXT DEFAULT 'anon'
	// );
}
