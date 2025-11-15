package ddl

import (
	"reflect"
	"testing"

	"etl/internal/config"
)

/*
Unit tests for InferTableDef and mapType.

We cover:
  - mapType’s canonical mappings (table-driven)
  - InferTableDef error on missing table name
  - InferTableDef default behavior with no transforms:
      - Columns preserved in order
      - Nullable = true
      - SQLType falls back to TEXT
No third-party dependencies are used.
*/

// TestMapType verifies the canonical string-to-SQL type mapping, including
// case-insensitivity and the TEXT fallback.
func TestMapType(t *testing.T) {
	t.Parallel()

	type tc struct {
		in   string
		want string
	}
	cases := []tc{
		{"", "TEXT"},
		{"text", "TEXT"},
		{"int", "BIGINT"},
		{"integer", "BIGINT"},
		{"bigint", "BIGINT"},
		{"BOOL", "BOOLEAN"},
		{"boolean", "BOOLEAN"},
		{"date", "DATE"},
		{"timestamp", "TIMESTAMPTZ"},
		{"timestamptz", "TIMESTAMPTZ"},
		{"unknown-kind", "TEXT"},
	}
	for _, c := range cases {
		got := mapType(c.in)
		if got != c.want {
			t.Fatalf("mapType(%q) = %q; want %q", c.in, got, c.want)
		}
	}
}

// TestInferTableDef_MissingTable checks the early error when the Postgres
// target table name is not provided.
func TestInferTableDef_MissingTable(t *testing.T) {
	t.Parallel()

	var p config.Pipeline // zero value; Storage.Postgres.Table == ""
	_, err := InferTableDef(p)
	if err == nil {
		t.Fatalf("InferTableDef() expected error for missing table, got nil")
	}
}

// TestInferTableDef_DefaultNoTransforms verifies that when no validate/coerce
// transforms are present, inference falls back to:
//   - Nullable = true
//   - SQLType = TEXT
//   - Column order preserved
func TestInferTableDef_DefaultNoTransforms(t *testing.T) {
	t.Parallel()

	p := config.Pipeline{}
	p.Storage.Postgres.Table = "public.users"
	p.Storage.Postgres.Columns = []string{"id", "name", "payload"}

	td, err := InferTableDef(p)
	if err != nil {
		t.Fatalf("InferTableDef() unexpected error: %v", err)
	}

	if td.FQN != "public.users" {
		t.Fatalf("FQN mismatch: got %q, want %q", td.FQN, "public.users")
	}
	if len(td.Columns) != 3 {
		t.Fatalf("columns length: got %d, want 3", len(td.Columns))
	}

	// Expected shape.
	want := []ColumnDef{
		{Name: "id", SQLType: "TEXT", Nullable: true},
		{Name: "name", SQLType: "TEXT", Nullable: true},
		{Name: "payload", SQLType: "TEXT", Nullable: true},
	}
	if !reflect.DeepEqual(stripPKDefaults(td.Columns), want) {
		t.Fatalf("columns mismatch:\n  got:  %#v\n  want: %#v", td.Columns, want)
	}
}

// stripPKDefaults normalizes out PrimaryKey and Default fields (which should be
// zero values in the default/no-transform path) so we can compare succinctly.
func stripPKDefaults(cs []ColumnDef) []ColumnDef {
	out := make([]ColumnDef, len(cs))
	for i, c := range cs {
		out[i] = ColumnDef{
			Name:     c.Name,
			SQLType:  c.SQLType,
			Nullable: c.Nullable,
			// PrimaryKey and Default intentionally omitted
		}
	}
	return out
}
