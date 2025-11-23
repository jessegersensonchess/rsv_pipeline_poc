package ddl

import "testing"

// TestMapType verifies that MapType maps a variety of logical type names into
// the expected SQLite column types and falls back to TEXT.
func TestMapType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		kind string
		want string
	}{
		{name: "int lower", kind: "int", want: "INTEGER"},
		{name: "integer", kind: "integer", want: "INTEGER"},
		{name: "bigint", kind: "bigint", want: "INTEGER"},
		{name: "int mixed", kind: "  InTeGeR  ", want: "INTEGER"},

		{name: "bool", kind: "bool", want: "INTEGER"},
		{name: "boolean", kind: "BOOLEAN", want: "INTEGER"},

		{name: "float", kind: "float", want: "REAL"},
		{name: "double", kind: "double", want: "REAL"},
		{name: "real", kind: "REAL", want: "REAL"},

		{name: "numeric", kind: "numeric", want: "NUMERIC"},
		{name: "decimal", kind: "decimal", want: "NUMERIC"},

		{name: "date", kind: "date", want: "TEXT"},
		{name: "timestamp", kind: "timestamp", want: "TEXT"},
		{name: "datetime", kind: "datetime", want: "TEXT"},
		{name: "timestamptz", kind: "timestamptz", want: "TEXT"},

		{name: "blob", kind: "blob", want: "BLOB"},
		{name: "bytes", kind: "bytes", want: "BLOB"},

		{name: "empty", kind: "", want: "TEXT"},
		{name: "spaces", kind: "   ", want: "TEXT"},
		{name: "string", kind: "string", want: "TEXT"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := MapType(tt.kind)
			if got != tt.want {
				t.Fatalf("MapType(%q) = %q, want %q", tt.kind, got, tt.want)
			}
		})
	}
}

// BenchmarkMapType measures the performance of MapType for a mix of logical
// types.
func BenchmarkMapType(b *testing.B) {
	kinds := []string{
		"int", "integer", "bigint",
		"bool", "boolean",
		"float", "double", "real",
		"numeric", "decimal",
		"date", "timestamp", "datetime", "timestamptz",
		"blob", "bytes",
		"", "string",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = MapType(kinds[i%len(kinds)])
	}
}
