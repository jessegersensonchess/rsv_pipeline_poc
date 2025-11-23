package ddl

import "testing"

// TestMapType verifies that MapType normalizes a variety of logical type
// names into the expected Postgres SQL types and defaults to TEXT.
func TestMapType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		kind string
		want string
	}{
		// Integer / bigint
		{name: "int lower", kind: "int", want: "BIGINT"},
		{name: "integer lower", kind: "integer", want: "BIGINT"},
		{name: "int mixed case", kind: " InTeGeR ", want: "BIGINT"},
		{name: "bigint lower", kind: "bigint", want: "BIGINT"},
		{name: "bigint upper", kind: "BIGINT", want: "BIGINT"},

		// Boolean
		{name: "bool lower", kind: "bool", want: "BOOLEAN"},
		{name: "boolean lower", kind: "boolean", want: "BOOLEAN"},
		{name: "boolean upper", kind: "BOOLEAN", want: "BOOLEAN"},

		// Date / timestamp
		{name: "date lower", kind: "date", want: "DATE"},
		{name: "timestamp lower", kind: "timestamp", want: "TIMESTAMPTZ"},
		{name: "timestamptz lower", kind: "timestamptz", want: "TIMESTAMPTZ"},
		{name: "timestamp upper", kind: "TIMESTAMP", want: "TIMESTAMPTZ"},

		// Default / unknown
		{name: "empty string", kind: "", want: "TEXT"},
		{name: "spaces only", kind: "   ", want: "TEXT"},
		{name: "string", kind: "string", want: "TEXT"},
		{name: "text", kind: "text", want: "TEXT"},
		{name: "jsonb", kind: "jsonb", want: "TEXT"},
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

// BenchmarkMapType measures the performance of MapType under a mixture of
// known and unknown logical types.
func BenchmarkMapType(b *testing.B) {
	kinds := []string{
		"int",
		"integer",
		"bigint",
		"bool",
		"boolean",
		"date",
		"timestamp",
		"timestamptz",
		"",
		"string",
		"text",
		"jsonb",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = MapType(kinds[i%len(kinds)])
	}
}
