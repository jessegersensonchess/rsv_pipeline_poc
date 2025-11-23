package ddl

import "testing"

// TestMapType verifies that MapType maps logical type names to the expected
// SQL Server column types using a variety of case and whitespace inputs.
func TestMapType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		kind string
		want string
	}{
		// Integer types.
		{name: "int lower", kind: "int", want: "BIGINT"},
		{name: "integer lower", kind: "integer", want: "BIGINT"},
		{name: "int with spaces", kind: "  int  ", want: "BIGINT"},
		{name: "integer mixed case", kind: " InTeGeR ", want: "BIGINT"},
		{name: "bigint lower", kind: "bigint", want: "BIGINT"},
		{name: "bigint upper", kind: "BIGINT", want: "BIGINT"},

		// Boolean types.
		{name: "bool lower", kind: "bool", want: "BIT"},
		{name: "boolean lower", kind: "boolean", want: "BIT"},
		{name: "boolean upper", kind: "BOOLEAN", want: "BIT"},
		{name: "boolean trimmed", kind: "  boolean  ", want: "BIT"},

		// Date and timestamp types.
		{name: "date lower", kind: "date", want: "DATE"},
		{name: "date upper", kind: "DATE", want: "DATE"},
		{name: "timestamp lower", kind: "timestamp", want: "DATETIME2"},
		{name: "datetime lower", kind: "datetime", want: "DATETIME2"},
		{name: "timestamptz lower", kind: "timestamptz", want: "DATETIME2"},
		{name: "timestamp mixed case", kind: " TimeStamp ", want: "DATETIME2"},

		// Numeric types.
		{name: "float", kind: "float", want: "DECIMAL(38, 10)"},
		{name: "double", kind: "double", want: "DECIMAL(38, 10)"},
		{name: "numeric", kind: "numeric", want: "DECIMAL(38, 10)"},
		{name: "decimal", kind: "decimal", want: "DECIMAL(38, 10)"},
		{name: "decimal upper", kind: "DECIMAL", want: "DECIMAL(38, 10)"},

		// UUID type.
		{name: "uuid lower", kind: "uuid", want: "UNIQUEIDENTIFIER"},
		{name: "uuid upper", kind: "UUID", want: "UNIQUEIDENTIFIER"},

		// Default string-ish types.
		{name: "empty string", kind: "", want: "NVARCHAR(MAX)"},
		{name: "spaces only", kind: "   ", want: "NVARCHAR(MAX)"},
		{name: "string keyword", kind: "string", want: "NVARCHAR(MAX)"},
		{name: "text keyword", kind: "text", want: "NVARCHAR(MAX)"},
		{name: "unknown type", kind: "foobar", want: "NVARCHAR(MAX)"},
		{name: "already sql type", kind: "VARCHAR(255)", want: "NVARCHAR(MAX)"},
	}

	for _, tt := range tests {
		tt := tt // capture for parallel subtests

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := MapType(tt.kind)
			if got != tt.want {
				t.Fatalf("MapType(%q) = %q, want %q", tt.kind, got, tt.want)
			}
		})
	}
}

// BenchmarkMapTypeKnownTypes measures the performance of MapType when
// called with a set of known logical types that are explicitly mapped.
func BenchmarkMapTypeKnownTypes(b *testing.B) {
	// Use a slice of representative known kinds to exercise the switch cases.
	kinds := []string{
		"int",
		"integer",
		"bigint",
		"bool",
		"boolean",
		"date",
		"timestamp",
		"datetime",
		"timestamptz",
		"float",
		"double",
		"numeric",
		"decimal",
		"uuid",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Use modulo indexing to iterate through the known types without
		// incurring additional allocations.
		kind := kinds[i%len(kinds)]
		_ = MapType(kind)
	}
}

// BenchmarkMapTypeUnknownTypes measures the performance of MapType when
// called with logical types that fall through to the default mapping.
func BenchmarkMapTypeUnknownTypes(b *testing.B) {
	kinds := []string{
		"",
		"  ",
		"string",
		"text",
		"custom_type",
		"VARCHAR(255)",
		"jsonb",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kind := kinds[i%len(kinds)]
		_ = MapType(kind)
	}
}

// BenchmarkMapTypeMixedInputs measures the performance of MapType when
// called with a realistic mix of known and unknown logical types.
func BenchmarkMapTypeMixedInputs(b *testing.B) {
	kinds := []string{
		"int",
		"integer",
		"bigint",
		"bool",
		"boolean",
		"date",
		"timestamp",
		"datetime",
		"timestamptz",
		"float",
		"double",
		"numeric",
		"decimal",
		"uuid",
		"",
		"  ",
		"string",
		"text",
		"unknown",
		"VARCHAR(255)",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kind := kinds[i%len(kinds)]
		_ = MapType(kind)
	}
}
