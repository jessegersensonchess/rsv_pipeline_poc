// Package ddl contains MSSQL-specific helpers for generating DDL.
//
// It maps logical types (from validation/contracts or transforms) into
// SQL Server types. The mapping is intentionally conservative and biased
// toward safe, widely-supported choices.
package ddl

import "strings"

// MapType maps a logical type string into a SQL Server column type.
//
// The input is typically a logical type name such as:
//
//	"int", "integer", "bigint", "bool", "boolean", "date",
//	"timestamp", "datetime", "string", "text"
//
// Unknown or empty kinds fall back to NVARCHAR(MAX).
func MapType(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "int", "integer":
		return "BIGINT"
	case "bigint":
		return "BIGINT"
	case "bool", "boolean":
		return "BIT"
	case "date":
		return "DATE"
	case "timestamp", "datetime", "timestamptz":
		return "DATETIME2"
	case "float", "double", "numeric", "decimal":
		return "DECIMAL(38, 10)"
	case "uuid":
		return "UNIQUEIDENTIFIER"
	default:
		// Default to a flexible Unicode string type.
		return "NVARCHAR(MAX)"
	}
}
