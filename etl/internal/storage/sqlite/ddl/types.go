// Package ddl contains SQLite-specific helpers for generating DDL.
//
// It maps logical types (from validation contracts or coerce transforms) into
// SQLite column types. The mapping is intentionally simple and biased toward
// common, portable choices.
package ddl

import "strings"

// MapType maps a logical type string (e.g., "int", "bool", "date") into a
// SQLite column type.
//
// SQLite supports dynamic typing, so this mapping prefers canonical affinities:
//   - integer-ish types -> INTEGER
//   - boolean          -> INTEGER (0/1)
//   - date/time        -> TEXT (ISO-8601) or NUMERIC (epoch) depending on your
//     conventions; here we use TEXT.
//   - others           -> TEXT
func MapType(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "int", "integer", "bigint":
		return "INTEGER"
	case "bool", "boolean":
		return "INTEGER" // 0/1
	case "float", "double", "real":
		return "REAL"
	case "numeric", "decimal":
		return "NUMERIC"
	case "date", "timestamp", "datetime", "timestamptz":
		return "TEXT" // store ISO-8601 strings
	case "blob", "bytes":
		return "BLOB"
	default:
		return "TEXT"
	}
}
