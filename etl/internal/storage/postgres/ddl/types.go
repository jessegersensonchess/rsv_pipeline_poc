// Package ddl contains Postgres-specific helpers for generating DDL.
package ddl

import "strings"

// MapType normalizes a loosely-specified logical type into a Postgres SQL type.
//
//	"int"/"integer"      -> BIGINT
//	"bigint"             -> BIGINT
//	"bool"/"boolean"     -> BOOLEAN
//	"date"               -> DATE
//	"timestamp"/"timestamptz" -> TIMESTAMPTZ
//	everything else      -> TEXT
func MapType(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "int", "integer":
		return "BIGINT"
	case "bigint":
		return "BIGINT"
	case "bool", "boolean":
		return "BOOLEAN"
	case "date":
		return "DATE"
	case "timestamp", "timestamptz":
		return "TIMESTAMPTZ"
	default:
		return "TEXT"
	}
}
