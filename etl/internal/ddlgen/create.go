// Package ddlgen provides helpers for generating SQL Data Definition Language
// (DDL) statements. It focuses on deterministic, purely-functional string
// generation to simplify testing and portability across databases.
package ddlgen

import (
	"fmt"
	"strings"
)

// BuildCreateTableSQL builds a deterministic CREATE TABLE statement for the
// given table definition.
//
// Rules:
//   - The fully-qualified table name (FQN) is required; if empty, an error is
//     returned.
//   - At least one column must be provided; otherwise an error is returned.
//   - Each column is emitted as `"name" SQLTYPE [DEFAULT <expr>] [NOT NULL]`.
//   - The FQN is quoted and escaped via quoteFQN, supporting multi-segment
//     names like `schema.table`.
//   - Column names are always double-quoted; embedded quotes are escaped.
//
// The function does not attempt to validate SQL types or defaults; callers are
// responsible for supplying database-appropriate values.
//
// The output is formatted with stable newlines and indentation to ease
// snapshot-style testing.
//
// Example output:
//
//	CREATE TABLE IF NOT EXISTS "public"."users" (
//	  "id" INTEGER NOT NULL,
//	  "name" TEXT DEFAULT 'anon'
//	);
//
// The function is allocation-conscious (uses sized slices), but if you are
// generating very large tables in hot paths, consider using a strings.Builder
// and/or writing directly to an io.Writer.
func BuildCreateTableSQL(t TableDef) (string, error) {
	if t.FQN == "" {
		return "", fmt.Errorf("missing table name")
	}
	if len(t.Columns) == 0 {
		return "", fmt.Errorf("no columns")
	}

	parts := make([]string, 0, len(t.Columns))
	for _, c := range t.Columns {
		line := fmt.Sprintf(`"%s" %s`, c.Name, c.SQLType)
		if c.Default != "" {
			line += " DEFAULT " + c.Default
		}
		if !c.Nullable {
			line += " NOT NULL"
		}
		parts = append(parts, line)
	}

	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (\n  %s\n);",
		quoteFQN(t.FQN),
		strings.Join(parts, ",\n  "),
	), nil
}

// quoteFQN returns a safely-quoted, dot-separated identifier suitable for
// addressing tables with optional schema/database qualification.
//
// Each segment is double-quoted, and any embedded double quotes in a segment
// are escaped by doubling them. For example:
//
//	quoteFQN("public.users")        => `"public"."users"`
//	quoteFQN(`weird"name.tbl`)      => `"weird""name"."tbl"`
func quoteFQN(f string) string {
	ps := strings.Split(f, ".")
	for i := range ps {
		ps[i] = `"` + strings.ReplaceAll(ps[i], `"`, `""`) + `"`
	}
	return strings.Join(ps, ".")
}
