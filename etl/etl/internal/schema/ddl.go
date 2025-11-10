package schema

import (
	"fmt"
	"sort"
	"strings"
)

// ColumnDef is a minimal description of a DB column.
type ColumnDef struct {
	Name       string // e.g., "pcv"
	SQLType    string // e.g., "INTEGER", "DATE", "TEXT", "BOOLEAN", "BIGINT"
	Nullable   bool
	PrimaryKey bool   // optional
	Default    string // raw SQL default, e.g., "generated always as identity", "now()"
}

// TableDef describes a table to (optionally) create.
type TableDef struct {
	FQN     string // e.g., "public.technicke_prohlidky"
	Columns []ColumnDef
}

// BuildCreateTableSQL emits a CREATE TABLE IF NOT EXISTS for Postgres.
func BuildCreateTableSQL(t TableDef) (string, error) {
	if t.FQN == "" {
		return "", fmt.Errorf("table name required")
	}
	if len(t.Columns) == 0 {
		return "", fmt.Errorf("at least one column required")
	}
	var cols []string
	var pks []string
	for _, c := range t.Columns {
		if c.Name == "" || c.SQLType == "" {
			return "", fmt.Errorf("column name and type required")
		}
		def := fmt.Sprintf(`"%s" %s`, c.Name, c.SQLType)
		if c.Default != "" {
			def += " DEFAULT " + c.Default
		}
		if !c.Nullable {
			def += " NOT NULL"
		}
		cols = append(cols, def)
		if c.PrimaryKey {
			pks = append(pks, `"`+c.Name+`"`)
		}
	}
	if len(pks) > 0 {
		sort.Strings(pks)
		cols = append(cols, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(pks, ",")))
	}
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n  %s\n);",
		quoteFQN(t.FQN), strings.Join(cols, ",\n  ")), nil
}

func quoteFQN(fqn string) string {
	parts := strings.Split(fqn, ".")
	for i := range parts {
		parts[i] = `"` + strings.ReplaceAll(parts[i], `"`, `""`) + `"`
	}
	return strings.Join(parts, ".")
}
