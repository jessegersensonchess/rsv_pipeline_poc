// Package ddl provides SQLite-specific helpers for generating CREATE TABLE
// statements from the generic ddl.TableDef model.
//
// The builder here:
//   - Uses simple double-quoted identifiers: "table", "col".
//   - Emits CREATE TABLE IF NOT EXISTS.
//   - Treats ColumnDef.Default as raw SQL.
//   - Renders PRIMARY KEY as a separate table constraint.
package ddl

import (
	"fmt"
	"strings"

	gddl "etl/internal/ddl"
)

// BuildCreateTableSQL returns a SQLite CREATE TABLE statement for the given
// table definition. The statement has the form:
//
//	CREATE TABLE IF NOT EXISTS "table" (
//	  "col1" TYPE [NOT NULL] [DEFAULT expr],
//	  "col2" TYPE,
//	  PRIMARY KEY ("pk1", "pk2")
//	);
//
// TableDef.FQN is interpreted as a table name; if it contains dots (e.g.,
// "main.events"), each segment is individually quoted.
func BuildCreateTableSQL(t gddl.TableDef) (string, error) {
	fqn := strings.TrimSpace(t.FQN)
	if fqn == "" {
		return "", fmt.Errorf("sqlite ddl: table FQN must not be empty")
	}
	if len(t.Columns) == 0 {
		return "", fmt.Errorf("sqlite ddl: at least one column is required")
	}

	cols := make([]string, 0, len(t.Columns)+1)
	pks := make([]string, 0, len(t.Columns))

	for _, c := range t.Columns {
		name := strings.TrimSpace(c.Name)
		if name == "" {
			return "", fmt.Errorf("sqlite ddl: column with empty name in table %s", fqn)
		}
		typ := strings.TrimSpace(c.SQLType)
		if typ == "" {
			return "", fmt.Errorf("sqlite ddl: column %s missing SQLType", name)
		}

		var sb strings.Builder
		sb.WriteString(quoteIdent(name))
		sb.WriteByte(' ')
		sb.WriteString(typ)

		if !c.Nullable {
			sb.WriteString(" NOT NULL")
		}

		if def := strings.TrimSpace(c.Default); def != "" {
			sb.WriteString(" DEFAULT ")
			sb.WriteString(def)
		}

		cols = append(cols, sb.String())

		if c.PrimaryKey {
			pks = append(pks, quoteIdent(name))
		}
	}

	if len(pks) > 0 {
		cols = append(cols,
			fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(pks, ", ")),
		)
	}

	stmt := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (\n  %s\n);",
		quoteFQN(fqn),
		strings.Join(cols, ",\n  "),
	)
	return stmt, nil
}

func quoteIdent(id string) string {
	return `"` + strings.ReplaceAll(id, `"`, `""`) + `"`
}

func quoteFQN(fqn string) string {
	parts := strings.Split(fqn, ".")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, quoteIdent(p))
	}
	return strings.Join(out, ".")
}
