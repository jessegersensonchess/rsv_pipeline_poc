// Package ddl contains Postgres-specific helpers for generating DDL.
//
// It builds CREATE TABLE statements for a generic ddl.TableDef, using
// Postgres-style quoting (double-quoted identifiers, escaped quotes, etc.).
package postgres

import (
	"fmt"
	"sort"
	"strings"

	gddl "etl/internal/ddl" // generic DDL model: TableDef, ColumnDef
)

// BuildCreateTableSQL builds a deterministic Postgres CREATE TABLE statement
// for the given table definition.
//
// Rules (same semantics as your existing builder):
//   - t.FQN (fully-qualified table name) must be non-empty.
//   - Each column must have a non-empty Name and SQLType.
//   - Primary-key columns are always rendered as NOT NULL, even if Nullable=true.
//   - PRIMARY KEY is rendered as a separate constraint clause using quoted
//     column names, sorted alphabetically for determinism.
//   - Identifiers are double-quoted; embedded double-quotes are escaped.
//   - The statement uses CREATE TABLE IF NOT EXISTS.
func BuildCreateTableSQL(t gddl.TableDef) (string, error) {
	fqn := strings.TrimSpace(t.FQN)
	if fqn == "" {
		return "", fmt.Errorf("postgres ddl: table FQN must not be empty")
	}
	if len(t.Columns) == 0 {
		return "", fmt.Errorf("postgres ddl: at least one column is required")
	}

	cols := make([]string, 0, len(t.Columns)+1)
	pks := make([]string, 0, len(t.Columns))

	for _, c := range t.Columns {
		name := strings.TrimSpace(c.Name)
		if name == "" {
			return "", fmt.Errorf("postgres ddl: column with empty name in table %s", fqn)
		}
		typ := strings.TrimSpace(c.SQLType)
		if typ == "" {
			return "", fmt.Errorf("postgres ddl: column %s missing SQLType", name)
		}

		// "colname" TYPE [NOT NULL] [DEFAULT expr]
		var sb strings.Builder
		sb.WriteString(quoteIdent(name))
		sb.WriteByte(' ')
		sb.WriteString(typ)

		// Primary keys are always NOT NULL, even if Nullable=true.
		notNull := !c.Nullable || c.PrimaryKey
		if notNull {
			sb.WriteString(" NOT NULL")
		}

		if def := strings.TrimSpace(c.Default); def != "" {
			sb.WriteString(" DEFAULT ")
			// default is raw SQL, no quoting here
			sb.WriteString(def)
		}

		cols = append(cols, sb.String())

		if c.PrimaryKey {
			pks = append(pks, quoteIdent(name))
		}
	}

	if len(pks) > 0 {
		sort.Strings(pks)
		cols = append(cols,
			fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(pks, ", ")),
		)
	}

	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (\n  %s\n);",
		quoteFQN(fqn),
		strings.Join(cols, ",\n  "),
	), nil
}

// quoteIdent quotes a single identifier segment for Postgres, e.g.:
//
//	quoteIdent(`pcv`)        => `"pcv"`
//	quoteIdent(`weird"name`) => `"weird""name"`
func quoteIdent(id string) string {
	return `"` + strings.ReplaceAll(id, `"`, `""`) + `"`
}

// quoteFQN quotes a possibly schema-qualified name like "public.users" to
// `"public"."users"`. Empty segments are ignored.
func quoteFQN(f string) string {
	parts := strings.Split(f, ".")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p == "" {
			continue
		}
		out = append(out, quoteIdent(p))
	}
	return strings.Join(out, ".")
}
