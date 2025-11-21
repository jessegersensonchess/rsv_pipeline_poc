// etl/internal/storage/mssql/ddl/create.go

// Package ddl provides MSSQL-specific helpers for generating CREATE TABLE
// statements from the generic ddl.TableDef model.
//
// The builder here:
//   - Uses SQL Server-style identifier quoting: [schema].[table], [col].
//   - Wraps CREATE TABLE in an IF OBJECT_ID(...) IS NULL guard since T-SQL
//     does not support CREATE TABLE IF NOT EXISTS.
//   - Treats ColumnDef.Default as raw SQL.
//   - Renders PRIMARY KEY constraints as a separate clause.
package ddl

import (
	"fmt"
	"strings"

	gddl "etl/internal/ddl"
)

// BuildCreateTableSQL returns a T-SQL script that creates a table matching
// the provided definition if it does not already exist.
//
// The generated script has the form:
//
//	IF OBJECT_ID(N'[schema].[table]', N'U') IS NULL
//	BEGIN
//	  CREATE TABLE [schema].[table] (
//	    [col1] TYPE [NOT NULL] [DEFAULT expr],
//	    [col2] TYPE,
//	    PRIMARY KEY ([pk1], [pk2])
//	  );
//	END
//
// The function validates that:
//   - TableDef.FQN is non-empty.
//   - At least one column is present.
//   - Each column has a non-empty Name and SQLType.
//
// It does not attempt to cover all T-SQL features; callers can append
// additional DDL statements if needed.
func BuildCreateTableSQL(t gddl.TableDef) (string, error) {
	fqn := strings.TrimSpace(t.FQN)
	if fqn == "" {
		return "", fmt.Errorf("mssql ddl: table FQN must not be empty")
	}
	if len(t.Columns) == 0 {
		return "", fmt.Errorf("mssql ddl: at least one column is required")
	}

	cols := make([]string, 0, len(t.Columns)+1)
	pks := make([]string, 0, len(t.Columns))

	for _, c := range t.Columns {
		name := strings.TrimSpace(c.Name)
		if name == "" {
			return "", fmt.Errorf("mssql ddl: column with empty name in table %s", fqn)
		}
		typ := strings.TrimSpace(c.SQLType)
		if typ == "" {
			return "", fmt.Errorf("mssql ddl: column %s missing SQLType", name)
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
			// Default is emitted as a raw SQL expression.
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

	fqnQuoted := quoteFQN(fqn)

	// Indent inner CREATE TABLE for readability.
	stmt := fmt.Sprintf(
		"IF OBJECT_ID(N'%s', N'U') IS NULL\nBEGIN\n  CREATE TABLE %s (\n    %s\n  );\nEND;",
		fqnQuoted,
		fqnQuoted,
		strings.Join(cols, ",\n    "),
	)

	return stmt, nil
}

// quoteIdent quotes a single identifier segment for SQL Server using
// bracket syntax, escaping any closing brackets.
//
//	name      -> [name]
//	weird]id  -> [weird]]id]
func quoteIdent(id string) string {
	return "[" + strings.ReplaceAll(id, "]", "]]") + "]"
}

// quoteFQN quotes a possibly schema-qualified table name, e.g.:
//
//	"dbo.Users"   -> [dbo].[Users]
//	"Users"       -> [Users]
//	"a.b.c"       -> [a].[b].[c]
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
