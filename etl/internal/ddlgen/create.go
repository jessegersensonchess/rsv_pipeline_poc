package ddlgen

import (
	"fmt"
	"strings"
)

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

func quoteFQN(f string) string {
	ps := strings.Split(f, ".")
	for i := range ps {
		ps[i] = `"` + strings.ReplaceAll(ps[i], `"`, `""`) + `"`
	}
	return strings.Join(ps, ".")
}
