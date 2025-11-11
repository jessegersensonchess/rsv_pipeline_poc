// Package ddl provides helpers to infer and generate SQL DDL from pipeline
// configuration and (optionally) a validation contract. The functions here are
// pure and deterministic, which makes them straightforward to test and reuse.
package ddl

import (
	"encoding/json"
	"fmt"
	"strings"

	"etl/internal/config"
	"etl/internal/schema" // only imports schema.Contract JSON structure
)

// ColumnDef describes a single column in a table definition produced or
// consumed by ddl. It intentionally uses simple, database-agnostic fields.
//
// Fields:
//   - Name: logical column name (unquoted; quoting/escaping happens at render time)
//   - SQLType: target SQL type (e.g., TEXT, BIGINT, TIMESTAMPTZ)
//   - Nullable: whether NULL is allowed
//   - PrimaryKey: whether the column is part of the primary key (not used by all generators)
//   - Default: raw default expression (e.g., 'anon', CURRENT_TIMESTAMP)
type ColumnDef struct {
	Name       string
	SQLType    string
	Nullable   bool
	PrimaryKey bool
	Default    string
}

// TableDef holds the fully-qualified table name (FQN) and an ordered list of
// columns. The FQN is expected in dotted form (e.g., "schema.table") and will
// be quoted/escaped by renderers as needed.
type TableDef struct {
	FQN     string
	Columns []ColumnDef
}

// InferTableDef derives a TableDef from a config.Pipeline. Its goal is to keep
// inference simple and predictable:
//
//   - The table name comes from p.Storage.Postgres.Table. If empty, an error is
//     returned.
//
//   - The column list is taken from p.Storage.Postgres.Columns, in order.
//
//   - If a "validate" transform exists and contains a non-empty JSON
//     "contract", the function uses that contract to determine type and
//     nullability: types are mapped via mapType, and Nullable is set to the
//     inverse of the contract's Field.Required.
//
//   - Otherwise (no contract found), the function looks for any "coerce"
//     transform(s) and merges their declared StringMap("types") overrides,
//     mapping each found type via mapType. In this mode, columns default to
//     Nullable = true.
//
// The function does not attempt to infer primary keys or defaults; callers can
// enrich the returned TableDef if needed.
func InferTableDef(p config.Pipeline) (TableDef, error) {
	table := p.Storage.Postgres.Table
	cols := p.Storage.Postgres.Columns
	if table == "" {
		return TableDef{}, fmt.Errorf("missing table")
	}

	// Find contract if present
	var contract *schema.Contract
	for _, t := range p.Transform {
		if t.Kind == "validate" {
			if raw := t.Options.Any("contract"); raw != nil {
				// Robustly accept arbitrary input by JSON round-tripping into schema.Contract.
				b, _ := json.Marshal(raw)
				var c schema.Contract
				if err := json.Unmarshal(b, &c); err == nil && len(c.Fields) > 0 {
					contract = &c
				}
			}
		}
	}

	defs := make([]ColumnDef, 0, len(cols))
	if contract != nil {
		// Contract-driven inference: build a quick index of fields by name.
		index := map[string]schema.Field{}
		for _, f := range contract.Fields {
			index[f.Name] = f
		}
		for _, name := range cols {
			f := index[name]
			defs = append(defs, ColumnDef{
				Name:       name,
				SQLType:    mapType(f.Type),
				Nullable:   !f.Required,
				PrimaryKey: false,
			})
		}
	} else {
		// Coerce-driven inference (fallback): merge all declared type hints.
		types := map[string]string{}
		for _, t := range p.Transform {
			if t.Kind == "coerce" {
				for k, v := range t.Options.StringMap("types") {
					types[k] = v
				}
			}
		}
		for _, name := range cols {
			defs = append(defs, ColumnDef{
				Name:     name,
				SQLType:  mapType(types[name]),
				Nullable: true,
			})
		}
	}

	return TableDef{FQN: table, Columns: defs}, nil
}

// mapType normalizes a loosely-specified logical type into a target SQL type.
// The mapping is case-insensitive and intentionally conservative:
//
//   - "int", "integer"  -> BIGINT
//   - "bigint"          -> BIGINT
//   - "bool", "boolean" -> BOOLEAN
//   - "date"            -> DATE
//   - "timestamp", "timestamptz" -> TIMESTAMPTZ
//   - anything else     -> TEXT
//
// This function is small and fast, and serves as a single point of control for
// adjusting type policy across the codebase.
func mapType(kind string) string {
	switch strings.ToLower(kind) {
	case "int", "integer":
		//return "INTEGER"
		return "BIGINT"
	case "bigint":
		return "BIGINT"
	case "bool", "boolean":
		return "BOOLEAN"
	case "date":
		return "DATE"
	case "timestamptz", "timestamp":
		return "TIMESTAMPTZ"
	default:
		return "TEXT"
	}
}
