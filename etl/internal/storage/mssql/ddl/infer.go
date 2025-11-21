// Package ddl provides helpers to infer an MSSQL table definition from a
// config.Pipeline and an optional validation contract.
//
// It is backend-specific because it:
//
//   - Reads table/columns from p.Storage.DB.
//   - Uses MSSQL MapType to map logical types into SQL Server types.
package ddl

import (
	"encoding/json"
	"fmt"

	"etl/internal/config"
	gddl "etl/internal/ddl"
	"etl/internal/schema"
)

// FromPipeline derives an MSSQL-oriented TableDef from a config.Pipeline.
//
// Inference rules:
//
//   - Table name is taken from p.Storage.DB.Table; if empty, an error is returned.
//   - Columns are taken from p.Storage.DB.Columns, in order.
//   - If a "validate" transform exists and contains a non-empty "contract",
//     the function:
//   - builds an index of fields by name, and
//   - maps each column's type via MapType(field.Type), and
//   - sets Nullable to !field.Required.
//   - Otherwise, it looks for "coerce" transforms and merges all
//     Options.StringMap("types") hints, mapping each via MapType. In this
//     fallback mode, columns default to Nullable = true.
//   - PrimaryKey and Default are not inferred here; callers may enrich the
//     returned TableDef if desired.
func FromPipeline(p config.Pipeline) (gddl.TableDef, error) {
	table := p.Storage.DB.Table
	cols := p.Storage.DB.Columns
	if table == "" {
		return gddl.TableDef{}, fmt.Errorf("mssql ddl: missing table")
	}

	// Try to locate an inline validation contract, if present.
	var contract *schema.Contract
	for _, t := range p.Transform {
		if t.Kind != "validate" {
			continue
		}
		raw := t.Options.Any("contract")
		if raw == nil {
			continue
		}
		// Round-trip the raw JSON into schema.Contract to be robust to
		// arbitrary shapes under "contract".
		b, err := json.Marshal(raw)
		if err != nil {
			continue
		}
		var c schema.Contract
		if err := json.Unmarshal(b, &c); err == nil && len(c.Fields) > 0 {
			contract = &c
			break
		}
	}

	defs := make([]gddl.ColumnDef, 0, len(cols))

	if contract != nil {
		// Contract-driven inference: build index of fields by name.
		index := make(map[string]schema.Field, len(contract.Fields))
		for _, f := range contract.Fields {
			index[f.Name] = f
		}

		for _, name := range cols {
			f := index[name] // zero value if missing; MapType("") -> NVARCHAR(MAX)
			defs = append(defs, gddl.ColumnDef{
				Name:       name,
				SQLType:    MapType(f.Type),
				Nullable:   !f.Required,
				PrimaryKey: false,
			})
		}
	} else {
		// Coerce-driven inference: collect all declared type hints.
		types := map[string]string{}
		for _, t := range p.Transform {
			if t.Kind != "coerce" {
				continue
			}
			for k, v := range t.Options.StringMap("types") {
				types[k] = v
			}
		}

		for _, name := range cols {
			defs = append(defs, gddl.ColumnDef{
				Name:     name,
				SQLType:  MapType(types[name]),
				Nullable: true,
			})
		}
	}

	return gddl.TableDef{
		FQN:     table,
		Columns: defs,
	}, nil
}
