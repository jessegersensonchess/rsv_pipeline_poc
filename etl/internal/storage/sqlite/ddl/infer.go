// Package ddl provides helpers to infer a SQLite table definition from a
// config.Pipeline and an optional validation contract. It is backend-specific
// because it maps logical types using SQLite MapType.
package ddl

import (
	"encoding/json"
	"fmt"

	"etl/internal/config"
	gddl "etl/internal/ddl"
	"etl/internal/schema"
)

// FromPipeline derives a SQLite-oriented TableDef from a config.Pipeline.
//
// Rules:
//   - Table name comes from p.Storage.DB.Table.
//   - Columns come from p.Storage.DB.Columns, in order.
//   - If a "validate" transform with a non-empty "contract" exists, use that
//     contract to drive type + nullability via MapType and Field.Required.
//   - Otherwise, fall back to "coerce" transforms and their StringMap("types")
//     hints, mapping each via MapType with Nullable = true.
func FromPipeline(p config.Pipeline) (gddl.TableDef, error) {
	table := p.Storage.DB.Table
	cols := p.Storage.DB.Columns
	if table == "" {
		return gddl.TableDef{}, fmt.Errorf("sqlite ddl: missing table")
	}

	// Try to locate an inline validation contract.
	var contract *schema.Contract
	for _, t := range p.Transform {
		if t.Kind != "validate" {
			continue
		}
		raw := t.Options.Any("contract")
		if raw == nil {
			continue
		}
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
		index := make(map[string]schema.Field, len(contract.Fields))
		for _, f := range contract.Fields {
			index[f.Name] = f
		}
		for _, name := range cols {
			f := index[name]
			defs = append(defs, gddl.ColumnDef{
				Name:       name,
				SQLType:    MapType(f.Type),
				Nullable:   !f.Required,
				PrimaryKey: false,
			})
		}
	} else {
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
