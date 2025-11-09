package ddlgen

import (
	"encoding/json"
	"fmt"
	"strings"

	"etl/internal/config"
	"etl/internal/schema" // only imports schema.Contract JSON structure
)

type ColumnDef struct {
	Name       string
	SQLType    string
	Nullable   bool
	PrimaryKey bool
	Default    string
}

type TableDef struct {
	FQN     string
	Columns []ColumnDef
}

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
