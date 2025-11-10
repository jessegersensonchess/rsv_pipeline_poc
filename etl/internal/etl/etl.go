// internal/etl/etl.go
package etl

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	sqlstor "etl/internal/storage/sqlite"
	"etl/internal/transformer/builtin"
	"etl/pkg/records"
)

type Field struct {
	Name     string `json:"name"`
	Type     string `json:"type"` // "text","integer","real","date","datetime","bool"
	Required bool   `json:"required"`
}

type Source struct {
	URL       string `json:"url"`
	Delimiter string `json:"delimiter"`
	Name      string `json:"name"` // table
}

type Config struct {
	Source     Source  `json:"source"`
	Fields     []Field `json:"fields"`
	PrimaryKey string  `json:"primary_key"`
	//	MaxRows    int     `json:"max_rows,omitempty"` // 0 = all

	// HeaderMap maps original CSV header -> canonical field name (from probe).
	// Example: { "PČV": "pcv", "Krátký text": "kratky_text" }
	HeaderMap map[string]string `json:"header_map,omitempty"`
}

// MarshalConfig stays for the web UI to download/save the JSON.
func MarshalConfig(cfg Config) ([]byte, error) {
	return json.MarshalIndent(cfg, "", "  ")
}

func Run(ctx context.Context, dbPath string, cfg Config) error {
	if cfg.Source.Name == "" {
		return fmt.Errorf("table name (source.name) required")
	}
	if len(cfg.Fields) == 0 {
		return fmt.Errorf("at least one field required")
	}

	// 1) Fetch CSV (stream)
	req, err := http.NewRequestWithContext(ctx, "GET", cfg.Source.URL, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("fetch csv: %s", resp.Status)
	}

	delim := ','
	if cfg.Source.Delimiter != "" {
		delim = rune(cfg.Source.Delimiter[0])
	}

	r := csv.NewReader(resp.Body)
	r.Comma = delim
	r.FieldsPerRecord = -1 // allow variable fields per row
	r.ReuseRecord = true
	r.LazyQuotes = true       // <-- tolerate unescaped quotes
	r.TrimLeadingSpace = true // <-- normalize leading spaces

	// 2) Header and index map
	header, err := r.Read()
	if err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	// Normalize header using HeaderMap so field names match ETL config.
	// We build an index keyed by the *canonical* name (post-map).
	idx := map[string]int{}
	for i, raw := range header {
		orig := strings.TrimSpace(raw)
		canonical := orig
		if cfg.HeaderMap != nil {
			if mapped, ok := cfg.HeaderMap[orig]; ok && mapped != "" {
				canonical = mapped
			}
		}
		idx[canonical] = i
	}

	// 3) Open SQLite & prepare DDL / insert
	db, err := sqlstor.Open(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	repo := sqlstor.New(db)
	if err := repo.CreateTable(ctx, cfg.Source.Name, toSQLiteDDL(cfg.Fields), cfg.PrimaryKey); err != nil {
		return err
	}

	// 4) Build Coerce transformer config (reuse your transformer)
	// Map UI types -> builtin.Coerce types
	typeMap := map[string]string{}
	for _, f := range cfg.Fields {
		switch strings.ToLower(f.Type) {
		case "integer":
			typeMap[f.Name] = "int"
		case "bool", "boolean":
			typeMap[f.Name] = "bool"
		case "date", "datetime", "timestamp":
			typeMap[f.Name] = "date"
		default:
			typeMap[f.Name] = "string"
		}
	}
	coercer := builtin.Coerce{
		Types: typeMap,
		// Pick a default layout your pipeline already uses; you can pass from UI if needed.
		// Using RFC3339 for datetime/text sources is common; tweak as your data needs.
		Layout: time.RFC3339,
	}

	// 5) Stream rows: csv → records.Record → Coerce.Apply → [][]any → INSERT
	batch := make([][]any, 0, 500)
	insert := func(rows [][]any) error {
		if len(rows) == 0 {
			return nil
		}
		cols := make([]string, len(cfg.Fields))
		for i, f := range cfg.Fields {
			cols[i] = f.Name
		}
		return repo.InsertRows(ctx, cfg.Source.Name, cols, rows)

		//		return repo.InsertRows(ctx, cfg.Source.Name, cfg.Fields, rows)
	}

	var seen int
	maxRows := 2000

	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("csv read: %w", err)
		}
		seen++
		if seen > maxRows {
			break
		}

		// assemble a Record with raw strings (using canonical field names)
		recMap := records.Record{}
		for _, f := range cfg.Fields {
			i, ok := idx[f.Name]
			if ok && i < len(rec) {
				recMap[f.Name] = strings.TrimSpace(rec[i])
			} else {
				recMap[f.Name] = nil
			}
		}

		// coerce in-place
		coercer.Apply([]records.Record{recMap})

		// build ordered row for SQLite insert
		row := make([]any, len(cfg.Fields))
		for j, f := range cfg.Fields {
			v := recMap[f.Name]
			// normalize bools into 0/1 for SQLite INTEGER columns
			if b, ok := v.(bool); ok {
				if b {
					v = 1
				} else {
					v = 0
				}
			}
			// normalize ints to 64-bit
			if s, ok := v.(string); ok && typeMap[f.Name] == "int" && s != "" {
				if i64, err := strconv.ParseInt(s, 10, 64); err == nil {
					v = i64
				} else {
					v = nil
				}
			}
			row[j] = v
		}
		batch = append(batch, row)
		if len(batch) >= 500 {
			if err := insert(batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if err := insert(batch); err != nil {
		return err
	}

	return nil
}

func toSQLiteDDL(fields []Field) []sqlstor.Column {
	out := make([]sqlstor.Column, len(fields))
	for i, f := range fields {
		sqlt := "TEXT"
		switch strings.ToLower(f.Type) {
		case "integer":
			sqlt = "INTEGER"
		case "real", "float", "double":
			sqlt = "REAL"
		case "bool", "boolean":
			sqlt = "INTEGER" // 0/1
		case "date", "datetime", "timestamp":
			sqlt = "TEXT" // store as text (layout handled by transformer)
		}
		out[i] = sqlstor.Column{Name: f.Name, SQLType: sqlt, NotNull: f.Required}
	}
	return out
}

func columnOrder(fields []Field) []string {
	cols := make([]string, len(fields))
	for i, f := range fields {
		cols[i] = f.Name
	}
	return cols
}
