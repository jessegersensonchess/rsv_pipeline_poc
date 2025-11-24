// etc/cmd/xmlprobe/main_test.go
package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"etl/internal/config"
	xmlparser "etl/internal/parser/xml"
	"etl/internal/storage"
)

// writeTempFile is a helper that writes content to a temporary file and returns its path.
func writeTempFile(t *testing.T, dir, prefix string, content []byte) string {
	t.Helper()

	if dir == "" {
		dir = t.TempDir()
	}
	f, err := os.CreateTemp(dir, prefix)
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	if _, err := f.Write(content); err != nil {
		f.Close()
		t.Fatalf("write temp file: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close temp file: %v", err)
	}
	return f.Name()
}

// TestReadStorageConfigAsPipeline verifies that readStorageConfigAsPipeline correctly
// interprets the storage block from an xmlprobe config, populating both a
// storage.Config for runtime use and a config.Pipeline for DDL inference.
func TestReadStorageConfigAsPipeline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		storageJSON string
		wantKind    string
		wantSC      storage.Config
		wantSpec    config.Pipeline
		wantErr     bool
	}{
		{
			name: "basic lowercase kind",
			storageJSON: `{
				"kind": "postgres",
				"dsn": "postgres://user:pass@host/db",
				"table": "public.events",
				"columns": ["id", "name"],
				"key_columns": ["id"],
				"date_column": "created_at",
				"auto_create_table": true
			}`,
			wantKind: "postgres",
			wantSC: storage.Config{
				Kind:       "postgres",
				DSN:        "postgres://user:pass@host/db",
				Table:      "public.events",
				Columns:    []string{"id", "name"},
				KeyColumns: []string{"id"},
				DateColumn: "created_at",
			},
			wantSpec: func() config.Pipeline {
				var p config.Pipeline
				p.Storage.Kind = "postgres"
				p.Storage.DB.DSN = "postgres://user:pass@host/db"
				p.Storage.DB.Table = "public.events"
				p.Storage.DB.Columns = []string{"id", "name"}
				p.Storage.DB.KeyColumns = []string{"id"}
				p.Storage.DB.DateColumn = "created_at"
				p.Storage.DB.AutoCreateTable = true
				return p
			}(),
		},
		{
			name: "capitalized Kind compatibility",
			storageJSON: `{
				"Kind": "sqlite",
				"dsn": "file:etl.db?cache=shared&_fk=1",
				"table": "events",
				"columns": ["id", "payload"],
				"key_columns": [],
				"date_column": "",
				"auto_create_table": false
			}`,
			wantKind: "sqlite",
			wantSC: storage.Config{
				Kind:       "sqlite",
				DSN:        "file:etl.db?cache=shared&_fk=1",
				Table:      "events",
				Columns:    []string{"id", "payload"},
				KeyColumns: []string{},
				DateColumn: "",
			},
			wantSpec: func() config.Pipeline {
				var p config.Pipeline
				p.Storage.Kind = "sqlite"
				p.Storage.DB.DSN = "file:etl.db?cache=shared&_fk=1"
				p.Storage.DB.Table = "events"
				p.Storage.DB.Columns = []string{"id", "payload"}
				p.Storage.DB.KeyColumns = []string{}
				p.Storage.DB.DateColumn = ""
				p.Storage.DB.AutoCreateTable = false
				return p
			}(),
		},
		{
			name:        "missing storage block returns error",
			storageJSON: ``,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		tt := tt // capture
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Build the full JSON config wrapper with optional storage block.
			wrapper := map[string]any{}
			if tt.storageJSON != "" {
				var raw json.RawMessage
				if err := json.Unmarshal([]byte(tt.storageJSON), &raw); err != nil {
					t.Fatalf("unmarshal storageJSON into RawMessage: %v", err)
				}
				wrapper["storage"] = raw
			}
			b, err := json.Marshal(wrapper)
			if err != nil {
				t.Fatalf("marshal wrapper: %v", err)
			}

			path := writeTempFile(t, "", "xmlprobe-config-*.json", b)

			gotSC, gotSpec, err := readStorageConfigAsPipeline(path)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("readStorageConfigAsPipeline(%s) error = nil, want non-nil", filepath.Base(path))
				}
				return
			}
			if err != nil {
				t.Fatalf("readStorageConfigAsPipeline(%s) error = %v", filepath.Base(path), err)
			}

			if gotSC.Kind != tt.wantKind {
				t.Fatalf("storage.Config.Kind = %q, want %q", gotSC.Kind, tt.wantKind)
			}
			if !reflect.DeepEqual(gotSC, tt.wantSC) {
				t.Fatalf("storage.Config mismatch.\n got: %#v\nwant: %#v", gotSC, tt.wantSC)
			}
			if !reflect.DeepEqual(gotSpec, tt.wantSpec) {
				t.Fatalf("config.Pipeline mismatch.\n got: %#v\nwant: %#v", gotSpec, tt.wantSpec)
			}
		})
	}
}

// TestInferColumnsFromConfig verifies that inferColumnsFromConfig builds a
// deterministic, sorted list of column names from XML config Fields and Lists.
//
// It constructs the xmlparser.Config via JSON parsing, but uses string-valued
// fields/lists because Config.Fields/Lists are maps of string values, and we
// only care about the keys.
func TestInferColumnsFromConfig(t *testing.T) {
	t.Parallel()

	// Minimal JSON that exercises Fields and Lists keys with string values.
	const cfgJSON = `{
		"record_tag": "X",
		"fields": {
			"z_field": "foo",
			"a_field": "bar"
		},
		"lists": {
			"list_b": "1",
			"list_a": "2"
		}
	}`

	cfg, err := xmlparser.ParseConfigJSON([]byte(cfgJSON))
	if err != nil {
		t.Fatalf("ParseConfigJSON: %v", err)
	}

	got := inferColumnsFromConfig(cfg)
	want := []string{"a_field", "list_a", "list_b", "z_field"}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("inferColumnsFromConfig() = %v, want %v", got, want)
	}
}

// TestRecordToRow verifies that recordToRow maps XML records into row slices
// in the expected column order and preserves nil for missing keys.
func TestRecordToRow(t *testing.T) {
	t.Parallel()

	columns := []string{"id", "name", "missing"}
	rec := xmlparser.Record{
		"id":   123,
		"name": "alice",
		// "missing" intentionally absent
	}

	got := recordToRow(rec, columns)
	if len(got) != len(columns) {
		t.Fatalf("recordToRow length = %d, want %d", len(got), len(columns))
	}

	if got[0] != 123 {
		t.Fatalf("row[0] = %v, want %v", got[0], 123)
	}
	if got[1] != "alice" {
		t.Fatalf("row[1] = %v, want %v", got[1], "alice")
	}
	if got[2] != nil {
		t.Fatalf("row[2] (missing column) = %v, want nil", got[2])
	}
}
