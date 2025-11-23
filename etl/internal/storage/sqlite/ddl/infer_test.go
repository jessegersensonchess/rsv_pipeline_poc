package ddl

import (
	"context"
	"strings"
	"testing"

	"etl/internal/config"
	gddl "etl/internal/ddl"
	"etl/internal/schema"
)

// TestFromPipelineMissingTable verifies that FromPipeline fails when the
// storage table is missing.
func TestFromPipelineMissingTable(t *testing.T) {
	t.Parallel()

	var p config.Pipeline
	// Table and Columns default to zero values.

	got, err := FromPipeline(p)
	if err == nil {
		t.Fatalf("FromPipeline() error = nil, want non-nil for missing table")
	}
	if !strings.Contains(err.Error(), "sqlite ddl: missing table") {
		t.Fatalf("FromPipeline() error = %q, want containing %q", err.Error(), "sqlite ddl: missing table")
	}
	if got.FQN != "" || len(got.Columns) != 0 {
		t.Fatalf("FromPipeline() result not empty on error: %+v", got)
	}
}

// TestFromPipelineContractDriven verifies that when a validation contract is
// present, FromPipeline uses it to drive types and nullability.
func TestFromPipelineContractDriven(t *testing.T) {
	t.Parallel()

	contract := schema.Contract{
		Fields: []schema.Field{
			{Name: "id", Type: "int", Required: true},
			{Name: "name", Type: "string", Required: false},
			{Name: "meta", Type: "blob", Required: false},
		},
	}

	var p config.Pipeline
	p.Storage.DB.Table = "events"
	p.Storage.DB.Columns = []string{"id", "name", "meta", "unknown"}
	p.Transform = []config.Transform{
		{
			Kind: "validate",
			Options: config.Options{
				"contract": contract,
			},
		},
	}

	got, err := FromPipeline(p)
	if err != nil {
		t.Fatalf("FromPipeline() error = %v", err)
	}

	if got.FQN != "events" {
		t.Fatalf("FromPipeline().FQN = %q, want %q", got.FQN, "events")
	}
	if len(got.Columns) != 4 {
		t.Fatalf("FromPipeline().Columns length = %d, want 4", len(got.Columns))
	}

	tests := []struct {
		idx      int
		name     string
		wantType string
		wantNull bool
	}{
		{idx: 0, name: "id", wantType: MapType("int"), wantNull: false},
		{idx: 1, name: "name", wantType: MapType("string"), wantNull: true},
		{idx: 2, name: "meta", wantType: MapType("blob"), wantNull: true},
		// Unknown field: zero-value Field => Type="", Required=false => Nullable = true.
		{idx: 3, name: "unknown", wantType: MapType(""), wantNull: true},
	}

	for _, tt := range tests {
		col := got.Columns[tt.idx]
		if col.Name != tt.name {
			t.Errorf("column[%d].Name = %q, want %q", tt.idx, col.Name, tt.name)
		}
		if col.SQLType != tt.wantType {
			t.Errorf("column[%d].SQLType = %q, want %q", tt.idx, col.SQLType, tt.wantType)
		}
		if col.Nullable != tt.wantNull {
			t.Errorf("column[%d].Nullable = %v, want %v", tt.idx, col.Nullable, tt.wantNull)
		}
		if col.PrimaryKey {
			t.Errorf("column[%d].PrimaryKey = true, want false (not inferred here)", tt.idx)
		}
	}
}

// TestFromPipelineContractInvalid ensures that if the contract cannot be
// decoded, FromPipeline falls back to coerce/default behavior instead of
// failing.
func TestFromPipelineContractInvalid(t *testing.T) {
	t.Parallel()

	rawContract := func(ctx context.Context) {}

	var p config.Pipeline
	p.Storage.DB.Table = "logs"
	p.Storage.DB.Columns = []string{"id", "payload"}
	p.Transform = []config.Transform{
		{
			Kind: "validate",
			Options: config.Options{
				"contract": rawContract,
			},
		},
	}

	got, err := FromPipeline(p)
	if err != nil {
		t.Fatalf("FromPipeline() error = %v", err)
	}

	if got.FQN != "logs" {
		t.Fatalf("FromPipeline().FQN = %q, want %q", got.FQN, "logs")
	}
	if len(got.Columns) != 2 {
		t.Fatalf("FromPipeline().Columns length = %d, want 2", len(got.Columns))
	}
	for i, col := range got.Columns {
		if col.Name == "" {
			t.Errorf("column[%d].Name is empty, want non-empty", i)
		}
		if col.SQLType != MapType("") {
			t.Errorf("column[%d].SQLType = %q, want %q", i, col.SQLType, MapType(""))
		}
		if !col.Nullable {
			t.Errorf("column[%d].Nullable = false, want true", i)
		}
	}
}

// TestFromPipelineCoerceDriven verifies that when no contract is present,
// FromPipeline uses coerce transforms and their type hints.
func TestFromPipelineCoerceDriven(t *testing.T) {
	t.Parallel()

	var p config.Pipeline
	p.Storage.DB.Table = "events"
	p.Storage.DB.Columns = []string{"id", "kind", "payload"}
	p.Transform = []config.Transform{
		{
			Kind: "coerce",
			Options: config.Options{
				"types": map[string]any{
					"id":      "int",
					"kind":    "string",
					"payload": "blob",
				},
			},
		},
		{
			Kind: "coerce",
			Options: config.Options{
				"types": map[string]any{
					"kind": "bool",
				},
			},
		},
	}

	got, err := FromPipeline(p)
	if err != nil {
		t.Fatalf("FromPipeline() error = %v", err)
	}

	if got.FQN != "events" {
		t.Fatalf("FromPipeline().FQN = %q, want %q", got.FQN, "events")
	}
	if len(got.Columns) != 3 {
		t.Fatalf("FromPipeline().Columns length = %d, want 3", len(got.Columns))
	}

	want := []gddl.ColumnDef{
		{Name: "id", SQLType: MapType("int"), Nullable: true},
		{Name: "kind", SQLType: MapType("bool"), Nullable: true},
		{Name: "payload", SQLType: MapType("blob"), Nullable: true},
	}

	for i := range want {
		if got.Columns[i].Name != want[i].Name {
			t.Errorf("column[%d].Name = %q, want %q", i, got.Columns[i].Name, want[i].Name)
		}
		if got.Columns[i].SQLType != want[i].SQLType {
			t.Errorf("column[%d].SQLType = %q, want %q", i, got.Columns[i].SQLType, want[i].SQLType)
		}
		if got.Columns[i].Nullable != want[i].Nullable {
			t.Errorf("column[%d].Nullable = %v, want %v", i, got.Columns[i].Nullable, want[i].Nullable)
		}
	}
}

// TestFromPipelineColumnOrder verifies that FromPipeline preserves the column
// order defined in the pipeline configuration.
func TestFromPipelineColumnOrder(t *testing.T) {
	t.Parallel()

	cols := []string{"c3", "c1", "c2"}

	var p config.Pipeline
	p.Storage.DB.Table = "test"
	p.Storage.DB.Columns = cols

	got, err := FromPipeline(p)
	if err != nil {
		t.Fatalf("FromPipeline() error = %v", err)
	}

	if len(got.Columns) != len(cols) {
		t.Fatalf("FromPipeline().Columns length = %d, want %d", len(got.Columns), len(cols))
	}
	for i, name := range cols {
		if got.Columns[i].Name != name {
			t.Errorf("column[%d].Name = %q, want %q", i, got.Columns[i].Name, name)
		}
	}
}

// BenchmarkFromPipelineContract measures the cost of contract-driven inference.
func BenchmarkFromPipelineContract(b *testing.B) {
	contract := schema.Contract{
		Fields: []schema.Field{
			{Name: "id", Type: "int", Required: true},
			{Name: "name", Type: "string", Required: false},
			{Name: "created_at", Type: "timestamp", Required: true},
		},
	}

	var p config.Pipeline
	p.Storage.DB.Table = "events"
	p.Storage.DB.Columns = []string{"id", "name", "created_at"}
	p.Transform = []config.Transform{
		{
			Kind: "validate",
			Options: config.Options{
				"contract": contract,
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := FromPipeline(p); err != nil {
			b.Fatalf("FromPipeline() error = %v", err)
		}
	}
}

// BenchmarkFromPipelineCoerce measures the cost of coerce-driven inference.
func BenchmarkFromPipelineCoerce(b *testing.B) {
	var p config.Pipeline
	p.Storage.DB.Table = "events"
	p.Storage.DB.Columns = []string{"id", "kind", "payload", "created_at"}
	p.Transform = []config.Transform{
		{
			Kind: "coerce",
			Options: config.Options{
				"types": map[string]any{
					"id":         "int",
					"kind":       "string",
					"payload":    "blob",
					"created_at": "timestamp",
				},
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := FromPipeline(p); err != nil {
			b.Fatalf("FromPipeline() error = %v", err)
		}
	}
}
