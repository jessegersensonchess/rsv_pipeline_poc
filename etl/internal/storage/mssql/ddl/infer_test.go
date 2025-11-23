package ddl

import (
	"context"
	"testing"

	"etl/internal/config"
	gddl "etl/internal/ddl"
	"etl/internal/schema"
)

// TestFromPipelineMissingTable verifies that FromPipeline fails when the
// target table name is missing.
func TestFromPipelineMissingTable(t *testing.T) {
	t.Parallel()

	var p config.Pipeline // zero value; Storage.DB.Table == ""

	got, err := FromPipeline(p)
	if err == nil {
		t.Fatalf("FromPipeline() error = nil, want non-nil for missing table")
	}
	if got.FQN != "" {
		t.Fatalf("FromPipeline().FQN = %q, want empty on error", got.FQN)
	}
	if len(got.Columns) != 0 {
		t.Fatalf("FromPipeline().Columns length = %d, want 0 on error", len(got.Columns))
	}
}

// TestFromPipelineContractDriven verifies that FromPipeline uses a validation
// contract when present to infer SQL types and nullability.
func TestFromPipelineContractDriven(t *testing.T) {
	t.Parallel()

	contract := schema.Contract{
		Fields: []schema.Field{
			{Name: "id", Type: "bigint", Required: true},
			{Name: "name", Type: "string", Required: false},
			{Name: "age", Type: "int", Required: false},
		},
	}

	var p config.Pipeline
	p.Storage.DB.Table = "dbo.Users"
	p.Storage.DB.Columns = []string{"id", "name", "age", "unknown_field"}
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

	if got.FQN != "dbo.Users" {
		t.Fatalf("FromPipeline().FQN = %q, want %q", got.FQN, "dbo.Users")
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
		{idx: 0, name: "id", wantType: MapType("bigint"), wantNull: false},
		{idx: 1, name: "name", wantType: MapType("string"), wantNull: true},
		{idx: 2, name: "age", wantType: MapType("int"), wantNull: true},
		// Unknown field: zero-value schema.Field => Type="", Required=false.
		{idx: 3, name: "unknown_field", wantType: MapType(""), wantNull: true},
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

// TestFromPipelineContractInvalid ensures that if the validate transform's
// contract cannot be decoded, FromPipeline falls back to coerce-driven
// inference (or default mappings) instead of failing.
func TestFromPipelineContractInvalid(t *testing.T) {
	t.Parallel()

	// Raw value that is unlikely to decode into schema.Contract.
	rawContract := func(ctx context.Context) {}

	var p config.Pipeline
	p.Storage.DB.Table = "Logs"
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

	if got.FQN != "Logs" {
		t.Fatalf("FromPipeline().FQN = %q, want %q", got.FQN, "Logs")
	}
	if len(got.Columns) != 2 {
		t.Fatalf("FromPipeline().Columns length = %d, want 2", len(got.Columns))
	}
	for i, col := range got.Columns {
		if col.Name == "" {
			t.Errorf("column[%d].Name is empty, want non-empty", i)
		}
		// With no valid contract and no coerce transforms, MapType("") is used
		// and Nullable defaults to true (coerce mode).
		if col.SQLType != MapType("") {
			t.Errorf("column[%d].SQLType = %q, want %q", i, col.SQLType, MapType(""))
		}
		if !col.Nullable {
			t.Errorf("column[%d].Nullable = false, want true", i)
		}
	}
}

// TestFromPipelineCoerceDriven verifies that FromPipeline uses coerce transforms
// to infer types when no validation contract is present, and that later coerce
// transforms override earlier type hints.
func TestFromPipelineCoerceDriven(t *testing.T) {
	t.Parallel()

	var p config.Pipeline
	p.Storage.DB.Table = "dbo.Events"
	p.Storage.DB.Columns = []string{"id", "kind", "payload"}
	p.Transform = []config.Transform{
		{
			Kind: "coerce",
			Options: config.Options{
				// Must be map[string]any so Options.StringMap("types") sees it.
				"types": map[string]any{
					"id":      "bigint",
					"kind":    "string",
					"payload": "text",
				},
			},
		},
		// Second coerce transform overrides one of the types.
		{
			Kind: "coerce",
			Options: config.Options{
				"types": map[string]any{
					"kind": "uuid",
				},
			},
		},
	}

	got, err := FromPipeline(p)
	if err != nil {
		t.Fatalf("FromPipeline() error = %v", err)
	}

	if got.FQN != "dbo.Events" {
		t.Fatalf("FromPipeline().FQN = %q, want %q", got.FQN, "dbo.Events")
	}

	want := []gddl.ColumnDef{
		{Name: "id", SQLType: MapType("bigint"), Nullable: true},
		{Name: "kind", SQLType: MapType("uuid"), Nullable: true},
		{Name: "payload", SQLType: MapType("text"), Nullable: true},
	}

	if len(got.Columns) != len(want) {
		t.Fatalf("FromPipeline().Columns length = %d, want %d", len(got.Columns), len(want))
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
		if got.Columns[i].PrimaryKey {
			t.Errorf("column[%d].PrimaryKey = true, want false (not inferred)", i)
		}
	}
}

// TestFromPipelineDeterministicColumnOrder verifies that FromPipeline preserves
// the column order defined in the pipeline configuration.
func TestFromPipelineDeterministicColumnOrder(t *testing.T) {
	t.Parallel()

	cols := []string{"c3", "c1", "c2"}

	var p config.Pipeline
	p.Storage.DB.Table = "dbo.Test"
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

// BenchmarkFromPipelineContract measures the cost of contract-driven inference
// when FromPipeline is provided with a validation contract.
func BenchmarkFromPipelineContract(b *testing.B) {
	contract := schema.Contract{
		Fields: []schema.Field{
			{Name: "id", Type: "bigint", Required: true},
			{Name: "name", Type: "string", Required: false},
			{Name: "created_at", Type: "timestamp", Required: true},
		},
	}

	var p config.Pipeline
	p.Storage.DB.Table = "dbo.Users"
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

// BenchmarkFromPipelineCoerce measures the cost of coerce-driven inference
// when FromPipeline is provided with type hints from coerce transforms.
func BenchmarkFromPipelineCoerce(b *testing.B) {
	var p config.Pipeline
	p.Storage.DB.Table = "dbo.Events"
	p.Storage.DB.Columns = []string{"id", "kind", "payload", "created_at"}
	p.Transform = []config.Transform{
		{
			Kind: "coerce",
			Options: config.Options{
				"types": map[string]any{
					"id":         "bigint",
					"kind":       "string",
					"payload":    "text",
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
