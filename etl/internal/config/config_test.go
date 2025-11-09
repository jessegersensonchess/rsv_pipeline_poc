package config

import (
	"encoding/json"
	"reflect"
	"testing"
	"unicode/utf8"
)

// -----------------------------------------------------------------------------
// Pipeline decoding tests
// -----------------------------------------------------------------------------
//
// These tests validate that the top-level Pipeline JSON structure decodes into
// the intended Go struct graph. The goal is to ensure the JSON schema used in
// pipeline files (configs/pipelines/*.json) maps cleanly to the Go types.
// We prefer parsing from JSON strings here to keep tests hermetic and focused
// on the API surface rather than filesystem wiring.

func TestPipeline_DecodeRoundTrip(t *testing.T) {
	t.Parallel()

	const js = `{
	  "source": { "kind": "file", "file": { "path": "testdata/rs.csv" } },
	  "parser": {
	    "kind": "csv",
	    "options": {
	      "has_header": true,
	      "comma": ",",
	      "trim_space": true,
	      "expected_fields": 9,
	      "header_map": { "A": "a", "B": "b" }
	    }
	  },
	  "transform": [
	    { "kind": "normalize", "options": {} },
	    { "kind": "coerce", "options": { "layout": "02.01.2006", "types": { "pcv": "int", "d": "date" } } },
	    { "kind": "require", "options": { "fields": ["pcv"] } }
	  ],
	  "storage": {
	    "kind": "postgres",
	    "postgres": {
	      "dsn": "postgresql://user:pass@host:5432/db?sslmode=disable",
	      "table": "public.technicke_prohlidky",
	      "columns": ["a","b","c"],
	      "key_columns": ["a","b"],
	      "date_column": "d"
	    }
	  },
	  "runtime": {
	    "reader_workers": 1,
	    "transform_workers": 4,
	    "loader_workers": 1,
	    "batch_size": 5000,
	    "channel_buffer": 2000
	  }
	}`

	var p Pipeline
	if err := json.Unmarshal([]byte(js), &p); err != nil {
		t.Fatalf("json.Unmarshal(Pipeline): %v", err)
	}

	// Source
	if p.Source.Kind != "file" || p.Source.File.Path != "testdata/rs.csv" {
		t.Fatalf("source decoded = %#v, want kind=file path=testdata/rs.csv", p.Source)
	}

	// Parser
	if p.Parser.Kind != "csv" {
		t.Fatalf("parser.kind = %q, want csv", p.Parser.Kind)
	}
	if got := p.Parser.Options.Bool("has_header", false); !got {
		t.Fatalf("parser.options.has_header = %v, want true", got)
	}
	if got := p.Parser.Options.Rune("comma", ';'); got != ',' {
		t.Fatalf("parser.options.comma = %q, want ','", got)
	}
	if got := p.Parser.Options.Int("expected_fields", 0); got != 9 {
		t.Fatalf("parser.options.expected_fields = %d, want 9", got)
	}
	if hm := p.Parser.Options.StringMap("header_map"); hm["A"] != "a" || hm["B"] != "b" {
		t.Fatalf("parser.options.header_map = %#v, want A->a B->b", hm)
	}

	// Transform (shape + spot-check options)
	if len(p.Transform) != 3 || p.Transform[0].Kind != "normalize" {
		t.Fatalf("transform decoded = %#v, want 3 steps with normalize first", p.Transform)
	}
	if got := p.Transform[1].Options.String("layout", ""); got != "02.01.2006" {
		t.Fatalf("coerce.layout = %q, want 02.01.2006", got)
	}
	if tt := p.Transform[1].Options.StringMap("types"); tt["pcv"] != "int" || tt["d"] != "date" {
		t.Fatalf("coerce.types = %#v, want {pcv:int d:date}", tt)
	}

	// Storage
	if p.Storage.Kind != "postgres" {
		t.Fatalf("storage.kind = %q, want postgres", p.Storage.Kind)
	}
	sp := p.Storage.Postgres
	if sp.DSN == "" || sp.Table != "public.technicke_prohlidky" {
		t.Fatalf("postgres: %#v", sp)
	}
	if !reflect.DeepEqual(sp.Columns, []string{"a", "b", "c"}) {
		t.Fatalf("columns = %#v, want [a b c]", sp.Columns)
	}
	if !reflect.DeepEqual(sp.KeyColumns, []string{"a", "b"}) {
		t.Fatalf("key_columns = %#v, want [a b]", sp.KeyColumns)
	}
	if sp.DateColumn != "d" {
		t.Fatalf("date_column = %q, want d", sp.DateColumn)
	}

	// Runtime
	if p.Runtime.ReaderWorkers != 1 || p.Runtime.TransformWorkers != 4 ||
		p.Runtime.LoaderWorkers != 1 || p.Runtime.BatchSize != 5000 ||
		p.Runtime.ChannelBuffer != 2000 {
		t.Fatalf("runtime decoded = %#v, want {1 4 1 5000 2000}", p.Runtime)
	}
}

// -----------------------------------------------------------------------------
// Options helper tests (hermetic).
// -----------------------------------------------------------------------------
//
// These tests validate minimal, deliberate coercion behavior and defaults. This
// protects against accidental changes in helper semantics that would silently
// alter pipeline behavior across the application.

func TestOptions_String_Bool_Int_Rune_DefaultsAndCoercion(t *testing.T) {
	t.Parallel()

	o := Options{
		"s": "hello",
		"b": true,
		"i": float64(42), // encoding/json decodes numbers as float64
		"r": ",",         // first rune will be used
	}

	// String
	if got := o.String("s", "def"); got != "hello" {
		t.Fatalf("String(s) = %q, want hello", got)
	}
	if got := o.String("missing", "def"); got != "def" {
		t.Fatalf("String(missing) = %q, want def", got)
	}

	// Bool
	if got := o.Bool("b", false); got != true {
		t.Fatalf("Bool(b) = %v, want true", got)
	}
	if got := o.Bool("missing", true); got != true {
		t.Fatalf("Bool(missing) = %v, want true", got)
	}

	// Int (float64 → int)
	if got := o.Int("i", 0); got != 42 {
		t.Fatalf("Int(i) = %d, want 42", got)
	}
	if got := o.Int("missing", 7); got != 7 {
		t.Fatalf("Int(missing) = %d, want 7", got)
	}

	// Rune (first rune from string)
	if got := o.Rune("r", ';'); got != ',' {
		t.Fatalf("Rune(r) = %q, want ','", got)
	}
	if got := o.Rune("missing", 'X'); got != 'X' {
		t.Fatalf("Rune(missing) = %q, want 'X'", got)
	}

	// Validate that Rune picks the FIRST rune (not byte) for multi-byte char.
	o["r2"] = "ž" // multi-byte UTF-8 rune
	r := o.Rune("r2", 'x')
	if r == 0 || !utf8.ValidRune(r) {
		t.Fatalf("Rune(r2) = %#U, want valid rune", r)
	}
	if string(r) != "ž" {
		t.Fatalf("Rune(r2) = %#U (%q), want ž", r, string(r))
	}
}

func TestOptions_StringMap_StringSlice_Any(t *testing.T) {
	t.Parallel()

	o := Options{
		"m": map[string]any{"A": "a", "B": "b", "X": 1}, // non-string value "X" must be ignored
		"s1": []any{
			"alpha", "beta", 3, // ints ignored
		},
		"s2": []string{"gamma", "delta"},
		"nested": map[string]any{
			"k": "v",
		},
	}

	// StringMap should include only string values and skip non-strings.
	sm := o.StringMap("m")
	if !reflect.DeepEqual(sm, map[string]string{"A": "a", "B": "b"}) {
		t.Fatalf("StringMap(m) = %#v, want {A:a B:b}", sm)
	}
	// Missing key → empty map (not nil).
	sm2 := o.StringMap("missing")
	if sm2 == nil || len(sm2) != 0 {
		t.Fatalf("StringMap(missing) = %#v, want empty map", sm2)
	}

	// StringSlice supports []any with strings and filters non-strings.
	ss1 := o.StringSlice("s1")
	if !reflect.DeepEqual(ss1, []string{"alpha", "beta"}) {
		t.Fatalf("StringSlice(s1) = %#v, want [alpha beta]", ss1)
	}
	// And the native []string case.
	ss2 := o.StringSlice("s2")
	if !reflect.DeepEqual(ss2, []string{"gamma", "delta"}) {
		t.Fatalf("StringSlice(s2) = %#v, want [gamma delta]", ss2)
	}
	// Missing key → nil (intentional to distinguish unspecified from empty).
	if got := o.StringSlice("missing"); got != nil {
		t.Fatalf("StringSlice(missing) = %#v, want nil", got)
	}

	// Any returns raw nested values for callers to unmarshal later.
	anyv := o.Any("nested")
	m, ok := anyv.(map[string]any)
	if !ok || m["k"] != "v" {
		t.Fatalf("Any(nested) = %#v, want map with k=v", anyv)
	}
	if o.Any("missing") != nil {
		t.Fatalf("Any(missing) should be nil when key absent")
	}
}

// -----------------------------------------------------------------------------
// Options.UnmarshalJSON behavior tests
// -----------------------------------------------------------------------------
//
// These tests ensure that decoding Options from JSON yields a non-nil, empty
// map when the field is missing or explicitly null. This avoids nil-checks at
// call sites and is a deliberate design choice for simplicity.

func TestOptions_UnmarshalJSON_NullYieldsEmptyMap(t *testing.T) {
	t.Parallel()

	type wrapper struct {
		Opts Options `json:"options"`
	}

	// options is explicitly null → non-nil, empty Options.
	const jsNull = `{"options": null}`
	var w wrapper
	if err := json.Unmarshal([]byte(jsNull), &w); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if w.Opts == nil || len(w.Opts) != 0 {
		t.Fatalf("Opts after null unmarshal = %#v, want non-nil empty map", w.Opts)
	}
}

func TestOptions_UnmarshalJSON_MissingYieldsEmptyMap(t *testing.T) {
	t.Parallel()

	type wrapper struct {
		Opts Options `json:"options"`
	}

	// options is missing entirely → non-nil, empty Options.
	const jsMissing = `{}`
	var w wrapper
	if err := json.Unmarshal([]byte(jsMissing), &w); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if w.Opts == nil || len(w.Opts) != 0 {
		t.Fatalf("Opts after missing unmarshal = %#v, want non-nil empty map", w.Opts)
	}
}

func TestOptions_UnmarshalJSON_ObjectDecodesAsMap(t *testing.T) {
	t.Parallel()

	type wrapper struct {
		Opts Options `json:"options"`
	}

	const jsObj = `{"options": {"a":"x","b":true,"n": 3}}`
	var w wrapper
	if err := json.Unmarshal([]byte(jsObj), &w); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if w.Opts.String("a", "") != "x" {
		t.Fatalf("Opts.String(a) = %q, want x", w.Opts.String("a", ""))
	}
	if w.Opts.Bool("b", false) != true {
		t.Fatalf("Opts.Bool(b) = %v, want true", w.Opts.Bool("b", false))
	}
	if w.Opts.Int("n", 0) != 3 {
		t.Fatalf("Opts.Int(n) = %d, want 3", w.Opts.Int("n", 0))
	}
}
