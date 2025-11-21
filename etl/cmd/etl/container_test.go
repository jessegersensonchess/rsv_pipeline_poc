package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"etl/internal/config"
)

// TestSplitFQN checks schema-qualified table identifiers get split correctly.
func TestSplitFQN(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   string
		want []string
	}{
		{"public.technicke_prohlidky", []string{"public", "technicke_prohlidky"}},
		{"vlastnik_vozidla", []string{"vlastnik_vozidla"}},
		{"a.b.c", []string{"a", "b", "c"}},
		{"", []string{}},
	}
	for _, c := range cases {
		got := splitFQN(c.in)
		if !reflect.DeepEqual([]string(got), c.want) {
			t.Errorf("splitFQN(%q) = %#v, want %#v", c.in, []string(got), c.want)
		}
	}
}

// TestGetenvIntAndPickInt verifies env fallback and pick semantics.
func TestGetenvIntAndPickInt(t *testing.T) {
	_ = os.Unsetenv("ETL_TEST_INT")
	if v := getenvInt("ETL_TEST_INT", 7); v != 7 {
		t.Fatalf("getenvInt unset = %d, want 7", v)
	}
	_ = os.Setenv("ETL_TEST_INT", "42")
	if v := getenvInt("ETL_TEST_INT", 7); v != 42 {
		t.Fatalf("getenvInt set = %d, want 42", v)
	}
	if v := pickInt(5, 9); v != 5 {
		t.Fatalf("pickInt(5,9) = %d, want 5", v)
	}
	if v := pickInt(0, 9); v != 9 {
		t.Fatalf("pickInt(0,9) = %d, want 9", v)
	}
}

// TestCoerceSpecExtraction ensures we can pull coerce settings from transforms.
func TestCoerceSpecExtraction(t *testing.T) {
	t.Parallel()

	p := config.Pipeline{
		Transform: []config.Transform{
			{Kind: "normalize"},
			{Kind: "coerce", Options: config.Options{
				"layout": "02.01.2006",
				"types": map[string]any{
					"pcv":         "int",
					"platnost_od": "date",
					"aktualni":    "bool",
				},
			}},
		},
	}

	types := coerceTypesFromSpec(p)
	if types["pcv"] != "int" || types["platnost_od"] != "date" || types["aktualni"] != "bool" {
		t.Fatalf("types extraction failed: %#v", types)
	}
	if layout := coerceLayoutFromSpec(p); layout != "02.01.2006" {
		t.Fatalf("layout extraction = %q, want 02.01.2006", layout)
	}
}

/////////////////////////////////////////////////

/*
Unit tests for the small, pure helpers and thin adapters in container.go.

We cover:
  - openSource: file path happy path + unsupported kind
  - getenvInt / pickInt: env parsing and defaulting
  - splitFQN: robust splitting with empty segments elided
  - coerceTypesFromSpec / coerceLayoutFromSpec: default behavior with no transforms
  - errAgg: capped aggregation semantics (limit, first N, total count)
  - requiredFromContract: default behavior with no validate transform

We intentionally avoid runStreamed (requires many subsystems).
*/

func TestOpenSource_FileAndUnsupported(t *testing.T) {
	t.Parallel()

	type tc struct {
		name      string
		spec      config.Pipeline
		wantBody  string
		wantError string // substring
	}
	tmpdir := t.TempDir()
	p := filepath.Join(tmpdir, "data.txt")
	if err := os.WriteFile(p, []byte("hello"), 0o644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	cases := []tc{
		{
			name: "file_ok",
			spec: config.Pipeline{
				Source: config.Source{
					Kind: "file",
					File: config.SourceFile{Path: p},
				},
			},
			wantBody: "hello",
		},
		{
			name: "unsupported_kind",
			spec: config.Pipeline{
				Source: config.Source{Kind: "ftp"},
			},
			wantError: "unsupported source.kind",
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			rc, err := openSource(context.Background(), c.spec)
			if c.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantError) {
					t.Fatalf("want error containing %q, got %v", c.wantError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("openSource: %v", err)
			}
			defer rc.Close()
			b, rerr := io.ReadAll(rc)
			if rerr != nil {
				t.Fatalf("read body: %v", rerr)
			}
			if string(b) != c.wantBody {
				t.Fatalf("body mismatch: got %q want %q", string(b), c.wantBody)
			}
		})
	}
}

func TestGetenvInt(t *testing.T) {
	// Unset -> default
	if got := getenvInt("ETL_TEST_INT_UNSET", 7); got != 7 {
		t.Fatalf("unset: got %d want 7", got)
	}

	// Invalid -> default
	t.Setenv("ETL_TEST_INT_BAD", "nope")
	if got := getenvInt("ETL_TEST_INT_BAD", 9); got != 9 {
		t.Fatalf("bad parse: got %d want 9", got)
	}

	// Valid -> parsed
	t.Setenv("ETL_TEST_INT_OK", "42")
	if got := getenvInt("ETL_TEST_INT_OK", 0); got != 42 {
		t.Fatalf("valid: got %d want 42", got)
	}
}

func TestPickInt(t *testing.T) {
	t.Parallel()

	type tc struct{ a, b, want int }
	cases := []tc{
		{a: 5, b: 10, want: 5},
		{a: 0, b: 10, want: 10},
		{a: -3, b: 8, want: 8},
	}
	for _, c := range cases {
		if got := pickInt(c.a, c.b); got != c.want {
			t.Fatalf("pickInt(%d,%d)=%d want %d", c.a, c.b, got, c.want)
		}
	}
}

func TestCoerceTypesAndLayout_Defaults(t *testing.T) {
	t.Parallel()

	var p config.Pipeline // zero value: no transforms
	if got := coerceTypesFromSpec(p); len(got) != 0 {
		t.Fatalf("coerceTypesFromSpec default: got %v want empty", got)
	}
	if got := coerceLayoutFromSpec(p); got != "02.01.2006" {
		t.Fatalf("coerceLayoutFromSpec default: got %q want %q", got, "02.01.2006")
	}
}

func TestErrAgg_LimitsAndBuckets(t *testing.T) {
	t.Parallel()

	a := newErrAgg(3) // capture first 3
	msgs := []string{"A", "B", "A", "C", "A", "D"}
	for _, m := range msgs {
		a.add(m)
	}
	// total count
	if a.count != len(msgs) {
		t.Fatalf("count=%d want %d", a.count, len(msgs))
	}
	// first limited
	if len(a.first) != 3 {
		t.Fatalf("first len=%d want 3", len(a.first))
	}
	// buckets
	if a.buckets["A"] != 3 || a.buckets["B"] != 1 || a.buckets["C"] != 1 || a.buckets["D"] != 1 {
		t.Fatalf("buckets=%v", a.buckets)
	}
}

func TestRequiredFromContract_DefaultNoValidate(t *testing.T) {
	t.Parallel()

	var p config.Pipeline // no validate transform
	if got := requiredFromContract(p); got != nil {
		t.Fatalf("requiredFromContract default: got %v want nil", got)
	}
}

/*
A simple concurrency smoke test for errAgg to ensure it doesn't panic
under parallel adds (lock correctness).
*/
func TestErrAgg_ConcurrentAdds(t *testing.T) {
	t.Parallel()

	a := newErrAgg(2)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				a.add("msg")
			}
		}(i)
	}
	wg.Wait()
	if a.count != 16*100 {
		t.Fatalf("count=%d want %d", a.count, 16*100)
	}
	if a.buckets["msg"] != 16*100 {
		t.Fatalf("bucket[msg]=%d want %d", a.buckets["msg"], 16*100)
	}
}
