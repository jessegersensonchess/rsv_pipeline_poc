package main

import (
	"os"
	"reflect"
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
	t.Parallel()

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
