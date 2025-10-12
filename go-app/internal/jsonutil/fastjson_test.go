package jsonutil

import "testing"

func TestFastJSONEncoder_EncodeRow(t *testing.T) {
	e := NewFastJSONEncoder([]string{"a", "b"})
	out := e.EncodeRow([]string{`hello`, `x"y`})
	got := string(out)
	// Must be valid JSON object with keys a and b in order, and proper escaping
	want1 := `{"a":"hello","b":"x\"y"}`
	if got != want1 {
		t.Fatalf("got %q want %q", got, want1)
	}
}
