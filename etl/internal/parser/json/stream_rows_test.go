package json

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"etl/internal/config"
	"etl/internal/transformer"
)

/*
TestStreamJSONRows_ArrayRoot verifies that StreamJSONRows correctly parses a
top-level JSON array of objects:

  - two records are emitted as *transformer.Row,
  - row.Line is a 1-based sequence counter,
  - row.V aligns with the provided columns slice in order,
  - onParseErr is not invoked.
*/
func TestStreamJSONRows_ArrayRoot(t *testing.T) {
	const jsonData = `[{"id":1,"name":"A"},{"id":2,"name":"B"}]`

	cols := []string{"id", "name"}
	opts := config.Options{}

	outCh := make(chan *transformer.Row, 4)
	var parseErrs []error

	err := StreamJSONRows(
		context.Background(),
		strings.NewReader(jsonData),
		cols,
		opts,
		outCh,
		func(_ int, err error) {
			parseErrs = append(parseErrs, err)
		},
	)
	if err != nil {
		t.Fatalf("StreamJSONRows returned error: %v", err)
	}
	if len(parseErrs) != 0 {
		t.Fatalf("onParseErr called %d times; want 0", len(parseErrs))
	}

	rows := drainRows(outCh)
	if got, want := len(rows), 2; got != want {
		t.Fatalf("got %d rows; want %d", got, want)
	}

	// First row: id=1, name="A", line=1
	if rows[0].Line != 1 || rows[1].Line != 2 {
		t.Fatalf("unexpected line numbers: got [%d,%d], want [1,2]", rows[0].Line, rows[1].Line)
	}

	id0, ok := rows[0].V[0].(float64) // encoding/json decodes numbers as float64
	if !ok || id0 != 1 {
		t.Fatalf("row[0].V[0] = %#v (type %T); want float64(1)", rows[0].V[0], rows[0].V[0])
	}
	name0, ok := rows[0].V[1].(string)
	if !ok || name0 != "A" {
		t.Fatalf("row[0].V[1] = %#v (type %T); want \"A\"", rows[0].V[1], rows[0].V[1])
	}

	id1, ok := rows[1].V[0].(float64)
	if !ok || id1 != 2 {
		t.Fatalf("row[1].V[0] = %#v (type %T); want float64(2)", rows[1].V[0], rows[1].V[0])
	}
	name1, ok := rows[1].V[1].(string)
	if !ok || name1 != "B" {
		t.Fatalf("row[1].V[1] = %#v (type %T); want \"B\"", rows[1].V[1], rows[1].V[1])
	}
}

/*
TestStreamJSONRows_ObjectEnvelope verifies that StreamJSONRows handles a
top-level object that acts as an envelope containing an array-of-object field:

  - findObjectSlice discovers the records array,
  - all elements of that array are emitted as rows,
  - fields are mapped by name into columns.
*/
func TestStreamJSONRows_ObjectEnvelope(t *testing.T) {
	const jsonData = `{
	  "records": [
	    {"id": 1, "name": "A"},
	    {"id": 2, "name": "B"}
	  ],
	  "meta": {"version": 1}
	}`

	cols := []string{"id", "name"}
	opts := config.Options{}

	outCh := make(chan *transformer.Row, 4)
	var parseErrs []error

	err := StreamJSONRows(
		context.Background(),
		strings.NewReader(jsonData),
		cols,
		opts,
		outCh,
		func(_ int, err error) {
			parseErrs = append(parseErrs, err)
		},
	)
	if err != nil {
		t.Fatalf("StreamJSONRows returned error: %v", err)
	}
	if len(parseErrs) != 0 {
		t.Fatalf("onParseErr called %d times; want 0", len(parseErrs))
	}

	rows := drainRows(outCh)
	if got, want := len(rows), 2; got != want {
		t.Fatalf("got %d rows; want %d", got, want)
	}
}

/*
TestStreamJSONRows_ObjectSingleRecord verifies that a single top-level JSON
object (no envelope) is treated as one logical record.
*/
func TestStreamJSONRows_ObjectSingleRecord(t *testing.T) {
	const jsonData = `{"id": 42, "name": "single"}`

	cols := []string{"id", "name"}
	opts := config.Options{}

	outCh := make(chan *transformer.Row, 2)
	err := StreamJSONRows(
		context.Background(),
		strings.NewReader(jsonData),
		cols,
		opts,
		outCh,
		nil,
	)
	if err != nil {
		t.Fatalf("StreamJSONRows returned error: %v", err)
	}

	rows := drainRows(outCh)
	if got, want := len(rows), 1; got != want {
		t.Fatalf("got %d rows; want %d", got, want)
	}
	id, ok := rows[0].V[0].(float64)
	if !ok || id != 42 {
		t.Fatalf("row[0].V[0] = %#v (type %T); want float64(42)", rows[0].V[0], rows[0].V[0])
	}
}

/*
TestStreamJSONRows_HeaderMap_NormalizesUnicodeKeys ensures that header_map is
honored, so that original JSON keys with spaces and diacritics are mapped to
normalized destination column names.

Scenario:

  - JSON object uses keys like "Identifikační číslo" and "Název".
  - parser.options.header_map maps those to "identifikacni_cislo" and "nazev".
  - columns slice is defined in terms of normalized names.
  - StreamJSONRows emits rows whose values for those columns are non-empty and
    match the original JSON payload.
*/
func TestStreamJSONRows_HeaderMap_NormalizesUnicodeKeys(t *testing.T) {
	const jsonData = `[
	  {"Identifikační číslo": "123", "Název": "Firma A"},
	  {"Identifikační číslo": "456", "Název": "Firma B"}
	]`

	cols := []string{"identifikacni_cislo", "nazev"}
	opts := config.Options{
		"header_map": map[string]string{
			"Identifikační číslo": "identifikacni_cislo",
			"Název":               "nazev",
		},
	}

	outCh := make(chan *transformer.Row, 4)
	err := StreamJSONRows(
		context.Background(),
		strings.NewReader(jsonData),
		cols,
		opts,
		outCh,
		nil,
	)
	if err != nil {
		t.Fatalf("StreamJSONRows returned error: %v", err)
	}

	rows := drainRows(outCh)
	if got, want := len(rows), 2; got != want {
		t.Fatalf("got %d rows; want %d", got, want)
	}

	for i, wantID := range []string{"123", "456"} {
		t.Run("row_"+wantID, func(t *testing.T) {
			if got := rows[i].V[0]; got != wantID {
				t.Fatalf("row[%d].V[0] = %#v; want %q", i, got, wantID)
			}
			if got := rows[i].V[1]; got == nil || got.(string) == "" {
				t.Fatalf("row[%d].V[1] empty; want non-empty name", i)
			}
		})
	}
}

/*
TestStreamJSONRows_NDJSON verifies that StreamJSONRows handles newline-delimited
JSON (NDJSON) where multiple top-level objects appear one after another:

  - the first object is decoded as the root map,
  - subsequent objects are read in the follow-up loop,
  - all objects become rows in order.
*/
func TestStreamJSONRows_NDJSON(t *testing.T) {
	const jsonData = `{"id":1}
{"id":2}
{"id":3}
`

	cols := []string{"id"}
	opts := config.Options{}

	outCh := make(chan *transformer.Row, 4)
	err := StreamJSONRows(
		context.Background(),
		strings.NewReader(jsonData),
		cols,
		opts,
		outCh,
		nil,
	)
	if err != nil {
		t.Fatalf("StreamJSONRows returned error: %v", err)
	}

	rows := drainRows(outCh)
	if got, want := len(rows), 3; got != want {
		t.Fatalf("got %d rows; want %d", got, want)
	}

	for i, row := range rows {
		id, ok := row.V[0].(float64)
		if !ok || id != float64(i+1) {
			t.Fatalf("row[%d].V[0] = %#v (type %T); want float64(%d)", i, row.V[0], row.V[0], i+1)
		}
	}
}

/*
TestStreamJSONRows_InvalidRootTypes exercises error handling for unsupported
top-level JSON shapes:

  - primitive root (e.g., "42") triggers an error and onParseErr,
  - array with non-object elements also yields an error.
*/
func TestStreamJSONRows_InvalidRootTypes(t *testing.T) {
	tests := []struct {
		name     string
		jsonData string
	}{
		{
			name:     "primitive_root",
			jsonData: `42`,
		},
		{
			name:     "array_with_non_object",
			jsonData: `[1, 2, 3]`,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			cols := []string{"id"}
			opts := config.Options{}
			outCh := make(chan *transformer.Row, 2)

			var parseErrs []error
			err := StreamJSONRows(
				context.Background(),
				strings.NewReader(tc.jsonData),
				cols,
				opts,
				outCh,
				func(_ int, err error) { parseErrs = append(parseErrs, err) },
			)
			if err == nil {
				t.Fatalf("expected error for %s; got nil", tc.name)
			}
			if len(parseErrs) == 0 {
				t.Fatalf("expected onParseErr to be called for %s; got 0 callbacks", tc.name)
			}
			if got := len(drainRows(outCh)); got != 0 {
				t.Fatalf("expected no rows for %s; got %d", tc.name, got)
			}
		})
	}
}

/*
TestReadHeaderMap verifies that readHeaderMap correctly extracts a header_map
from parser options for both map[string]string and map[string]any values and
ignores non-string entries.
*/
func TestReadHeaderMap(t *testing.T) {
	tests := []struct {
		name string
		opts config.Options
		want map[string]string
	}{
		{
			name: "map_string_string",
			opts: config.Options{
				"header_map": map[string]string{
					"A": "a",
					"B": "b",
				},
			},
			want: map[string]string{"A": "a", "B": "b"},
		},
		{
			name: "map_string_any",
			opts: config.Options{
				"header_map": map[string]any{
					"A": "a",
					"B": 123, // non-string -> ignored
				},
			},
			want: map[string]string{"A": "a"},
		},
		{
			name: "missing_header_map",
			opts: config.Options{},
			want: map[string]string{},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := readHeaderMap(tc.opts)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("readHeaderMap() = %#v; want %#v", got, tc.want)
			}
		})
	}
}

/*
TestFindObjectSlice checks that findObjectSlice:

  - returns the first array-of-object field when present,
  - returns nil when no suitable array-of-object field exists or when the array
    contains non-object values.
*/
func TestFindObjectSlice(t *testing.T) {
	obj1 := map[string]any{"id": 1}
	obj2 := map[string]any{"id": 2}

	tests := []struct {
		name string
		root map[string]any
		want []map[string]any
	}{
		{
			name: "records_array_of_objects",
			root: map[string]any{
				"records": []any{obj1, obj2},
				"meta":    map[string]any{"v": 1},
			},
			want: []map[string]any{obj1, obj2},
		},
		{
			name: "no_object_array",
			root: map[string]any{
				"nums": []any{1, 2, 3},
			},
			want: nil,
		},
		{
			name: "mixed_array_rejected",
			root: map[string]any{
				"mixed": []any{obj1, 2},
			},
			want: nil,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := findObjectSlice(tc.root)
			if tc.want == nil {
				if got != nil {
					t.Fatalf("expected nil; got %#v", got)
				}
				return
			}
			if len(got) != len(tc.want) {
				t.Fatalf("len(got)=%d; want %d", len(got), len(tc.want))
			}
			for i := range got {
				if !reflect.DeepEqual(got[i], tc.want[i]) {
					t.Fatalf("got[%d]=%#v; want %#v", i, got[i], tc.want[i])
				}
			}
		})
	}
}

/*
TestRecordToRowJSON verifies that recordToRowJSON maps values from a JSON
object into a []any aligned with the given columns slice and that missing
keys become nil.
*/
func TestRecordToRowJSON(t *testing.T) {
	obj := map[string]any{
		"a": 1,
		"b": "x",
	}
	cols := []string{"a", "b", "c"} // "c" missing from obj

	row := recordToRowJSON(obj, cols)
	if got, want := len(row), len(cols); got != want {
		t.Fatalf("len(row)=%d; want %d", got, want)
	}
	if row[0] != 1 {
		t.Fatalf("row[0] = %#v; want 1", row[0])
	}
	if row[1] != "x" {
		t.Fatalf("row[1] = %#v; want \"x\"", row[1])
	}
	if row[2] != nil {
		t.Fatalf("row[2] = %#v; want nil", row[2])
	}
}

// drainRows reads all currently buffered rows from a channel without blocking.
// It assumes the producer has finished sending and that the channel is buffered
// with sufficient capacity for the test scenario.
func drainRows(ch chan *transformer.Row) []*transformer.Row {
	var out []*transformer.Row
	for {
		if len(ch) == 0 {
			break
		}
		out = append(out, <-ch)
	}
	return out
}
