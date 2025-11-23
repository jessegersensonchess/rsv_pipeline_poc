package json

import (
	"encoding/json"
	"io"
	"reflect"
	"strings"
	"testing"

	"etl/internal/config"
	"etl/pkg/records"
)

/*
TestFromConfigOptions_BoolMapping verifies that FromConfigOptions correctly
maps config.Options into JSON Options:

  - when "allow_arrays" is true, Options.AllowArrays is true,
  - when "allow_arrays" is false or missing, Options.AllowArrays is false.
*/
func TestFromConfigOptions_BoolMapping(t *testing.T) {
	tests := []struct {
		name string
		opt  config.Options
		want bool
	}{
		{
			name: "allow_arrays_true",
			opt:  config.Options{"allow_arrays": true},
			want: true,
		},
		{
			name: "allow_arrays_false",
			opt:  config.Options{"allow_arrays": false},
			want: false,
		},
		{
			name: "allow_arrays_missing",
			opt:  config.Options{},
			want: false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := FromConfigOptions(tc.opt)
			if got.AllowArrays != tc.want {
				t.Fatalf("FromConfigOptions(%#v).AllowArrays = %v; want %v", tc.opt, got.AllowArrays, tc.want)
			}
		})
	}
}

/*
TestDecoderNext_NDJSONObjectsAndPrimitives verifies Decoder.Next on a mixed
NDJSON stream:

  - primitive top-level values (e.g. 42) are skipped,
  - object top-level values are converted into records.Record,
  - EOF is returned when the stream is exhausted.
*/
func TestDecoderNext_NDJSONObjectsAndPrimitives(t *testing.T) {
	const ndjson = `{"id":1,"name":"a"}
42
{"id":2,"name":"b"}
`

	d := NewDecoder(strings.NewReader(ndjson), Options{})

	// First object
	rec1, err := d.Next()
	if err != nil {
		t.Fatalf("Next() 1 returned error: %v", err)
	}
	if got, ok := rec1["id"].(json.Number); !ok || got.String() != "1" {
		t.Fatalf("rec1[\"id\"] = %#v (type %T); want json.Number(\"1\")", rec1["id"], rec1["id"])
	}
	if got, ok := rec1["name"].(string); !ok || got != "a" {
		t.Fatalf("rec1[\"name\"] = %#v (type %T); want \"a\"", rec1["name"], rec1["name"])
	}

	// Second object (after skipping primitive 42)
	rec2, err := d.Next()
	if err != nil {
		t.Fatalf("Next() 2 returned error: %v", err)
	}
	if got, ok := rec2["id"].(json.Number); !ok || got.String() != "2" {
		t.Fatalf("rec2[\"id\"] = %#v (type %T); want json.Number(\"2\")", rec2["id"], rec2["id"])
	}

	// EOF on next call
	rec3, err := d.Next()
	if err != io.EOF {
		t.Fatalf("Next() 3 = (%#v, %v); want (nil, io.EOF)", rec3, err)
	}
}

/*
TestDecoderNext_RejectsNonObjectOnlyStream ensures that when the stream
contains only non-object top-level values, Decoder.Next eventually returns
EOF without yielding any records.
*/
func TestDecoderNext_RejectsNonObjectOnlyStream(t *testing.T) {
	const data = `1
"two"
[3]
`

	d := NewDecoder(strings.NewReader(data), Options{})

	rec, err := d.Next()
	if err != io.EOF {
		t.Fatalf("Next() on non-object-only stream = (%#v, %v); want (nil, io.EOF)", rec, err)
	}
}

/*
TestDecodeAll_EmptyInput verifies that DecodeAll returns (nil, nil) for an
empty reader.
*/
func TestDecodeAll_EmptyInput(t *testing.T) {
	recs, err := DecodeAll(strings.NewReader(""), Options{})
	if err != nil {
		t.Fatalf("DecodeAll on empty input returned error: %v", err)
	}
	if recs != nil {
		t.Fatalf("DecodeAll on empty input = %#v; want nil slice", recs)
	}
}

/*
TestDecodeAll_ObjectRoot verifies that a single top-level JSON object is
decoded into one records.Record with matching fields.
*/
func TestDecodeAll_ObjectRoot(t *testing.T) {
	const data = `{"id":1,"name":"a"}`

	recs, err := DecodeAll(strings.NewReader(data), Options{})
	if err != nil {
		t.Fatalf("DecodeAll returned error: %v", err)
	}
	if got, want := len(recs), 1; got != want {
		t.Fatalf("len(recs)=%d; want %d", got, want)
	}

	want := records.Record{
		"id":   json.Number("1"),
		"name": "a",
	}
	if !reflect.DeepEqual(recs[0], want) {
		t.Fatalf("DecodeAll object root mismatch:\n got: %#v\nwant: %#v", recs[0], want)
	}
}

/*
TestDecodeAll_ArrayRootAllowArrays verifies that a single top-level JSON array
of objects is expanded into records when Options.AllowArrays is true:

  - each array element must be an object,
  - DecodeAll returns one records.Record per element.
*/
func TestDecodeAll_ArrayRootAllowArrays(t *testing.T) {
	const data = `[{"id":1},{"id":2}]`

	recs, err := DecodeAll(strings.NewReader(data), Options{AllowArrays: true})
	if err != nil {
		t.Fatalf("DecodeAll returned error: %v", err)
	}
	if got, want := len(recs), 2; got != want {
		t.Fatalf("len(recs)=%d; want %d", got, want)
	}
	if got := recs[0]["id"].(json.Number).String(); got != "1" {
		t.Fatalf("recs[0][\"id\"] = %q; want \"1\"", got)
	}
	if got := recs[1]["id"].(json.Number).String(); got != "2" {
		t.Fatalf("recs[1][\"id\"] = %q; want \"2\"", got)
	}
}

/*
TestDecodeAll_ArrayRootDisallowed verifies that when the top-level value is an
array and Options.AllowArrays is false, DecodeAll returns an error.
*/
func TestDecodeAll_ArrayRootDisallowed(t *testing.T) {
	const data = `[{"id":1},{"id":2}]`

	recs, err := DecodeAll(strings.NewReader(data), Options{AllowArrays: false})
	if err == nil {
		t.Fatalf("DecodeAll with allow_arrays=false on array root = %#v, nil; want non-nil error", recs)
	}
}

/*
TestDecodeAll_ArrayRootNonObjectElement ensures that a top-level array with
a non-object element causes DecodeAll to fail with a descriptive error.
*/
func TestDecodeAll_ArrayRootNonObjectElement(t *testing.T) {
	const data = `[{"id":1}, 2]`

	recs, err := DecodeAll(strings.NewReader(data), Options{AllowArrays: true})
	if err == nil {
		t.Fatalf("DecodeAll on array with non-object element = %#v, nil; want error", recs)
	}
}

/*
TestDecodeAll_UnsupportedRootType verifies that unsupported top-level JSON
types (such as a primitive) produce an error.
*/
func TestDecodeAll_UnsupportedRootType(t *testing.T) {
	const data = `42`

	recs, err := DecodeAll(strings.NewReader(data), Options{})
	if err == nil {
		t.Fatalf("DecodeAll on primitive root = %#v, nil; want error", recs)
	}
}

/*
TestDecodeAll_IncludesTrailingNDJSON documents the current behavior of DecodeAll
with trailing NDJSON-style content:

  - The first decoded root object is processed,
  - Trailing objects that remain in the json.Decoder's internal buffer are
    also consumed via the Decoder/Next helper,
  - In practice this means a root object followed by NDJSON on the same stream
    yields multiple records when the trailing bytes are still buffered.
*/
func TestDecodeAll_IncludesTrailingNDJSON(t *testing.T) {
	const data = `{"id":1}
{"id":2}
`

	recs, err := DecodeAll(strings.NewReader(data), Options{})
	if err != nil {
		t.Fatalf("DecodeAll returned error: %v", err)
	}

	// Current behavior: both objects are returned (root + trailing NDJSON).
	if got, want := len(recs), 2; got != want {
		t.Fatalf("len(recs)=%d; want %d (root plus trailing object)", got, want)
	}

	if got := recs[0]["id"].(json.Number).String(); got != "1" {
		t.Fatalf("recs[0][\"id\"] = %q; want \"1\"", got)
	}
	if got := recs[1]["id"].(json.Number).String(); got != "2" {
		t.Fatalf("recs[1][\"id\"] = %q; want \"2\"", got)
	}
}
