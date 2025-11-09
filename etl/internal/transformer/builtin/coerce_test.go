package builtin

import (
	"etl/pkg/records"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestCoerce(t *testing.T) {
	recs := []records.Record{{
		"pcv":       "10",
		"date_from": "13.08.2018",
		"rm_code":   "3203",
	}}
	c := Coerce{Types: map[string]string{"pcv": "int", "rm_code": "int", "date_from": "date"}, Layout: "02.01.2006"}
	out := c.Apply(recs)
	if _, ok := out[0]["pcv"].(int); !ok {
		t.Fatalf("pcv not int")
	}
	if _, ok := out[0]["rm_code"].(int); !ok {
		t.Fatalf("rm_code not int")
	}
	if _, ok := out[0]["date_from"].(time.Time); !ok {
		t.Fatalf("date_from not time.Time")
	}
}

/*
TestCoerceApply_Basics verifies that Coerce.Apply converts string values
to int, bool, date (time.Time), or leaves string as-is when type "string"
is specified and the value is a string parseable under the given layout.
*/
func TestCoerceApply_Basics(t *testing.T) {
	layout := "2006-01-02"
	c := Coerce{
		Types: map[string]string{
			"i": "int",
			"b": "bool",
			"d": "date",
			"s": "string",
		},
		Layout: layout,
	}

	in := []records.Record{
		{
			"i": "42",
			"b": "true",
			"d": "2025-11-09",
			"s": "hello",
		},
	}
	out := c.Apply(in)
	if out == nil || len(out) != 1 {
		t.Fatalf("Apply returned invalid slice: %#v", out)
	}
	r := out[0]

	// int
	if v, ok := r["i"].(int); !ok || v != 42 {
		t.Fatalf(`"i" got %#v (type %T); want int(42)`, r["i"], r["i"])
	}
	// bool
	if v, ok := r["b"].(bool); !ok || v != true {
		t.Fatalf(`"b" got %#v (type %T); want bool(true)`, r["b"], r["b"])
	}
	// date
	if v, ok := r["d"].(time.Time); !ok || v.Format(layout) != "2025-11-09" {
		t.Fatalf(`"d" got %#v (type %T); want time.Time(2025-11-09)`, r["d"], r["d"])
	}
	// "string" target leaves string as string
	if v, ok := r["s"].(string); !ok || v != "hello" {
		t.Fatalf(`"s" got %#v (type %T); want string("hello")`, r["s"], r["s"])
	}
}

/*
TestCoerceApply_InvalidsPreserve verifies that when parsing fails, the original
string value is left unchanged (no partial writes).
*/
func TestCoerceApply_InvalidsPreserve(t *testing.T) {
	c := Coerce{
		Types: map[string]string{
			"i": "int",
			"b": "bool",
			"d": "date",
		},
		Layout: "2006-01-02",
	}
	in := []records.Record{
		{"i": "not-an-int", "b": "nope", "d": "11/09/2025"},
	}
	orig := deepCopy(in)

	out := c.Apply(in)
	if !reflect.DeepEqual(out, orig) {
		t.Fatalf("invalid values should remain unchanged:\n got: %#v\nwant: %#v", out, orig)
	}
}

/*
TestCoerceApply_MissingNilNonString verifies that:
  - missing fields are ignored,
  - nil values are ignored,
  - non-string values are left untouched (no re-coercion).
*/
func TestCoerceApply_MissingNilNonString(t *testing.T) {
	c := Coerce{
		Types: map[string]string{
			"a": "int",
			"b": "bool",
			"c": "date",
		},
		Layout: "2006-01-02",
	}
	tm := time.Date(2025, 11, 9, 0, 0, 0, 0, time.UTC)
	in := []records.Record{
		{
			// missing "a" -> ignored
			"b": nil, // nil -> ignored
			"c": tm,  // already time.Time -> untouched
			"x": 123, // extra field
		},
	}
	orig := deepCopy(in)

	out := c.Apply(in)
	if !reflect.DeepEqual(out, orig) {
		t.Fatalf("non-string/missing/nil should be unchanged:\n got: %#v\nwant: %#v", out, orig)
	}
}

/*
TestCoerceApply_InPlaceMutation verifies that Apply mutates records in place
and does not replace the record maps or the slice header.
We validate by comparing map identities and by ensuring the returned slice
shares the same first element pointer when non-empty.
*/
func TestCoerceApply_InPlaceMutation(t *testing.T) {
	c := Coerce{
		Types: map[string]string{"i": "int"},
	}
	r0 := records.Record{"i": "7", "keep": "v"}
	in := []records.Record{r0}

	// Capture identities before Apply.
	firstElemPtr := &in[0]
	mapPtrBefore := reflect.ValueOf(in[0]).Pointer()

	out := c.Apply(in)

	// Same slice element address (in-place).
	if &out[0] != firstElemPtr {
		t.Fatalf("Apply replaced the slice element; want in-place")
	}
	// Same map identity.
	mapPtrAfter := reflect.ValueOf(out[0]).Pointer()
	if mapPtrAfter != mapPtrBefore {
		t.Fatalf("Apply replaced the map; want in-place")
	}
	// Field coerced
	if out[0]["i"] != 7 {
		t.Fatalf(`"i" coerced value got %#v; want 7`, out[0]["i"])
	}
	// Non-target field preserved
	if out[0]["keep"] != "v" {
		t.Fatalf(`"keep" changed to %#v; want "v"`, out[0]["keep"])
	}
}

/*
TestCoerceApply_CaseSensitivity documents that the current implementation
expects exact type keys ("int","bool","date","string"). If a different string
is provided in Types, it is ignored.
*/
func TestCoerceApply_CaseSensitivity(t *testing.T) {
	c := Coerce{
		Types:  map[string]string{"i": "Int"}, // not recognized
		Layout: "2006-01-02",
	}
	in := []records.Record{{"i": "5"}}
	out := c.Apply(in)
	if v, ok := out[0]["i"].(string); !ok || v != "5" {
		t.Fatalf(`unrecognized type should be ignored; got %#v (type %T)`, out[0]["i"], out[0]["i"])
	}
}

/*
TestCoerceApply_EmptyTypesIsNoop verifies that an empty Types map produces a
no-op and does not allocate unnecessarily.
*/
func TestCoerceApply_EmptyTypesIsNoop(t *testing.T) {
	c := Coerce{Types: nil, Layout: "2006-01-02"}
	in := []records.Record{{"a": "1"}, {"b": "false"}}
	orig := deepCopy(in)

	out := c.Apply(in)
	if !reflect.DeepEqual(out, orig) {
		t.Fatalf("empty Types should be a no-op")
	}
	allocs := testingAllocsPerRun(500, func() {
		_ = c.Apply(in)
	})
	// Allow tiny noise across Go versions
	if allocs > 0.10 {
		t.Fatalf("allocs/op=%.2f; want <= 0.10", allocs)
	}
}

// --- test helpers ---

// deepCopy makes a shallow slice copy and shallow map copies for comparison.
func deepCopy(in []records.Record) []records.Record {
	out := make([]records.Record, len(in))
	for i := range in {
		m := make(records.Record, len(in[i]))
		for k, v := range in[i] {
			m[k] = v
		}
		out[i] = m
	}
	return out
}

func testingAllocsPerRun(runs int, f func()) float64 {
	return testing.AllocsPerRun(runs, f)
}

/*
BenchmarkCoerce_AllPass_IntBoolDate measures throughput when all fields
successfully coerce (hot path).
*/
func BenchmarkCoerce_AllPass_IntBoolDate(b *testing.B) {
	layout := "2006-01-02"
	c := Coerce{
		Types:  map[string]string{"i": "int", "b": "bool", "d": "date"},
		Layout: layout,
	}

	const N = 30000
	in := make([]records.Record, N)
	for i := 0; i < N; i++ {
		in[i] = records.Record{
			"i": strconv.Itoa(i),
			"b": "true",
			"d": "2025-11-09",
			"x": "untouched",
		}
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c.Apply(in)
	}
}

/*
BenchmarkCoerce_MixedFailures alternates coercible/uncertain inputs to show
failure-path cost when parse errors occur (should not write back).
*/
func BenchmarkCoerce_MixedFailures(b *testing.B) {
	layout := "2006-01-02"
	c := Coerce{
		Types:  map[string]string{"i": "int", "b": "bool", "d": "date"},
		Layout: layout,
	}

	const N = 30000
	in := make([]records.Record, N)
	for i := 0; i < N; i++ {
		valI := strconv.Itoa(i)
		valB := "true"
		valD := "2025-11-09"
		if i%2 == 1 {
			valI = "x"        // int fail
			valB = "maybe"    // bool fail
			valD = "11/09/25" // date fail
		}
		in[i] = records.Record{
			"i": valI,
			"b": valB,
			"d": valD,
		}
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c.Apply(in)
	}
}

/*
BenchmarkCoerce_DateHeavy isolates date parsing cost with varying layouts.
*/
func BenchmarkCoerce_DateHeavy(b *testing.B) {
	c := Coerce{
		Types:  map[string]string{"d": "date"},
		Layout: "2006-01-02", // simple numeric layout; adjust to your real one if needed
	}

	const N = 20000
	in := make([]records.Record, N)
	for i := 0; i < N; i++ {
		// A handful of unique dates to avoid pathological caching assumptions.
		day := (i % 28) + 1
		in[i] = records.Record{"d": time.Date(2025, time.November, day, 0, 0, 0, 0, time.UTC).Format(c.Layout)}
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c.Apply(in)
	}
}
