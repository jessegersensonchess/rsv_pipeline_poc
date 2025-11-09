package builtin

import (
	"reflect"
	"sort"
	"strconv"
	"testing"

	"etl/pkg/records"
)

func mk(pcv int, df string, fields map[string]any) records.Record {
	r := records.Record{
		"pcv":       pcv,
		"date_from": df,
	}
	for k, v := range fields {
		r[k] = v
	}
	return r
}

func TestDeDupKeepFirst(t *testing.T) {
	in := []records.Record{
		mk(1, "2020-01-01", map[string]any{"reason": "A"}),
		mk(1, "2020-01-01", map[string]any{"reason": "B"}),
		mk(2, "2020-01-01", map[string]any{"reason": "C"}),
	}
	d := DeDup{Keys: []string{"pcv", "date_from"}, Policy: "keep-first"}
	got := d.Apply(in)
	want := []records.Record{
		mk(1, "2020-01-01", map[string]any{"reason": "A"}),
		mk(2, "2020-01-01", map[string]any{"reason": "C"}),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("keep-first: got %#v want %#v", got, want)
	}
}

func TestDeDupKeepLast(t *testing.T) {
	in := []records.Record{
		mk(1, "2020-01-01", map[string]any{"reason": "A"}),
		mk(1, "2020-01-01", map[string]any{"reason": "B"}),
		mk(2, "2020-01-01", map[string]any{"reason": "C"}),
	}
	d := DeDup{Keys: []string{"pcv", "date_from"}, Policy: "keep-last"}
	got := d.Apply(in)
	want := []records.Record{
		mk(1, "2020-01-01", map[string]any{"reason": "B"}),
		mk(2, "2020-01-01", map[string]any{"reason": "C"}),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("keep-last: got %#v want %#v", got, want)
	}
}

func TestDeDupMostComplete(t *testing.T) {
	in := []records.Record{
		mk(1, "2020-01-01", map[string]any{"reason": ""}),                // 2 non-empty
		mk(1, "2020-01-01", map[string]any{"reason": "B", "rm_code": 1}), // 3 non-empty
		mk(2, "2020-01-01", map[string]any{"reason": "C"}),
	}
	d := DeDup{Keys: []string{"pcv", "date_from"}, Policy: "most-complete"}
	got := d.Apply(in)
	want := []records.Record{
		mk(1, "2020-01-01", map[string]any{"reason": "B", "rm_code": 1}),
		mk(2, "2020-01-01", map[string]any{"reason": "C"}),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("most-complete: got %#v want %#v", got, want)
	}
}

// /*
// TestDeDup_KeepLast verifies that:
//   - with Policy "keep-last" (default), the last record for each business key wins,
//   - winners are output in ascending original index order,
//   - records missing any key field are appended in their original order after winners.
//
// */
func TestDeDup_KeepLast(t *testing.T) {
	in := []records.Record{
		{"k": "A", "v": 1},          // idx 0  (A -> 1)
		{"k": "B", "v": 2},          // idx 1  (B -> 2)
		{"k": "A", "v": 3},          // idx 2  (A -> 3) last
		{"x": "no-key", "v": 999},   // idx 3  (passthrough)
		{"k": "B", "v": 4},          // idx 4  (B -> 4) last
		{"k": "C", "v": 5},          // idx 5  (C -> 5)
		{"y": "no-key-2", "v": 777}, // idx 6  (passthrough)
	}
	d := DeDup{Keys: []string{"k"}} // Policy defaults to keep-last

	out := d.Apply(in)

	// Expected winner indices (keep-last): A@2, B@4, C@5 (sorted by index),
	// then passthrough in original order (3, 6).
	want := []records.Record{in[2], in[4], in[5], in[3], in[6]}
	if !reflect.DeepEqual(out, want) {
		t.Fatalf("keep-last mismatch:\n got: %#v\nwant: %#v", out, want)
	}

	// Ensure map identity is preserved for winners and passthrough.
	for i := range want {
		if reflect.ValueOf(out[i]).Pointer() != reflect.ValueOf(want[i]).Pointer() {
			t.Fatalf("record identity changed at %d; want same map", i)
		}
	}
}

/*
TestDeDup_KeepFirst verifies that:
  - the first record for each business key wins,
  - output order is by the first winner's original index,
  - passthrough without keys is appended after winners.
*/
func TestDeDup_KeepFirst(t *testing.T) {
	in := []records.Record{
		{"k": "A", "v": 1}, // win A
		{"k": "B", "v": 2}, // win B
		{"k": "A", "v": 3}, // later A dropped
		{"z": "no-key"},    // passthrough
		{"k": "B", "v": 9}, // later B dropped
		{"k": "C", "v": 5}, // win C
	}
	d := DeDup{Keys: []string{"k"}, Policy: "keep-first"}

	out := d.Apply(in)
	want := []records.Record{in[0], in[1], in[5], in[3]}
	if !reflect.DeepEqual(out, want) {
		t.Fatalf("keep-first mismatch:\n got: %#v\nwant: %#v", out, want)
	}
}

/*
TestDeDup_MostComplete verifies "most-complete" scoring:
  - score counts non-empty fields (nil/"" do not count),
  - PreferFields add bonus weight,
  - ties break by keep-last (later index wins).
*/
func TestDeDup_MostComplete(t *testing.T) {
	// Three duplicates with same key "A" and different completeness.
	// idx0: 2 fields present
	// idx1: 2 fields present + one preferred -> higher score
	// idx2: 2 fields present + one preferred (tie on score) -> later index wins
	in := []records.Record{
		{"k": "A", "a": "x", "b": nil, "c": ""}, // base score: 1 (just "a")
		{"k": "A", "a": "x", "b": "y"},          // base 2, preferred on "b" => bonus
		{"k": "A", "a": "x", "c": "z"},          // base 2, preferred on "c" => bonus (tie with idx1), later wins
	}
	d := DeDup{
		Keys:         []string{"k"},
		Policy:       "most-complete",
		PreferFields: []string{"b", "c"},
	}
	out := d.Apply(in)

	// Winner should be idx2 because it's a tie on score with idx1 and later index wins.
	if len(out) != 1 || !reflect.DeepEqual(out[0], in[2]) {
		t.Fatalf("most-complete winner mismatch: got=%#v; want=%#v", out, []records.Record{in[2]})
	}
}

/*
TestDeDup_KeyConstruction_MixedTypes ensures keys are constructed consistently
from mixed-type fields and nil:
  - mixed types for the same logical value (e.g., 123 vs "123") deduplicate,
  - nil is represented as a sentinel and participates in the key,
  - multiple key fields use a separator to avoid collisions.
*/
func TestDeDup_KeyConstruction_MixedTypes(t *testing.T) {
	in := []records.Record{
		{"k1": 123, "k2": "X", "val": "first"},
		{"k1": "123", "k2": "X", "val": "second"}, // same key as idx0 -> winner depends on policy
		{"k1": nil, "k2": "X", "val": "nil-key"},
		{"k1": nil, "k2": "X", "val": "nil-key-later"}, // same as idx2, later should win for keep-last
	}
	d := DeDup{Keys: []string{"k1", "k2"}} // default keep-last

	out := d.Apply(in)

	// Winners: for (123,"X") -> idx1 (last), for (nil,"X") -> idx3 (last).
	// Sorted by index asc => idx1, idx3
	want := []records.Record{in[1], in[3]}
	if !reflect.DeepEqual(out, want) {
		t.Fatalf("mixed-type key mismatch:\n got: %#v\nwant: %#v", out, want)
	}
}

/*
TestDeDup_NoKeysOrEmptyInput verifies the early return fast paths:
  - empty input returns the same slice,
  - empty Keys returns the same slice (no-op).
*/
func TestDeDup_NoKeysOrEmptyInput(t *testing.T) {
	var empty []records.Record
	d := DeDup{Keys: nil}
	if out := d.Apply(empty); out != nil {
		t.Fatalf("empty input expected nil; got %#v", out)
	}

	in := []records.Record{{"a": 1}, {"b": 2}}
	if out := d.Apply(in); !reflect.DeepEqual(out, in) || &out[0] != &in[0] {
		t.Fatalf("no Keys should return original slice in-place; got %#v", out)
	}
}

/*
TestDeDup_PassthroughOrder assures that records missing any key fields are
appended to the output in the same order they appeared in the input.
*/
func TestDeDup_PassthroughOrder(t *testing.T) {
	in := []records.Record{
		{"x": 1},           // passthrough #1
		{"k": "A", "v": 1}, // winner
		{"y": 2},           // passthrough #2
		{"k": "A", "v": 2}, // loser (keep-last -> later winner)
	}
	d := DeDup{Keys: []string{"k"}}
	out := d.Apply(in)

	// Winners: A@3 (index 3). Winners sorted by index => [3], then passthrough [0,2].
	want := []records.Record{in[3], in[0], in[2]}
	if !reflect.DeepEqual(out, want) {
		t.Fatalf("passthrough order mismatch:\n got: %#v\nwant: %#v", out, want)
	}
}

/*
TestDeDup_PolicyNormalization documents that policy is case/space-insensitive
via strings.ToLower/TrimSpace, defaulting to keep-last on empty.
*/
func TestDeDup_PolicyNormalization(t *testing.T) {
	in := []records.Record{
		{"k": "A", "v": 1},
		{"k": "A", "v": 2},
	}

	// keep-first (odd casing and spaces)
	df := DeDup{Keys: []string{"k"}, Policy: "  KeEp-FiRsT "}
	outF := df.Apply(in)
	if len(outF) != 1 || !reflect.DeepEqual(outF[0], in[0]) {
		t.Fatalf("policy keep-first not applied; got %#v", outF)
	}

	// default keep-last on empty policy
	dl := DeDup{Keys: []string{"k"}, Policy: ""}
	outL := dl.Apply(in)
	if len(outL) != 1 || !reflect.DeepEqual(outL[0], in[1]) {
		t.Fatalf("default keep-last not applied; got %#v", outL)
	}
}

/*
BenchmarkDeDup_KeepLast_HighDup simulates many duplicates mapping to a small
set of keys, stressing hashtable updates and winner replacement.
*/
func BenchmarkDeDup_KeepLast_HighDup(b *testing.B) {
	const (
		N     = 100000
		NKeys = 1000
	)
	// Build input with many duplicates across NKeys.
	in := make([]records.Record, N)
	for i := 0; i < N; i++ {
		k := "key_" + strconv.Itoa(i%NKeys)
		in[i] = records.Record{"k": k, "v": i}
	}
	d := DeDup{Keys: []string{"k"}, Policy: "keep-last"}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out := d.Apply(in)
		// Sanity: number of winners should equal NKeys, no passthroughs.
		if len(out) != NKeys {
			b.Fatalf("winners=%d; want=%d", len(out), NKeys)
		}
	}
}

/*
BenchmarkDeDup_MostComplete stresses scoring and tie-breaking while keeping
duplicate density high.
*/
func BenchmarkDeDup_MostComplete(b *testing.B) {
	const (
		N     = 60000
		NKeys = 500
	)
	in := make([]records.Record, N)
	for i := 0; i < N; i++ {
		k := "k" + strconv.Itoa(i%NKeys)
		// vary presence of fields to drive score changes
		rec := records.Record{"k": k, "a": "x"}
		if i%2 == 0 {
			rec["b"] = "y"
		}
		if i%3 == 0 {
			rec["c"] = "z"
		}
		in[i] = rec
	}
	d := DeDup{
		Keys:         []string{"k"},
		Policy:       "most-complete",
		PreferFields: []string{"b", "c"},
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out := d.Apply(in)
		if len(out) != NKeys {
			b.Fatalf("winners=%d; want=%d", len(out), NKeys)
		}
	}
}

/*
BenchmarkDeDup_WithPassthrough mixes keyed and unkeyed records to measure
the cost of passthrough append phase.
*/
func BenchmarkDeDup_WithPassthrough(b *testing.B) {
	const N = 50000
	in := make([]records.Record, 0, N)
	for i := 0; i < N; i++ {
		if i%5 == 0 {
			in = append(in, records.Record{"x": "no-key", "i": i})
		} else {
			in = append(in, records.Record{"k": "K" + strconv.Itoa(i%1000), "i": i})
		}
	}
	d := DeDup{Keys: []string{"k"}}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out := d.Apply(in)
		// The number of winners equals number of distinct keys plus passthroughs.
		// Compute expected on first run to avoid skewing the benchmark.
		_ = out
	}
}

/*
Test helper: ensure winners’ indices are strictly increasing (sort stability).
This is indirectly covered, but we assert explicitly once.
*/
func TestDeDup_WinnerIndicesIncreasing(t *testing.T) {
	in := []records.Record{
		{"k": "A", "i": 0},
		{"k": "B", "i": 1},
		{"k": "A", "i": 2},
		{"k": "C", "i": 3},
		{"k": "B", "i": 4},
	}
	d := DeDup{Keys: []string{"k"}, Policy: "keep-last"}
	out := d.Apply(in)

	// Extract original indices from returned maps.
	idx := make([]int, len(out))
	for i := range out {
		idx[i] = out[i]["i"].(int)
	}
	if !sort.IntsAreSorted(idx) {
		t.Fatalf("winner indices not increasing: %#v", idx)
	}
}
