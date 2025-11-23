package builtin

import (
	"reflect"
	"testing"

	"etl/pkg/records"
)

/*
TestNormalizeApply_TableDriven verifies the core normalization semantics of
Normalize.Apply:

  - Replaces U+00A0 NO-BREAK SPACE (NBSP) with ASCII space.
  - Trims leading/trailing ASCII whitespace (space, tab, LF, CR) when present.
  - Leaves non-string values unchanged.
  - Applies changes in place (record maps are mutated, slice is reused).
*/
func TestNormalizeApply_TableDriven(t *testing.T) {
	tests := []struct {
		name string
		in   []records.Record
		want []records.Record
	}{
		{
			name: "no_strings_no_change",
			in: []records.Record{
				{"a": 1, "b": true, "c": nil},
			},
			want: []records.Record{
				{"a": 1, "b": true, "c": nil},
			},
		},
		{
			name: "simple_trim_spaces",
			in: []records.Record{
				{"a": " foo ", "b": "\tbar\n"},
			},
			want: []records.Record{
				{"a": "foo", "b": "bar"},
			},
		},
		{
			name: "nbsp_replaced_and_trimmed",
			in: []records.Record{
				{
					// " NBSP foo NBSP "
					"a": " " + nbspace + "foo" + nbspace + " ",
				},
			},
			want: []records.Record{
				{"a": "foo"},
			},
		},
		{
			name: "nbsp_internal_only_not_trimmed",
			in: []records.Record{
				{"a": "foo" + nbspace + "bar"},
			},
			want: []records.Record{
				{"a": "foo bar"}, // NBSP → space, no edge whitespace
			},
		},
		{
			name: "mixed_types_partial_changes",
			in: []records.Record{
				{
					"a": " foo ",          // trim only
					"b": "bar" + nbspace,  // NBSP at end -> replace + trim
					"c": 42,               // unchanged (non-string)
					"d": nil,              // unchanged
					"e": "baz",            // unchanged
					"f": "\nqux\r",        // trim
					"g": nbspace + "x",    // NBSP at start -> replace + trim
					"h": "x" + nbspace,    // NBSP at end -> replace + trim
					"i": nbspace + " y  ", // NBSP + spaces -> replace + trim
				},
			},
			want: []records.Record{
				{
					"a": "foo",
					"b": "bar",
					"c": 42,
					"d": nil,
					"e": "baz",
					"f": "qux",
					"g": "x",
					"h": "x",
					"i": "y",
				},
			},
		},
		{
			name: "multiple_records_independent",
			in: []records.Record{
				{"a": " foo ", "b": "bar"},
				{"a": "baz", "b": nbspace + "qux" + nbspace},
			},
			want: []records.Record{
				{"a": "foo", "b": "bar"},
				{"a": "baz", "b": "qux"},
			},
		},
		{
			name: "unchanged_strings_skipped",
			in: []records.Record{
				{"a": "foo", "b": "bar"},
			},
			want: []records.Record{
				{"a": "foo", "b": "bar"},
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Capture original map identities to verify in-place mutation of maps.
			origMapPtrs := make([]uintptr, len(tc.in))
			for i := range tc.in {
				origMapPtrs[i] = reflect.ValueOf(tc.in[i]).Pointer()
			}

			// Capture address of first element for in-place slice check.
			var firstOrigElem *records.Record
			if len(tc.in) > 0 {
				firstOrigElem = &tc.in[0]
			}

			out := Normalize{}.Apply(tc.in)

			// Verify content equals expected.
			if !reflect.DeepEqual(out, tc.want) {
				t.Fatalf("Normalize.Apply() mismatch:\n got: %#v\nwant: %#v", out, tc.want)
			}

			// Verify slice is reused when non-empty (no reallocation).
			if len(out) > 0 && firstOrigElem != nil {
				if &out[0] != firstOrigElem {
					t.Fatalf("Normalize.Apply did not operate on the original slice: &out[0] != &in[0]")
				}
			}

			// Verify record maps are the same identity (mutated in place).
			for i := range out {
				gotPtr := reflect.ValueOf(out[i]).Pointer()
				if gotPtr != origMapPtrs[i] {
					t.Fatalf("record map identity changed at index %d; want in-place mutation", i)
				}
			}
		})
	}
}

/*
TestNormalizeApply_EmptyInputs verifies behavior for nil and empty slices.

Normalize.Apply should:
  - Return nil when given a nil slice.
  - Return an empty slice unchanged when given an empty slice.
*/
func TestNormalizeApply_EmptyInputs(t *testing.T) {
	var nilSlice []records.Record
	if got := (Normalize{}).Apply(nilSlice); got != nil {
		t.Fatalf("Normalize.Apply(nil) = %#v; want nil", got)
	}

	empty := []records.Record{}
	if got := (Normalize{}).Apply(empty); got == nil || len(got) != 0 {
		t.Fatalf("Normalize.Apply(empty) = %#v; want empty slice", got)
	}
}

/*
TestHasEdgeSpace verifies that HasEdgeSpace detects leading/trailing ASCII
whitespace and ignores interior-only whitespace.
*/
func TestHasEdgeSpace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want bool
	}{
		{name: "empty", in: "", want: false},
		{name: "no_spaces", in: "foo", want: false},
		{name: "leading_space", in: " foo", want: true},
		{name: "trailing_space", in: "foo ", want: true},
		{name: "both_spaces", in: " foo ", want: true},
		{name: "internal_space_only", in: "f oo", want: false},
		{name: "leading_tab", in: "\tfoo", want: true},
		{name: "trailing_tab", in: "foo\t", want: true},
		{name: "leading_newline", in: "\nfoo", want: true},
		{name: "trailing_newline", in: "foo\n", want: true},
		{name: "leading_carriage_return", in: "\rfoo", want: true},
		{name: "trailing_carriage_return", in: "foo\r", want: true},
		{name: "internal_tab_only", in: "f\too", want: false},
		{name: "single_space", in: " ", want: true},
		{name: "single_tab", in: "\t", want: true},
		{name: "single_newline", in: "\n", want: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := HasEdgeSpace(tt.in)
			if got != tt.want {
				t.Fatalf("HasEdgeSpace(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

/*
BenchmarkHasEdgeSpace measures the cost of checking for leading/trailing
whitespace on strings with and without edge whitespace.
*/
func BenchmarkHasEdgeSpace(b *testing.B) {
	cases := []string{
		"foo",         // no edges
		" foo",        // leading
		"foo ",        // trailing
		"\tfoo\n",     // both
		"f oo",        // internal only
		"     foo",    // multiple leading
		"foo     ",    // multiple trailing
		"\r\nfoo\r\n", // newlines/CR at edges
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = HasEdgeSpace(cases[i%len(cases)])
	}
}

/*
BenchmarkNormalizeApply measures the performance of Normalize.Apply for a batch
of records containing a mix of strings with NBSP, edge whitespace, and
already-normalized values.
*/
func BenchmarkNormalizeApply(b *testing.B) {
	recordsBatch := []records.Record{
		{"a": " foo ", "b": "bar", "c": "baz" + nbspace + "qux"},
		{"a": nbspace + "x", "b": "y" + nbspace, "c": "z"},
		{"a": "nochange", "b": "another", "c": 123},
		{"a": "\tleading", "b": "trailing\n", "c": nil},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Normalize{}.Apply(recordsBatch)
	}
}

/*
TestRequireApply_TableA covers core filtering semantics:

  - A record is kept only if all required fields exist AND are non-nil AND,
    when the value is a string, it is non-empty ("").
  - Non-string values (e.g., 0, false) are considered present and non-empty
    by the current implementation.
  - The output preserves the relative order of surviving records.
  - The function filters **in place** by reslicing the input slice.
*/
func TestRequireApply_TableA(t *testing.T) {
	tests := []struct {
		name    string
		fields  []string
		in      []records.Record
		wantIdx []int // indices from 'in' that should survive, in order
	}{
		{
			name:   "single_required_all_present",
			fields: []string{"a"},
			in: []records.Record{
				{"a": "x"},
				{"a": "y"},
			},
			wantIdx: []int{0, 1},
		},
		{
			name:   "single_required_missing_dropped",
			fields: []string{"a"},
			in: []records.Record{
				{"b": "x"},     // missing 'a' -> drop
				{"a": "ok"},    // keep
				{"a": ""},      // empty string -> drop
				{"a": nil},     // nil -> drop
				{"a": "value"}, // keep
			},
			wantIdx: []int{1, 4},
		},
		{
			name:   "multi_required_all_must_exist",
			fields: []string{"a", "b"},
			in: []records.Record{
				{"a": "x"},                        // missing b -> drop
				{"a": "x", "b": "y"},              // keep
				{"a": "x", "b": ""},               // empty string -> drop
				{"a": nil, "b": "y"},              // nil -> drop
				{"a": "x", "b": 0},                // non-string zero value -> keep
				{"a": "x", "b": false},            // non-string false -> keep
				{"a": "x", "b": interface{}(nil)}, // typed nil -> drop
			},
			wantIdx: []int{1, 4, 5},
		},
		{
			name:   "no_required_fields_keep_all",
			fields: nil,
			in: []records.Record{
				{"a": nil},
				{"b": ""},
				{},
			},
			wantIdx: []int{0, 1, 2},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			req := Require{Fields: tc.fields}

			// Copy for expected-content comparison (not for address checks).
			inCopy := append([]records.Record(nil), tc.in...)

			// Capture the address of the first element of the ORIGINAL slice,
			// so we can verify in-place reslicing if index 0 survives.
			var firstOrigAddr *records.Record
			if len(tc.in) > 0 {
				firstOrigAddr = &tc.in[0]
			}

			out := req.Apply(tc.in)

			// Build expected slice from the original-order maps.
			var want []records.Record
			for _, i := range tc.wantIdx {
				want = append(want, inCopy[i])
			}
			if !reflect.DeepEqual(out, want) {
				t.Fatalf("Apply output mismatch:\n got: %#v\nwant: %#v", out, want)
			}

			// In-place filtering check:
			// If at least one element survives and the first surviving index is 0,
			// then &out[0] should equal the address of the ORIGINAL first element.
			if len(out) > 0 && len(tc.wantIdx) > 0 && tc.wantIdx[0] == 0 && firstOrigAddr != nil {
				if &out[0] != firstOrigAddr {
					t.Fatalf("Apply did not reslice in place: &out[0] != &original[0]")
				}
			}

			// Ensure it did not allocate new record maps (same identities).
			for j := range out {
				if reflect.ValueOf(out[j]).Pointer() != reflect.ValueOf(want[j]).Pointer() {
					t.Fatalf("record map identity changed at %d; want in-place", j)
				}
			}
		})
	}
}

/*
TestRequireApply_EmptyInputsA verifies behavior for nil/empty input slices
and empty field lists.
*/
func TestRequireApply_EmptyInputsA(t *testing.T) {
	req := Require{Fields: []string{"a"}}

	var nilSlice []records.Record
	if got := req.Apply(nilSlice); got != nil {
		t.Fatalf("Apply(nil) = %#v; want nil", got)
	}

	empty := []records.Record{}
	if got := req.Apply(empty); got == nil || len(got) != 0 {
		t.Fatalf("Apply(empty) = %#v; want empty slice", got)
	}

	keepAll := Require{} // no required fields
	in := []records.Record{{}, {}, {}}
	if got := keepAll.Apply(in); len(got) != 3 {
		t.Fatalf("Apply with no fields: kept %d; want 3", len(got))
	}
}
