package builtin

import (
	"reflect"
	"testing"

	"etl/pkg/records"
)

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
