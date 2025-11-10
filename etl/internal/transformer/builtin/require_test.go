package builtin

import (
	"reflect"
	"testing"

	"etl/pkg/records"
)

/*
TestRequireApply_Table covers core filtering semantics:

  - A record is kept only if all required fields exist AND are non-nil AND,
    when the value is a string, it is non-empty ("").
  - Non-string values (e.g., 0, false) are considered present and non-empty.
  - The output preserves order of surviving records.
  - Filtering is done in-place (resliced), confirmed by low allocations and
    preserved record map identities.
*/
func TestRequireApply_Table(t *testing.T) {
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
				{"a": "x"},                        // miss b -> drop
				{"a": "x", "b": "y"},              // keep
				{"a": "x", "b": ""},               // empty string -> drop
				{"a": nil, "b": "y"},              // nil -> drop
				{"a": "x", "b": 0},                // non-string zero -> keep
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

			// Copy for expected content comparison.
			inCopy := append([]records.Record(nil), tc.in...)

			out := req.Apply(tc.in)

			// Build expected slice by referencing original record maps.
			var want []records.Record
			for _, i := range tc.wantIdx {
				want = append(want, inCopy[i])
			}
			if !reflect.DeepEqual(out, want) {
				t.Fatalf("Apply output mismatch:\n got: %#v\nwant: %#v", out, want)
			}

			// Allocation check: Apply should be near-zero alloc in steady state.
			allocs := testing.AllocsPerRun(500, func() {
				_ = req.Apply(tc.in)
			})
			if allocs > 0.10 {
				t.Fatalf("Apply allocs/op = %.2f; want <= 0.10", allocs)
			}

			// Ensure record maps are unchanged (identity match).
			for j := range out {
				if reflect.ValueOf(out[j]).Pointer() != reflect.ValueOf(want[j]).Pointer() {
					t.Fatalf("record map identity changed at %d; want in-place", j)
				}
			}
		})
	}
}

/*
TestRequireApply_EmptyInputs verifies correct behavior for nil/empty inputs and
empty field lists.
*/
func TestRequireApply_EmptyInputs(t *testing.T) {
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
