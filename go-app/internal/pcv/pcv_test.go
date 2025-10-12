package pcv

import (
	"regexp"
	"strings"
	"testing"
)

var digitsOnly = regexp.MustCompile(`^\d+$`)

// TestFindPCVIndex_Table verifies that FindPCVIndex returns the index of the
// "PČV" header (exact match after TrimSpace), or -1 when absent. This table-
// driven form makes it trivial to add more header variants later.
func TestFindPCVIndex_Table(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		headers []string
		want    int
	}{
		{"found_exact", []string{"A", "PČV", "C"}, 1},
		{"found_trimmed", []string{"A", "  PČV  ", "C"}, 1},
		{"not_found", []string{"A", "B", "C"}, -1},
		{"empty_headers", nil, -1},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := FindPCVIndex(tc.headers); got != tc.want {
				t.Fatalf("FindPCVIndex(%v) = %d, want %d", tc.headers, got, tc.want)
			}
		})
	}
}

// TestExtractPCV_Table exercises ExtractPCV’s full decision tree with compact,
// independent cases so maintainers can extend coverage by adding rows.
//
// Covered behaviors:
//  1. Direct-by-index (aligned).
//  2. Tail-aligned index (lengths differ).
//  3. Status-based scan (aligned) including no-match fallthrough in tryFrom.
//  4. Status-based scan (tail-aligned) including both success and no-match.
//  5. Last-resort tail scan.
//  6. No match anywhere.
//  7. tryFrom out-of-range (si < 0 || si >= len(fields)) early return.
func TestExtractPCV_Table(t *testing.T) {
	t.Parallel()

	type in struct {
		headers   []string
		fields    []string
		pcvIdx    int
		statusIdx int
	}
	type out struct {
		val int64
		str string
	}

	cases := []struct {
		name string
		in   in
		want out
	}{
		{
			name: "direct_by_index_success",
			in:   in{headers: []string{"PČV", "X"}, fields: []string{"12345", "x"}, pcvIdx: 0, statusIdx: -1},
			want: out{val: 12345, str: "12345"},
		},
		{
			name: "direct_by_index_nondigit",
			in:   in{headers: []string{"PČV"}, fields: []string{"ABC"}, pcvIdx: 0, statusIdx: -1},
			want: out{val: 0, str: ""},
		},
		{
			name: "tail_aligned_success_fields_longer_than_headers",
			in:   in{headers: []string{"A", "B", "PČV", "D"}, fields: []string{"A", "B", "X", "Y", "123456", "Z"}, pcvIdx: 2, statusIdx: -1},
			want: out{val: 123456, str: "123456"},
		},
		{
			name: "tail_aligned_index_out_of_range",
			in:   in{headers: []string{"A", "PČV"}, fields: []string{"onlyone"}, pcvIdx: 1, statusIdx: -1},
			want: out{val: 0, str: ""},
		},
		{
			// Covers tryFrom’s loop producing no matches (-> return 0,"",false)
			// because candidate tokens are too short (<5) or non-digit.
			name: "status_right_scan_no_match_fallthrough",
			in:   in{headers: []string{"Status", "Other"}, fields: []string{"STAT", "1234", "abcd", "   "}, pcvIdx: -1, statusIdx: 0},
			want: out{val: 0, str: ""},
		},
		{
			// Covers tryFrom’s success path when aligned (len >= 5).
			name: "status_right_scan_success_len_ge_5",
			in:   in{headers: []string{"Status", "Other"}, fields: []string{"STAT", "00001", "garbage"}, pcvIdx: -1, statusIdx: 0},
			want: out{val: 1, str: "00001"},
		},
		{
			// Explicitly covers the early out-of-range check inside tryFrom:
			// statusIdx equals len(fields) triggers si >= len(fields).
			name: "status_tryFrom_out_of_range_branch",
			in:   in{headers: []string{"Status"}, fields: []string{"only"}, pcvIdx: -1, statusIdx: 1},
			want: out{val: 0, str: ""},
		},
		{
			// Covers the tail-aligned Status block (lengths differ) with success.
			name: "status_tail_aligned_success_when_lengths_differ",
			in: in{
				headers:   []string{"H1", "Status", "H3", "H4"},
				fields:    []string{"X", "Y", "Z", "W", "00002"}, // longer row; PCV sits just after tail-aligned status
				pcvIdx:    -1,
				statusIdx: 1,
			},
			want: out{val: 2, str: "00002"},
		},
		{
			// Covers the tail-aligned Status block when invoked but still no match,
			// to ensure the “third block” code path executes even on failure.
			name: "status_tail_aligned_no_match_when_lengths_differ",
			in: in{
				headers:   []string{"H1", "Status", "H3", "H4"},
				fields:    []string{"X", "Y", "Z", "W", "abcd", "1234"}, // tokens exist but invalid (non-digit/len<5)
				pcvIdx:    -1,
				statusIdx: 1,
			},
			want: out{val: 0, str: ""},
		},
		{
			name: "last_resort_tail_scan_len_ge_6",
			in:   in{headers: []string{"H"}, fields: []string{"a", "b", "c", "999999"}, pcvIdx: -1, statusIdx: -1},
			want: out{val: 999999, str: "999999"},
		},
		{
			name: "no_match_anywhere",
			in:   in{headers: []string{"H1", "H2"}, fields: []string{"abc", "def", "ghi"}, pcvIdx: -1, statusIdx: -1},
			want: out{val: 0, str: ""},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			gotV, gotS := ExtractPCV(tc.in.headers, tc.in.fields, tc.in.pcvIdx, tc.in.statusIdx, digitsOnly)
			if gotV != tc.want.val || gotS != tc.want.str {
				t.Fatalf("ExtractPCV got (%d,%q), want (%d,%q)", gotV, gotS, tc.want.val, tc.want.str)
			}
		})
	}
}

// TestExtractPCVWithRSVFallback_Table validates the RSV fallback behavior.
// It includes edge cases around the ",RSV," anchor, minimal part counts,
// digit validation, quoted/space-padded candidates, and overflow parse errors.
// The first case demonstrates that a successful ExtractPCV result takes
// precedence over the fallback (the fallback is bypassed entirely).
func TestExtractPCVWithRSVFallback_Table(t *testing.T) {
	t.Parallel()

	type in struct {
		headers   []string
		fields    []string
		pcvIdx    int
		statusIdx int
		raw       string
	}
	type out struct {
		val int64
		str string
	}

	veryBig := func() string {
		// > 19 digits → guaranteed int64 overflow.
		return "9" + strings.Repeat("0", 19)
	}

	cases := []struct {
		name string
		in   in
		want out
	}{
		{
			name: "bypass_fallback_when_extractpcv_succeeds",
			in: in{
				headers:   []string{"PČV", "X"},
				fields:    []string{"77777", "x"},
				pcvIdx:    0,
				statusIdx: -1,
				raw:       ",RSV,x,12345,tail", // fallback value must be ignored
			},
			want: out{val: 77777, str: "77777"},
		},
		{
			name: "anchor_missing_returns_zero",
			in:   in{headers: []string{"H"}, fields: []string{"nope"}, pcvIdx: -1, statusIdx: -1, raw: "no RSV here"},
			want: out{val: 0, str: ""},
		},
		{
			name: "parts_less_than_4_returns_zero",
			in:   in{headers: []string{"H"}, fields: []string{"nope"}, pcvIdx: -1, statusIdx: -1, raw: ",RSV,x"},
			want: out{val: 0, str: ""},
		},
		{
			name: "non_digit_candidate_returns_zero",
			in:   in{headers: []string{"H"}, fields: []string{"nope"}, pcvIdx: -1, statusIdx: -1, raw: ",RSV,x,ABCD,tail"},
			want: out{val: 0, str: ""},
		},
		{
			name: "quoted_and_spaced_digits_ok",
			in:   in{headers: []string{"H"}, fields: []string{"nope"}, pcvIdx: -1, statusIdx: -1, raw: `,RSV,x,  "00123"  ,tail`},
			want: out{val: 123, str: "00123"},
		},
		{
			name: "parse_overflow_returns_zero",
			in:   in{headers: []string{"H"}, fields: []string{"nope"}, pcvIdx: -1, statusIdx: -1, raw: ",RSV,x," + veryBig() + ",tail"},
			want: out{val: 0, str: ""},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			gotV, gotS := ExtractPCVWithRSVFallback(tc.in.headers, tc.in.fields, tc.in.pcvIdx, tc.in.statusIdx, digitsOnly, tc.in.raw)
			if gotV != tc.want.val || gotS != tc.want.str {
				t.Fatalf("ExtractPCVWithRSVFallback got (%d,%q), want (%d,%q)", gotV, gotS, tc.want.val, tc.want.str)
			}
		})
	}
}
