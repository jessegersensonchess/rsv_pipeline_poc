package ints

import (
	"strconv"
	"strings"
	"testing"
)

// TestExtractInts_Basic exercises the core behavior of ExtractInts on a variety
// of simple inputs. The goal is to document and lock in the intended behavior:
//
//   - Return all contiguous digit runs as integers.
//   - Treat non-digit characters as separators.
//   - Return (nil, nil) when there are no numbers.
//   - Return (nil, nil) on parse error (e.g., overflow), matching the existing
//     "fail soft" semantics used by higher-level ETL code.
func TestExtractInts_Basic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want []int
	}{
		{
			name: "empty string yields nil slice",
			in:   "",
			want: nil,
		},
		{
			name: "no digits yields nil slice",
			in:   "abc def",
			want: nil,
		},
		{
			name: "single number embedded in text",
			in:   "id=123",
			want: []int{123},
		},
		{
			name: "multiple numbers separated by letters and spaces",
			in:   "a12b34c 56",
			want: []int{12, 34, 56},
		},
		{
			name: "leading and trailing digits",
			in:   "123abc456",
			want: []int{123, 456},
		},
		{
			name: "digits separated by punctuation",
			in:   "n:42,m:7;z=99",
			want: []int{42, 7, 99},
		},
	}

	for _, tt := range tests {
		tt := tt // capture range variable
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ExtractInts(tt.in)
			// For all these cases, we expect no error.
			if err != nil {
				t.Fatalf("ExtractInts(%q) error = %v, want nil", tt.in, err)
			}
			if !equalIntSlices(got, tt.want) {
				t.Fatalf("ExtractInts(%q) = %#v, want %#v", tt.in, got, tt.want)
			}
		})
	}
}

// TestExtractInts_NoDigitsExplicit documents that ExtractInts returns (nil, nil)
// when no digits are present at all. This is important to distinguish from the
// case where an empty slice "means something" in callers; here we choose the
// simpler nil to represent "no numbers".
func TestExtractInts_NoDigitsExplicit(t *testing.T) {
	t.Parallel()

	got, err := ExtractInts("no numbers here")
	if err != nil {
		t.Fatalf("ExtractInts error = %v, want nil", err)
	}
	if got != nil {
		t.Fatalf("ExtractInts should return nil slice when no numbers, got %#v", got)
	}
}

// TestExtractInts_OutOfRange verifies the "fail soft" behavior when a digit
// run cannot be converted to int (for example, because it is too large).
//
// The contract: we do *not* bubble up the strconv.Atoi error; instead we
// return (nil, nil) so callers can treat the entire line as unusable if they
// wish, without having to worry about error handling at every call site.
func TestExtractInts_OutOfRange(t *testing.T) {
	t.Parallel()

	// Construct a numeric string that should overflow int on most platforms by
	// repeating a large digit multiple times. strconv.Atoi should return an
	// error for this value.
	tooBig := strings.Repeat("9", 40) // 40-digit number >> max int64
	input := "prefix " + tooBig + " suffix"

	got, err := ExtractInts(input)

	// By design, we do not propagate the parse error, so err should be nil.
	if err != nil {
		t.Fatalf("ExtractInts(%q) returned non-nil error = %v; want nil (fail-soft)", input, err)
	}
	// On parse error, the function returns nil, nil to signal "invalid line".
	if got != nil {
		t.Fatalf("ExtractInts(%q) = %#v, want nil on parse error", input, got)
	}
}

// equalIntSlices is a small helper for slice comparison. We avoid reflect.DeepEqual
// here to keep the tests focused and explicit about what we compare.
func equalIntSlices(a, b []int) bool {
	if a == nil && b == nil {
		return true
	}
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if b[i] != v {
			return false
		}
	}
	return true
}

// BenchmarkExtractInts_Short measures ExtractInts on a short, typical line of
// text containing a few numbers. This helps catch regressions in the hot path
// where we parse IDs from log lines or CSV fields.
func BenchmarkExtractInts_Short(b *testing.B) {
	line := "user=123 action=42 latency_ms=987"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ExtractInts(line)
	}
}

// BenchmarkExtractInts_Long measures ExtractInts on a longer string with more
// numbers, approximating a worst-case-ish scenario in ETL logs or large text
// fields.
func BenchmarkExtractInts_Long(b *testing.B) {
	// Build a line with many numeric fields to stress the parser.
	var sb strings.Builder
	for i := 0; i < 50; i++ {
		if i > 0 {
			sb.WriteByte(' ')
		}
		sb.WriteString("id=")
		sb.WriteString(strconv.Itoa(i))
		sb.WriteString(" value=")
		sb.WriteString(strconv.Itoa(i * 10))
	}
	line := sb.String()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ExtractInts(line)
	}
}
