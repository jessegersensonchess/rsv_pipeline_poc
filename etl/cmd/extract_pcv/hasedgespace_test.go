// hasedgespace_test.go contains unit tests for the HasEdgeSpace helper
// function. HasEdgeSpace is a micro-optimization that performs O(1) byte
// checks on the first and last characters of a string to determine whether
// the string begins or ends with common ASCII whitespace.
//
// The tests verify that the function correctly identifies leading and
// trailing whitespace without requiring a full scan of the string.
package main

import "testing"

/*
TestHasEdgeSpace verifies the behavior of HasEdgeSpace across a variety of
input strings, including empty strings, strings without whitespace, and
strings with whitespace at the beginning or end.

The goal is to ensure that HasEdgeSpace is a faithful and efficient proxy
for detecting whether trimming might be necessary, without producing false
positives or negatives.
*/
func TestHasEdgeSpace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want bool
	}{
		{
			name: "empty_string",
			in:   "",
			want: false,
		},
		{
			name: "no_whitespace",
			in:   "abc",
			want: false,
		},
		{
			name: "leading_space",
			in:   " abc",
			want: true,
		},
		{
			name: "trailing_space",
			in:   "abc ",
			want: true,
		},
		{
			name: "leading_and_trailing_space",
			in:   " abc ",
			want: true,
		},
		{
			name: "leading_tab",
			in:   "\tabc",
			want: true,
		},
		{
			name: "trailing_newline",
			in:   "abc\n",
			want: true,
		},
	}

	for _, tt := range tests {
		tt := tt // capture
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := HasEdgeSpace(tt.in)
			if got != tt.want {
				t.Fatalf("HasEdgeSpace(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}
