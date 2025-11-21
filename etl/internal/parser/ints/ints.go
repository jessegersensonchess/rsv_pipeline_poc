// Package ints provides helpers for extracting integer values from text lines.
// It is intended for ETL tasks where free-form text fields may contain
// embedded numeric identifiers.
package ints

import (
	"strconv"
	"unicode"
)

// ExtractInts scans s and returns all contiguous digit sequences as integers.
//
// It treats any run of Unicode digits as a number, converts it via strconv.Atoi,
// and appends it to the result slice. Non-digit characters separate numbers.
//
// On conversion error (e.g., out-of-range), it returns nil, nil to signal that
// the caller may want to drop the line entirely. This matches existing behavior
// in some ETL tools that prefer "fail soft on malformed numbers".
func ExtractInts(s string) ([]int, error) {
	var out []int
	var current []rune

	for _, r := range s {
		if unicode.IsDigit(r) {
			current = append(current, r)
		} else {
			if len(current) > 0 {
				n, err := parseRun(current)
				if err != nil {
					return nil, nil
				}
				out = append(out, n)
				current = nil
			}
		}
	}

	// Trailing number at end of string.
	if len(current) > 0 {
		n, err := parseRun(current)
		if err != nil {
			return nil, nil
		}
		out = append(out, n)
	}

	return out, nil
}

func parseRun(run []rune) (int, error) {
	numStr := string(run)
	return strconv.Atoi(numStr)
}
