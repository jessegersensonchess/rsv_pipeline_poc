package builtin

import (
	"strings"

	"etl/pkg/records"
)

// nbspace is U+00A0 NO-BREAK SPACE.
const nbspace = "\u00A0"

type Normalize struct{}

// Apply normalizes top-level string fields in-place:
//   - replace NBSP (U+00A0) with ASCII space
//   - trim leading/trailing ASCII whitespace
//
// Work-avoidance optimizations:
//   - Only call ReplaceAll if the string actually contains NBSP.
//   - Only call TrimSpace if there is obvious edge whitespace.
//   - Skip the map write if the value is unchanged.
func (Normalize) Apply(in []records.Record) []records.Record {
	for _, r := range in {
		for k, v := range r {
			s, ok := v.(string)
			if !ok {
				continue
			}
			orig := s

			// Replace NBSP only if present.
			if strings.Contains(s, nbspace) {
				s = strings.ReplaceAll(s, nbspace, " ")
			}

			// Trim only if edges look like whitespace.
			if HasEdgeSpace(s) {
				s = strings.TrimSpace(s)
			}

			// Avoid map write if unchanged (saves hash/write barrier work).
			if s != orig {
				r[k] = s
			}
		}
	}
	return in
}

// HasEdgeSpace reports whether s starts or ends with common ASCII whitespace.
// Fast O(1) byte checks; no allocations.
func HasEdgeSpace(s string) bool {
	n := len(s)
	if n == 0 {
		return false
	}
	b0, b1 := s[0], s[n-1]
	return b0 == ' ' || b0 == '\t' || b0 == '\n' || b0 == '\r' ||
		b1 == ' ' || b1 == '\t' || b1 == '\n' || b1 == '\r'
}
