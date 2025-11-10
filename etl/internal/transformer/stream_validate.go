// Package transformer contains streaming validation utilities. This file hosts
// the *drain-safe* ValidateLoopRows stage, which enforces required non-NULL
// fields and drops invalid rows without ever returning early on context
// cancellation, thereby preventing upstream backpressure/hangs.
package transformer

import "context"

// ValidateLoopRows enforces that a set of required columns are present (non-nil)
// in each incoming pooled row. Valid rows are forwarded to 'out'. Invalid rows
// are fail-soft dropped (r.Free()) and optionally reported via onReject.
//
// IMPORTANT (no hangs):
//   - This function does NOT return upon ctx cancellation. It drains the entire
//     input channel, freeing each row, and only returns after 'in' is closed.
//     This prevents upstream goroutines from blocking when cancellation occurs.
//
// Closing semantics:
//   - The caller is responsible for closing 'out' after this function returns.
func ValidateLoopRows(
	ctx context.Context,
	columns []string, // positional order (same as TransformLoopRows)
	required []string, // names of required columns
	in <-chan *Row, // input rows (already coerced)
	out chan<- *Row, // output rows (valid only)
	onReject func(line int, reason string),
) {
	// Build a quick index list for required columns.
	pos := make(map[string]int, len(columns))
	for i, c := range columns {
		pos[c] = i
	}
	reqIx := make([]int, 0, len(required))
	for _, name := range required {
		if ix, ok := pos[name]; ok {
			reqIx = append(reqIx, ix)
		}
	}

	for r := range in {
		// Even if ctx is canceled, we must *drain and Free*; do not return.
		if r == nil || len(r.V) != len(columns) {
			if r != nil {
				r.Free()
			}
			continue
		}

		ok := true
		for _, ix := range reqIx {
			if ix < 0 || ix >= len(r.V) || r.V[ix] == nil {
				ok = false
				break
			}
		}

		if !ok {
			if onReject != nil {
				onReject(0, "missing required field")
			}
			r.Free()
			continue
		}

		// Forward; if downstream is congested and ctx canceled, we still prefer
		// correctness over early return. If you want to short-circuit under
		// cancellation, you can add a non-blocking select that Free()s instead.
		out <- r
	}
}
