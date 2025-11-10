// Package transformer provides streaming, allocation-conscious transforms.
// This file defines a pooled Row type used across parser → transformer → loader
// to significantly reduce heap churn and GC pressure.
package transformer

import "sync"

// Row is a pooled container holding a positional row for database COPY.
//
// Contract:
//   - The owner goroutine writes into r.V[0:colCount] (no re-slice growth).
//   - After the row has been successfully persisted, the loader **must**
//     call r.Free() to return it to the pool.
//   - Do not retain references to r or r.V beyond the owning stage.
//
// We keep V as []any to feed pgx CopyFromRows directly.
type Row struct {
	V []any
}

var rowPool sync.Pool

// GetRow returns a pooled Row with capacity for colCount fields and length set
// to colCount. All elements are zeroed for safety.
func GetRow(colCount int) *Row {
	if v := rowPool.Get(); v != nil {
		r := v.(*Row)
		// Ensure capacity; grow only rarely.
		if cap(r.V) < colCount {
			r.V = make([]any, colCount)
		}
		r.V = r.V[:colCount]
		for i := range r.V {
			r.V[i] = nil
		}
		return r
	}
	return &Row{V: make([]any, colCount)}
}

// Free returns the Row to the pool. The caller must not use r after Free().
func (r *Row) Free() {
	// Optional: shrink very large rows; omitted for speed.
	rowPool.Put(r)
}
