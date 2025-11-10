// Package storage contains storage-agnostic contracts and utilities.
// This file implements a batched loader for *pooled* CSV rows, which avoids
// extra allocations by passing [][]any views of the pooled rows to the backend.
//
// Logging: on every successful flush, a concise progress line is emitted with
// running totals and instantaneous rows/sec since the previous flush. Logging
// here is intentionally light and synchronous; if you need structured or
// rate-limited logging, wrap this function at the call site.
package storage

import (
	"context"
	"fmt"
	"log"
	"time"
)

// RowCopyFn matches repository.CopyFrom but takes [][]any; the loader will
// extract [][]any from pooled rows before calling the backend.
type RowCopyFn func(ctx context.Context, columns []string, rows [][]any) (int64, error)

// RowLike is a tiny adapter to avoid import cycles here. In your codebase, you
// can import transformer.Row directly and delete RowLike entirely.
type RowLike struct {
	V        []any  // positional values aligned to target columns
	FreeFunc func() // returns the row to the pool; must be safe to call once
}

// Free returns the row to its pool when FreeFunc is set.
func (r *RowLike) Free() {
	if r != nil && r.FreeFunc != nil {
		r.FreeFunc()
	}
}

// LoadBatchesRows consumes pooled rows, builds [][]any batches, invokes copyFn,
// and after a flush returns each row to the pool via Row.Free().
// It returns the total number of inserted rows.
//
// Cancellation: if ctx is canceled, the function stops promptly and returns
// (total, ctx.Err()). Any rows accumulated in the current batch are NOT freed
// here because ownership/lifetime is controlled by the caller's pipeline
// (the caller typically drains and frees on cancel to avoid deadlocks).
func LoadBatchesRows(
	ctx context.Context,
	columns []string,
	in <-chan *RowLike,
	batchSize int,
	copyFn RowCopyFn,
) (int64, error) {
	if batchSize <= 0 {
		return 0, fmt.Errorf("batchSize must be > 0")
	}
	if copyFn == nil {
		return 0, fmt.Errorf("copyFn must not be nil")
	}

	var (
		total     int64
		batches   int64
		batchRows = make([]*RowLike, 0, batchSize) // rows to Free() after COPY
		slab      = make([][]any, 0, batchSize)    // [][]any view for COPY
		start     = time.Now()
		//lastFlushTS = start
		//lastTotal   int64
	)

	flush := func() error {
		if len(batchRows) == 0 {
			return nil
		}

		// Build [][]any view without per-row allocation; reuse slab capacity.
		slab = slab[:0]
		for _, r := range batchRows {
			slab = append(slab, r.V)
		}

		n, err := copyFn(ctx, columns, slab)
		total += n

		// On success, return rows to pool.
		for _, r := range batchRows {
			r.Free()
		}
		batchRows = batchRows[:0]

		if err != nil {
			log.Printf("loader_rows: COPY failed after=%d total=%d err=%v", n, total, err)

			// COPY failed; let the caller handle cancel/drain policy.
			return err
		}

		// Progress log: instantaneous rows/sec since last flush + totals.
		batches++
		//		now := time.Now()

		now := time.Since(start).Truncate(time.Millisecond)
		rps := float64(0)

		if total > 0 {
			rps = float64(total) / now.Seconds()
		}
		log.Printf(
			"batch #%d: rps=%.0f inserted=%d total_inserted=%d elapsed=%s",
			batches,
			rps,
			n,
			total,
			now,
		)
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return total, ctx.Err()

		case r, ok := <-in:
			if !ok {
				// Input closed: flush remaining rows.
				if err := flush(); err != nil {
					return total, err
				}
				log.Printf("loader_rows: input closed, total_inserted=%d", total)

				return total, nil
			}
			batchRows = append(batchRows, r)
			if len(batchRows) >= batchSize {
				if err := flush(); err != nil {
					return total, err
				}
			}
		}
	}
}
