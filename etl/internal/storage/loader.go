// Package storage contains storage-agnostic contracts and utilities. This file
// implements a generic, batched loader that drains typed rows from a channel
// and invokes a provided bulk-insert function (CopyFn) per batch.
//
// Backends (Postgres, MySQL, MSSQL, etc.) can implement CopyFn using their
// most efficient primitives (e.g., Postgres COPY, MySQL multi-row INSERT).
package storage

import (
	"context"
	"fmt"
)

// CopyFn abstracts a backend's bulk insert capability. Implementations should
// insert the provided rows (aligned to 'columns' order) and return the number
// of rows reported as inserted. The function should be safe for repeated calls
// and cancel promptly when ctx is done.
type CopyFn func(ctx context.Context, columns []string, rows [][]any) (int64, error)

// LoadBatches drains typed rows from 'in', groups them into batches of size
// 'batchSize', and calls 'copyFn' for each non-empty batch. It returns the total
// number of rows reported by copyFn and the first error encountered.
//
// The function returns when the input channel is closed or the context is
// canceled. It never buffers more than one batch plus the channel's pending
// items and reuses batch storage across calls to minimize allocations.
func LoadBatches(
	ctx context.Context,
	columns []string,
	in <-chan []any,
	batchSize int,
	copyFn CopyFn,
) (int64, error) {
	if batchSize <= 0 {
		return 0, fmt.Errorf("batchSize must be > 0")
	}
	if copyFn == nil {
		return 0, fmt.Errorf("copyFn must not be nil")
	}

	var (
		total int64
		batch = make([][]any, 0, batchSize)
		flush = func() error {
			if len(batch) == 0 {
				return nil
			}
			n, err := copyFn(ctx, columns, batch)
			total += n
			// Reuse allocated slice; keep capacity to avoid churn.
			batch = batch[:0]
			return err
		}
	)

	for {
		select {
		case <-ctx.Done():
			return total, ctx.Err()

		case row, ok := <-in:
			if !ok {
				// Channel closed: flush remaining rows.
				if err := flush(); err != nil {
					return total, err
				}
				return total, nil
			}
			batch = append(batch, row)
			if len(batch) >= batchSize {
				if err := flush(); err != nil {
					return total, err
				}
			}
		}
	}
}
