// Package postgres contains streaming/batched loading utilities for Postgres.
// It focuses on:
//   - Batching rows into COPY operations to minimize round-trips.
//   - Avoiding whole-file materialization (rows arrive via a channel).
//   - Simple, testable design by inverting the COPY call via a function type.
package postgres

import (
	"context"
	"fmt"
)

// CopyFn abstracts the COPY operation. In production, the function should call
// pgx's CopyFrom; in tests, a fake function can verify batching behavior.
type CopyFn func(ctx context.Context, columns []string, rows [][]any) (int64, error)

// LoadBatches drains typed rows ([]any) from 'in', groups them into batches of
// size 'batchSize', and calls 'copyFn' for each non-empty batch. It returns the
// total number of rows reported by copyFn and the first error encountered.
//
// The function returns when the input channel is closed or context is canceled.
// It never buffers more than one batch plus the channel's pending items.
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
			// reuse backing array
			batch = batch[:0]
			return err
		}
	)

	fmt.Println("DEBUG: asdflkj")
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
