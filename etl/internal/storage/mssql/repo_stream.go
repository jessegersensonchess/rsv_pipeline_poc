// Package mssql contains streaming/batched loading utilities for MSSQL.
// Identical behavior to the Postgres implementation, parameterized by a CopyFn.
package mssql

import (
	"context"
	"fmt"
)

// CopyFn abstracts the bulk copy operation.
type CopyFn func(ctx context.Context, columns []string, rows [][]any) (int64, error)

// LoadBatches drains typed rows from 'in', groups into batches of 'batchSize',
// and invokes 'copyFn' for each non-empty batch. Returns the sum of inserted
// rows and the first error encountered.
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
