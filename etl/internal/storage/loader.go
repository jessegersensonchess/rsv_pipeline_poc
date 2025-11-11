// Package storage contains storage-agnostic contracts and utilities.
// This file implements a generic, batched loader that drains typed rows from a
// channel and invokes a provided bulk-insert function (CopyFn) per batch.
//
// Backends (Postgres, MySQL, MSSQL, etc.) can implement CopyFn using their
// most efficient primitives (e.g., Postgres COPY, MySQL multi-row INSERT).
//
// Logging: on every successful flush, a concise progress line is emitted with
// running totals and instantaneous rows/sec since the previous flush.
package storage

import (
	"context"
	"fmt"
	"log"
	"time"
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
// Cancellation: returns (total, ctx.Err()) when canceled. Progress is logged on
// each successful flush.
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
	// If the caller passes a pre-canceled context, fail fast deterministically.
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	var (
		total   int64
		batches int64
		batch   = make([][]any, 0, batchSize)
		start   = time.Now()
		//lastTotal int64
	)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		// Recheck cancellation before doing IO.
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := copyFn(ctx, columns, batch)
		//n, err := copyFn(ctx, columns, batch)
		total += n

		// Reuse allocated slice; keep capacity to avoid churn.
		// pending := len(batch)
		batch = batch[:0]

		if err != nil {
			log.Printf("loader: COPY failed after=%d total=%d err=%v", n, total, err)
			return err
		}

		// Progress log per successful batch.
		batches++
		// insertedSinceLast := total - lastTotal
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
			//time.Since(start).Truncate(time.Millisecond),
			//			sinceLast.Truncate(time.Millisecond),
		)
		// lastTotal = total

		return nil
	}

	for {
		// Optional: recheck here to avoid reading from 'in' if canceled mid-loop.
		if err := ctx.Err(); err != nil {
			return total, err
		}
		select {
		case <-ctx.Done():
			return total, ctx.Err()

		case row, ok := <-in:
			if !ok {
				// Channel closed: flush remaining rows.
				if err := flush(); err != nil {
					return total, err
				}
				log.Printf("loader: input closed, total_inserted=%d", total)

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
