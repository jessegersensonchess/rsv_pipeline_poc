package storage

import (
	"context"
	"fmt"
)

// RowCopyFn matches repository.CopyFrom but takes [][]any; the loader will
// extract [][]any from pooled rows before calling the backend.
type RowCopyFn func(ctx context.Context, columns []string, rows [][]any) (int64, error)

// RowLike is a tiny adapter to avoid import cycles here. In your codebase, you
// can import transformer.Row directly and delete RowLike entirely.
type RowLike struct {
	V        []any
	FreeFunc func()
}

func (r *RowLike) Free() {
	if r.FreeFunc != nil {
		r.FreeFunc()
	}
}

// LoadBatchesRows consumes pooled rows, builds [][]any batches, invokes copyFn,
// and after a successful flush returns each row to the pool via Row.Free().
// It returns the total number of inserted rows.
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

	var total int64
	batch := make([]*RowLike, 0, batchSize)
	slab := make([][]any, 0, batchSize)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		// Build [][]any view without per-row alloc; reuse slab backing array.
		slab = slab[:0]
		for _, r := range batch {
			slab = append(slab, r.V)
		}
		n, err := copyFn(ctx, columns, slab)
		total += n
		// Return rows to pool only on success.
		for _, r := range batch {
			r.Free()
		}
		// reuse batch backing array
		batch = batch[:0]
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return total, ctx.Err()
		case r, ok := <-in:
			if !ok {
				if err := flush(); err != nil {
					return total, err
				}
				return total, nil
			}
			batch = append(batch, r)
			if len(batch) >= batchSize {
				if err := flush(); err != nil {
					return total, err
				}
			}
		}
	}
}
