package storage

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestLoadBatches_Basic verifies rows are grouped into batches and copyFn is
// called with the expected counts. It also checks the total equals the sum of
// all successful copyFn returns.
func TestLoadBatches_Basic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	columns := []string{"c1", "c2"}

	in := make(chan []any, 8)
	for i := 0; i < 7; i++ {
		in <- []any{i, "x"}
	}
	close(in)

	var calls int32
	copyFn := func(_ context.Context, _ []string, rows [][]any) (int64, error) {
		atomic.AddInt32(&calls, 1)
		return int64(len(rows)), nil
	}

	total, err := LoadBatches(ctx, columns, in, 3, copyFn)
	if err != nil {
		t.Fatalf("LoadBatches error: %v", err)
	}
	if total != 7 {
		t.Fatalf("total rows %d, want 7", total)
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Fatalf("copyFn calls %d, want 3 (3+3+1)", got)
	}
}

// TestLoadBatches_ErrorPropagation ensures the first copy error is propagated
// and processing stops after that batch.
func TestLoadBatches_ErrorPropagation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	columns := []string{"c"}

	in := make(chan []any, 5)
	for i := 0; i < 5; i++ {
		in <- []any{i}
	}
	close(in)

	wantErr := errors.New("copy failed")
	var batches int
	copyFn := func(_ context.Context, _ []string, rows [][]any) (int64, error) {
		batches++
		if batches == 2 {
			return int64(len(rows)), wantErr
		}
		return int64(len(rows)), nil
	}

	total, err := LoadBatches(ctx, columns, in, 2, copyFn)
	if !errors.Is(err, wantErr) {
		t.Fatalf("want error %v, got %v", wantErr, err)
	}
	// Total must include rows from successful batches (at least the first 2).
	if total < 4 {
		t.Fatalf("total rows %d, want >= 4", total)
	}
}

// TestLoadBatches_ContextCancel checks the loader exits on context cancellation.
func TestLoadBatches_ContextCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	columns := []string{"c"}
	in := make(chan []any, 1)
	in <- []any{1}

	// copyFn sleeps to simulate slow I/O; cancel triggers early exit.
	copyFn := func(ctx context.Context, _ []string, rows [][]any) (int64, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(2 * time.Second):
			return int64(len(rows)), nil
		}
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := LoadBatches(ctx, columns, in, 2, copyFn)
		errCh <- err
	}()

	cancel() // cancel promptly
	close(in)

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected cancellation error, got nil")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("LoadBatches did not return after context cancel")
	}
}
