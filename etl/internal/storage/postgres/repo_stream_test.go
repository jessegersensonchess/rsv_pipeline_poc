package postgres

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestLoadBatches_Basic verifies that rows are grouped into batches and the
// copyFn is called with the expected counts. It also checks that the total
// number of rows returned matches the sum of copyFn results.
func TestLoadBatches_Basic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	columns := []string{"pcv", "typ"}

	in := make(chan []any, 10)
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
	columns := []string{"pcv"}

	in := make(chan []any, 5)
	for i := 0; i < 5; i++ {
		in <- []any{i}
	}
	close(in)

	var batches int32
	copyErr := errors.New("copy failed")
	copyFn := func(_ context.Context, _ []string, rows [][]any) (int64, error) {
		batches++
		if batches == 2 {
			return int64(len(rows)), copyErr
		}
		return int64(len(rows)), nil
	}

	total, err := LoadBatches(ctx, columns, in, 2, copyFn)
	if !errors.Is(err, copyErr) {
		t.Fatalf("want error %v, got %v", copyErr, err)
	}
	// First batch: 2, second batch: 2 (error), remaining 1 may or may not be read,
	// but total must include rows reported by successful batches.
	if total < 4 {
		t.Fatalf("total rows %d, want >= 4", total)
	}
}

// TestLoadBatches_ContextCancel checks the loader exits on context cancellation.
func TestLoadBatches_ContextCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	columns := []string{"pcv"}
	in := make(chan []any)

	// copyFn sleeps to simulate a slow COPY; we cancel the context while blocked.
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

	// Feed one row and then cancel promptly.
	in <- []any{1}
	cancel()
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

// NOTE: If you've migrated the generic batcher to storage.LoadBatches, you can
// adapt these tests to that package by changing the import and function name.

// CopyFn signature for the local LoadBatches in this package (repo_stream.go).
type testCopyFn = func(ctx context.Context, columns []string, rows [][]any) (int64, error)

// TestLoadBatches_Errors exercises early argument validation paths:
//   - batchSize <= 0 → error
//   - copyFn == nil  → error
func TestLoadBatches_Errors(t *testing.T) {
	t.Parallel()

	ch := make(chan []any)
	close(ch)

	if _, err := LoadBatches(context.Background(), []string{"a"}, ch, 0, func(context.Context, []string, [][]any) (int64, error) {
		return 0, nil
	}); err == nil {
		t.Fatal("batchSize <= 0: expected error, got nil")
	}

	if _, err := LoadBatches(context.Background(), []string{"a"}, ch, 1, nil); err == nil {
		t.Fatal("nil copyFn: expected error, got nil")
	}
}

// TestLoadBatches_EmptyFlush ensures that when the input channel closes with no
// pending rows, the internal flush is a no-op and returns nil.
func TestLoadBatches_EmptyFlush(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ch := make(chan []any)
	close(ch) // immediately closed, so first flush sees an empty batch

	calls := 0
	fn := func(ctx context.Context, cols []string, rows [][]any) (int64, error) {
		calls++
		return int64(len(rows)), nil
	}

	total, err := LoadBatches(ctx, []string{"a"}, ch, 2, fn)
	if err != nil {
		t.Fatalf("LoadBatches error: %v", err)
	}
	if total != 0 {
		t.Fatalf("total = %d, want 0", total)
	}
	if calls != 0 {
		t.Fatalf("copyFn calls = %d, want 0 (empty flush should be a no-op)", calls)
	}
}

// TestLoadBatches_FinalFlushError validates that an error returned from copyFn
// during the *final flush after channel close* is propagated to the caller.
func TestLoadBatches_FinalFlushError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ch := make(chan []any, 2)
	ch <- []any{1}
	close(ch)

	wantErr := errors.New("boom")
	fn := func(ctx context.Context, cols []string, rows [][]any) (int64, error) {
		// Only called once on final flush.
		return int64(len(rows)), wantErr
	}

	_, err := LoadBatches(ctx, []string{"a"}, ch, 2, fn)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected final flush error %v, got %v", wantErr, err)
	}
}

// TestLoadBatches_Cancel validates that context cancellation unblocks and
// returns promptly with ctx.Err().
func TestLoadBatches_Cancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())

	ch := make(chan []any, 1)
	ch <- []any{1}

	fn := func(ctx context.Context, cols []string, rows [][]any) (int64, error) {
		// Simulate a slow insert; cancellation should interrupt.
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(2 * time.Second):
			return int64(len(rows)), nil
		}
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := LoadBatches(ctx, []string{"a"}, ch, 2, fn)
		errCh <- err
	}()

	cancel()
	close(ch)

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected cancellation error, got nil")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("LoadBatches did not return after context cancel")
	}
}
