package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

/*
Test helpers
*/

// mkRowLikes returns n pooled rows whose Free() toggles an internal bit and
// increments *frees. Trackers allow per-row assertions (e.g., freed or not).
func mkRowLikes(n int, frees *int32) ([]*RowLike, []*rowTracker) {
	rows := make([]*RowLike, 0, n)
	trk := make([]*rowTracker, 0, n)
	for i := 0; i < n; i++ {
		rt := &rowTracker{}
		r := &RowLike{
			V: []any{i},
			FreeFunc: func() {
				if atomic.SwapUint32(&rt.freed, 1) == 0 {
					atomic.AddInt32(frees, 1)
				} else {
					atomic.AddUint32(&rt.doubleFree, 1) // track double-free if it happens
				}
			},
		}
		rows = append(rows, r)
		trk = append(trk, rt)
	}
	return rows, trk
}

type rowTracker struct {
	freed      uint32
	doubleFree uint32
}

type copySpyRows struct {
	mu        sync.Mutex
	calls     int
	rowsSeen  int64
	batches   []int // size of each batch
	colsCalls [][]string
	failAfter int           // if >0, the call number at which to start erroring
	err       error         // error to return when failing
	delay     time.Duration // optional per-call delay
}

func (s *copySpyRows) fn(ctx context.Context, columns []string, rows [][]any) (int64, error) {
	if s.delay > 0 {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(s.delay):
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	s.calls++
	s.rowsSeen += int64(len(rows))
	s.batches = append(s.batches, len(rows))
	cc := make([]string, len(columns))
	copy(cc, columns)
	s.colsCalls = append(s.colsCalls, cc)

	if s.failAfter > 0 && s.calls >= s.failAfter {
		if s.err == nil {
			s.err = errors.New("forced error")
		}
		return int64(len(rows)), s.err
	}
	return int64(len(rows)), nil
}

/*
Unit tests
*/

// TestLoadBatchesRows_ArgValidation verifies validation of batchSize and copyFn.
func TestLoadBatchesRows_ArgValidation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ch := make(chan *RowLike)
	close(ch)

	if _, err := LoadBatchesRows(ctx, nil, ch, 0, func(context.Context, []string, [][]any) (int64, error) { return 0, nil }); err == nil {
		t.Fatalf("expected error for batchSize <= 0")
	}
	if _, err := LoadBatchesRows(ctx, nil, ch, 1, nil); err == nil {
		t.Fatalf("expected error for nil copyFn")
	}
}

// TestLoadBatchesRows_Basic covers empty input, exact multiples, and a partial tail.
// It also ensures all rows are freed on success.
func TestLoadBatchesRows_Basic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		totalRows   int
		batchSize   int
		wantCalls   int
		wantBatches []int
	}{
		{name: "empty", totalRows: 0, batchSize: 128, wantCalls: 0, wantBatches: nil},
		{name: "exact_multiple", totalRows: 300, batchSize: 100, wantCalls: 3, wantBatches: []int{100, 100, 100}},
		{name: "partial_final", totalRows: 250, batchSize: 128, wantCalls: 2, wantBatches: []int{128, 122}},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var frees int32
			rows, trackers := mkRowLikes(tc.totalRows, &frees)

			columns := []string{"id"}
			in := make(chan *RowLike, tc.totalRows)
			for _, r := range rows {
				in <- r
			}
			close(in)

			// Quiet logs for happy path
			prev := log.Default().Writer()
			log.SetOutput(io.Discard)
			defer log.SetOutput(prev)

			spy := &copySpyRows{}
			total, err := LoadBatchesRows(context.Background(), columns, in, tc.batchSize, spy.fn)
			if err != nil {
				t.Fatalf("LoadBatchesRows error: %v", err)
			}
			if int(total) != tc.totalRows {
				t.Fatalf("total inserted = %d, want %d", total, tc.totalRows)
			}
			if spy.calls != tc.wantCalls {
				t.Fatalf("copy calls = %d, want %d", spy.calls, tc.wantCalls)
			}
			if tc.wantBatches != nil {
				if len(spy.batches) != len(tc.wantBatches) {
					t.Fatalf("batches count = %d, want %d (%v)", len(spy.batches), len(tc.wantBatches), spy.batches)
				}
				for i := range tc.wantBatches {
					if spy.batches[i] != tc.wantBatches[i] {
						t.Fatalf("batch %d size = %d, want %d", i, spy.batches[i], tc.wantBatches[i])
					}
				}
			}
			// Columns threaded through unchanged on every call
			for _, gotCols := range spy.colsCalls {
				if len(gotCols) != 1 || gotCols[0] != "id" {
					t.Fatalf("columns mismatch: got %v, want %v", gotCols, columns)
				}
			}
			// All rows must be freed on success.
			if frees != int32(tc.totalRows) {
				t.Fatalf("frees = %d, want %d", frees, tc.totalRows)
			}
			// No double frees.
			for i, tr := range trackers {
				if atomic.LoadUint32(&tr.doubleFree) != 0 {
					t.Fatalf("row[%d] double freed", i)
				}
			}
		})
	}
}

// TestLoadBatchesRows_ErrorPropagation verifies returned error and current Free() behavior.
// NOTE: As implemented, rows are freed even when copyFn returns an error.
// We assert that first 2 batches' rows are freed, and the tail (never flushed) is not.
func TestLoadBatchesRows_ErrorPropagation(t *testing.T) {
	t.Parallel()

	var frees int32
	rows, trackers := mkRowLikes(10, &frees)

	in := make(chan *RowLike, len(rows))
	for _, r := range rows {
		in <- r
	}
	close(in)

	spy := &copySpyRows{failAfter: 2, err: errors.New("boom")}
	total, err := LoadBatchesRows(context.Background(), []string{"id"}, in, 4, spy.fn)

	// Expect the error from the second call.
	if err == nil || err.Error() != "boom" {
		t.Fatalf("expected error 'boom', got %v", err)
	}
	// Two calls: 4 and 4, error on 2nd. Tail of 2 is not attempted.
	if spy.calls != 2 {
		t.Fatalf("calls = %d, want 2", spy.calls)
	}
	// Total includes rows reported by failing call.
	if total != 8 {
		t.Fatalf("total = %d, want 8", total)
	}

	// Current implementation frees rows even when the batch errored.
	// So first 8 should be freed, final 2 should not.
	for i := 0; i < 8; i++ {
		if atomic.LoadUint32(&trackers[i].freed) == 0 {
			t.Fatalf("row[%d] expected freed", i)
		}
	}
	for i := 8; i < 10; i++ {
		if atomic.LoadUint32(&trackers[i].freed) != 0 {
			t.Fatalf("row[%d] should not be freed (never flushed)", i)
		}
	}
	if frees != 8 {
		t.Fatalf("frees = %d, want 8", frees)
	}
}

// TestLoadBatchesRows_CanceledPre ensures immediate cancellation returns promptly
// and does not free unflushed rows (ownership stays with caller).
func TestLoadBatchesRows_CanceledPre(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var frees int32
	// IMPORTANT: Do not enqueue or close; the recv case must not be ready.
	in := make(chan *RowLike)

	spy := &copySpyRows{}
	total, err := LoadBatchesRows(ctx, []string{"id"}, in, 3, spy.fn)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if total != 0 {
		t.Fatalf("total = %d, want 0", total)
	}
	if spy.calls != 0 {
		t.Fatalf("copy calls = %d, want 0", spy.calls)
	}
	// No frees occur because there was no successful flush and no rows were read.
	if frees != 0 {
		t.Fatalf("frees = %d, want 0", frees)
	}
}

// TestLoadBatchesRows_CanceledAfterFirstFlush cancels deterministically after the first successful copy.
func TestLoadBatchesRows_CanceledAfterFirstFlush(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var frees int32
	rows, trackers := mkRowLikes(25, &frees)

	in := make(chan *RowLike, len(rows))
	for _, r := range rows {
		in <- r
	}

	// Cancel after first successful flush
	var once sync.Once
	spy := &copySpyRows{delay: 1 * time.Millisecond}
	wrapped := func(ctx context.Context, cols []string, v [][]any) (int64, error) {
		n, err := spy.fn(ctx, cols, v)
		once.Do(cancel)
		return n, err
	}

	total, err := LoadBatchesRows(ctx, []string{"id"}, in, 10, wrapped)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	// At least one call happened (exactly one with this pattern, usually).
	if spy.calls < 1 {
		t.Fatalf("expected >=1 copy call")
	}
	// Flushed rows freed; unflushed remain.
	for i := 0; i < 10; i++ {
		if atomic.LoadUint32(&trackers[i].freed) == 0 {
			t.Fatalf("row[%d] should be freed (flushed before cancel)", i)
		}
	}
	for i := 10; i < len(trackers); i++ {
		if atomic.LoadUint32(&trackers[i].freed) != 0 {
			t.Fatalf("row[%d] should not be freed (not flushed when canceled)", i)
		}
	}
	if int(frees) != 10 {
		t.Fatalf("frees = %d, want 10", frees)
	}
	// Reported total equals rowsSeen.
	if total != spy.rowsSeen {
		t.Fatalf("total %d != rowsSeen %d", total, spy.rowsSeen)
	}
}

// TestLoadBatchesRows_Logs ensures logging emits something and does not panic.
// We avoid coupling to exact formatting.
func TestLoadBatchesRows_Logs(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	prev := log.Default().Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(prev)

	var frees int32
	rows, _ := mkRowLikes(5, &frees)
	in := make(chan *RowLike, len(rows))
	for _, r := range rows {
		in <- r
	}
	close(in)

	spy := &copySpyRows{}
	if _, err := LoadBatchesRows(context.Background(), []string{"id"}, in, 4, spy.fn); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if buf.Len() == 0 {
		t.Fatalf("expected some log output")
	}
}

/*
Benchmarks
*/

// BenchmarkLoadBatchesRows_Throughput measures best-case throughput with no backend latency.
// It intentionally discards logs to minimize allocation noise.
func BenchmarkLoadBatchesRows_Throughput(b *testing.B) {
	rowsPerRun := 50_000
	for _, bs := range []int{64, 256, 1024} {
		b.Run(benchName("rows_batch", bs), func(b *testing.B) {
			columns := []string{"id"}
			prev := log.Default().Writer()
			log.SetOutput(io.Discard)
			defer log.SetOutput(prev)

			for i := 0; i < b.N; i++ {
				var frees int32
				rows, _ := mkRowLikes(rowsPerRun, &frees)
				in := make(chan *RowLike, rowsPerRun)
				for _, r := range rows {
					in <- r
				}
				close(in)

				spy := &copySpyRows{}
				_, _ = LoadBatchesRows(context.Background(), columns, in, bs, spy.fn)
			}
		})
	}
}

// BenchmarkLoadBatchesRows_WithLatency simulates a backend with per-batch latency (e.g., roundtrips).
func BenchmarkLoadBatchesRows_WithLatency(b *testing.B) {
	rowsPerRun := 10_000
	lat := 200 * time.Microsecond
	for _, bs := range []int{100, 500, 1000} {
		b.Run(benchName("rows_latency", bs), func(b *testing.B) {
			columns := []string{"id"}
			prev := log.Default().Writer()
			log.SetOutput(io.Discard)
			defer log.SetOutput(prev)

			for i := 0; i < b.N; i++ {
				var frees int32
				rows, _ := mkRowLikes(rowsPerRun, &frees)
				in := make(chan *RowLike, rowsPerRun)
				for _, r := range rows {
					in <- r
				}
				close(in)

				spy := &copySpyRows{delay: lat}
				_, _ = LoadBatchesRows(context.Background(), columns, in, bs, spy.fn)
			}
		})
	}
}
