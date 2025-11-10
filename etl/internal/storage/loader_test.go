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

//// TestLoadBatches_ErrorPropagation ensures the first copy error is propagated
//// and processing stops after that batch.
//func TestLoadBatches_ErrorPropagation(t *testing.T) {
//	t.Parallel()
//
//	ctx := context.Background()
//	columns := []string{"c"}
//
//	in := make(chan []any, 5)
//	for i := 0; i < 5; i++ {
//		in <- []any{i}
//	}
//	close(in)
//
//	wantErr := errors.New("copy failed")
//	var batches int
//	copyFn := func(_ context.Context, _ []string, rows [][]any) (int64, error) {
//		batches++
//		if batches == 2 {
//			return int64(len(rows)), wantErr
//		}
//		return int64(len(rows)), nil
//	}
//
//	total, err := LoadBatches(ctx, columns, in, 2, copyFn)
//	if !errors.Is(err, wantErr) {
//		t.Fatalf("want error %v, got %v", wantErr, err)
//	}
//	// Total must include rows from successful batches (at least the first 2).
//	if total < 4 {
//		t.Fatalf("total rows %d, want >= 4", total)
//	}
//}

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

/*
Test helpers
*/

func mkRows(n int) [][]any {
	out := make([][]any, n)
	for i := 0; i < n; i++ {
		// trivial single-column payload; type is 'any' as required
		out[i] = []any{i}
	}
	return out
}

type copySpy struct {
	mu        sync.Mutex
	calls     int
	rowsSeen  int64
	batches   []int // len of each batch
	columns   [][]string
	failAfter int // if >0, return error starting with this call #
	err       error
	delay     time.Duration
}

func (s *copySpy) fn(ctx context.Context, columns []string, rows [][]any) (int64, error) {
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
	// capture copy of columns per call
	cc := make([]string, len(columns))
	copy(cc, columns)
	s.columns = append(s.columns, cc)

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

// TestLoadBatches_ArgValidation verifies argument validation.
func TestLoadBatches_ArgValidation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ch := make(chan []any)
	close(ch)

	if _, err := LoadBatches(ctx, nil, ch, 0, func(context.Context, []string, [][]any) (int64, error) { return 0, nil }); err == nil {
		t.Fatalf("expected error for batchSize <= 0")
	}
	if _, err := LoadBatches(ctx, nil, ch, 1, nil); err == nil {
		t.Fatalf("expected error for nil copyFn")
	}
}

// TestLoadBatches_BasicAndRemainder covers empty input, exact multiple, and partial final batch.
func TestLoadBatches_BasicAndRemainder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		totalRows   int
		batchSize   int
		wantCalls   int
		wantBatches []int
	}{
		{
			name:        "empty input",
			totalRows:   0,
			batchSize:   100,
			wantCalls:   0,
			wantBatches: nil,
		},
		{
			name:        "exact multiple",
			totalRows:   300,
			batchSize:   100,
			wantCalls:   3,
			wantBatches: []int{100, 100, 100},
		},
		{
			name:        "partial final batch",
			totalRows:   250,
			batchSize:   128,
			wantCalls:   2,
			wantBatches: []int{128, 122},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			columns := []string{"id"}
			in := make(chan []any, tc.totalRows)
			for _, r := range mkRows(tc.totalRows) {
				in <- r
			}
			close(in)

			// silence logging for the happy-path tests to keep output clean
			prev := log.Default().Writer()
			log.SetOutput(io.Discard)
			defer log.SetOutput(prev)

			spy := &copySpy{}
			gotTotal, err := LoadBatches(context.Background(), columns, in, tc.batchSize, spy.fn)
			if err != nil {
				t.Fatalf("LoadBatches returned error: %v", err)
			}
			if gotTotal != int64(tc.totalRows) {
				t.Fatalf("total inserted = %d, want %d", gotTotal, tc.totalRows)
			}
			if spy.calls != tc.wantCalls {
				t.Fatalf("copy calls = %d, want %d", spy.calls, tc.wantCalls)
			}
			// check per-batch sizes when specified
			if tc.wantBatches != nil {
				if len(spy.batches) != len(tc.wantBatches) {
					t.Fatalf("batches len = %d, want %d (%v)", len(spy.batches), len(tc.wantBatches), spy.batches)
				}
				for i := range tc.wantBatches {
					if spy.batches[i] != tc.wantBatches[i] {
						t.Fatalf("batch %d size = %d, want %d", i, spy.batches[i], tc.wantBatches[i])
					}
				}
			}
			// columns threaded through unchanged
			for _, gotCols := range spy.columns {
				if len(gotCols) != len(columns) || gotCols[0] != "id" {
					t.Fatalf("columns mismatch: got %v, want %v", gotCols, columns)
				}
			}
		})
	}
}

// TestLoadBatches_ErrorPropagation ensures the first copyFn error is returned.
func TestLoadBatches_ErrorPropagation(t *testing.T) {
	t.Parallel()

	in := make(chan []any, 10)
	for _, r := range mkRows(10) {
		in <- r
	}
	close(in)

	spy := &copySpy{failAfter: 2, err: errors.New("boom")}
	total, err := LoadBatches(context.Background(), []string{"id"}, in, 4, spy.fn)

	if err == nil || err.Error() != "boom" {
		t.Fatalf("expected error 'boom', got: %v", err)
	}
	// Calls: batch 1 (4), batch 2 (4) returns error, final batch (2) should NOT be attempted
	if spy.calls != 2 {
		t.Fatalf("copyFn calls = %d, want 2", spy.calls)
	}
	// Rows counted include the rows the failing call reported as inserted.
	if total != int64(8) {
		t.Fatalf("total inserted = %d, want 8", total)
	}
}

// TestLoadBatches_CanceledPre verifies immediate cancellation returns context error and no copy attempts.
func TestLoadBatches_CanceledPre(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	in := make(chan []any, 5)
	for _, r := range mkRows(5) {
		in <- r
	}
	// do not close; cancellation should short-circuit
	spy := &copySpy{}
	total, err := LoadBatches(ctx, []string{"id"}, in, 3, spy.fn)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if total != 0 {
		t.Fatalf("total = %d, want 0", total)
	}
	if spy.calls != 0 {
		t.Fatalf("copyFn calls = %d, want 0", spy.calls)
	}
}

func TestLoadBatches_CanceledMidStream(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan []any, 64)
	go func() {
		for _, r := range mkRows(50) {
			in <- r
		}
		// we can leave it open or close it; cancellation will terminate the loader
	}()

	// cancel on the first successful flush to guarantee ≥1 call happened
	var once sync.Once
	spy := &copySpy{delay: 2 * time.Millisecond}
	wrapped := func(ctx context.Context, cols []string, rows [][]any) (int64, error) {
		n, err := spy.fn(ctx, cols, rows)
		once.Do(cancel)
		return n, err
	}

	total, err := LoadBatches(ctx, []string{"id"}, in, 10, wrapped)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if spy.calls < 1 {
		t.Fatalf("expected at least one copy call before cancel")
	}
	for i, sz := range spy.batches {
		if sz <= 0 || sz > 10 {
			t.Fatalf("batch %d size %d out of range", i, sz)
		}
	}
	if total != int64(spy.rowsSeen) {
		t.Fatalf("reported total %d != rowsSeen %d", total, spy.rowsSeen)
	}
}

// TestLoadBatches_Logs ensure no panic and that a final line is emitted.
// (We don't make assertions on exact content to avoid coupling to formatting.)
func TestLoadBatches_Logs(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	prev := log.Default().Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(prev)

	in := make(chan []any, 5)
	for _, r := range mkRows(5) {
		in <- r
	}
	close(in)

	spy := &copySpy{}
	if _, err := LoadBatches(context.Background(), []string{"id"}, in, 4, spy.fn); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if buf.Len() == 0 {
		t.Fatalf("expected some log output")
	}
}

/*
Benchmarks
*/

// BenchmarkLoadBatches_Throughput measures best-case throughput with no backend latency.
func BenchmarkLoadBatches_Throughput(b *testing.B) {
	rowsPerRun := 50_000

	for _, bs := range []int{64, 256, 1024} {
		b.Run(
			benchName("batch", bs),
			func(b *testing.B) {
				columns := []string{"id"}
				spy := &copySpy{}
				for i := 0; i < b.N; i++ {
					in := make(chan []any, rowsPerRun)
					for _, r := range mkRows(rowsPerRun) {
						in <- r
					}
					close(in)

					// discard logs to avoid allocation noise
					prev := log.Default().Writer()
					log.SetOutput(io.Discard)
					_, _ = LoadBatches(context.Background(), columns, in, bs, spy.fn)
					log.SetOutput(prev)
				}
			},
		)
	}
}

// BenchmarkLoadBatches_WithLatency simulates a backend that has per-batch latency, like network + DB roundtrip.
func BenchmarkLoadBatches_WithLatency(b *testing.B) {
	rowsPerRun := 10_000
	lat := 200 * time.Microsecond

	for _, bs := range []int{100, 500, 1000} {
		b.Run(
			benchName("batch_latency", bs),
			func(b *testing.B) {
				columns := []string{"id"}
				for i := 0; i < b.N; i++ {
					in := make(chan []any, rowsPerRun)
					for _, r := range mkRows(rowsPerRun) {
						in <- r
					}
					close(in)

					spy := &copySpy{delay: lat}
					prev := log.Default().Writer()
					log.SetOutput(io.Discard)
					_, _ = LoadBatches(context.Background(), columns, in, bs, spy.fn)
					log.SetOutput(prev)
				}
			},
		)
	}
}

func benchName(prefix string, bs int) string {
	var buf bytes.Buffer
	buf.WriteString(prefix)
	buf.WriteString("_")
	buf.WriteString("size=")
	// ints are small; fmt avoided to keep noise down in benchmark names
	buf.WriteString(ittoa(bs))
	return buf.String()
}

// ittoa avoids fmt in benchmark name creation.
func ittoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
