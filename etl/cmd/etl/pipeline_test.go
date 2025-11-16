package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"etl/internal/config"
	"etl/internal/transformer"
)

// Test_newRuntimeConfig_prefersSpecOverEnv verifies that the runtime configuration
// takes values from the spec when present and ignores environment defaults.
//
// It does not touch OS environment; it relies on pickInt/getenvInt behavior
// being stable.
func Test_newRuntimeConfig_prefersSpecOverEnv(t *testing.T) {
	spec := config.Pipeline{}
	spec.Runtime.ReaderWorkers = 3
	spec.Runtime.TransformWorkers = 5
	spec.Runtime.LoaderWorkers = 2
	spec.Runtime.BatchSize = 1234
	spec.Runtime.ChannelBuffer = 256

	rt := newRuntimeConfig(spec)

	if got, want := rt.readerWorkers, 3; got != want {
		t.Fatalf("readerWorkers = %d, want %d", got, want)
	}
	if got, want := rt.transformers, 5; got != want {
		t.Fatalf("transformers = %d, want %d", got, want)
	}
	if got, want := rt.loaderWorkers, 2; got != want {
		t.Fatalf("loaderWorkers = %d, want %d", got, want)
	}
	if got, want := rt.batchSize, 1234; got != want {
		t.Fatalf("batchSize = %d, want %d", got, want)
	}
	if got, want := rt.bufferSize, 256; got != want {
		t.Fatalf("bufferSize = %d, want %d", got, want)
	}
}

// Test_runLoader_flushesRows ensures that runLoader:
//
//   - calls the COPY function with the expected batch of values,
//   - updates the counters, and
//   - returns without reporting an error on a successful flush.
//
// We do not assert that Row.Free was called directly because transformer.Row
// is defined in another package and runLoader requires *transformer.Row
// concretely.
func Test_runLoader_flushesRows(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	validCh := make(chan *transformer.Row, 2)
	errCh := make(chan RowErr, 1)

	// Prepare two rows with simple values.
	r1 := &transformer.Row{V: []any{"a", 1}}
	r2 := &transformer.Row{V: []any{"b", 2}}

	validCh <- r1
	validCh <- r2
	close(validCh)

	var (
		copyCalled bool
		copyRows   [][]any
	)

	copyFn := func(_ context.Context, _ []string, rows [][]any) (int64, error) {
		copyCalled = true
		// Capture rows passed to COPY to validate content.
		copyRows = append(copyRows, rows...)
		return int64(len(rows)), nil
	}

	var stats counters
	var wg sync.WaitGroup
	wg.Add(1)

	cfg := loaderConfig{
		ctx:       ctx,
		cancel:    cancel,
		validCh:   validCh,
		errCh:     errCh,
		copyFn:    copyFn,
		batchSize: 10,
		columns:   []string{"col1", "col2"},
		stats:     &stats,
		// Use real clock; we don't care about exact timing in this test.
		clockNowFn: time.Now,
	}

	go runLoader(cfg, &wg)
	wg.Wait()

	select {
	case e := <-errCh:
		t.Fatalf("unexpected loader error: %v", e.Err)
	default:
	}

	if !copyCalled {
		t.Fatalf("copyFn was not called")
	}

	if got, want := stats.inserted.Load(), int64(2); got != want {
		t.Fatalf("inserted = %d, want %d", got, want)
	}

	if len(copyRows) != 2 {
		t.Fatalf("copyRows length = %d, want 2", len(copyRows))
	}
}

// Test_runLoader_cancelsOnCopyError verifies that a COPY error:
//
//   - is reported on errCh,
//   - triggers cancellation, and
//   - makes the loader exit.
func Test_runLoader_cancelsOnCopyError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	validCh := make(chan *transformer.Row, 1)
	errCh := make(chan RowErr, 1)

	r := &transformer.Row{V: []any{"x", 1}}
	validCh <- r
	close(validCh)

	expectedErr := errors.New("copy failed")

	copyFn := func(_ context.Context, _ []string, _ [][]any) (int64, error) {
		return 0, expectedErr
	}

	var stats counters
	var wg sync.WaitGroup
	wg.Add(1)

	cfg := loaderConfig{
		ctx:        ctx,
		cancel:     cancel,
		validCh:    validCh,
		errCh:      errCh,
		copyFn:     copyFn,
		batchSize:  1,
		columns:    []string{"col1"},
		stats:      &stats,
		clockNowFn: time.Now,
	}

	go runLoader(cfg, &wg)
	wg.Wait()

	select {
	case e := <-errCh:
		if !errors.Is(e.Err, expectedErr) {
			t.Fatalf("err = %v, want %v", e.Err, expectedErr)
		}
	default:
		t.Fatalf("expected an error on errCh")
	}
}

// Benchmark_runLoader provides a micro-benchmark for the loader worker. It
// focuses on the batching and stats-update overhead in isolation by using an
// in-memory copy function and a pre-filled channel of rows.
func Benchmark_runLoader(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, batchSize := range []int{128, 1024, 8192} {
		b.Run(
			"batch_"+fmt.Sprint(batchSize),
			func(b *testing.B) {
				validCh := make(chan *transformer.Row, batchSize)
				errCh := make(chan RowErr, 1)

				// Pre-allocate rows reused across iterations.
				rows := make([]*transformer.Row, batchSize)
				for i := range rows {
					rows[i] = &transformer.Row{V: []any{"v", i}}
				}

				copyFn := func(_ context.Context, _ []string, rows [][]any) (int64, error) {
					return int64(len(rows)), nil
				}

				var wg sync.WaitGroup
				var stats counters

				cfg := loaderConfig{
					ctx:        ctx,
					cancel:     cancel,
					validCh:    validCh,
					errCh:      errCh,
					copyFn:     copyFn,
					batchSize:  batchSize,
					columns:    []string{"col"},
					stats:      &stats,
					clockNowFn: time.Now,
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					// Re-fill the channel for each iteration.
					for _, r := range rows {
						validCh <- r
					}
					close(validCh)

					wg.Add(1)
					go runLoader(cfg, &wg)
					wg.Wait()

					// Drain any errors.
					select {
					case <-errCh:
					default:
					}

					// Recreate channel for next iteration.
					validCh = make(chan *transformer.Row, batchSize)
					cfg.validCh = validCh
				}
			},
		)
	}
}
