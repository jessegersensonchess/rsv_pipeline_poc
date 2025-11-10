// Package mssql contains tests and benchmarks for the streaming batch loader
// used by the MSSQL adapter.
package mssql

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestLoadBatches verifies that LoadBatches flushes rows in the correct batch
// sizes, forwards the columns slice, and returns the total copied count.
func TestLoadBatches(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	columns := []string{"a", "b"}
	in := make(chan []any, 10)
	for i := 0; i < 5; i++ {
		in <- []any{i, i * 2}
	}
	close(in)

	var calls int
	copyFn := func(ctx context.Context, cols []string, rows [][]any) (int64, error) {
		calls++
		if len(cols) != len(columns) {
			return 0, errors.New("columns mismatch")
		}
		return int64(len(rows)), nil
	}

	total, err := LoadBatches(ctx, columns, in, 2, copyFn)
	if err != nil {
		t.Fatalf("LoadBatches error: %v", err)
	}
	if total != 5 {
		t.Fatalf("expected total=5, got %d", total)
	}
	if calls != 3 { // 2+2+1
		t.Fatalf("expected 3 flush calls, got %d", calls)
	}
}

// TestLoadBatches_Errors ensures that LoadBatches validates inputs and returns
// appropriate errors for invalid batch sizes or nil copy functions.
func TestLoadBatches_Errors(t *testing.T) {
	ctx := context.Background()
	columns := []string{"a"}

	_, err := LoadBatches(ctx, columns, make(chan []any), 0, func(context.Context, []string, [][]any) (int64, error) {
		return 0, nil
	})
	if err == nil {
		t.Fatal("expected error for batchSize <= 0")
	}

	_, err = LoadBatches(ctx, columns, make(chan []any), 1, nil)
	if err == nil {
		t.Fatal("expected error for nil copyFn")
	}
}

// BenchmarkLoadBatches measures the overhead of LoadBatches for different batch
// sizes using a no-op copy function to isolate batching cost.
func BenchmarkLoadBatches(b *testing.B) {
	ctx := context.Background()
	columns := []string{"a", "b", "c"}
	rows := 1000

	makeChan := func() chan []any {
		in := make(chan []any, rows)
		for i := 0; i < rows; i++ {
			in <- []any{i, i + 1, i + 2}
		}
		close(in)
		return in
	}

	copyFn := func(ctx context.Context, _ []string, rows [][]any) (int64, error) {
		return int64(len(rows)), nil
	}

	b.Run("batch_100", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			in := makeChan()
			if _, err := LoadBatches(ctx, columns, in, 100, copyFn); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("batch_1000", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			in := makeChan()
			if _, err := LoadBatches(ctx, columns, in, 1000, copyFn); err != nil {
				b.Fatal(err)
			}
		}
	})
}
