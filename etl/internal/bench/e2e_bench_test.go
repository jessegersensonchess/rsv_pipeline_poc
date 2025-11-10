package bench

import (
	"context"
	"testing"

	"etl/internal/storage"
	"etl/internal/transformer"
)

// BenchmarkEndToEnd exercises the pooled row transform + batch loader without
// touching a real database. It uses a fake copyFn that increments counters.
// Run with:
//
//	go test -run=^$ -bench ^BenchmarkEndToEnd$ -cpuprofile cpu.out -memprofile mem.out -count=1
func BenchmarkEndToEnd(b *testing.B) {
	ctx := context.Background()

	cols := []string{"pcv", "typ", "stav", "platnost_od", "aktualni"}
	spec := transformer.BuildCoerceSpecFromTypes(
		map[string]string{
			"pcv":         "int",
			"typ":         "text",
			"stav":        "text",
			"platnost_od": "date",
			"aktualni":    "bool",
		},
		"02.01.2006",
		nil, nil,
	)

	// Producer: pooled rows with realistic strings.
	in := make(chan *transformer.Row, 8192)
	go func() {
		defer close(in)
		for i := 0; i < b.N; i++ {
			r := transformer.GetRow(len(cols))
			r.V[0] = "123456" // pcv
			r.V[1] = "E - Evidenční"
			r.V[2] = "Nezjištěno"
			r.V[3] = "07.10.2011" // date
			r.V[4] = "True"       // bool
			in <- r
		}
	}()

	// Transform in place.
	out := make(chan *transformer.Row, 8192)
	go func() {
		transformer.TransformLoopRows(ctx, cols, in, out, spec)
		close(out)
	}()

	// Fake copyFn that just returns the count.
	copyFn := func(ctx context.Context, columns []string, rows [][]any) (int64, error) {
		return int64(len(rows)), nil
	}

	b.ResetTimer()
	n, err := storage.LoadBatchesRows(ctx, cols, toRowLike(out), 4096, copyFn)
	b.StopTimer()

	if err != nil {
		b.Fatalf("LoadBatchesRows: %v", err)
	}
	_ = n // keep the compiler happy
}

// toRowLike adapts transformer.Row to storage.RowLike without allocation.
func toRowLike(in <-chan *transformer.Row) <-chan *storage.RowLike {
	out := make(chan *storage.RowLike, 8192)
	go func() {
		defer close(out)
		for r := range in {
			row := r
			out <- &storage.RowLike{
				V:        row.V,
				FreeFunc: row.Free,
			}
		}
	}()
	return out
}
