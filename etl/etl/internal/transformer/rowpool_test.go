package transformer

import (
	"runtime"
	"sync"
	"testing"
)

/*
TestGetRow_LengthAndZeroing verifies that GetRow returns a row of the requested
length with all elements cleared to nil; it also ensures that after a Free(),
a subsequent GetRow returns a zeroed slice again (guarding against stale data).
*/
func TestGetRow_LengthAndZeroing(t *testing.T) {
	const n = 3

	r := GetRow(n)
	if r == nil {
		t.Fatal("GetRow returned nil")
	}
	if got := len(r.V); got != n {
		t.Fatalf("len(V)=%d; want %d", got, n)
	}
	for i, v := range r.V {
		if v != nil {
			t.Fatalf("V[%d]=%v; want nil", i, v)
		}
	}

	// Fill, free, and ensure zeroing on the next GetRow.
	r.V[0], r.V[1], r.V[2] = "x", 123, true
	r.Free()

	r2 := GetRow(n)
	defer r2.Free()
	if got := len(r2.V); got != n {
		t.Fatalf("after reuse, len(V)=%d; want %d", got, n)
	}
	for i, v := range r2.V {
		if v != nil {
			t.Fatalf("after reuse, V[%d]=%v; want nil", i, v)
		}
	}
}

/*
TestGetRow_CapacityGrowth verifies that a pooled row whose capacity is too
small is grown to accommodate a larger colCount, and that the resulting Row
has the correct length.
*/
func TestGetRow_CapacityGrowth(t *testing.T) {
	rSmall := GetRow(2)
	capSmall := cap(rSmall.V)
	rSmall.Free()

	rBig := GetRow(5)
	defer rBig.Free()

	if got := len(rBig.V); got != 5 {
		t.Fatalf("len(V)=%d; want 5", got)
	}
	if cap(rBig.V) < 5 {
		t.Fatalf("cap(V)=%d; want >= 5", cap(rBig.V))
	}

	// If we happened to reuse, capacity should be >= previous capacity (not required),
	// but at minimum it must satisfy the requested size.
	_ = capSmall // documented for clarity; not asserted to avoid flakiness.
}

/*
TestGetRow_ZeroColCount verifies that requesting zero columns yields a non-nil
Row with an empty slice; Free must succeed without panic.
*/
func TestGetRow_ZeroColCount(t *testing.T) {
	r := GetRow(0)
	if r == nil {
		t.Fatal("GetRow(0) returned nil Row")
	}
	if len(r.V) != 0 {
		t.Fatalf("len(V)=%d; want 0", len(r.V))
	}
	r.Free()
}

/*
TestGetRow_ConcurrentSafety performs concurrent GetRow/Free cycles to ensure
the pool is safe under parallel access. Run with -race to let the race detector
validate there are no data races within the pool usage.
*/
func TestGetRow_ConcurrentSafety(t *testing.T) {
	const workers = 8
	const iters = 2000
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				r := GetRow(4)
				// Write some values; indices must be in-bounds.
				r.V[0], r.V[1], r.V[2], r.V[3] = "a", "b", "c", "d"
				r.Free()
			}
		}()
	}
	wg.Wait()
}

/*
TestGetRow_FreeReuseLowAllocs checks that, after a warm-up allocation,
steady-state GetRow/Free cycles are low-allocation. We allow a tiny budget
to avoid flakiness across Go versions.
*/
func TestGetRow_FreeReuseLowAllocs(t *testing.T) {
	// Warm up pool.
	GetRow(3).Free()
	runtime.GC()

	allocs := testingAllocsPerRun(1000, func() {
		r := GetRow(3)
		r.Free()
	})
	// In steady state this should be ~0.0 allocs/op; allow a small budget.
	if allocs > 0.1 {
		t.Fatalf("allocs/op=%0.2f; want <= 0.10", allocs)
	}
}

// testingAllocsPerRun wraps testing.AllocsPerRun to keep the test file tidy.
func testingAllocsPerRun(runs int, f func()) float64 {
	return testing.AllocsPerRun(runs, f)
}

/*
BenchmarkGetRow_Free_SameSize measures steady-state throughput of GetRow/Free
with a constant column count (common case in pipelines).
*/
func BenchmarkGetRow_Free_SameSize(b *testing.B) {
	const cols = 8
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r := GetRow(cols)
		r.V[0] = "x" // touch a field to keep V live
		r.Free()
	}
}

/*
BenchmarkGetRow_Free_Growing exercises occasional capacity growth (e.g., schema
changes or heterogeneous inputs).
*/
func BenchmarkGetRow_Free_Growing(b *testing.B) {
	b.ReportAllocs()
	col := 1
	for i := 0; i < b.N; i++ {
		col = (col%32 + 1) // 1..32 columns
		r := GetRow(col)
		if col > 0 {
			r.V[0] = "x"
		}
		r.Free()
	}
}

/*
BenchmarkGetRow_Free_AlternateSizes alternates between a small and a large row
to stress zeroing cost and capacity reuse behavior.
*/
func BenchmarkGetRow_Free_AlternateSizes(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		size := 4
		if i&1 == 1 {
			size = 64
		}
		r := GetRow(size)
		if size > 0 {
			r.V[size-1] = "y"
		}
		r.Free()
	}
}

/*
BenchmarkGetRow_Free_Parallel exercises the pool under parallel contention.
*/
func BenchmarkGetRow_Free_Parallel(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r := GetRow(8)
			r.Free()
		}
	})
}
