package main

import (
	"bufio"
	crand "crypto/rand"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
)

// ---------- testdata helpers ----------

type genCfg struct {
	nLines      int     // total lines
	lineLen     int     // length excluding newline
	crlf        bool    // add "\r\n"
	uniqueRatio float64 // fraction of lines unique vs duplicates
	alpha       string  // charset
	seed        int64
}

func genLines(cfg genCfg) [][]byte {
	if cfg.alpha == "" {
		cfg.alpha = "abcdefghijklmnopqrstuvwxyz0123456789_-"
	}
	rng := rand.New(rand.NewSource(cfg.seed)) // no v2, no uint64 xor
	// Build a base pool for duplicates to come from.
	pool := make([][]byte, int(math.Max(1, float64(cfg.nLines)*(1-cfg.uniqueRatio))))
	for i := range pool {
		pool[i] = randLine(rng, cfg.lineLen, cfg.alpha, cfg.crlf)
	}
	out := make([][]byte, cfg.nLines)
	for i := 0; i < cfg.nLines; i++ {
		if rng.Float64() < cfg.uniqueRatio {
			out[i] = randLine(rng, cfg.lineLen, cfg.alpha, cfg.crlf)
		} else {
			out[i] = pool[rng.Intn(len(pool))]
		}
	}
	return out
}

func randLine(rng *rand.Rand, n int, alpha string, crlf bool) []byte {
	b := make([]byte, n+1)
	for i := 0; i < n; i++ {
		b[i] = alpha[rng.Intn(len(alpha))]
	}
	b[n] = '\n'
	if crlf {
		// convert "\n" -> "\r\n" without extra alloc
		b = append([]byte{}, b[:n]...)
		b = append(b, '\r', '\n')
	}
	return b
}

func writeTempFile(tb testing.TB, lines [][]byte) (path string, size int64) {
	tb.Helper()
	dir := tb.TempDir()
	path = filepath.Join(dir, "f")
	f, err := os.Create(path)
	if err != nil {
		tb.Fatal(err)
	}
	defer f.Close()
	bw := bufio.NewWriterSize(f, 4<<20)
	var n int64
	for _, ln := range lines {
		nn, _ := bw.Write(ln)
		n += int64(nn)
	}
	if err := bw.Flush(); err != nil {
		tb.Fatal(err)
	}
	st, _ := f.Stat()
	return path, st.Size()
}

// overlaps: returns file2 with a controllable overlap with file1.
// keepFirstLineHeader: if true, file2 starts with file1[0] as CSV header.
func makePair(tb testing.TB, n1, n2, len1 int, crlf bool, overlap float64, keepFirstLineHeader bool) (file1, file2 string, sz1, sz2 int64) {
	tb.Helper()
	l1 := genLines(genCfg{nLines: n1, lineLen: len1, crlf: crlf, uniqueRatio: 0.7, seed: 1})
	l2 := genLines(genCfg{nLines: n2, lineLen: len1, crlf: crlf, uniqueRatio: 0.7, seed: 2})

	// mix some overlap
	ov := int(float64(min(n1, n2)) * overlap)
	for i := 0; i < ov; i++ {
		l2[i] = l1[i%len(l1)]
	}

	if keepFirstLineHeader && len(l1) > 0 {
		l2[0] = l1[0]
	}

	file1, sz1 = writeTempFile(tb, l1)
	file2, sz2 = writeTempFile(tb, l2)
	return
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ---------- microbenchmarks ----------

func BenchmarkHash48(b *testing.B) {
	data := make([]byte, 1024)
	_, _ = crand.Read(data)
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = hash48b(data)
	}
}

func BenchmarkBinarySearch32_MissVsHit(b *testing.B) {
	// Build bins of various sizes to see logN behavior and cache effects.
	for _, sz := range []int{32, 128, 512, 4096, 65536} {
		a := make([]uint32, sz)
		for i := range a {
			a[i] = uint32(i*2 + 1) // ensure "miss" quickly
		}
		b.Run(fmt.Sprintf("size=%d/miss", sz), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if binarySearch32(a, 0) {
					b.Fatal("impossible")
				}
			}
		})
		b.Run(fmt.Sprintf("size=%d/hit", sz), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if !binarySearch32(a, a[sz/2]) {
					b.Fatal("miss")
				}
			}
		})
	}
}

func BenchmarkStrip(b *testing.B) {
	line := []byte(strings.Repeat("x", 128) + "\r")
	b.Run("stripCR=true", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = stripCR(line, true)
		}
	})
	b.Run("stripCR=false", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = stripCR(line, false)
		}
	})
}

func BenchmarkFastStripNL(b *testing.B) {
	line := []byte(strings.Repeat("x", 128) + "\n")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = fastStripNL(line)
	}
}

// ---------- indexing benchmarks ----------

func BenchmarkBuildIndex16_Parallel(b *testing.B) {
	f1, _, sz1, _ := makePair(b, 1_000_000, 10, 64, false, 0.0, false)
	b.ReportAllocs()
	b.SetBytes(sz1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx, err := buildIndex16Parallel(f1, false, runtime.GOMAXPROCS(0), 2<<20)
		if err != nil {
			b.Fatal(err)
		}
		_ = idx
	}
}

// ---------- scanning (hot loop) ----------

// ---------- end-to-end ----------

func BenchmarkEndToEnd(b *testing.B) {
	f1, f2, _, sz2 := makePair(b, 1_000_000, 1_000_000, 64, false, 0.7, true)

	b.ReportAllocs()
	b.SetBytes(sz2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// keep GC comparable to main()
		if os.Getenv("GOGC") == "" {
			runtime.GC()
			debugSetGCPercent(800)
		}
		idx, err := buildIndex16Parallel(f1, false, runtime.GOMAXPROCS(0), 2<<20)
		if err != nil {
			b.Fatal(err)
		}

		f2h, err := os.Open(f2)
		if err != nil {
			b.Fatal(err)
		}
		st2, _ := f2h.Stat()
		filesz := st2.Size()

		parts := runtime.GOMAXPROCS(0) * 32
		ranges, err := splitRanges(f2h, parts, defaultBlkSz)
		if err != nil {
			b.Fatal(err)
		}

		bw := bufio.NewWriter(io.Discard) // measure compute + syscalls sans disk
		var wmu sync.Mutex

		jobs := make(chan fileRange, len(ranges))
		for _, rg := range ranges {
			jobs <- rg
		}
		close(jobs)

		var wg sync.WaitGroup
		workers := runtime.GOMAXPROCS(0)
		wg.Add(workers)
		for w := 0; w < workers; w++ {
			go func() {
				defer wg.Done()
				for rg := range jobs {
					_ = scanRange(f2h, filesz, rg, idx, bw, &wmu, false, defaultOutputFlushSz, nil)
				}
			}()
		}
		wg.Wait()
		_ = bw.Flush()
		_ = f2h.Close()
	}
}

// small helper (avoid importing runtime/debug all over)
func debugSetGCPercent(p int) {
	defer func() { _ = recover() }()
	debug.SetGCPercent(p)
}

func BenchmarkBuildIndex16Parallel(b *testing.B) {
	// Create a temporary directory for the test
	tmpDir, err := ioutil.TempDir("", "BenchmarkOptimizedBuildIndex16Parallel")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir) // Clean up temp directory after the benchmark

	// Generate a sample test file in the temp directory
	testFilePath := filepath.Join(tmpDir, "sample.txt")
	fileContent := "line1\nline2\nline3\nline4\r\n" // Example content
	err = ioutil.WriteFile(testFilePath, []byte(fileContent), 0644)
	if err != nil {
		b.Fatalf("Failed to write to test file: %v", err)
	}

	// Define parameters for the benchmark
	stripCRLF := false
	workers := 4
	minBlk := int64(1024 * 1024) // Example block size (1 MB)

	// Reset the timer to exclude setup time from the benchmark
	b.ResetTimer()

	// Run the benchmark for N iterations
	for i := 0; i < b.N; i++ {
		_, err := buildIndex16Parallel(testFilePath, stripCRLF, workers, minBlk)
		if err != nil {
			b.Fatalf("Benchmark failed with error: %v", err)
		}
	}
}
