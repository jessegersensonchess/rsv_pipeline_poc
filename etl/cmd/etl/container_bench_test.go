package main

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"etl/internal/config"
)

/*
Micro-benchmarks for hot-path helpers.

We avoid runStreamed: its throughput depends on external systems. These
benchmarks aim to keep helper regressions visible.
*/

func BenchmarkSplitFQN(b *testing.B) {
	inputs := []string{
		"t",
		"s.t",
		"a.b.c",
		".leading..dots.",
		"deep.schema.with.many.segments.table",
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for _, s := range inputs {
			_ = splitFQN(s)
		}
	}
}

func BenchmarkErrAgg_Add(b *testing.B) {
	a := newErrAgg(8)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		a.add("same-message-" + strconv.Itoa(i&7))
	}
}

func BenchmarkGetenvInt(b *testing.B) {
	// Hot-path where env is set and parse succeeds.
	b.Setenv("ETL_BENCH_INT", "123")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = getenvInt("ETL_BENCH_INT", 0)
	}
}

func BenchmarkOpenSource_File(b *testing.B) {
	dir := b.TempDir()
	p := filepath.Join(dir, "x.txt")
	if err := os.WriteFile(p, []byte("payload"), 0o644); err != nil {
		b.Fatalf("write temp: %v", err)
	}
	spec := config.Pipeline{
		Source: config.Source{
			Kind: "file",
			File: config.SourceFile{Path: p},
		},
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rc, err := openSource(context.Background(), spec)
		if err != nil {
			b.Fatal(err)
		}
		rc.Close()
	}
}
