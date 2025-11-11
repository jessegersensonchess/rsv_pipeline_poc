package ddlgen

import (
	"strconv"
	"testing"

	"etl/internal/config"
)

/*
Micro-benchmarks for the most allocation-sensitive helpers in infer.go.

We focus on:
  - mapType: trivial, but called many times across columns
  - InferTableDef with no transforms and varying column counts
*/

func BenchmarkMapType(b *testing.B) {
	inputs := []string{
		"", "int", "integer", "bigint",
		"bool", "BOOLEAN", "date", "timestamp", "timestamptz",
		"custom", "TEXT",
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for _, s := range inputs {
			_ = mapType(s)
		}
	}
}

func BenchmarkInferTableDef_NoTransforms_Small(b *testing.B) {
	p := config.Pipeline{}
	p.Storage.Postgres.Table = "bench.small"
	p.Storage.Postgres.Columns = []string{"id", "name", "payload"}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := InferTableDef(p); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInferTableDef_NoTransforms_Wide(b *testing.B) {
	const n = 128
	p := config.Pipeline{}
	p.Storage.Postgres.Table = "bench.wide"
	p.Storage.Postgres.Columns = make([]string, n)
	for i := 0; i < n; i++ {
		p.Storage.Postgres.Columns[i] = "c" + strconv.Itoa(i)
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := InferTableDef(p); err != nil {
			b.Fatal(err)
		}
	}
}
