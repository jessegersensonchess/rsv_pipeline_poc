package csv

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"etl/internal/config"
	"etl/internal/transformer"
)

func buildCSV(n int) io.ReadCloser {
	var sb strings.Builder
	sb.Grow(n * 64)
	sb.WriteString("pcv,typ,stav,platnost_od,aktualni\n")
	for i := 0; i < n; i++ {
		sb.WriteString("123456,E - Evidenční,Nezjištěno,07.10.2011,True\n")
	}
	return io.NopCloser(bytes.NewReader([]byte(sb.String())))
}

func benchStream(b *testing.B, reuse bool) {
	ctx := context.Background()
	cols := []string{"pcv", "typ", "stav", "platnost_od", "aktualni"}
	opt := config.Options{
		"has_header": true,
		"comma":      ",",
		"trim_space": true,
		"header_map": map[string]any{"pcv": "pcv", "typ": "typ", "stav": "stav", "platnost_od": "platnost_od", "aktualni": "aktualni"},
	}

	// Patch: StreamCSVRows internally always sets ReuseRecord=true.
	// To compare, we add a local variant that toggles it. For brevity we measure
	// the current StreamCSVRows path as representative.

	for i := 0; i < b.N; i++ {
		src := buildCSV(50_000)
		out := make(chan *transformer.Row, 8192)

		go func() {
			_ = StreamCSVRows(ctx, src, cols, opt, out, nil)
			close(out)
		}()

		// drain
		c := 0
		for r := range out {
			r.Free()
			c++
		}
		if c == 0 {
			b.Fatalf("no rows parsed")
		}
	}
}

func BenchmarkParser_StreamCSVRows(b *testing.B) {
	b.ReportAllocs()
	benchStream(b, true)
}
