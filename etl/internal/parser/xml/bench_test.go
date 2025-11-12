package xmlparser

import (
	"context"
	"runtime"
	"strings"
	"testing"
)

func synth(n int) string {
	var b strings.Builder
	b.WriteString("<Root>")
	for i := 0; i < n; i++ {
		b.WriteString("<Rec><A>1</A><B>2</B></Rec>")
	}
	b.WriteString("</Root>")
	return b.String()
}

func BenchmarkParseStream_Streaming(b *testing.B) {
	cfg := Config{RecordTag: "Rec", Fields: map[string]string{"a": "A", "b": "B"}}
	comp, _ := Compile(cfg)
	xml := synth(20000)
	opts := Options{Workers: runtime.NumCPU(), Queue: 4 * runtime.NumCPU(), BufSize: 1 << 20}
	ctx := context.Background()

	b.ReportAllocs()
	b.SetBytes(int64(len(xml)))
	for i := 0; i < b.N; i++ {
		out := make(chan Record, opts.Queue)
		go func() {
			for range out {
			}
		}()
		if err := ParseStream(ctx, strings.NewReader(xml), nil, "Rec", comp, opts, out); err != nil {
			b.Fatal(err)
		}
	}
}
