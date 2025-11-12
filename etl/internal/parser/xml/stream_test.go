package xmlparser

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"
)

func TestParseStream_Streaming(t *testing.T) {
	cfg := Config{
		RecordTag: "Rec",
		Fields:    map[string]string{"a": "A"},
	}
	comp, _ := Compile(cfg)
	xml := `<Root><Rec><A>1</A></Rec><Rec><A>2</A></Rec></Root>`
	out := make(chan Record, 4)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := ParseStream(ctx, strings.NewReader(xml), nil, "Rec", comp, Options{
		Workers: 2, Queue: 4, BufSize: 64 << 10,
	}, out); err != nil {
		t.Fatal(err)
	}
	close(out)
	sum := 0
	for r := range out {
		if r["a"] == "1" || r["a"] == "2" {
			sum++
		}
	}
	if sum != 2 {
		t.Fatalf("expected 2 records, got %d", sum)
	}
}

func TestParseStream_ZeroCopy(t *testing.T) {
	cfg := Config{RecordTag: "X", Fields: map[string]string{"v": "V"}}
	comp, _ := Compile(cfg)
	xml := `<Root><X><V>k</V></X></Root>`
	out := make(chan Record, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := ParseStream(ctx, io.NopCloser(nil), []byte(xml), "X", comp, Options{
		Workers: 1, Queue: 2, ZeroCopy: true,
	}, out); err != nil {
		t.Fatal(err)
	}
	close(out)
	if len(out) != 1 {
		t.Fatalf("expected 1")
	}
}
