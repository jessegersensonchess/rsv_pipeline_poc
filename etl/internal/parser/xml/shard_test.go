package xmlparser

import (
	"io"
	"testing"
)

const sample = `<Root><Rec><A>1</A></Rec><Rec><A>2</A></Rec></Root>`

func TestShardStreaming(t *testing.T) {
	jc := make(chan Job, 4)
	err := ShardStreaming(stringsReader(sample), "Rec", 1<<16, jc)
	close(jc)
	if err != nil {
		t.Fatal(err)
	}
	cnt := 0
	for j := range jc {
		if len(j.Bytes) == 0 {
			t.Fatalf("empty shard")
		}
		cnt++
	}
	if cnt != 2 {
		t.Fatalf("got %d shards", cnt)
	}
}

func TestShardZeroCopy(t *testing.T) {
	jc := make(chan Job, 4)
	if err := ShardZeroCopy([]byte(sample), "Rec", jc); err != nil {
		t.Fatal(err)
	}
	close(jc)
	n := 0
	for range jc {
		n++
	}
	if n != 2 {
		t.Fatalf("got %d shards", n)
	}
}

// stringsReader avoids importing bytes for a one-off.
func stringsReader(s string) *stringReader { return &stringReader{s: s} }

type stringReader struct {
	s string
	i int
}

func (r *stringReader) Read(p []byte) (int, error) {
	if r.i >= len(r.s) {
		return 0, io.EOF
	}
	n := copy(p, r.s[r.i:])
	r.i += n
	return n, nil
}
