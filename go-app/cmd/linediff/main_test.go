package main

import (
	"bufio"
	"bytes"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
)

// mockReaderAt allows us to simulate file2 in memory.
type mockReaderAt struct {
	data []byte
}

func (m *mockReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n := copy(p, m.data[off:])
	if int(off)+n >= len(m.data) {
		return n, io.EOF
	}
	return n, nil
}

// helper for building an Index16 in tests (no file I/O)
func buildIndexFromStrings(lines []string) *Index16 {
	counts := make([]int, 1<<16)

	// Pass 1: count
	for _, s := range lines {
		b := []byte(s)
		h := hash48b(b)
		counts[uint16(h>>32)]++
	}

	// offsets
	off := make([]int, (1<<16)+1)
	total := 0
	for i := 0; i < (1 << 16); i++ {
		off[i] = total
		total += counts[i]
	}
	off[1<<16] = total

	// allocate
	data := make([]uint32, total)
	cursor := make([]int, len(off))
	copy(cursor, off)

	// Pass 2: fill
	for _, s := range lines {
		b := []byte(s)
		h := hash48b(b)
		top := uint16(h >> 32)
		pos := cursor[top]
		data[pos] = uint32(h & 0xFFFFFFFF)
		cursor[top] = pos + 1
	}

	// sort bins
	for i := 0; i < (1 << 16); i++ {
		lo, hi := off[i], off[i+1]
		if hi-lo > 1 {
			sort.Slice(data[lo:hi], func(a, b int) bool { return data[lo+a] < data[lo+b] })
		}
	}

	return &Index16{off: off, data: data}
}

func TestIndexContains(t *testing.T) {
	known := []string{"alpha", "beta", "gamma"}
	idx := buildIndexFromStrings(known)

	for _, s := range known {
		h := hash48b([]byte(s))
		if !idx.Contains(h) {
			t.Fatalf("expected to find %q in index", s)
		}
	}

	missing := []string{"ALPHA", "z", "beta2"}
	for _, s := range missing {
		h := hash48b([]byte(s))
		if idx.Contains(h) {
			t.Fatalf("unexpected hit for %q", s)
		}
	}
}

func TestScanRangeSimple(t *testing.T) {
	// file1 index contains a,b,c
	idx := buildIndexFromStrings([]string{"a", "b", "c"})

	// file2 has a,b,c (hits), d,e (misses)
	data := "a\nb\nc\nd\ne\n"
	r := &mockReaderAt{data: []byte(data)}

	var out bytes.Buffer
	wmu := sync.Mutex{}
	flushSz := 8

	err := scanRange(
		r,
		int64(len(data)),
		fileRange{0, int64(len(data))},
		idx,
		&out,
		&wmu,
		true,
		flushSz,
	)
	if err != nil {
		t.Fatalf("scanRange error: %v", err)
	}

	res := out.String()
	if res != "d\ne\n" {
		t.Fatalf("unexpected out: %q", res)
	}
}

func TestScanRangeUnterminatedFinalLine(t *testing.T) {
	// file1 contains foo
	idx := buildIndexFromStrings([]string{"foo"})

	// file2 has foo and bar BUT bar has no final newline
	data := "foo\nbar"
	r := &mockReaderAt{data: []byte(data)}

	var out bytes.Buffer
	wmu := sync.Mutex{}

	err := scanRange(
		r,
		int64(len(data)),
		fileRange{0, int64(len(data))},
		idx,
		&out,
		&wmu,
		true,
		1024,
	)
	if err != nil {
		t.Fatalf("scanRange error: %v", err)
	}

	res := out.String()
	if res != "bar\n" {
		t.Fatalf("expected bar\\n, got %q", res)
	}
}

func TestScanRangeCRLF(t *testing.T) {
	idx := buildIndexFromStrings([]string{"alpha"})

	data := []byte("alpha\r\nbeta\r\n")
	r := &mockReaderAt{data: data}

	var out bytes.Buffer
	wmu := sync.Mutex{}

	err := scanRange(
		r,
		int64(len(data)),
		fileRange{0, int64(len(data))},
		idx,
		&out,
		&wmu,
		true, // strip CR
		1024,
	)
	if err != nil {
		t.Fatalf("scanRange error: %v", err)
	}

	res := strings.TrimSpace(out.String())
	if res != "beta" {
		t.Fatalf("expected beta, got %q", res)
	}
}

// Test header emit logic
func TestHeaderEmit(t *testing.T) {
	header := "col1,col2\n"
	body := "a,b\nc,d\n"
	data := []byte(header + body)

	buf := bytes.NewBuffer(nil)
	bw := bufio.NewWriter(buf)

	// emit header as main() does
	hdrReader := bytes.NewReader(data)
	r := bufio.NewReader(hdrReader)
	line, err := r.ReadBytes('\n')
	if err != nil && err != io.EOF {
		t.Fatalf("bad header read: %v", err)
	}
	if _, werr := bw.Write(line); werr != nil {
		t.Fatalf("bad header write: %v", werr)
	}
	_ = bw.Flush()

	out := buf.String()
	if out != header {
		t.Fatalf("expected header %q, got %q", header, out)
	}
}
