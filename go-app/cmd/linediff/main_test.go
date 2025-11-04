package main

import (
	"bufio"
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
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

func makeTempFileOfSize(t *testing.T, size int64) *os.File {
	t.Helper()
	dir := t.TempDir()
	fp := filepath.Join(dir, "tmp.bin")
	f, err := os.Create(fp)
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	// Ensure the file is exactly `size` bytes.
	if err := f.Truncate(size); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return f
}

// Test 1: Basic scan range
func TestScanRangeSimple(t *testing.T) {
	// file1 index contains a,b,c
	idx := buildIndexFromStrings([]string{"a", "b", "c"})

	// file2 has a,b,c (hits), d,e (misses)
	data := "a\nb\nc\nd\ne\n"
	r := &mockReaderAt{data: []byte(data)}

	// per-worker context and opts
	wc := &WorkerCtx{
		Buf: make([]byte, defaultBufSz),
		Out: make([]byte, 0, 8),
	}
	opts := Opts{
		StripCRLF: true,
		FlushSz:   8,
		BufSz:     defaultBufSz,
		TmpSz:     defaultTmpSz,
	}

	out, err := scanRange(
		r,
		int64(len(data)),
		fileRange{0, int64(len(data))},
		idx,
		wc,
		opts,
	)
	if err != nil {
		t.Fatalf("scanRange error: %v", err)
	}

	res := string(out)
	if res != "d\ne\n" {
		t.Fatalf("unexpected out: %q", res)
	}
}

// Test 2: Unterminated final line
func TestScanRangeUnterminatedFinalLine(t *testing.T) {
	// file1 contains foo
	idx := buildIndexFromStrings([]string{"foo"})

	// file2 has foo and bar BUT bar has no final newline
	data := "foo\nbar"
	r := &mockReaderAt{data: []byte(data)}

	wc := &WorkerCtx{
		Buf: make([]byte, defaultBufSz),
		Out: make([]byte, 0, 1024),
	}
	opts := Opts{
		StripCRLF: true,
		FlushSz:   1024,
		BufSz:     defaultBufSz,
		TmpSz:     defaultTmpSz,
	}

	out, err := scanRange(
		r,
		int64(len(data)),
		fileRange{0, int64(len(data))},
		idx,
		wc,
		opts,
	)
	if err != nil {
		t.Fatalf("scanRange error: %v", err)
	}

	res := string(out)
	if res != "bar\n" {
		t.Fatalf("expected bar\\n, got %q", res)
	}
}

// Test 3: Test CRLF stripping (conversion from CRLF to LF)
func TestScanRangeCRLF(t *testing.T) {
	idx := buildIndexFromStrings([]string{"alpha"})

	data := []byte("alpha\r\nbeta\r\n")
	r := &mockReaderAt{data: data}

	wc := &WorkerCtx{
		Buf: make([]byte, defaultBufSz),
		Out: make([]byte, 0, 1024),
	}
	opts := Opts{
		StripCRLF: true, // strip CR
		FlushSz:   1024,
		BufSz:     defaultBufSz,
		TmpSz:     defaultTmpSz,
	}

	out, err := scanRange(
		r,
		int64(len(data)),
		fileRange{0, int64(len(data))},
		idx,
		wc,
		opts,
	)
	if err != nil {
		t.Fatalf("scanRange error: %v", err)
	}

	res := strings.TrimSpace(string(out))
	if res != "beta" {
		t.Fatalf("expected beta, got %q", res)
	}
}

func TestSplitRanges(t *testing.T) {
	type args struct {
		size   int64
		parts  int
		minBlk int64
	}
	tests := []struct {
		name    string
		args    args
		want    []fileRange
		wantNil bool // when we specifically expect a nil slice
	}{
		{
			name: "zero-size file returns nil slice",
			args: args{size: 0, parts: 4, minBlk: 1},
			// function returns (nil, nil) for size==0
			wantNil: true,
		},
		{
			name: "even split into 4 parts",
			args: args{size: 100, parts: 4, minBlk: 1},
			want: []fileRange{
				{start: 0, end: 25},
				{start: 25, end: 50},
				{start: 50, end: 75},
				{start: 75, end: 100},
			},
		},
		{
			name: "uneven split with remainder (parts=3) produces small tail range",
			args: args{size: 100, parts: 3, minBlk: 1},
			// chunk = floor(100/3)=33; ranges step by 33, last one caps at size
			want: []fileRange{
				{start: 0, end: 33},
				{start: 33, end: 66},
				{start: 66, end: 99},
				{start: 99, end: 100},
			},
		},
		{
			name: "minBlk overrides small chunks",
			args: args{size: 100, parts: 10, minBlk: 30},
			// size/parts = 10, minBlk=30 => chunk=30
			want: []fileRange{
				{start: 0, end: 30},
				{start: 30, end: 60},
				{start: 60, end: 90},
				{start: 90, end: 100},
			},
		},
		{
			name: "parts less than 1 treated as 1",
			args: args{size: 10, parts: 0, minBlk: 1},
			// parts -> 1, chunk=size/1=10
			want: []fileRange{
				{start: 0, end: 10},
			},
		},
		{
			name: "exact multiple with minBlk larger than size/parts",
			args: args{size: 96, parts: 16, minBlk: 24},
			// size/parts=6 -> chunk=24 -> exact 4 chunks
			want: []fileRange{
				{start: 0, end: 24},
				{start: 24, end: 48},
				{start: 48, end: 72},
				{start: 72, end: 96},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			f := makeTempFileOfSize(t, tt.args.size)
			defer f.Close()

			got, err := splitRanges(f, tt.args.parts, tt.args.minBlk)
			if err != nil {
				t.Fatalf("splitRanges returned error: %v", err)
			}

			if tt.wantNil {
				if got != nil {
					t.Fatalf("expected nil slice, got %#v", got)
				}
				return
			}

			if !equalRanges(got, tt.want) {
				t.Fatalf("ranges mismatch.\n got: %#v\nwant: %#v", got, tt.want)
			}
		})
	}
}

func equalRanges(a, b []fileRange) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].start != b[i].start || a[i].end != b[i].end {
			return false
		}
	}
	return true
}

func TestBuildIndex16Parallel(t *testing.T) {
	// Create a temporary directory
	tmpDir, err := ioutil.TempDir("", "buildIndex16ParallelTest")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir) // Clean up temp directory after test

	// Generate a sample test file in the temp directory
	testFilePath := filepath.Join(tmpDir, "sample.txt")
	fileContent := "line1\nline2\nline3\nline4\r\n"
	err = ioutil.WriteFile(testFilePath, []byte(fileContent), 0644)
	if err != nil {
		t.Fatalf("Failed to write to test file: %v", err)
	}

	// Now test your buildIndex16Parallel function with this file
	stripCRLF := false
	workers := 4
	minBlk := int64(1024 * 1024) // Example block size (1 MB)

	index, err := buildIndex16Parallel(testFilePath, stripCRLF, workers, minBlk)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if index == nil {
		t.Fatalf("Expected a non-nil Index16")
	}

	// Add further validation of the resulting index as needed
}
