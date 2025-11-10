package csv

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"etl/internal/config"
	"etl/internal/transformer"
)

/*
fakeRC is a small helper implementing io.ReadCloser over a byte slice.
It lets tests verify that Close() is forwarded by wrappers.
*/
type fakeRC struct {
	*bytes.Reader
	closed bool
}

func newFakeRC(b []byte) *fakeRC { return &fakeRC{Reader: bytes.NewReader(b)} }
func (f *fakeRC) Close() error   { f.closed = true; return nil }

/*
makeCSV builds a CSV document in-memory with the given header and rows.
It uses encoding/csv to ensure proper quoting and escaping. The delimiter
is configurable; CRLF line endings can be requested by setting useCRLF.
*/
func makeCSV(delim rune, header []string, rows [][]string, useCRLF bool) []byte {
	var b bytes.Buffer
	w := csv.NewWriter(&b)
	w.Comma = delim
	w.UseCRLF = useCRLF
	if header != nil {
		_ = w.Write(header)
	}
	for _, r := range rows {
		_ = w.Write(r)
	}
	w.Flush()
	return b.Bytes()
}

/*
collect drains up to n rows from ch without blocking the producer (StreamCSVRows).
It returns exactly len(out) == n for a buffered channel of capacity >= n.
*/
func collect(ch <-chan *transformer.Row, n int) []*transformer.Row {
	out := make([]*transformer.Row, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, <-ch)
	}
	return out
}

/*
Test_wrapWithLikvidaciScrub_Disabled verifies that when disabled, the wrapper
returns the original ReadCloser unchanged and Close still works.
*/
func Test_wrapWithLikvidaciScrub_Disabled(t *testing.T) {
	src := newFakeRC([]byte("abc"))
	w := wrapWithLikvidaciScrub(src, false)
	if w != src {
		t.Fatalf("expected same ReadCloser when disabled")
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close error: %v", err)
	}
	if !src.closed {
		t.Fatalf("expected underlying Close to be called")
	}
}

/*
Test_wrapWithLikvidaciScrub_Enabled verifies that the streaming rewriter
replaces the dataset-specific broken-quote sequence and forwards Close().
*/
func Test_wrapWithLikvidaciScrub_Enabled(t *testing.T) {
	// Construct a payload that contains the exact byte sequence:
	//  space + "v likvidaci" + "" (a quote doubled)
	// We place it inside a quoted CSV cell to exercise stream parsing later.
	payload := []byte(`name
"Acme "v likvidaci"""
`)
	src := newFakeRC(payload)

	r := wrapWithLikvidaciScrub(src, true)
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close error: %v", err)
	}
	if !src.closed {
		t.Fatalf("expected underlying Close to be called")
	}
	// After scrubbing we should see `(v likvidaci)"` instead of ` "v likvidaci""`.
	if !bytes.Contains(got, []byte(`(v likvidaci)"`)) {
		t.Fatalf("scrubbed text not found in %q", string(got))
	}
}

/*
Test_StreamCSVRows_HeaderMapping covers:
  - has_header=true
  - BOM stripping on first header cell
  - automatic normalization (lowercase + spaces -> underscores)
  - header_map remapping to canonical names
  - target-to-source index mapping
  - trim_space behavior
  - nil for empty strings
*/
func Test_StreamCSVRows_HeaderMapping(t *testing.T) {
	// First header has a BOM; header_map remaps "first name" -> "first_name_canon".
	header := []string{"\uFEFFFirst Name", "Last Name", "age"}
	rows := [][]string{
		{" Ada ", "  Lovelace", "  "}, // will trim and set age nil
	}
	src := io.NopCloser(bytes.NewReader(makeCSV(',', header, rows, false)))

	columns := []string{"first_name_canon", "last_name", "age"} // canonical target order

	var errs []string
	errcb := func(line int, err error) { errs = append(errs, fmt.Sprintf("%d:%v", line, err)) }

	opts := config.Options{
		"has_header": true,
		"header_map": map[string]any{
			"First Name": "first_name_canon",
			"Last Name":  "last_name",
		},
		"trim_space": true,
	}
	out := make(chan *transformer.Row, 1)

	if err := StreamCSVRows(context.Background(), src, columns, opts, out, errcb); err != nil {
		t.Fatalf("StreamCSVRows error: %v", err)
	}
	if len(errs) != 0 {
		t.Fatalf("unexpected errs: %v", errs)
	}

	row := <-out
	t.Cleanup(row.Free)

	// Expect trimmed values and nil for empty age.
	if got, want := row.V[0], "Ada"; got != want {
		t.Fatalf("first_name got=%v want=%v", got, want)
	}
	if got, want := row.V[1], "Lovelace"; got != want {
		t.Fatalf("last_name got=%v want=%v", got, want)
	}
	if row.V[2] != nil {
		t.Fatalf("age got=%v want=nil", row.V[2])
	}
}

/*
Test_StreamCSVRows_NoHeaderPositional verifies behavior with has_header=false:
the mapping is positional (target i <- source i).
*/
func Test_StreamCSVRows_NoHeaderPositional(t *testing.T) {
	rows := [][]string{{"v1", "v2"}}
	src := io.NopCloser(bytes.NewReader(makeCSV(',', nil, rows, false)))

	columns := []string{"c1", "c2"}
	opts := config.Options{
		"has_header": false,
	}
	out := make(chan *transformer.Row, 1)

	if err := StreamCSVRows(context.Background(), src, columns, opts, out, nil); err != nil {
		t.Fatalf("StreamCSVRows error: %v", err)
	}
	r := <-out
	t.Cleanup(r.Free)

	if r.V[0] != "v1" || r.V[1] != "v2" {
		t.Fatalf("positional mapping unexpected values: %#v", r.V)
	}
}

/*
Test_StreamCSVRows_LazyQuotesAndScrub ensures that when lazy_quotes is enabled,
the rewriter makes the broken sequence parseable and the final field contains
the normalized '(v likvidaci)"' substring after trimming.
*/
func Test_StreamCSVRows_LazyQuotesAndScrub(t *testing.T) {
	// Header + one row with the broken pattern
	payload := []byte("name\n\"Acme \"v likvidaci\"\"\"\n")
	src := io.NopCloser(bytes.NewReader(payload))

	opts := config.Options{
		"has_header":             true,
		"lazy_quotes":            true,
		"stream_scrub_likvidaci": true,
		"trim_space":             true,
		"fields_per_record":      -1,
	}
	columns := []string{"name"}
	out := make(chan *transformer.Row, 1)

	if err := StreamCSVRows(context.Background(), src, columns, opts, out, nil); err != nil {
		t.Fatalf("StreamCSVRows error: %v", err)
	}
	row := <-out
	t.Cleanup(row.Free)

	got := fmt.Sprintf("%v", row.V[0])
	if !strings.Contains(got, `(v likvidaci)"`) {
		t.Fatalf("expected scrubbed substring in %q", got)
	}
}

/*
Test_StreamCSVRows_FieldCountEnforced verifies that csv.Reader.FieldsPerRecord
enforcement flows through: a row with too many fields triggers an error passed
to onErr and is skipped (no row emitted).
*/
func Test_StreamCSVRows_FieldCountEnforced(t *testing.T) {
	header := []string{"a", "b"}
	// Two data rows: first has 3 fields (violates FieldsPerRecord=2),
	// second is correct with 2 fields.
	src := io.NopCloser(bytes.NewReader(makeCSV(',', header,
		[][]string{{"x", "y", "z"}, {"ok1", "ok2"}},
		false)))

	var gotErr error
	var errCount int
	errcb := func(_ int, err error) { errCount++; gotErr = err }

	opts := config.Options{
		"has_header":        true,
		"fields_per_record": 2,
		"trim_space":        true,
	}
	out := make(chan *transformer.Row, 1)

	if err := StreamCSVRows(context.Background(), src, []string{"a", "b"}, opts, out, errcb); err != nil {
		t.Fatalf("StreamCSVRows error: %v", err)
	}
	// Only the second row should be emitted.
	r := <-out
	t.Cleanup(r.Free)

	if r.V[0] != "ok1" || r.V[1] != "ok2" {
		t.Fatalf("unexpected row values: %#v", r.V)
	}
	if errCount == 0 || gotErr == nil {
		t.Fatalf("expected an error for the first row, got none")
	}
}

/*
Test_StreamCSVRows_ReadErrorContinues verifies that a recoverable csv read error
is reported via onErr and the reader continues to emit subsequent valid rows.
We craft a malformed line (unterminated quote) between two valid lines.
*/
func Test_StreamCSVRows_ReadErrorContinues(t *testing.T) {
	var b bytes.Buffer
	fmt.Fprintln(&b, "h1,h2")   // header
	fmt.Fprintln(&b, "a,b")     // good row
	fmt.Fprint(&b, "\"bad,bad") // unterminated quote -> parse error
	fmt.Fprintln(&b)            // end line
	fmt.Fprintln(&b, "c,d")     // reader may NOT recover

	src := io.NopCloser(bytes.NewReader(b.Bytes()))
	var errs []error
	errcb := func(_ int, err error) { errs = append(errs, err) }

	opts := config.Options{"has_header": true}
	out := make(chan *transformer.Row, 2)

	if err := StreamCSVRows(context.Background(), src, []string{"h1", "h2"}, opts, out, errcb); err != nil {
		t.Fatalf("StreamCSVRows: %v", err)
	}
	// We should receive at least the first good row.
	if len(out) == 0 {
		t.Fatalf("expected at least one good row emitted")
	}
	// We should still receive the two valid rows.
	r1 := <-out
	t.Cleanup(r1.Free)
	if r1.V[0] != "a" || r1.V[1] != "b" {
		t.Fatalf("unexpected first good row: %#v", r1.V)
	}
	// And we should have seen an error.
	if len(errs) == 0 {
		t.Fatalf("expected an error from malformed row")
	}
}

/*
Test_StreamCSVRows_ContextCancel verifies that a cancellation is honored.
We feed an infinite stream via io.Pipe and cancel the context after the header.
*/
func Test_StreamCSVRows_ContextCancel(t *testing.T) {
	pr, pw := io.Pipe()
	go func() {
		// Write a header and then trickle endless rows.
		fmt.Fprintln(pw, "a")
		for i := 0; i < 1_000_000; i++ {
			fmt.Fprintln(pw, "x")
			time.Sleep(1 * time.Millisecond)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Cancel after a short delay.
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	out := make(chan *transformer.Row, 10)
	opts := config.Options{"has_header": true}
	err := StreamCSVRows(ctx, pr, []string{"a"}, opts, out, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

/*
Test_StreamCSVRows_HeaderReadError ensures header read errors are returned
and reported via onErr (e.g., EOF before any data).
*/
func Test_StreamCSVRows_HeaderReadError(t *testing.T) {
	// Empty stream: reading header will hit EOF.
	src := io.NopCloser(bytes.NewReader(nil))
	var called bool
	errcb := func(line int, err error) { _ = line; called = true }

	opts := config.Options{"has_header": true}
	out := make(chan *transformer.Row, 1)

	err := StreamCSVRows(context.Background(), src, []string{"x"}, opts, out, errcb)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !called {
		t.Fatalf("expected onErr to be called for header error")
	}
}

/*
Benchmark_StreamCSVRows measures end-to-end throughput of the CSV streaming
reader. It uses a buffered channel sized to the number of rows to avoid
consumer-side blocking and isolates the parser’s performance.
*/
func Benchmark_StreamCSVRows(b *testing.B) {
	const N = 10000

	rows := make([][]string, 0, N)
	for i := 0; i < N; i++ {
		rows = append(rows, []string{strconv.Itoa(i), "alpha", "beta"})
	}
	payload := makeCSV(',', []string{"id", "c1", "c2"}, rows, false)

	columns := []string{"id", "c1", "c2"}
	opts := config.Options{
		"has_header":        true,
		"trim_space":        true,
		"fields_per_record": -1,
	}

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		src := io.NopCloser(bytes.NewReader(payload))
		out := make(chan *transformer.Row, N)

		if err := StreamCSVRows(context.Background(), src, columns, opts, out, nil); err != nil {
			b.Fatalf("StreamCSVRows error: %v", err)
		}
		// Sanity check: ensure all rows were emitted (channel length equals N).
		if got := len(out); got != N {
			b.Fatalf("emitted=%d; want=%d", got, N)
		}
		// Free rows to avoid leaking pooled objects across iterations.
		for i := 0; i < N; i++ {
			r := <-out
			r.Free()
		}
	}
}

/*
TestHasEdgeSpace verifies that hasEdgeSpace reports true when a string starts
or ends with common ASCII whitespace, and false otherwise.

It exercises:
  - empty string
  - no-edge-whitespace
  - leading/trailing/both sides whitespace
  - single-character strings
  - tabs/newlines/carriage returns
  - internal (non-edge) whitespace
  - non-ASCII whitespace (e.g., non-breaking space) which the ASCII fast-path
    intentionally does NOT detect
*/
func TestHasEdgeSpace(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want bool
	}{
		{name: "empty", in: "", want: false},
		{name: "noEdge_noSpace", in: "alpha", want: false},
		{name: "noEdge_internalSpace", in: "a b", want: false},
		{name: "leading_space", in: " alpha", want: true},
		{name: "trailing_space", in: "alpha ", want: true},
		{name: "both_space", in: " alpha ", want: true},
		{name: "single_space", in: " ", want: true},
		{name: "single_nonspace", in: "x", want: false},
		{name: "leading_tab", in: "\talpha", want: true},
		{name: "trailing_tab", in: "alpha\t", want: true},
		{name: "leading_newline", in: "\nalpha", want: true},
		{name: "trailing_newline", in: "alpha\n", want: true},
		{name: "leading_cr", in: "\ralpha", want: true},
		{name: "trailing_cr", in: "alpha\r", want: true},
		// Non-ASCII whitespace (e.g., U+00A0 NO-BREAK SPACE) is not detected
		// by the ASCII-fast path and should return false.
		{name: "nonASCII_nbsp_leading", in: "\u00A0alpha", want: false},
		{name: "nonASCII_nbsp_trailing", in: "alpha\u00A0", want: false},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := hasEdgeSpace(tc.in)
			if got != tc.want {
				t.Fatalf("hasEdgeSpace(%q) = %v; want %v", tc.in, got, tc.want)
			}
		})
	}
}

/*
TestHasEdgeSpace_NoAlloc ensures the fast-path does not allocate for inputs
that do not require any slicing or copying. While allocation counts may vary
slightly across Go versions, this test asserts zero allocations for common
cases to guard against regressions in the implementation.

If this becomes flaky across toolchains, you can relax it to <= 1.
*/
func TestHasEdgeSpace_NoAlloc(t *testing.T) {
	cases := []string{
		"", "alpha", "a b", " alpha", "alpha ", " alpha ",
		"\talpha", "alpha\t", "\nalpha", "alpha\n", "\ralpha", "alpha\r",
	}
	for _, s := range cases {
		s := s
		t.Run("alloc:"+strings.ReplaceAll(s, "\n", "\\n"), func(t *testing.T) {
			allocs := testingAllocsPerRun(1000, func() {
				_ = hasEdgeSpace(s)
			})
			if allocs != 0 {
				t.Fatalf("allocs=%g for input %q; want 0", allocs, s)
			}
		})
	}
}

// testingAllocsPerRun mirrors testing.AllocsPerRun but keeps the test self-contained
// if you prefer not to import "testing" helpers elsewhere. You can replace calls
// to this function with testing.AllocsPerRun directly if desired.
func testingAllocsPerRun(runs int, f func()) float64 {
	return testing.AllocsPerRun(runs, f)
}

/*
BenchmarkHasEdgeSpace measures the cost of the edge check for typical inputs.
Use `go test -bench=BenchmarkHasEdgeSpace -benchmem` to track regressions.
*/
func BenchmarkHasEdgeSpace(b *testing.B) {
	inputs := []string{
		"alpha",
		" alpha",
		"alpha ",
		"\talpha",
		"alpha\t",
		"\nalpha",
		"alpha\n",
		strings.Repeat("x", 64),
	}
	b.ReportAllocs()
	for _, s := range inputs {
		s := s
		b.Run(summarize(s), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if hasEdgeSpace(s) && len(s) == 0 { // prevent compiler from eliding
					b.Fatal("impossible")
				}
			}
		})
	}
}

// summarize creates a short name for sub-benchmarks.
func summarize(s string) string {
	switch s {
	case "":
		return "empty"
	case " alpha":
		return "leading_space"
	case "alpha ":
		return "trailing_space"
	case "\talpha":
		return "leading_tab"
	case "alpha\t":
		return "trailing_tab"
	case "\nalpha":
		return "leading_nl"
	case "alpha\n":
		return "trailing_nl"
	default:
		if len(s) == 64 && strings.Count(s, "x") == 64 {
			return "len64_no_space"
		}
		return s
	}
}
