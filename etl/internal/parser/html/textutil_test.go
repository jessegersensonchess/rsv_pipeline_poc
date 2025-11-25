// These tests verify the behavior of the small HTML/text normalization helpers
// in this package:
//
//   - StripHTML: remove <...>-style tags.
//   - CollapseWhitespace: normalize runs of whitespace to a single ASCII space.
//   - NormalizeText: convenience function combining StripHTML + CollapseWhitespace.
//
// The functions are intentionally simple and heuristic; the tests document
// the current behavior so it remains stable even as internals evolve.

package html

import (
	"strings"
	"testing"
)

// TestStripHTML exercises StripHTML on a variety of inputs, including simple
// HTML-like content and some edge cases.
//
// The goal is to lock in the current semantics:
//   - Anything between '<' and the next '>' is treated as a tag and dropped.
//   - The delimiters '<' and '>' themselves are also removed.
//   - This is not a full HTML parser. It will also remove math-like expressions
//     such as "1 < 2 and 3 > 2" by design.
func TestStripHTML(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "empty string",
			in:   "",
			want: "",
		},
		{
			name: "no tags present",
			in:   "plain text only",
			want: "plain text only",
		},
		{
			name: "simple tag pair",
			in:   "<b>bold</b>",
			want: "bold",
		},
		{
			name: "prefix and suffix tags",
			in:   "<p>Hello</p> world <br>",
			want: "Hello world ",
		},
		{
			name: "nested-ish tags (heuristic)",
			in:   "<div><span>inner</span> text</div>",
			want: "inner text",
		},
		{
			name: "attributes inside tag",
			in:   `<a href="https://example.com">link</a>`,
			want: "link",
		},
		{
			name: "unclosed tag",
			in:   "text <b not closed",
			want: "text ",
		},
		{
			name: "dangling closing tag",
			in:   "hello </b>world",
			want: "hello world",
		},
		{
			// This documents the heuristic nature: we do not distinguish between
			// HTML tags and literal comparisons. Everything between '<' and '>'
			// is removed.
			name: "angle brackets in text",
			in:   "1 < 2 and 3 > 2",
			want: "1  2",
		},
	}

	for _, tt := range tests {
		tt := tt // capture loop variable
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := StripHTML(tt.in)
			if got != tt.want {
				t.Fatalf("StripHTML(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// TestCollapseWhitespace checks that CollapseWhitespace:
//
//   - Reduces runs of ASCII whitespace (space, tab, newline, carriage return)
//     to a single ASCII space.
//   - Trims leading and trailing whitespace.
//   - Leaves non-ASCII whitespace (e.g., NBSP) unchanged.
//
// This behavior is intentionally simple and biased toward ETL text cleanup.
func TestCollapseWhitespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "empty string",
			in:   "",
			want: "",
		},
		{
			name: "no whitespace",
			in:   "text",
			want: "text",
		},
		{
			name: "leading and trailing spaces",
			in:   "   hello  ",
			want: "hello",
		},
		{
			name: "multiple spaces collapsed",
			in:   "hello   world",
			want: "hello world",
		},
		{
			name: "tabs and newlines",
			in:   "hello\tworld\nand\ragain",
			want: "hello world and again",
		},
		{
			name: "mixed whitespace runs",
			in:   "a \t\n\r  b",
			want: "a b",
		},
		{
			name: "already normalized",
			in:   "a b c",
			want: "a b c",
		},
		{
			name: "only whitespace becomes empty",
			in:   " \t\n\r ",
			want: "",
		},
		{
			// NBSP is not treated as whitespace here; it is preserved.
			name: "non-breaking space preserved",
			in:   "a\u00a0b",
			want: "a\u00a0b",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := CollapseWhitespace(tt.in)
			if got != tt.want {
				t.Fatalf("CollapseWhitespace(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// TestNormalizeText verifies that NormalizeText is equivalent to
// CollapseWhitespace(StripHTML(s)), and documents a few useful combined cases.
//
// This is the common "clean this snippet for display or indexing" operation.
func TestNormalizeText(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "empty string",
			in:   "",
			want: "",
		},
		{
			name: "strip tags and collapse spaces",
			in:   "  <p>Hello   <b>world</b></p>\n",
			want: "Hello world",
		},
		{
			name: "only tags and whitespace yields empty",
			in:   "  <div>\n\t</div>  ",
			want: "",
		},
		{
			name: "text around tags",
			in:   "  prefix <span>mid</span>  suffix ",
			want: "prefix mid suffix",
		},
		{
			// Here again we document that comparisons are treated as tags; this
			// clarifies that NormalizeText should not be used where '<' and '>'
			// must be preserved.
			name: "angle brackets in text",
			in:   "a < b and c > d",
			want: "a d",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := NormalizeText(tt.in)
			if got != tt.want {
				t.Fatalf("NormalizeText(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// BenchmarkStripHTML_Short measures StripHTML on a small, typical HTML snippet.
// This helps catch regressions in the hot path where we clean small fields.
func BenchmarkStripHTML_Short(b *testing.B) {
	s := `<p>Hello <b>world</b> &amp; friends</p>`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = StripHTML(s)
	}
}

// BenchmarkStripHTML_Long measures StripHTML on a longer HTML-ish blob to
// approximate real content (e.g., larger descriptions or paragraphs).
func BenchmarkStripHTML_Long(b *testing.B) {
	fragment := `<p>Hello <b>world</b> &amp; friends with some <a href="#">links</a> and <em>emphasis</em>.</p>`
	s := strings.Repeat(fragment, 50)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = StripHTML(s)
	}
}

// BenchmarkCollapseWhitespace_Short measures CollapseWhitespace on a short
// string with redundant whitespace.
func BenchmarkCollapseWhitespace_Short(b *testing.B) {
	s := "  hello\tworld \n and   again  "

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CollapseWhitespace(s)
	}
}

// BenchmarkCollapseWhitespace_Long measures CollapseWhitespace on a longer
// string to approximate real-world usage (many fields or sentences).
func BenchmarkCollapseWhitespace_Long(b *testing.B) {
	fragment := "  hello\tworld \n and   again  "
	s := strings.Repeat(fragment, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CollapseWhitespace(s)
	}
}

// BenchmarkNormalizeText_Long measures the combined StripHTML + CollapseWhitespace
// path via NormalizeText on a longer HTML-ish input.
func BenchmarkNormalizeText_Long(b *testing.B) {
	fragment := `<div>  Hello <b>world</b> &amp; friends
with   some	irregular   whitespace   and <a href="#">links</a>.
</div>`
	s := strings.Repeat(fragment, 50)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NormalizeText(s)
	}
}

func TestExtractBetween_StartAndEnd(t *testing.T) {
	in := "before [START]hello world[END] after"
	got, ok := ExtractBetween(in, "[START]", "[END]")
	if !ok {
		t.Fatalf("expected ok=true, got false")
	}
	if want := "hello world"; got != want {
		t.Fatalf("got %q; want %q", got, want)
	}
}

func TestExtractBetween_NoStart_OnlyEnd(t *testing.T) {
	in := "hello world [END] trailing"
	got, ok := ExtractBetween(in, "", "[END]")
	if !ok {
		t.Fatalf("expected ok=true, got false")
	}
	if want := "hello world "; got != want {
		t.Fatalf("got %q; want %q", got, want)
	}
}

func TestExtractBetween_OnlyStart_NoEnd(t *testing.T) {
	in := "prefix <tag>payload goes here"
	got, ok := ExtractBetween(in, "<tag>", "")
	if !ok {
		t.Fatalf("expected ok=true, got false")
	}
	if want := "payload goes here"; got != want {
		t.Fatalf("got %q; want %q", got, want)
	}
}

func TestExtractBetween_StartNotFound(t *testing.T) {
	in := "no markers here"
	if got, ok := ExtractBetween(in, "[MISSING]", "[END]"); ok || got != "" {
		t.Fatalf("expected empty/false, got %q/%v", got, ok)
	}
}

func TestExtractBetween_EndNotFound(t *testing.T) {
	in := "prefix [START] but no end marker"
	if got, ok := ExtractBetween(in, "[START]", "[END]"); ok || got != "" {
		t.Fatalf("expected empty/false, got %q/%v", got, ok)
	}
}

func TestExtractBetween_EmptyResultIsFalse(t *testing.T) {
	in := "x[START][END]y"
	if got, ok := ExtractBetween(in, "[START]", "[END]"); ok || got != "" {
		t.Fatalf("expected empty/false, got %q/%v", got, ok)
	}
}

func TestExtractBetween_FirstOccurrenceOnly(t *testing.T) {
	in := "A<mark>one</mark>B<mark>two</mark>C"
	got, ok := ExtractBetween(in, "<mark>", "</mark>")
	if !ok {
		t.Fatalf("expected ok=true, got false")
	}
	if want := "one"; got != want {
		t.Fatalf("got %q; want %q", got, want)
	}
}
