// These tests verify the behavior of the small HTML/text normalization helpers
// in this package:
//
//   - StripTags: remove <...>-style tags.
//   - CollapseWhitespace: normalize runs of whitespace to a single ASCII space.
//   - NormalizeText: convenience function combining StripTags + CollapseWhitespace.
//
// The functions are intentionally simple and heuristic; the tests document
// the current behavior so it remains stable even as internals evolve.

package html

import (
	"strings"
	"testing"
)

// TestStripTags exercises StripTags on a variety of inputs, including simple
// HTML-like content and some edge cases.
//
// The goal is to lock in the current semantics:
//   - Anything between '<' and the next '>' is treated as a tag and dropped.
//   - The delimiters '<' and '>' themselves are also removed.
//   - This is not a full HTML parser. It will also remove math-like expressions
//     such as "1 < 2 and 3 > 2" by design.
func TestStripTags(t *testing.T) {
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

			got := StripTags(tt.in)
			if got != tt.want {
				t.Fatalf("StripTags(%q) = %q, want %q", tt.in, got, tt.want)
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
// CollapseWhitespace(StripTags(s)), and documents a few useful combined cases.
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

// BenchmarkStripTags_Short measures StripTags on a small, typical HTML snippet.
// This helps catch regressions in the hot path where we clean small fields.
func BenchmarkStripTags_Short(b *testing.B) {
	s := `<p>Hello <b>world</b> &amp; friends</p>`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = StripTags(s)
	}
}

// BenchmarkStripTags_Long measures StripTags on a longer HTML-ish blob to
// approximate real content (e.g., larger descriptions or paragraphs).
func BenchmarkStripTags_Long(b *testing.B) {
	fragment := `<p>Hello <b>world</b> &amp; friends with some <a href="#">links</a> and <em>emphasis</em>.</p>`
	s := strings.Repeat(fragment, 50)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = StripTags(s)
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

// BenchmarkNormalizeText_Long measures the combined StripTags + CollapseWhitespace
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
