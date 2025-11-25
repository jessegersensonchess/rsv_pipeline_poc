// internal/parser/html/textutil.go

// Package html provides small, allocation-conscious helpers for working with
// HTML-like text. It intentionally does not attempt full HTML parsing; instead,
// it focuses on simple, predictable text normalization primitives that are
// cheap to apply in ETL pipelines:
//
//   - StripHTML: remove <...> tag sequences from a string.
//   - CollapseWhitespace: reduce runs of whitespace to a single space.
//
// These functions operate on strings and return strings, making them easy to
// compose in parser and transformer stages.
package html

import "strings"

// StripHTML removes simplistic HTML/markup tags of the form <...> from s.
// It scans runes and treats any characters between '<' and the next '>' as
// a tag to be dropped. The delimiters themselves are also removed.
//
// This is a very lightweight heuristic, not a full HTML parser. It is good for
// cleaning up HTML-ish snippets where tags do not contain '<' or '>' in
// attribute values and where malformed markup is rare.
func StripHTML(s string) string {
	if s == "" {
		return s
	}

	var b strings.Builder
	b.Grow(len(s)) // heuristic: upper bound

	inTag := false
	for _, r := range s {
		switch r {
		case '<':
			inTag = true
		case '>':
			// End of a tag; resume writing on next rune.
			inTag = false
		default:
			if !inTag {
				b.WriteRune(r)
			}
		}
	}
	return b.String()
}

// CollapseWhitespace replaces consecutive whitespace characters with a single
// ASCII space (' ') and trims leading and trailing whitespace.
//
// Whitespace is treated as any of: space, tab, newline, or carriage return.
// This keeps behavior predictable and efficient for typical HTML/text cleanup
// without pulling in unicode tables.
func CollapseWhitespace(s string) string {
	if s == "" {
		return s
	}

	var b strings.Builder
	b.Grow(len(s))

	seenSpace := false
	for _, r := range s {
		switch r {
		case ' ', '\t', '\n', '\r':
			if !seenSpace {
				b.WriteByte(' ')
				seenSpace = true
			}
		default:
			b.WriteRune(r)
			seenSpace = false
		}
	}

	return strings.TrimSpace(b.String())
}

// NormalizeText is a convenience that first strips HTML-like tags from s
// and then collapses whitespace. It is a common “clean this snippet for
// display or indexing” operation.
func NormalizeText(s string) string {
	if s == "" {
		return s
	}
	return CollapseWhitespace(StripHTML(s))
}

// ExtractBetween returns the substring of s between start and end.
//
// Semantics:
//
//   - If start is non-empty, extraction begins immediately *after* the first
//     occurrence of start. If start is not found, it returns "", false.
//   - If start is empty, extraction begins at the start of s.
//   - If end is non-empty, extraction ends *before* the first occurrence of end
//     after the chosen start offset. If end is not found, it returns "", false.
//   - If end is empty, extraction goes to the end of s.
//   - If the resulting slice would be empty, it returns "", false.
//
// The boolean result is true only when a non-empty span was found.
func ExtractBetween(s, start, end string) (string, bool) {
	from := 0
	if start != "" {
		idx := strings.Index(s, start)
		if idx == -1 {
			return "", false
		}
		from = idx + len(start)
	}

	to := len(s)
	if end != "" {
		rel := strings.Index(s[from:], end)
		if rel == -1 {
			return "", false
		}
		to = from + rel
	}

	if from >= to {
		return "", false
	}
	return s[from:to], true
}
