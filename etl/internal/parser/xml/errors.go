package xmlparser

import "strings"

// isTruncErr returns true when a tokenization error indicates a truncated or
// partial XML stream. encoding/xml does not expose a sentinel, so we match
// the common message substrings used by its errors. This keeps the parser
// tolerant for shard-based and zero-copy modes.
func isTruncErr(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "unexpected EOF") ||
		strings.Contains(s, "XML syntax error")
}
