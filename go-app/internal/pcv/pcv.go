// Package pcv provides utilities for locating and extracting the "PČV" field
// (an identifier) from heterogeneous CSV-like data. Real-world inputs can be
// misaligned vs. headers, vary in length, or require fallbacks based on nearby
// columns or raw-line substrings. The helpers below prioritize correctness and
// robustness over strict CSV semantics.
package pcv

import (
	"regexp"
	"strconv"
	"strings"
)

// FindPCVIndex scans headers for an exact "PČV" header (after TrimSpace).
// It returns the 0-based index of the first match or -1 if not found.
// This is typically used to seed ExtractPCV with a candidate column index.
func FindPCVIndex(headers []string) int {
	for i, h := range headers {
		if strings.TrimSpace(h) == "PČV" {
			return i
		}
	}
	return -1
}

// ExtractPCV attempts to parse a PČV value from fields using several heuristics,
// in priority order:
//
//  1. Direct by header index (pcvIdx) when rows and headers are aligned.
//  2. Tail-aligned index when row length != header length (preserve relative
//     position from the end).
//  3. If a Status column index is provided (statusIdx), scan up to 10 fields
//     to the right of Status (and a tail-aligned equivalent when lengths differ).
//  4. Last resort: scan the tail region for a plausible numeric identifier.
//
// digitsOnly supplies the caller's digit-policy (usually `^\d+$`).
// On success it returns (pcv, rawStringUsed). On failure it returns (0, "").
func ExtractPCV(headers, fields []string, pcvIdx, statusIdx int, digitsOnly *regexp.Regexp) (int64, string) {
	parse := func(s string) (int64, error) { return strconv.ParseInt(s, 10, 64) }

	// 1) Direct by header index (if aligned)
	if pcvIdx >= 0 && pcvIdx < len(fields) {
		s := strings.TrimSpace(fields[pcvIdx])
		if digitsOnly.MatchString(s) {
			if v, err := parse(s); err == nil {
				return v, s
			}
		}
	}

	// 2) Tail-aligned index when row length != header length
	if len(fields) != len(headers) && pcvIdx >= 0 {
		tailOffset := (len(headers) - 1) - pcvIdx
		idx := (len(fields) - 1) - tailOffset
		if idx >= 0 && idx < len(fields) {
			s := strings.TrimSpace(fields[idx])
			if digitsOnly.MatchString(s) {
				if v, err := parse(s); err == nil {
					return v, s
				}
			}
		}
	}

	// 3) If we know Status, look right after it (typically Status -> PČV)
	if statusIdx >= 0 {
		tryFrom := func(si int) (int64, string, bool) {
			if si < 0 || si >= len(fields) {
				return 0, "", false
			}
			for i := si + 1; i < len(fields) && i <= si+10; i++ {
				s := strings.TrimSpace(fields[i])
				if digitsOnly.MatchString(s) && len(s) >= 5 {
					if v, err := parse(s); err == nil {
						return v, s, true
					}
				}
			}
			return 0, "", false
		}
		// 3a) Aligned scan to the right of Status.
		if v, s, ok := tryFrom(statusIdx); ok {
			return v, s
		}
		// 3b) Tail-aligned scan when lengths differ.
		if len(fields) != len(headers) {
			tailOffsetS := (len(headers) - 1) - statusIdx
			si := (len(fields) - 1) - tailOffsetS
			if v, s, ok := tryFrom(si); ok {
				return v, s
			}
		}
	}

	// 4) Last resort: scan the tail region for a plausible identifier (6+ digits).
	start := len(fields) - 16
	if start < 0 {
		start = 0
	}
	for i := len(fields) - 1; i >= start; i-- {
		s := strings.TrimSpace(fields[i])
		if digitsOnly.MatchString(s) && len(s) >= 6 {
			if v, err := parse(s); err == nil {
				return v, s
			}
		}
	}

	return 0, ""
}

// ExtractPCVWithRSVFallback first tries ExtractPCV (header/Status-based logic).
// If that yields 0, it falls back to scanning the raw input line for the literal
// ",RSV," and then selecting the 4th comma-delimited field from that anchor:
//
//	grep -o ,RSV,.* | cut -f4 -d","   // => index 3 within the ",RSV,..." slice
//
// The candidate is trimmed for spaces and surrounding single/double quotes
// before digit/parse validation.
func ExtractPCVWithRSVFallback(
	headers, fields []string,
	pcvIdx, statusIdx int,
	digitsOnly *regexp.Regexp,
	rawLine string,
) (int64, string) {
	// 1) Try the normal heuristics
	if v, s := ExtractPCV(headers, fields, pcvIdx, statusIdx, digitsOnly); v != 0 {
		return v, s
	}

	// 2) Fallback: look for ",RSV," literal and take the 4th field after it.
	const anchor = ",RSV,"
	i := strings.Index(rawLine, anchor)
	if i == -1 {
		return 0, ""
	}

	// Keep the leading comma to match CLI pipeline behavior exactly.
	sub := rawLine[i:] // starts with ",RSV,..."
	parts := strings.Split(sub, ",")
	// parts[0] == ""  parts[1] == "RSV"  parts[2] == <f2>  parts[3] == <PCV>
	if len(parts) < 4 {
		return 0, ""
	}
	candidate := strings.TrimSpace(parts[3])
	// Strip surrounding quotes if present.
	candidate = strings.Trim(candidate, `"'`)

	if !digitsOnly.MatchString(candidate) {
		return 0, ""
	}
	v, err := strconv.ParseInt(candidate, 10, 64)
	if err != nil {
		return 0, ""
	}
	return v, candidate
}
