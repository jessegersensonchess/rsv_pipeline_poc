// Package csvutil contains resilient CSV parsing helpers for "dirty" inputs.
// The standard library csv.Reader is intentionally strict; production data can
// include unbalanced quotes, embedded CRLF inside quoted fields, stray commas,
// and ad-hoc space-delimited fallbacks. These helpers aim to parse such inputs
// predictably while preserving as much information as possible.
package csvutil

import (
	"bufio"
	"io"
	"regexp"
	"strings"
)

// DateRe matches the canonical Czech date format "dd.mm.yyyy" (zero-padded).
// It is used by space-delimited repair logic to detect trailing date columns.
var DateRe = regexp.MustCompile(`^\d{2}\.\d{2}\.\d{4}$`)

/*
	============================================================
	SECTION: CSV parsing helpers (unchanged, with clarifying docs)
	============================================================
	Design notes:
	- ReadLogicalCSVLine reads a *logical* CSV row that may span multiple
	  physical lines if a quoted field contains CRLF.
	- ParseCSVLineLoose is a tolerant splitter that accepts inner quotes,
	  doubled quotes (""), and quote closures that appear right before a
	  delimiter (or end).
	- LexBySpaceWithQuotes provides a tokenizer for space-delimited fallbacks.
	- ParseSpaceSeparatedRow re-assembles a canonical 9-field row from the
	  token stream, attempting to locate and peel off trailing date columns.
	- RepairOverlongCommaFields fixes rows where a stray comma split the name
	  across multiple fields, e.g. `Doe, John, Jr.`.
*/

// ReadLogicalCSVLine reads one logical CSV line from r. If a quoted field spans
// multiple physical lines (contains CRLF), the function keeps reading until it
// observes a structurally plausible closing quote and delimiter boundary. On
// EOF without a trailing newline, it returns the accumulated content as the
// final logical line. If r is already at EOF, io.EOF is returned.
func ReadLogicalCSVLine(r *bufio.Reader) (string, error) {
	var sb strings.Builder
	inQuotes := false
	atStartOfField := true
	firstChunk := true

	for {
		part, err := r.ReadString('\n') // CRLF -> "\r\n"
		if err != nil && err != io.EOF && !(err == io.EOF && part != "") {
			return "", err
		}
		part = strings.TrimRight(part, "\r\n")

		// If we are continuing a quoted field across physical lines,
		// re-insert the CRLF boundary between chunks.
		if !firstChunk && inQuotes {
			sb.WriteString("\r\n")
		}
		sb.WriteString(part)
		firstChunk = false

		// Lightweight scanner to decide whether we are still "inside quotes".
		i := 0
		for i < len(part) {
			ch := part[i]
			switch ch {
			case ',':
				if !inQuotes {
					atStartOfField = true
				}
				i++
			case '"':
				if inQuotes {
					// Handle doubled quotes ("") as escaped quote.
					if i+1 < len(part) && part[i+1] == '"' {
						i += 2
						continue
					}
					// Consider quotes closed when followed by delimiter or end.
					j := i + 1
					for j < len(part) && (part[j] == ' ' || part[j] == '\t') {
						j++
					}
					if j >= len(part) || part[j] == ',' {
						inQuotes = false
						atStartOfField = false
						i++
						continue
					}
					// Otherwise, treat as literal and keep scanning.
					i++
				} else {
					// Opening quote valid only at start of a field.
					if atStartOfField {
						inQuotes = true
						atStartOfField = false
						i++
					} else {
						// Quote inside an unquoted field -> literal.
						i++
					}
				}
			default:
				if !inQuotes {
					atStartOfField = false
				}
				i++
			}
		}

		// Stop if we believe the logical record is closed, or we hit EOF.
		if !inQuotes || err == io.EOF {
			if sb.Len() == 0 && err == io.EOF {
				return "", io.EOF
			}
			return sb.String(), nil
		}
	}
}

// ParseCSVLineLoose splits a single CSV line into fields with a tolerant
// strategy:
//   - Inner quotes inside unquoted fields are kept as literals.
//   - Doubled quotes ("") inside quoted fields are interpreted either as:
//   - an escaped quote when followed by delimiter or end (possibly with
//     trailing spaces), which also *closes* the quoted field; or
//   - a literal quote if more content follows before a delimiter.
//   - Commas inside quoted fields are preserved.
//
// The function never returns an error; malformed constructs degrade gracefully.
func ParseCSVLineLoose(line string) ([]string, error) {
	var fields []string
	var sb strings.Builder
	inQuotes := false
	atStartOfField := true
	i := 0

	for i < len(line) {
		ch := line[i]
		switch ch {
		case ',':
			if inQuotes {
				sb.WriteByte(',')
			} else {
				fields = append(fields, sb.String())
				sb.Reset()
				atStartOfField = true
			}
			i++
		case '"':
			if inQuotes {
				// Doubled quote path: treat as escaped quote, possibly closing.
				if i+1 < len(line) && line[i+1] == '"' {
					j := i + 2
					for j < len(line) && (line[j] == ' ' || line[j] == '\t') {
						j++
					}
					if j >= len(line) || line[j] == ',' {
						// Close field at delimiter or end.
						sb.WriteByte('"')
						inQuotes = false
						atStartOfField = false
						i += 2
						continue
					}
					// Not at a delimiter/end: keep a literal quote and continue.
					sb.WriteByte('"')
					i += 2
					continue
				}
				// Single quote: close only if followed by delimiter or end.
				j := i + 1
				for j < len(line) && (line[j] == ' ' || line[j] == '\t') {
					j++
				}
				if j >= len(line) || line[j] == ',' {
					inQuotes = false
					atStartOfField = false
					i++
					continue
				}
				// Otherwise treat as literal.
				sb.WriteByte('"')
				i++
			} else {
				// Opening quote permitted only at the start of a field.
				if atStartOfField {
					inQuotes = true
					atStartOfField = false
					i++
				} else {
					sb.WriteByte('"')
					i++
				}
			}
		default:
			sb.WriteByte(ch)
			if !inQuotes {
				atStartOfField = false
			}
			i++
		}
	}
	fields = append(fields, sb.String())
	return fields, nil
}

// LexBySpaceWithQuotes tokenizes a line using spaces as delimiters while
// honoring quoted regions (with support for doubled quotes). Multiple adjacent
// spaces collapse to a single delimiter outside quotes; inside quotes they're
// preserved verbatim.
func LexBySpaceWithQuotes(line string) []string {
	tokens := []string{}
	var sb strings.Builder
	inQuotes := false
	i := 0
	flush := func() {
		if sb.Len() > 0 {
			tokens = append(tokens, sb.String())
			sb.Reset()
		}
	}
	for i < len(line) {
		ch := line[i]
		switch ch {
		case '"':
			if inQuotes {
				if i+1 < len(line) && line[i+1] == '"' {
					sb.WriteByte('"')
					i += 2
					continue
				}
				inQuotes = false
				i++
			} else {
				inQuotes = true
				i++
			}
		case ' ', '\t':
			if inQuotes {
				sb.WriteByte(ch)
				i++
			} else {
				flush()
				for i < len(line) && (line[i] == ' ' || line[i] == '\t') {
					i++
				}
			}
		default:
			sb.WriteByte(ch)
			i++
		}
	}
	flush()
	return tokens
}

// ParseSpaceSeparatedRow attempts to parse a "dirty" space-delimited record
// into a canonical 9-field schema:
//
//	0: pcv
//	1: typ
//	2: vztah
//	3: aktualni
//	4: ico
//	5: name
//	6: addr
//	7: d1 (optional; dd.mm.yyyy or empty)
//	8: d2 (optional; dd.mm.yyyy or empty)
//
// The function peels up to two trailing date tokens if present, validates the
// minimum arity, and returns (nil, false) when reconstruction is not possible.
func ParseSpaceSeparatedRow(line string) ([]string, bool) {
	toks := LexBySpaceWithQuotes(line)
	if len(toks) < 6 {
		return nil, false
	}
	var d2, d1 string
	if len(toks) >= 1 && DateRe.MatchString(toks[len(toks)-1]) {
		d2 = toks[len(toks)-1]
		toks = toks[:len(toks)-1]
	}
	if len(toks) >= 1 && DateRe.MatchString(toks[len(toks)-1]) {
		d1 = toks[len(toks)-1]
		toks = toks[:len(toks)-1]
	}
	if len(toks) < 5 {
		return nil, false
	}
	pcv, typ, vztah, aktualni, ico := toks[0], toks[1], toks[2], toks[3], toks[4]
	rest := toks[5:]
	if len(rest) == 0 {
		return nil, false
	}
	name := rest[0]
	rest = rest[1:]
	addr := strings.Join(rest, " ")
	out := []string{pcv, typ, vztah, aktualni, ico, name, addr}
	if d1 != "" {
		out = append(out, d1)
	} else {
		out = append(out, "")
	}
	if d2 != "" {
		out = append(out, d2)
	} else {
		out = append(out, "")
	}
	if len(out) != 9 {
		return nil, false
	}
	return out, true
}

// RepairOverlongCommaFields repairs a record that was split into more than
// 9 comma-delimited fields because the "name" column contained stray commas.
// Expected canonical form is 9 fields total, where the last two fields are
// optional dates ("" or dd.mm.yyyy). If the input is not structurally
// compatible, the function returns (nil, false).
func RepairOverlongCommaFields(fields []string) ([]string, bool) {
	if len(fields) <= 9 {
		return nil, false
	}
	last := len(fields) - 1
	d2 := fields[last]
	d1 := fields[last-1]
	// The final two fields must be dates or empty placeholders.
	if !(d2 == "" || DateRe.MatchString(strings.TrimSpace(d2))) {
		return nil, false
	}
	if !(d1 == "" || DateRe.MatchString(strings.TrimSpace(d1))) {
		return nil, false
	}
	// Recompute the address index (position of the canonical address).
	addrIdx := last - 2
	if addrIdx < 6 { // Defensive: should not occur for len(fields) > 9.
		return nil, false
	}
	head := fields[:5]
	name := strings.Join(fields[5:addrIdx], ",")
	addr := fields[addrIdx]

	out := make([]string, 0, 9)
	out = append(out, head...)
	out = append(out, name, addr, d1, d2)
	if len(out) != 9 {
		return nil, false
	}
	return out, true
}
