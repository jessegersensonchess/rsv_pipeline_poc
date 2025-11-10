// Package csv implements a streaming CSV parser with optional, targeted
// on-the-fly scrubbing for known bad sequences in real-world data. It avoids
// whole-file buffering and can handle very large inputs (multi-GB) safely.
package csv

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"strings"

	"etl/pkg/records"
)

// Options configures the CSV parser behavior. All fields are optional; sensible
// defaults are applied when a field is zero.
type Options struct {
	// HasHeader indicates whether the first row contains column headers.
	HasHeader bool

	// Comma specifies the field delimiter. When zero, ',' is used.
	Comma rune

	// TrimSpace trims leading/trailing ASCII spaces from each field value.
	TrimSpace bool

	// ExpectedFields, when > 0, enforces a fixed field count per record. Rows
	// with a different width are skipped (soft-fail) and counted.
	ExpectedFields int

	// HeaderMap maps source header names to canonical keys (e.g., localization
	// to snake_case). Only applies when HasHeader is true.
	HeaderMap map[string]string

	// StreamScrubLikvidaci enables a targeted streaming fix that rewrites the
	// specific broken sequence:
	//   ` "v likvidaci`  ->  ` ""v likvidaci`
	// before the bytes reach encoding/csv. This uses a small rolling carry and
	// never buffers the whole file. When enabled, the CSV reader is configured
	// in a lenient mode (LazyQuotes, variable field count) and the parser then
	// enforces ExpectedFields after read.
	StreamScrubLikvidaci bool
}

// Parser parses CSV input according to Options. It is safe to reuse across
// inputs, but Parser itself is not concurrency-safe.
type Parser struct{ opt Options }

// NewParser constructs a Parser with the provided Options.
func NewParser(opt Options) *Parser { return &Parser{opt: opt} }

// utf8BOM is stripped from the first header cell if present.
const utf8BOM = "\uFEFF"

// streamingRewriter is an io.Reader that performs a streaming, rolling
// find/replace: it replaces all occurrences of pat with repl without buffering
// the entire stream. To correctly match sequences that may span chunk
// boundaries, it retains the last len(pat)-1 bytes (carry) from each processed
// block and prepends them to the next block before replacement.
type streamingRewriter struct {
	br    *bufio.Reader
	pat   []byte
	repl  []byte
	carry []byte       // last len(pat)-1 bytes retained between reads
	buf   bytes.Buffer // pending output to satisfy Read
	eof   bool
}

// newStreamingRewriter wraps r with a rewriter that replaces pat with repl.
func newStreamingRewriter(r io.Reader, pat, repl []byte) *streamingRewriter {
	capacity := 0
	if n := len(pat) - 1; n > 0 {
		capacity = n
	}
	return &streamingRewriter{
		br:    bufio.NewReaderSize(r, 64*1024),
		pat:   pat,
		repl:  repl,
		carry: make([]byte, 0, capacity),
	}
}

// Read implements io.Reader. It fills p from the internal buffer; when empty,
// it reads the next chunk from the underlying reader, performs rolling
// replacement, and withholds the trailing len(pat)-1 bytes as carry for the
// next call. On EOF it flushes the remaining carry.
func (sr *streamingRewriter) Read(p []byte) (int, error) {
	// Serve buffered output if present.
	if sr.buf.Len() > 0 {
		return sr.buf.Read(p)
	}
	if sr.eof {
		return 0, io.EOF
	}

	// Read next chunk.
	tmp := make([]byte, 64*1024)
	n, rerr := sr.br.Read(tmp)
	if n > 0 {
		block := tmp[:n]

		// Prepend carry to handle cross-boundary matches.
		if len(sr.carry) > 0 {
			joined := make([]byte, 0, len(sr.carry)+len(block))
			joined = append(joined, sr.carry...)
			joined = append(joined, block...)
			block = joined
		}

		// Replace occurrences in the working block.
		if len(sr.pat) > 0 && !bytes.Equal(sr.pat, sr.repl) {
			block = bytes.ReplaceAll(block, sr.pat, sr.repl)
		}

		// Retain the last k bytes as new carry; emit the rest.
		k := len(sr.pat) - 1
		if k < 0 {
			k = 0
		}
		if k > 0 && len(block) > k {
			emit := block[:len(block)-k]
			sr.buf.Write(emit)
			// Refresh carry.
			sr.carry = append(sr.carry[:0], block[len(block)-k:]...)
		} else {
			// Not enough to safely emit; keep entire block in carry.
			sr.carry = append(sr.carry[:0], block...)
		}
	}

	// Handle read error/EOF after processing chunk.
	if rerr == io.EOF {
		// Flush any remaining carry; no further reads will occur.
		if len(sr.carry) > 0 {
			sr.buf.Write(sr.carry)
			sr.carry = sr.carry[:0]
		}
		sr.eof = true
	} else if rerr != nil {
		// Propagate other read errors.
		return 0, rerr
	}

	// Serve buffered output if available.
	if sr.buf.Len() > 0 {
		return sr.buf.Read(p)
	}
	// If EOF reached and nothing buffered, signal EOF.
	if sr.eof {
		return 0, io.EOF
	}

	// No data yet; try again on next Read call (upper layers will call again).
	return 0, nil
}

// Parse consumes CSV records from r and returns the parsed rows along with the
// number of rows that were skipped due to parse errors or field-count
// mismatches. It never buffers the entire input; when StreamScrubLikvidaci is
// enabled, an on-the-fly rewriter is used to fix the known bad sequence.
func (p *Parser) Parse(r io.Reader) ([]records.Record, int, error) {
	// Optional streaming scrub: fix the known broken sequence on-the-fly.
	if p.opt.StreamScrubLikvidaci {
		r = newStreamingRewriter(r, []byte(` "v likvidaci""`), []byte(` (v likvidaci)"`))
	}

	cr := csv.NewReader(r)
	if p.opt.Comma != 0 {
		cr.Comma = p.opt.Comma
	}

	// When scrubbing, relax csv.Reader so it doesn't abort early on any residual
	// quoting oddities; we still enforce width after reading each row.
	if p.opt.StreamScrubLikvidaci {
		cr.LazyQuotes = true
		cr.FieldsPerRecord = -1
	}

	var headers []string
	var out []records.Record
	var skipped int

	// Header handling.
	if p.opt.HasHeader {
		h, err := cr.Read()
		if err != nil {
			return nil, 0, fmt.Errorf("read csv header: %w", err)
		}
		headers = normalizeHeaders(h, p.opt)
	} else if p.opt.ExpectedFields > 0 {
		headers = make([]string, p.opt.ExpectedFields)
		for i := range headers {
			headers[i] = fmt.Sprintf("col_%d", i)
		}
	}
	limit := 400
	// Read body rows.
	for line := 1; ; line++ {
		row, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			if skipped < limit {
				// Soft-fail this row and continue.
				log.Printf("Skipping row %d: %v", line, err)
			}
			skipped++
			continue
		}

		// Enforce expected width when known (by headers or explicit ExpectedFields).
		if len(headers) > 0 && len(row) != len(headers) {
			if skipped < limit {
				log.Printf("Skipping row %d: incorrect number of fields (expected %d, got %d)", line, len(headers), len(row))
			}
			skipped++
			continue
		}

		rec := make(records.Record, len(row))
		for i, val := range row {
			if p.opt.TrimSpace {
				val = strings.TrimSpace(val)
			}
			key := keyFor(i, headers)
			rec[key] = emptyToNil(val)
		}
		out = append(out, rec)
	}

	return out, skipped, nil
}

// keyFor returns the column key for index idx, using headers when available,
// otherwise synthesizing a "col_N" name.
func keyFor(idx int, headers []string) string {
	if idx < len(headers) && headers[idx] != "" {
		return headers[idx]
	}
	return fmt.Sprintf("col_%d", idx)
}

// emptyToNil converts an empty string to nil; all other values are returned as-is.
func emptyToNil(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// normalizeHeaders produces canonical header keys using HeaderMap (when
// provided) and simple normalization (lowercase, spaces to underscores). It
// also strips a UTF-8 BOM from the first cell if present.
func normalizeHeaders(h []string, opt Options) []string {
	res := make([]string, len(h))
	for i, col := range h {
		c := strings.TrimSpace(col)
		if i == 0 {
			c = strings.TrimPrefix(c, utf8BOM)
		}
		if opt.HeaderMap != nil {
			if m, ok := opt.HeaderMap[c]; ok {
				res[i] = m
				continue
			}
		}
		res[i] = strings.ReplaceAll(strings.ToLower(c), " ", "_")
	}
	return res
}
