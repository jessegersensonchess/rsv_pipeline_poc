package csv

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"strings"

	"etl/internal/config"
	"etl/internal/transformer"
	"etl/internal/transformer/builtin"
)

// Before: import ( ... "bytes" "bufio" ... )

// scrubRuleLikvidaci is the dataset-specific normalization that fixes a
// frequent broken-quote sequence in organization names:
//
//	Input  :  ` "v likvidaci""`   (space, quote, text, quote, quote)
//	Output :  ` (v likvidaci)"`   (space, open paren, text, close paren, quote)
//
// The rule is applied as a streaming byte replacement; it never loads the whole
// file in memory. We keep it opt-in via the `stream_scrub_likvidaci` parser
// option so other datasets are unaffected.
var (
	scrubFromLikvidaci = []byte(` "v likvidaci""`)
	scrubToLikvidaci   = []byte(` (v likvidaci)"`)
)

// wrapWithLikvidaciScrub wraps r in a streaming rewriter that corrects the
// `" v likvidaci""` → `(v likvidaci)"` broken-quote pattern.
// If disabled, it returns r unchanged.
//
// The returned value preserves Close() by embedding the underlying ReadCloser.
func wrapWithLikvidaciScrub(r io.ReadCloser, enable bool) io.ReadCloser {
	if !enable {
		return r
	}

	// Provide a wrapper that implements io.ReadCloser:
	//  - Read: comes from the streaming rewriter
	//  - Close: forwarded to the underlying reader
	type rc struct {
		io.Reader
		io.Closer
	}

	return &rc{
		Reader: newStreamingRewriter(r, scrubFromLikvidaci, scrubToLikvidaci),
		Closer: r,
	}
}

// StreamCSVRows streams CSV into pooled *transformer.Row objects aligned to the
// target 'columns' order. It reuses csv.Reader buffers (ReuseRecord=true) and
// copies cells directly into row.V[i] as strings or nil.
//
// Header handling:
//   - If options.has_header==true, the first line is treated as header and
//     mapped via options.header_map (source-name -> canonical).
//   - We then build dest→source mapping: colIx[targetIndex] = sourceIndex.
//     Missing target columns map to -1 (NULL).
//   - If has_header==false, we assume positional mapping (colIx[i] = i).
//
// Tuning/robustness (all optional via options):
//   - comma (string; first rune used; default ',')
//   - trim_space (bool; default true)
//   - lazy_quotes (bool; default false) → csv.Reader.LazyQuotes
//   - fields_per_record (int; 0=default, -1=variable, >0=enforce)
//   - stream_scrub_likvidaci (bool) → dataset-specific byte scrub pass
//
// onErr(line, err) receives recoverable row errors (soft-drop).
func StreamCSVRows(
	ctx context.Context,
	src io.ReadCloser,
	columns []string,
	opt config.Options,
	out chan<- *transformer.Row,
	onErr func(line int, err error),
) error {
	defer src.Close()

	// Options
	hasHeader := opt.Bool("has_header", true)
	comma := opt.Rune("comma", ',')
	trim := opt.Bool("trim_space", true)
	hm := opt.StringMap("header_map")
	lazy := opt.Bool("lazy_quotes", false)
	fieldsPer := opt.Int("fields_per_record", 0)

	// Optional streaming scrub for the `" v likvidaci""` glitch.
	r := wrapWithLikvidaciScrub(src, opt.Bool("stream_scrub_likvidaci", false))

	cr := csv.NewReader(r)
	cr.Comma = comma
	cr.ReuseRecord = true
	cr.LazyQuotes = lazy
	if fieldsPer != 0 {
		cr.FieldsPerRecord = fieldsPer
	} else {
		cr.FieldsPerRecord = -1 // tolerant by default
	}

	// Build dest→source mapping.
	colIx := make([]int, len(columns)) // colIx[target] = source index, or -1
	for i := range colIx {
		colIx[i] = -1
	}

	line := 0
	read := func() ([]string, error) { line++; return cr.Read() }

	// Header
	if hasHeader {
		hdr, err := read()
		if err != nil {
			if onErr != nil {
				onErr(line, fmt.Errorf("read header: %w", err))
			}
			return err
		}
		srcToIdx := make(map[string]int, len(hdr))
		for i, h := range hdr {
			if builtin.HasEdgeSpace(h) {
				h = strings.TrimSpace(h)
			}
			if i == 0 {
				h = strings.TrimPrefix(h, "\uFEFF") // strip BOM
			}
			if mapped, ok := hm[h]; ok {
				h = mapped
			} else {
				h = strings.ReplaceAll(strings.ToLower(h), " ", "_")
			}
			srcToIdx[h] = i
		}
		for t, target := range columns {
			if si, ok := srcToIdx[target]; ok {
				colIx[t] = si
			}
		}
	} else {
		for i := range columns {
			colIx[i] = i // positional
		}
	}

	// Progress heartbeat
	const logEveryN = 50_000
	// lastLog := time.Now()
	rowsSeen := 0

	for {
		// cooperative cancel
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rec, err := read()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			if onErr != nil {
				onErr(line, fmt.Errorf("csv read: %w", err))
			}
			continue
		}

		row := transformer.GetRow(len(columns))
		// Fill by TARGET index using dest→source mapping.
		for t := range columns {
			si := colIx[t]
			if si < 0 || si >= len(rec) {
				row.V[t] = nil
				continue
			}
			v := rec[si]
			if trim && builtin.HasEdgeSpace(v) {
				v = strings.TrimSpace(v)
			}
			if v == "" {
				row.V[t] = nil
			} else {
				row.V[t] = v
			}
		}

		// Emit
		select {
		case out <- row:
			rowsSeen++
			// if rowsSeen%logEveryN == 0 || time.Since(lastLog) > 5*time.Second {
			if rowsSeen%logEveryN == 0 {
				log.Printf("reader: line=%d emitted=%d", line, rowsSeen)
			}
		case <-ctx.Done():
			row.Free()
			return ctx.Err()
		}
	}
}
