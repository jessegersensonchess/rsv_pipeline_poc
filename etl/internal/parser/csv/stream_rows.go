package csv

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"etl/internal/config"
	"etl/internal/transformer"
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
//   - We then build a sourceIndex for each target column. Missing cols produce
//     NULLs.
//   - If has_header==false, we assume file columns already match 'columns'
//     by position.
//
// Tuning/robustness (all optional via options):
//   - comma (string; first rune used; default ',')
//   - trim_space (bool; default true)
//   - lazy_quotes (bool; default false) → csv.Reader.LazyQuotes
//   - fields_per_record (int; 0=default, -1=variable width, >0=exact width)
//   - stream_scrub_likvidaci (bool) → dataset-specific on-the-fly scrub
//
// onErr(line, err) receives recoverable row errors (soft-drop).
// This function is cancellation-aware and returns promptly on ctx.Done(), but
// it **does not** close 'out'; the caller is responsible for closing channels.
func StreamCSVRows(
	ctx context.Context,
	src io.ReadCloser, // caller-owned; will be closed by this function
	columns []string, // target column order
	opt config.Options, // parser options
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
	fieldsPer := opt.Int("fields_per_record", 0) // 0=default; -1=variable; >0=enforce

	// Dataset-specific streaming scrub (confirmed pattern).
	r := wrapWithLikvidaciScrub(src, opt.Bool("stream_scrub_likvidaci", false))

	cr := csv.NewReader(r)
	cr.Comma = comma
	cr.ReuseRecord = true
	cr.LazyQuotes = lazy
	if fieldsPer != 0 {
		cr.FieldsPerRecord = fieldsPer
	} else {
		// Be a bit more tolerant by default on messy datasets.
		cr.FieldsPerRecord = -1
	}

	// Build header mapping.
	var srcToIdx map[string]int
	colIx := make([]int, len(columns)) // input->output positional map (-1 missing)
	line := 0

	read := func() ([]string, error) {
		line++
		return cr.Read()
	}

	// Cancellation check *before* first Read to avoid hanging in I/O.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Header
	if hasHeader {
		hdr, err := read()
		if err != nil {
			if onErr != nil {
				onErr(line, fmt.Errorf("read header: %w", err))
			}
			return err
		}
		// Normalize header cells → canonical names
		srcToIdx = make(map[string]int, len(hdr))
		for i, h := range hdr {
			h = strings.TrimSpace(h)
			if i == 0 {
				// Strip UTF-8 BOM.
				h = strings.TrimPrefix(h, "\uFEFF")
			}
			if mapped, ok := hm[h]; ok {
				h = mapped
			} else {
				h = strings.ReplaceAll(strings.ToLower(h), " ", "_")
			}
			srcToIdx[h] = i
		}
		for i, target := range columns {
			if idx, ok := srcToIdx[target]; ok {
				colIx[i] = idx
			} else {
				colIx[i] = -1
			}
		}
	} else {
		// Positional mapping 1:1
		for i := range columns {
			colIx[i] = i
		}
	}

	// Progress heartbeat for very large files.
	const logEveryN = 200_000
	lastLog := time.Now()
	rowsSeen := 0

	for {
		// Cancellation check between records to avoid getting stuck in Read().
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

		// Build a pooled row with len(columns) and fill via mapping.
		row := transformer.GetRow(len(columns))
		// Initialize to NULLs, then place values where mapped.
		for i := range row.V {
			row.V[i] = nil
		}

		for i := 0; i < len(rec) && i < len(colIx); i++ {
			outPos := colIx[i]
			if outPos < 0 || outPos >= len(row.V) {
				continue
			}
			v := rec[i]
			if trim {
				v = strings.TrimSpace(v)
			}
			if v == "" {
				row.V[outPos] = nil
			} else {
				row.V[outPos] = v
			}
		}

		// Emit downstream, remain cancellation-aware.
		select {
		case out <- row:
			// progress
			rowsSeen++
			if rowsSeen%logEveryN == 0 || time.Since(lastLog) > 5*time.Second {
				log.Printf("reader: line=%d emitted=%d", line, rowsSeen)
				lastLog = time.Now()
			}
		case <-ctx.Done():
			row.Free()
			return ctx.Err()
		}
	}
}

// StreamCSVRows streams CSV into pooled *transformer.Row objects aligned to the
// target 'columns' order. It reuses csv.Reader buffers (ReuseRecord=true) and
// copies cells directly into row.V[i] as strings or nil.
//
// Header handling:
//   - If options.has_header==true, the first line is treated as header and
//     mapped via options.header_map (source-name -> canonical).
//   - We then build a sourceIndex for each target column. Missing cols produce
//     NULLs.
//   - If has_header==false, we assume file columns already match 'columns'
//     by position.
//
// onErr(line, err) receives recoverable row errors (soft-drop).
func xxStreamCSVRows(
	ctx context.Context,
	src io.ReadCloser, // caller-owned; will be closed by this function
	columns []string, // target column order
	opt config.Options, // parser options (has_header, comma, trim_space, header_map)
	out chan<- *transformer.Row,
	onErr func(line int, err error),
) error {
	defer src.Close()

	hasHeader := opt.Bool("has_header", true)
	comma := opt.Rune("comma", ',')
	trim := opt.Bool("trim_space", true)
	hm := opt.StringMap("header_map")

	r := src // io.ReadCloser
	r = wrapWithLikvidaciScrub(r, opt.Bool("stream_scrub_likvidaci", false))

	// cr := csv.NewReader(src)
	cr := csv.NewReader(r)
	cr.Comma = comma
	cr.ReuseRecord = true
	cr.FieldsPerRecord = -1 // allow variable; we validate per-row

	var srcToIdx map[string]int
	var colIx []int // len(columns), each index into input row, or -1 if missing
	line := 0
	read := func() ([]string, error) { line++; return cr.Read() }

	// Header
	if hasHeader {
		hdr, err := read()
		if err != nil {
			return fmt.Errorf("read header: %w", err)
		}
		srcToIdx = make(map[string]int, len(hdr))
		for i, h := range hdr {
			h = strings.TrimSpace(h)
			if i == 0 {
				// Strip UTF-8 BOM.
				h = strings.TrimPrefix(h, "\uFEFF")
			}
			if mapped, ok := hm[h]; ok {
				h = mapped
			} else {
				h = strings.ReplaceAll(strings.ToLower(h), " ", "_")
			}
			srcToIdx[h] = i
		}
		colIx = make([]int, len(columns))
		for i, target := range columns {
			if idx, ok := srcToIdx[target]; ok {
				colIx[i] = idx
			} else {
				colIx[i] = -1
			}
		}
	} else {
		// Positional mapping 1:1
		colIx = make([]int, len(columns))
		for i := range columns {
			colIx[i] = i
		}
	}

	for {
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
		for i := range columns {
			srcIdx := colIx[i]
			if srcIdx < 0 || srcIdx >= len(rec) {
				row.V[i] = nil
				continue
			}
			v := rec[srcIdx]
			if trim {
				v = strings.TrimSpace(v)
			}
			if v == "" {
				row.V[i] = nil
			} else {
				row.V[i] = v
			}
		}

		select {
		case <-ctx.Done():
			row.Free()
			return ctx.Err()
		case out <- row:
		}
	}
}
