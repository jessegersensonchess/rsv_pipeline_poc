package csv

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strings"

	"etl/internal/config"
	"etl/internal/transformer"
)

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
func StreamCSVRows(
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

	cr := csv.NewReader(src)
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
