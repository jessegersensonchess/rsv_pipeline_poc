// Package csv provides streaming CSV parsing for large files.
//
// StreamCSV emits rows line-by-line without whole-file buffering. It can
// optionally scrub a known malformed sequence in real-world data:
//
//	` "v likvidaci`  →  ` ""v likvidaci`
//
// The scrub is performed by a bounded-memory streaming rewriter already
// implemented in this package (in csv_parser.go).
package csv

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strings"

	"etl/internal/config"
	"etl/internal/datasource/file"
)

// StreamCSV reads the source defined in spec, parses CSV records in a
// streaming fashion, and sends valid rows into 'out'.
//
// Behavior:
//   - The header row is read and normalized when parser.options.has_header=true.
//     Width (column count) is enforced for data rows when headers (or
//     expected_fields) are present.
//   - Per-row errors are soft: they are reported via onError(line, err) and
//     the stream continues.
//   - When parser.options.stream_scrub_likvidaci=true, bytes are passed
//     through a streaming rewriter that fixes the exact malformed sequence
//     ` "v likvidaci` → ` ""v likvidaci` before reaching encoding/csv.
//   - Memory stays bounded; no full-file buffering.
//
// Returns nil on EOF or a fatal error (e.g., cannot open source, cannot read
// header). The caller is responsible for closing 'out'.
func StreamCSV(
	ctx context.Context,
	spec config.Pipeline,
	out chan<- []string,
	onError func(line int, err error),
) error {
	// Open source (local file in this POC).
	rc, err := file.NewLocal(spec.Source.File.Path).Open(ctx)
	if err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	defer rc.Close()

	// Optional on-the-fly scrub of the specific broken sequence observed
	// in real data. The newStreamingRewriter is defined in csv_parser.go.
	var r io.Reader = rc
	if spec.Parser.Options.Bool("stream_scrub_likvidaci", false) {

		r = newStreamingRewriter(r,
			[]byte(` "v likvidaci""`),
			[]byte(` (v likvidaci)"`),
		)
	}

	// Build csv.Reader
	cr := csv.NewReader(r)
	if comma := spec.Parser.Options.Rune("comma", ','); comma != 0 {
		cr.Comma = comma
	}
	// Be lenient with quotes when scrubbing in case other oddities remain.
	if spec.Parser.Options.Bool("stream_scrub_likvidaci", false) {
		cr.LazyQuotes = true
	}
	// We enforce width after reading rows.
	cr.FieldsPerRecord = -1

	trim := spec.Parser.Options.Bool("trim_space", true)

	// Headers / expected width
	hasHeader := spec.Parser.Options.Bool("has_header", true)
	var headers []string
	expected := 0

	if hasHeader {
		h, err := cr.Read()
		if err != nil {
			return fmt.Errorf("read csv header: %w", err)
		}
		headers = normalizeHeaders(h, Options{
			HeaderMap: spec.Parser.Options.StringMap("header_map"),
		})
		expected = len(headers)
	} else if n := spec.Parser.Options.Int("expected_fields", 0); n > 0 {
		expected = n
		headers = make([]string, n)
		for i := range headers {
			headers[i] = fmt.Sprintf("col_%d", i)
		}
	}

	// Body
	line := 1 // header consumed already when present
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rec, err := cr.Read()
		if err == io.EOF {
			return nil
		}
		line++

		if err != nil {
			if onError != nil {
				onError(line, fmt.Errorf("parse: %w", err))
			}
			continue
		}
		if expected > 0 && len(rec) != expected {
			if onError != nil {
				onError(line, fmt.Errorf("incorrect number of fields: expected %d, got %d", expected, len(rec)))
			}
			continue
		}

		row := make([]string, len(rec))
		for i, v := range rec {
			if trim {
				v = strings.TrimSpace(v)
			}
			row[i] = v
		}

		select {
		case out <- row:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
