// Package xml provides an XML parser and streaming helpers used by the ETL
// pipeline. This file adds a streaming adapter that converts XML records into
// *transformer.Row values suitable for the generic ETL pipeline in cmd/etl.
//
// The high-level flow:
//
//   1. Build an xml.Config from parser options (config.Options), using the same
//      JSON shape as xmlprobe-generated configs.
//   2. Compile the config into a Compiled parser.
//   3. Run ParseStream with a simple, single-reader Options configuration.
//   4. For each emitted Record (map[string]any), build a *transformer.Row whose
//      V slice is ordered according to the ETL storage columns.
//   5. Send rows to the 'out' channel for downstream transform/validate/load.
//
// This mirrors the CSV streaming adapter (StreamCSVRows) so the ETL engine can
// treat CSV and XML uniformly after the parsing stage.

package xmlparser

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"etl/internal/config"
	"etl/internal/transformer"
)

// StreamXMLRows parses XML from r according to the configuration embedded in
// parserOpts and streams records as *transformer.Row into 'out'.
//
// Contract:
//
//   - parserOpts must be JSON-compatible with xml.Config (i.e., the same
//     structure as configs produced by xmlprobe). We rely on json.Marshal +
//     ParseConfigJSON to build the config.
//
//   - columns is the ordered list of destination columns. For each emitted
//     Record, we construct a []any where row[i] corresponds to columns[i]. The
//     transformer/loader stages rely on this alignment.
//
//   - The function is intentionally single-threaded from the ETL perspective:
//     it does not spawn its own worker pool or zerocopy sharding. Concurrency
//     is controlled at the ETL layer via runtime.reader_workers.
//
//   - onParseErr is reserved for per-record parse errors. In this initial
//     implementation we treat parse failures as fatal and return an error;
//     per-record error reporting can be added later if ParseStream exposes it.
func StreamXMLRows(
	ctx context.Context,
	r io.Reader,
	columns []string,
	parserOpts config.Options,
	out chan<- *transformer.Row,
	onParseErr func(line int, err error),
) error {
	// 1) Build xml.Config from the parser options by JSON round-tripping.
	// This leverages the existing xml.Config JSON layout used by xmlprobe.
	cfgBytes, err := json.Marshal(parserOpts)
	if err != nil {
		return fmt.Errorf("xml: marshal parser options: %w", err)
	}

	cfg, err := ParseConfigJSON(cfgBytes)
	if err != nil {
		return fmt.Errorf("xml: parse config from options: %w", err)
	}

	if cfg.RecordTag == "" {
		return fmt.Errorf("xml: RecordTag is required in parser.options")
	}

	// 2) Compile the XML config into a Compiled parser.
	comp, err := Compile(cfg)
	if err != nil {
		return fmt.Errorf("xml: compile config: %w", err)
	}

	// 3) Construct simple, single-reader options for ParseStream.
	// We explicitly avoid xmlprobe-style concurrency/zerocopy knobs here.
	opts := Options{
		Workers:       1,
		Queue:         0, // let ParseStream choose reasonable defaults
		BufSize:       0, // use internal defaults
		ZeroCopy:      false,
		UltraFast:     false,
		Schema:        false,
		PreserveOrder: false,
		OrderWindow:   0,
	}

	// 4) Bridge ParseStream's Record channel into *transformer.Row values.
	recCh := make(chan Record, 64)
	errCh := make(chan error, 1)

	// Run ParseStream in a dedicated goroutine so we can consume recCh in this
	// function and map records to rows without deadlocking.
	go func() {
		errCh <- ParseStream(ctx, r, nil, cfg.RecordTag, comp, opts, recCh)
		close(recCh)
	}()

	// We do not have a "natural" line number in XML the way CSV does, so we
	// approximate by using a monotonically increasing sequence number. This is
	// sufficient for diagnostics and pipeline error reporting.
	var line int

	for rec := range recCh {
		// Increment our logical "line counter" for each record.
		line++

		// Convert the XML record (map[string]any) into an ordered []any.
		values := recordToRow(rec, columns)

		row := &transformer.Row{
			Line: line,
			V:    values,
		}

		select {
		case out <- row:
			// Successfully forwarded to the next stage.
		case <-ctx.Done():
			// Context canceled: stop early. The caller will handle cleanup.
			return ctx.Err()
		}
	}

	// Wait for ParseStream to finish and propagate any fatal error.
	if err := <-errCh; err != nil && err != io.EOF {
		// For now, we treat all ParseStream errors as fatal rather than
		// per-record parse errors. If per-record error callbacks are added to
		// ParseStream later, we can thread them through onParseErr.
		if onParseErr != nil {
			onParseErr(0, err)
		}
		return fmt.Errorf("xml: parse stream: %w", err)
	}

	return nil
}

// recordToRow maps an XML Record (a map[string]any) into a []any aligned with
// the given columns slice. Missing keys become nil.
//
// This helper is intentionally small and deterministic; it is used both by the
// ETL pipeline and (optionally) XML-focused tools such as xmlprobe to ensure
// consistent mapping from logical record fields to storage rows.
func recordToRow(rec Record, columns []string) []any {
	row := make([]any, len(columns))
	for i, col := range columns {
		row[i] = rec[col] // nil if missing
	}
	return row
}
