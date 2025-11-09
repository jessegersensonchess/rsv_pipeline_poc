// Package main wires the ETL pipeline end-to-end for both legacy (buffered)
// and streaming (channel-based, batched) execution modes. This file keeps the
// CLI layer thin: it depends only on storage-agnostic interfaces and never
// imports database drivers or backend-specific packages directly.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"etl/internal/config"
	"etl/internal/datasource/file"
	"etl/internal/ddlgen"
	"etl/internal/parser"
	csvparser "etl/internal/parser/csv"
	"etl/internal/schema"
	"etl/internal/storage"
	_ "etl/internal/storage/postgres" // register "postgres" backend
	"etl/internal/transformer"
	"etl/internal/transformer/builtin"
)

// openSource is retained for legacy paths (parser.Parse usage).
func openSource(ctx context.Context, spec config.Pipeline) (io.ReadCloser, error) {
	switch spec.Source.Kind {
	case "file":
		return file.NewLocal(spec.Source.File.Path).Open(ctx)
	default:
		return nil, fmt.Errorf("unsupported source.kind=%s", spec.Source.Kind)
	}
}

// buildParser maps parser configuration into a concrete parser implementation.
func buildParser(p config.Parser) (parser.Parser, error) {
	switch p.Kind {
	case "csv":
		opt := csvparser.Options{
			HasHeader:            p.Options.Bool("has_header", true),
			Comma:                p.Options.Rune("comma", ','),
			TrimSpace:            p.Options.Bool("trim_space", true),
			ExpectedFields:       p.Options.Int("expected_fields", 0),
			HeaderMap:            p.Options.StringMap("header_map"),
			StreamScrubLikvidaci: p.Options.Bool("stream_scrub_likvidaci", true),
		}
		return csvparser.NewParser(opt), nil
	default:
		return nil, fmt.Errorf("unsupported parser.kind=%s", p.Kind)
	}
}

// decodeInlineContract retrieves and decodes the inline validation contract
// from a transform's options. It round-trips through JSON to the schema type.
func decodeInlineContract(opt config.Options) (schema.Contract, error) {
	raw := opt.Any("contract")
	if raw == nil {
		return schema.Contract{}, fmt.Errorf("validate.options.contract is required")
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return schema.Contract{}, fmt.Errorf("contract marshal: %w", err)
	}
	var c schema.Contract
	if err := json.Unmarshal(b, &c); err != nil {
		return schema.Contract{}, fmt.Errorf("contract unmarshal: %w", err)
	}
	if c.Name == "" {
		c.Name = "inline"
	}
	return c, nil
}

// buildTransformers constructs the transformer chain from configuration.
func buildTransformers(ts []config.Transform) (transformer.Chain, error) {
	var c transformer.Chain
	for _, t := range ts {
		switch t.Kind {
		case "validate":
			contract, err := decodeInlineContract(t.Options)
			if err != nil {
				return nil, err
			}
			var rejectCount int
			v := builtin.Validate{
				Contract:   contract,
				DateLayout: t.Options.String("date_layout", ""),
				Policy:     t.Options.String("policy", "lenient"),
				Reject: func(r builtin.RejectedRow) {
					rejectCount++
					if rejectCount <= 5 {
						log.Printf("validate reject: %s | raw=%v", r.Reason, r.Raw)
					}
					if rejectCount == 6 {
						log.Printf("... additional rejections suppressed ...")
					}
				},
			}
			c = append(c, v)
		case "normalize":
			c = append(c, builtin.Normalize{})
		case "dedupe":
			c = append(c, builtin.DeDup{
				Keys:         t.Options.StringSlice("keys"),
				Policy:       t.Options.String("policy", "keep-last"),
				PreferFields: t.Options.StringSlice("prefer_fields"),
			})
		case "coerce":
			c = append(c, builtin.Coerce{
				Types:  t.Options.StringMap("types"),
				Layout: t.Options.String("layout", "02.01.2006"),
			})
		case "require":
			c = append(c, builtin.Require{
				Fields: t.Options.StringSlice("fields"),
			})
		default:
			return nil, fmt.Errorf("unsupported transformer.kind=%s", t.Kind)
		}
	}
	if c == nil {
		c = transformer.Chain{}
	}
	return c, nil
}

// ----------------------------------------------------------------------------
// Streaming, concurrent path with pooled rows
// ----------------------------------------------------------------------------

// RowErr is a lightweight error container for per-line errors.
type RowErr struct {
	Line int
	Err  error
}

// runStreamed executes CSV → coerce → validate → storage in a fully streaming,
// batched, concurrent fashion with pooled row reuse. It enforces fail-soft
// semantics (bad rows are dropped before DB), aggregates parse/validate errors,
// and emits both per-batch progress and end-of-run summary statistics.
//

// Per-batch logging is emitted on each successful flush with running totals.
// runStreamed executes CSV → coerce → validate → storage in a fully streaming,
// batched, concurrent fashion with pooled row reuse. It enforces fail-soft
// semantics (bad rows are dropped before DB), aggregates parse/validate errors,
// and emits both per-batch progress and end-of-run summary statistics.
//
// Stats reported:
//   - processed:    rows that entered the coerce stage (i.e., parsed successfully)
//   - parseErrors:  lines the CSV reader could not parse (capped detailed logs)
//   - validateDrop: rows dropped by required-field validation
//   - inserted:     rows successfully flushed to the database
//   - batches:      number of COPY batches flushed
//
// Concurrency model:
//
//	Reader (CSV; usually 1)
//	     → tap (counts "processed")
//	     → N Transformers (coerce, in-place on pooled rows)
//	     → Validator (required fields; fail-soft)
//	     → Loader (COPY in batches)
//
// Back-pressure is provided by bounded channels; peak memory is O(batchSize +
// channelBuffer). The loader calls COPY in batches for throughput and, on any
// fatal DB error, cancels the context and drains/frees remaining rows to avoid
// goroutine leaks or deadlocks.
func runStreamed(ctx context.Context, spec config.Pipeline) error {
	// Tunables (12-factor): config first, env fallback.
	readerW := pickInt(spec.Runtime.ReaderWorkers, getenvInt("ETL_READER_WORKERS", 1))
	transW := pickInt(spec.Runtime.TransformWorkers, getenvInt("ETL_TRANSFORM_WORKERS", 4))
	loaderW := pickInt(spec.Runtime.LoaderWorkers, getenvInt("ETL_LOADER_WORKERS", 1)) // 1 writer per table is usually best
	batchSz := pickInt(spec.Runtime.BatchSize, getenvInt("ETL_BATCH_SIZE", 10000))
	bufSz := pickInt(spec.Runtime.ChannelBuffer, getenvInt("ETL_CH_BUFFER", 4096))

	// Stats counters (atomic across goroutines).
	type counters struct {
		processed       atomic.Int64 // rows entering coerce stage
		parseErrors     atomic.Int64 // csv read errors
		validateRejects atomic.Int64 // rows rejected by validate
		inserted        atomic.Int64 // rows inserted via COPY
		batches         atomic.Int64 // COPY batches flushed
	}
	var c counters

	// Aggregate (cap) parse and validate errors; log at end.
	parseAgg := newErrAgg(5)
	validateAgg := newErrAgg(5)

	log.Printf("stream runtime: readers=%d transformers=%d loaders=%d batch=%d buffer=%d",
		readerW, transW, loaderW, batchSz, bufSz)

	// Storage repository via factory (backend-agnostic).
	repo, err := storage.New(ctx, storage.Config{
		Kind:       spec.Storage.Kind,
		DSN:        spec.Storage.Postgres.DSN,
		Table:      spec.Storage.Postgres.Table,
		Columns:    spec.Storage.Postgres.Columns,
		KeyColumns: spec.Storage.Postgres.KeyColumns,
		DateColumn: spec.Storage.Postgres.DateColumn,
	})
	if err != nil {
		return err
	}
	defer repo.Close()

	// Optional: create table from config/contract before any write.
	if spec.Storage.Postgres.AutoCreateTable {
		td, err := ddlgen.InferTableDef(spec)
		if err != nil {
			return fmt.Errorf("infer table: %w", err)
		}
		ddl, err := ddlgen.BuildCreateTableSQL(td)
		if err != nil {
			return fmt.Errorf("build ddl: %w", err)
		}
		if err := repo.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("apply ddl: %w", err)
		}
		log.Printf("ensured table: %s", spec.Storage.Postgres.Table)
	}

	// COPY function remains abstracted behind the repository.
	copyFn := func(ctx context.Context, columns []string, rows [][]any) (int64, error) {
		return repo.CopyFrom(ctx, columns, rows)
	}

	// Cancellation: if the loader hits a fatal DB error, cancel upstream quickly.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan RowErr, 256)

	// Pooled row channels end-to-end.
	rawRowCh := make(chan *transformer.Row, bufSz)     // parser → tap
	tapRawCh := make(chan *transformer.Row, bufSz)     // tap → transformers (counts "processed")
	coercedRowCh := make(chan *transformer.Row, bufSz) // transformers → validate
	validCh := make(chan *transformer.Row, bufSz)      // validate → loader

	// Error/metrics collector: keep generic stage errors visible.
	var wgErr sync.WaitGroup
	wgErr.Add(1)
	go func() {
		defer wgErr.Done()
		log.Printf("loader: started with batchSize=%d columns=%d", batchSz, len(spec.Storage.Postgres.Columns))
		for e := range errCh {
			if e.Err != nil {
				if e.Line > 0 {
					log.Printf("row %d: %v", e.Line, e.Err)
				} else {
					log.Printf("stage err: %v", e.Err)
				}
			}
		}
	}()

	// --- 1) Reader (CSV streaming into pooled rows) ---
	var wgR sync.WaitGroup
	wgR.Add(readerW)
	for i := 0; i < readerW; i++ {
		go func() {
			defer wgR.Done()
			onParseErr := func(_ int, err error) {
				if err != nil {
					parseAgg.add(err.Error())
					c.parseErrors.Add(1)
				}
			}
			src, err := openSource(ctx, spec)
			if err != nil {
				errCh <- RowErr{Err: fmt.Errorf("source open: %w", err)}
				return
			}
			// Stream into pooled rows aligned to storage.Columns.
			if err := csvparser.StreamCSVRows(ctx, src, spec.Storage.Postgres.Columns, spec.Parser.Options, rawRowCh, onParseErr); err != nil {
				errCh <- RowErr{Err: err}
			}
		}()
	}

	// --- 1.5) Tap: count "processed" and forward to transformers --------------
	// This replaces the per-row temporary channel adapter and allows
	// TransformLoopRows to run as a long-lived, efficient streaming loop.
	var wgTap sync.WaitGroup
	wgTap.Add(1)
	go func() {
		defer wgTap.Done()
		defer close(tapRawCh)
		for r := range rawRowCh {
			c.processed.Add(1)
			select {
			case tapRawCh <- r:
			case <-ctx.Done():
				// If canceled, free the row to avoid leaks and return.
				r.Free()
				return
			}
		}
	}()

	// --- 2) Transformer pool (coerce in place on pooled rows) ---
	var wgT sync.WaitGroup
	wgT.Add(transW)

	coerceSpec := transformer.BuildCoerceSpecFromTypes(
		coerceTypesFromSpec(spec),
		coerceLayoutFromSpec(spec),
		nil, nil,
	)
	if err := transformer.ValidateSpecSanity(spec.Storage.Postgres.Columns, coerceSpec); err != nil {
		return fmt.Errorf("coerce spec sanity: %w", err)
	}

	for i := 0; i < transW; i++ {
		go func() {
			defer wgT.Done()
			// Long-lived streaming loop; no per-row channels.
			transformer.TransformLoopRows(ctx, spec.Storage.Postgres.Columns, tapRawCh, coercedRowCh, coerceSpec)
		}()
	}

	// Close coercedRowCh after readers, tap, and transformers complete.
	go func() {
		wgR.Wait()
		close(rawRowCh) // no more input into tap
		wgTap.Wait()
		wgT.Wait()
		close(coercedRowCh)
	}()

	// --- 3) Validate required fields (drop rows before DB) ---
	required := requiredFromContract(spec) // names marked `"required": true` in validate.contract
	var wgV sync.WaitGroup
	wgV.Add(1)
	go func() {
		defer wgV.Done()
		transformer.ValidateLoopRows(
			ctx,
			spec.Storage.Postgres.Columns,
			required,
			coercedRowCh,
			validCh,
			func(_ int, _ string) {
				validateAgg.add("missing required field")
				c.validateRejects.Add(1)
			},
		)
		// Close downstream when validator finishes.
		close(validCh)
	}()

	// --- 4) Loader — read valid rows, build batches, COPY, and always free rows ---
	var wgL sync.WaitGroup
	wgL.Add(loaderW)
	for i := 0; i < loaderW; i++ {
		go func() {
			defer wgL.Done()

			start := time.Now()
			lastFlush := start

			// Local batch buffers: values for COPY and parallel slice of rows for Free().
			bVals := make([][]any, 0, batchSz)
			bRows := make([]*transformer.Row, 0, batchSz)

			flush := func() error {
				if len(bVals) == 0 {
					return nil
				}
				n := len(bVals)
				// Call COPY via repository.
				if _, err := copyFn(ctx, spec.Storage.Postgres.Columns, bVals); err != nil {
					return err
				}
				// On success, free all rows in the batch.
				for _, r := range bRows {
					r.Free()
				}
				// Update stats and log per-batch.
				c.inserted.Add(int64(n))
				batchNum := c.batches.Add(1)
				now := time.Now()
				elapsed := now.Sub(start)
				rate := int64(float64(c.inserted.Load()) / elapsed.Seconds())
				log.Printf(
					"batch #%d: rps=%v inserted=%d total_inserted=%d elapsed=%s since_last=%s",
					batchNum,
					rate,
					n,
					c.inserted.Load(),
					now.Sub(start).Truncate(time.Millisecond),
					now.Sub(lastFlush).Truncate(time.Millisecond),
				)
				lastFlush = now
				// Reuse backing arrays.
				bVals = bVals[:0]
				bRows = bRows[:0]
				return nil
			}

			// Consume valid rows until channel closes or context cancels.
			for {
				select {
				case <-ctx.Done():
					// Context canceled (e.g., other loader failed): drain and free to avoid backpressure.
					for r := range validCh {
						r.Free()
					}
					return

				case r, ok := <-validCh:
					if !ok {
						// Final flush on channel close.
						if err := flush(); err != nil {
							errCh <- RowErr{Err: err}
							cancel()
						}
						return
					}
					bVals = append(bVals, r.V)
					bRows = append(bRows, r)
					if len(bVals) >= batchSz {
						if err := flush(); err != nil {
							// Report, cancel upstream, then drain & free remaining rows to prevent hang.
							errCh <- RowErr{Err: err}
							cancel()
							for r := range validCh {
								r.Free()
							}
							return
						}
					}
				}
			}
		}()
	}

	// Join stages and close error stream.
	wgV.Wait()
	wgL.Wait()
	close(errCh)
	wgErr.Wait()

	// Final summaries.
	if parseAgg.count > 0 {
		log.Printf("parse errors: %d (showing first %d)", parseAgg.count, len(parseAgg.first))
		for i, s := range parseAgg.first {
			log.Printf("  #%03d: %s", i+1, s)
		}
	}
	if validateAgg.count > 0 {
		log.Printf("validation rejects: %d (showing first %d)", validateAgg.count, len(validateAgg.first))
		for i, s := range validateAgg.first {
			log.Printf("  #%03d: %s", i+1, s)
		}
	}

	// Global stats: processed, dropped, inserted, batches.
	log.Printf(
		"summary: processed=%d parse_errors=%d validate_dropped=%d inserted=%d batches=%d",
		c.processed.Load(), c.parseErrors.Load(), c.validateRejects.Load(), c.inserted.Load(), c.batches.Load(),
	)

	return nil
}

// ----------------------------------------------------------------------------
// Small helpers
// ----------------------------------------------------------------------------

// getenvInt reads an int from environment, returning def when unset/invalid.
func getenvInt(k string, def int) int {
	if s := os.Getenv(k); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			return n
		}
	}
	return def
}

// pickInt chooses the first positive value 'a', otherwise returns 'b'.
func pickInt(a, b int) int {
	if a > 0 {
		return a
	}
	return b
}

// coerceTypesFromSpec extracts a union of "types" maps from the transform list.
// If no coerce step exists, it returns an empty map (text passthrough).
func coerceTypesFromSpec(p config.Pipeline) map[string]string {
	out := map[string]string{}
	for _, t := range p.Transform {
		if t.Kind == "coerce" {
			m := t.Options.StringMap("types")
			for k, v := range m {
				out[k] = v
			}
		}
	}
	return out
}

// coerceLayoutFromSpec returns the first "coerce.layout" encountered, or a
// sensible default for CZ (02.01.2006) when unspecified.
func coerceLayoutFromSpec(p config.Pipeline) string {
	for _, t := range p.Transform {
		if t.Kind == "coerce" {
			if s := t.Options.String("layout", ""); s != "" {
				return s
			}
		}
	}
	return "02.01.2006"
}

// splitFQN converts "schema.table" into {"schema","table"}; used in some places.
func splitFQN(fqn string) []string {
	parts := strings.Split(fqn, ".")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// errAgg aggregates errors
type errAgg struct {
	limit   int
	count   int
	first   []string
	buckets map[string]int
}

func newErrAgg(limit int) *errAgg {
	return &errAgg{limit: limit, buckets: make(map[string]int)}
}
func (a *errAgg) add(msg string) {
	a.buckets[msg]++
	if a.count < a.limit {
		a.first = append(a.first, msg)
	}
	a.count++
}

func requiredFromContract(p config.Pipeline) []string {
	for _, t := range p.Transform {
		if t.Kind == "validate" {
			if raw := t.Options.Any("contract"); raw != nil {
				b, _ := json.Marshal(raw)
				var c schema.Contract
				if json.Unmarshal(b, &c) == nil {
					var req []string
					for _, f := range c.Fields {
						if f.Required {
							req = append(req, f.Name)
						}
					}
					return req
				}
			}
		}
	}
	return nil
}
