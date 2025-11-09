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
			StreamScrubLikvidaci: p.Options.Bool("stream_scrub_likvidaci", false),
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

// runStreamed executes CSV → transform → storage in a streaming, batched,
// concurrent fashion with pooled row reuse.
func runStreamed(ctx context.Context, spec config.Pipeline) error {
	readerW := pickInt(spec.Runtime.ReaderWorkers, getenvInt("ETL_READER_WORKERS", 1))
	transW := pickInt(spec.Runtime.TransformWorkers, getenvInt("ETL_TRANSFORM_WORKERS", 4))
	loaderW := pickInt(spec.Runtime.LoaderWorkers, getenvInt("ETL_LOADER_WORKERS", 1)) // 1 writer per table is usually best
	batchSz := pickInt(spec.Runtime.BatchSize, getenvInt("ETL_BATCH_SIZE", 10000))
	bufSz := pickInt(spec.Runtime.ChannelBuffer, getenvInt("ETL_CH_BUFFER", 4096))

	log.Printf("stream runtime: readers=%d transformers=%d loaders=%d batch=%d buffer=%d",
		readerW, transW, loaderW, batchSz, bufSz)

	// Storage repository via factory.
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
	} else {
		log.Printf("DEBUG: 383")

	}

	copyFn := func(ctx context.Context, columns []string, rows [][]any) (int64, error) {
		return repo.CopyFrom(ctx, columns, rows)
	}

	errCh := make(chan RowErr, 256)

	// Channels with pooled rows end-to-end.
	rawRowCh := make(chan *transformer.Row, bufSz)
	coercedRowCh := make(chan *transformer.Row, bufSz)

	// Error/metrics collector
	var wgErr sync.WaitGroup
	wgErr.Add(1)
	go func() {
		defer wgErr.Done()
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

	// 1) Reader (CSV streaming into pooled rows)
	var wgR sync.WaitGroup
	wgR.Add(readerW)
	for i := 0; i < readerW; i++ {
		go func() {
			defer wgR.Done()
			onErr := func(line int, err error) { errCh <- RowErr{Line: line, Err: err} }

			src, err := openSource(ctx, spec)
			if err != nil {
				errCh <- RowErr{Err: fmt.Errorf("source open: %w", err)}
				return
			}
			if err := csvparser.StreamCSVRows(ctx, src, spec.Storage.Postgres.Columns, spec.Parser.Options, rawRowCh, onErr); err != nil {
				errCh <- RowErr{Err: err}
			}
		}()
	}

	// 2) Transformer pool (in-place on pooled rows)
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
			transformer.TransformLoopRows(ctx, spec.Storage.Postgres.Columns, rawRowCh, coercedRowCh, coerceSpec)
		}()
	}

	// Close coercedRowCh once all readers and transformers finish.
	go func() {
		wgR.Wait()
		close(rawRowCh) // no more input
		wgT.Wait()
		close(coercedRowCh)
	}()

	// 3) Loader workers — adapt *transformer.Row to storage.RowLike and load.
	var wgL sync.WaitGroup
	wgL.Add(loaderW)
	for i := 0; i < loaderW; i++ {
		go func() {
			defer wgL.Done()

			// Adapter channel for the loader (RowLike has FreeFunc).
			adaptCh := make(chan *storage.RowLike, bufSz)
			var wgA sync.WaitGroup
			wgA.Add(1)
			go func() {
				defer wgA.Done()
				for r := range coercedRowCh {
					adaptCh <- &storage.RowLike{
						V:        r.V,
						FreeFunc: r.Free,
					}
				}
				close(adaptCh)
			}()

			if _, err := storage.LoadBatchesRows(ctx, spec.Storage.Postgres.Columns, adaptCh, batchSz, copyFn); err != nil {
				errCh <- RowErr{Err: err}
			}
			wgA.Wait()
		}()
	}

	// Join and complete.
	wgL.Wait()
	close(errCh)
	wgErr.Wait()
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
