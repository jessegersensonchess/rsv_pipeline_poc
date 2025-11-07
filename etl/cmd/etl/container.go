package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"time"

	"etl/internal/config"
	"etl/internal/datasource/file"
	"etl/internal/parser"
	csvparser "etl/internal/parser/csv"
	"etl/internal/schema"
	"etl/internal/storage/postgres"
	"etl/internal/transformer"
	"etl/internal/transformer/builtin"
)

func run(ctx context.Context, spec config.Pipeline, verbose bool, t0 time.Time) error {
	// 1) Source selection (file-only in this POC)
	var src io.ReadCloser
	var err error
	switch spec.Source.Kind {
	case "file":
		src, err = file.NewLocal(spec.Source.File.Path).Open(ctx)
	default:
		return fmt.Errorf("unsupported source.kind=%s", spec.Source.Kind)
	}
	if err != nil {
		return fmt.Errorf("source open: %w", err)
	}
	defer src.Close()

	// 2) Parser selection via registry
	pr, err := buildParser(spec.Parser)
	if err != nil {
		return err
	}

	// Initialize the start time for parsing
	parseStart := time.Now()

	recs, skippedRows, err := pr.Parse(src)
	if err != nil {
		return fmt.Errorf("parse: %w", err)
	}

	if verbose {
		log.Printf("parsed: %d records in %s", len(recs), time.Since(parseStart).Truncate(time.Millisecond))
		log.Printf("skipped: %d rows due to parsing errors", skippedRows)

	}

	// 3) Transformers: build a chain from config
	chain, err := buildTransformers(spec.Transform)
	if err != nil {
		return err
	}
	tfStart := time.Now()

	before := len(recs)
	recs = chain.Apply(recs)
	after := len(recs)

	if verbose {
		log.Printf("transformed: %d -> %d records (delta=%+d) in %s",
			before, after, after-before, time.Since(tfStart).Truncate(time.Millisecond))
	}

	// 4) Storage selection; Postgres repository configured with columns & keys from config
	switch spec.Storage.Kind {
	case "postgres":
		repo, close, err := postgres.NewRepository(ctx, postgres.Config{
			DSN:        spec.Storage.Postgres.DSN,
			Table:      spec.Storage.Postgres.Table,
			Columns:    spec.Storage.Postgres.Columns,
			KeyColumns: spec.Storage.Postgres.KeyColumns,
		})
		if err != nil {
			return err
		}
		defer close()

		// Initialize loadStart for timing the load phase
		loadStart := time.Now()

		// Pass the key_columns and date_column as arguments to BulkUpsert
		n, err := repo.BulkUpsert(ctx, recs, spec.Storage.Postgres.KeyColumns, spec.Storage.Postgres.DateColumn)
		if err != nil {
			return fmt.Errorf("aload: %w, %v", err, n)
		}
		//n, err := repo.BulkUpsert(ctx, recs)
		if err != nil {
			return fmt.Errorf("load: %w", err)
		}

		if verbose {
			log.Printf("loaded: inserted=%d rows into %s in %s (total elapsed %s)",
				n, spec.Storage.Postgres.Table,
				time.Since(loadStart).Truncate(time.Millisecond),
				time.Since(t0).Truncate(time.Millisecond))
		}
	default:
		return fmt.Errorf("unsupported storage.kind=%s (POC wires postgres)", spec.Storage.Kind)
	}

	return nil
}

func openSource(ctx context.Context, spec config.Pipeline) (io.ReadCloser, error) {
	switch spec.Source.Kind {
	case "file":
		return file.NewLocal(spec.Source.File.Path).Open(ctx)
	default:
		return nil, fmt.Errorf("unsupported source.kind=%s", spec.Source.Kind)
	}
}

func buildParser(p config.Parser) (parser.Parser, error) {
	switch p.Kind {
	case "csv":
		// Map generic options into CSV options; all fields optional in config.
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

func decodeInlineContract(opt config.Options) (schema.Contract, error) {
	// Fetch the raw "contract" object from options (as map[string]any), then JSON round-trip into schema.Contract.
	raw := opt.Any("contract") // <-- if your Options doesn't have Any(), see note below
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
				DateLayout: t.Options.String("date_layout", ""), // optional global fallback
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
			// keys: required []string
			// policy: "keep-first" | "keep-last" | "most-complete" (default keep-last)
			c = append(c, builtin.DeDup{
				Keys:         t.Options.StringSlice("keys"),
				Policy:       t.Options.String("policy", "keep-last"),
				PreferFields: t.Options.StringSlice("prefer_fields"),
			})
		case "coerce":
			// coerce types based on a field->type map; layout for dates
			c = append(c, builtin.Coerce{
				Types:  t.Options.StringMap("types"), // e.g., {"pcv":"int","date_from":"date"}
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
