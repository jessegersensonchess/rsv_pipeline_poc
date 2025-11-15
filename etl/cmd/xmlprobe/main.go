// Command xmlprobe is a unified CLI for inspecting XML inputs and generating
// starter JSON configs for the project's XML parser. It is tolerant to
// truncated inputs (e.g., partial files) and uses only the Go standard library.
//
// Example usage:
//
//	# Discover all relative paths under the record tag and print a report.
//	xmlprobe -i sample.xml -record_tag PubmedArticle -discover > report.json
//
//	# Guess record tag from the file, then generate a starter config.
//	xmlprobe -i sample.xml -generate-config > config.json
//
//	# Use an existing config to source record_tag for discovery.
//	xmlprobe -i sample.xml -config configs/pipelines/sample.json -discover
//
//	# Parse using a config (high-perf flags available)
//	xmlprobe -i sample.xml -config config.json \
//	    -workers 9 -queue 256 -bufsize 65536 -zerocopy -ultrafast -schema
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"

	"etl/internal/config"
	"etl/internal/inspect"
	xmlparser "etl/internal/parser/xml"
	"etl/internal/schema/ddl"
	"etl/internal/storage"
	_ "etl/internal/storage/postgres" // register "postgres" backend
)

// xmlStorageJSON is the JSON shape for xmlprobe's storage block.
// It is flat and uses lowercase names, but we also tolerate "Kind".
type xmlStorageJSON struct {
	Kind            string   `json:"kind"` // preferred
	DSN             string   `json:"dsn"`
	Table           string   `json:"table"`
	Columns         []string `json:"columns"`
	KeyColumns      []string `json:"key_columns"`
	DateColumn      string   `json:"date_column"`
	AutoCreateTable bool     `json:"auto_create_table"`
}

// xmlStorageJSONCompat captures legacy/alternative keys we want to support.
type xmlStorageJSONCompat struct {
	Kind string `json:"Kind"` // accept capitalized Kind as in your example
}

func main() {
	var (
		// Core
		inputPath  = flag.String("i", "", "input XML file path")
		configPath = flag.String("config", "", "JSON config file (required for parse mode unless -record_tag used with -discover/-generate-config)")
		recordTag  = flag.String("record_tag", "", "override record tag (e.g., PubmedArticle). If empty, try to guess or read from -config")

		// Modes
		discover    = flag.Bool("discover", false, "scan XML and output a JSON report of all relative paths/attrs under the record tag")
		generateCfg = flag.Bool("generate-config", false, "generate a starter config JSON inferred from discovery")

		// Output formatting
		pretty = flag.Bool("pretty", false, "pretty-print JSON output")

		// Performance flags (parse mode)
		workers     = flag.Int("workers", 2, "number of worker goroutines (0=auto: 1)")
		queue       = flag.Int("queue", 0, "job/result channel capacity (0=auto: 4*workers)")
		bufSize     = flag.Int("bufsize", 1024, "reader buffer size for non-zerocopy (bytes; 0=1<<20)")
		zeroCopy    = flag.Bool("zerocopy", false, "use zero-copy sharding (reads entire file into memory)")
		ultraFast   = flag.Bool("ultrafast", false, "enable schema-like raw byte fast path")
		schemaFlag  = flag.Bool("schema", false, "alias of -ultrafast (kept for parity)")
		preserveOrd = flag.Bool("preserve_order", false, "preserve record order (lower throughput)")
		orderWindow = flag.Int("order_window", 0, "bounded reordering window (0=unordered)")

		// New: load mode (step 3 of ETL)
		loadToStorage = flag.Bool("load", false, "load parsed records into storage defined in config.storage instead of printing NDJSON")
	)
	flag.Parse()

	if *inputPath == "" {
		log.Fatal("missing -i")
	}

	// Open input file for modes that need it.
	f, err := os.Open(*inputPath)
	if err != nil {
		log.Fatalf("open input: %v", err)
	}
	defer f.Close()

	// ---- DISCOVERY / CONFIG GENERATION ----
	if *discover || *generateCfg {
		rt := strings.TrimSpace(*recordTag)
		if rt == "" && *configPath != "" {
			if cfgRT, err := readConfigRecordTag(*configPath); err == nil && cfgRT != "" {
				rt = cfgRT
			}
		}
		if rt == "" {
			if _, err := f.Seek(0, io.SeekStart); err != nil {
				log.Fatalf("seek: %v", err)
			}
			if g, err := inspect.GuessRecordTag(io.LimitReader(f, 1<<20)); err == nil {
				rt = g
			}
			if rt == "" {
				log.Fatal("could not determine record_tag; provide -record_tag or -config")
			}
		}

		// Run discovery from start (trunc-safe).
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			log.Fatalf("seek: %v", err)
		}
		rep, err := inspect.Discover(f, rt)
		if err != nil {
			log.Fatalf("discover: %v", err)
		}

		if *discover {
			generateReport(rep, *pretty)
			return
		}
		if *generateCfg {
			// Starter XML parser config
			cfg := inspect.StarterConfigFrom(rep)

			// Convert to a generic map so we can append "storage"
			b, err := xmlparser.MarshalConfigJSON(cfg, false)
			if err != nil {
				log.Fatalf("marshal starter config: %v", err)
			}

			var m map[string]any
			if err := json.Unmarshal(b, &m); err != nil {
				log.Fatalf("unmarshal starter config to map: %v", err)
			}

			// Auto-infer storage.columns from fields + lists
			cols := inferColumnsFromConfig(cfg)

			// Attach storage stub (flat JSON with lowercase keys)
			storageJSON := xmlStorageJSON{
				Kind:            "postgres",
				DSN:             "",
				Table:           "",
				Columns:         cols,
				KeyColumns:      nil,
				DateColumn:      "",
				AutoCreateTable: false,
			}
			m["storage"] = storageJSON

			enc := json.NewEncoder(os.Stdout)
			if *pretty {
				enc.SetIndent("", "  ")
			}
			if err := enc.Encode(m); err != nil {
				log.Fatalf("encode starter config with storage: %v", err)
			}
			return
		}
	}

	// ---- PARSE / LOAD MODE (default when no discovery/gen flags) ----
	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "No action specified. Use -discover or -generate-config, or provide -config to parse.")
		flag.Usage()
		os.Exit(2)
	}

	// Load & compile XML parser config (parser-only view).
	cfg, err := readConfig(*configPath)
	if err != nil {
		log.Fatalf("read config: %v", err)
	}
	comp, err := xmlparser.Compile(cfg)
	if err != nil {
		log.Fatalf("compile config: %v", err)
	}

	// Optional: load storage block and corresponding minimal pipeline spec if requested.
	var storeCfg *storage.Config
	var pipeSpec *config.Pipeline
	if *loadToStorage {
		sc, spec, err := readStorageConfigAsPipeline(*configPath)
		if err != nil {
			log.Fatalf("read storage config: %v", err)
		}
		if sc.Kind == "" {
			log.Fatalf("config.storage.kind is required when using -load")
		}
		if spec.Storage.Postgres.Table == "" {
			log.Fatalf("config.storage.table is required when using -load")
		}
		if len(spec.Storage.Postgres.Columns) == 0 {
			log.Fatalf("config.storage.columns must be non-empty when using -load")
		}
		storeCfg = &sc
		pipeSpec = &spec
	}

	// Build Options from flags.
	opts := xmlparser.Options{
		Workers:       *workers,
		Queue:         *queue,
		BufSize:       *bufSize,
		ZeroCopy:      *zeroCopy,
		UltraFast:     *ultraFast || *schemaFlag,
		Schema:        *schemaFlag,
		PreserveOrder: *preserveOrd,
		OrderWindow:   *orderWindow,
	}

	// Prepare reader/bytes depending on zerocopy.
	var rdr io.Reader
	var data []byte
	if opts.ZeroCopy {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			log.Fatalf("seek: %v", err)
		}
		data, err = io.ReadAll(f)
		if err != nil {
			log.Fatalf("read all (zerocopy): %v", err)
		}
	} else {
		rdr = f
	}

	if storeCfg != nil && pipeSpec != nil {
		if err := runParserAndLoad(rdr, data, cfg.RecordTag, comp, opts, storeCfg, pipeSpec); err != nil {
			log.Fatalf("%v", err)
		}
	} else {
		runParser(rdr, data, cfg.RecordTag, comp, opts)
	}
}

// runParser executes ParseStream and NDJSON-prints records to stdout.
// It closes the output channel when ParseStream returns to avoid deadlocks.
func runParser(r io.Reader, data []byte, recordTag string, comp xmlparser.Compiled, opts xmlparser.Options) {
	out := make(chan xmlparser.Record, maxInt(4, opts.Queue))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		// ParseStream writes to 'out' and returns when done.
		errCh <- xmlparser.ParseStream(ctx, r, data, recordTag, comp, opts, out)
		close(out) // IMPORTANT: close so the printer loop below terminates cleanly
	}()

	enc := json.NewEncoder(os.Stdout)

	for rec := range out {
		if err := enc.Encode(rec); err != nil {
			log.Printf("encode record: %v", err)
		}
	}

	// Wait for ParseStream to finish and report any fatal error.
	if err := <-errCh; err != nil {
		log.Fatalf("parse: %v", err)
	}
}

// runParserAndLoad parses XML and bulk-loads into configured storage.
// It uses the same abstraction level as container.go:
//   - storage.New(repo)
//   - ddl.InferTableDef(spec) + BuildCreateTableSQL + repo.Exec (if AutoCreateTable)
//   - repo.CopyFrom for COPY.
func runParserAndLoad(
	r io.Reader,
	data []byte,
	recordTag string,
	comp xmlparser.Compiled,
	opts xmlparser.Options,
	storeCfg *storage.Config,
	spec *config.Pipeline,
) error {
	out := make(chan xmlparser.Record, maxInt(4, opts.Queue))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Storage repo backed by factory
	repo, err := storage.New(ctx, *storeCfg)
	if err != nil {
		return fmt.Errorf("storage init: %w", err)
	}
	defer repo.Close()

	// Optional: create table from config/contract before any write,
	// using the same pattern as container.go.
	if spec.Storage.Postgres.AutoCreateTable {
		td, err := ddl.InferTableDef(*spec)
		if err != nil {
			return fmt.Errorf("infer table: %w", err)
		}
		ddlSQL, err := ddl.BuildCreateTableSQL(td)
		if err != nil {
			return fmt.Errorf("build ddl: %w", err)
		}
		if err := repo.Exec(ctx, ddlSQL); err != nil {
			return fmt.Errorf("apply ddl: %w", err)
		}
		log.Printf("ensured table: %s", spec.Storage.Postgres.Table)
	}

	// COPY function remains abstracted behind the repository.
	copyFn := func(ctx context.Context, columns []string, rows [][]any) (int64, error) {
		return repo.CopyFrom(ctx, columns, rows)
	}

	// Cancellation: if loader hits a fatal DB error, cancel upstream quickly.
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- xmlparser.ParseStream(ctx, r, data, recordTag, comp, opts, out)
		close(out)
	}()

	const batchSize = 5000
	cols := storeCfg.Columns
	rows := make([][]any, 0, batchSize)
	var inserted int64
	batchNum := 0

	flush := func() error {
		if len(rows) == 0 {
			return nil
		}
		n, err := copyFn(ctx, cols, rows)
		if err != nil {
			return err
		}
		inserted += n
		batchNum++
		log.Printf("batch=%d inserted=%d total_inserted=%d", batchNum, n, inserted)
		rows = rows[:0]
		return nil
	}

	for rec := range out {
		row := recordToRow(rec, cols)
		rows = append(rows, row)
		if len(rows) >= batchSize {
			if err := flush(); err != nil {
				cancel()
				return fmt.Errorf("copy: %w", err)
			}
		}
	}
	if err := flush(); err != nil {
		cancel()
		return fmt.Errorf("copy: %w", err)
	}

	if err := <-errCh; err != nil {
		return fmt.Errorf("parse: %w", err)
	}

	log.Printf("load complete: total_inserted=%d batches=%d", inserted, batchNum)
	return nil
}

func readConfigRecordTag(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	cfg, err := xmlparser.ParseConfigJSON(b)
	if err != nil {
		return "", err
	}
	return cfg.RecordTag, nil
}

func readConfig(path string) (xmlparser.Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return xmlparser.Config{}, err
	}
	return xmlparser.ParseConfigJSON(b)
}

// readStorageConfigAsPipeline pulls the "storage" block from the XML config JSON,
// maps it both into storage.Config (for storage.New) and a minimal config.Pipeline
// (for ddl.InferTableDef), using a flat JSON shape. It also tolerates "Kind".
func readStorageConfigAsPipeline(path string) (storage.Config, config.Pipeline, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return storage.Config{}, config.Pipeline{}, err
	}
	var wrapper struct {
		Storage json.RawMessage `json:"storage"`
	}
	if err := json.Unmarshal(b, &wrapper); err != nil {
		return storage.Config{}, config.Pipeline{}, err
	}
	if len(wrapper.Storage) == 0 {
		return storage.Config{}, config.Pipeline{}, fmt.Errorf("missing storage block")
	}

	var sj xmlStorageJSON
	_ = json.Unmarshal(wrapper.Storage, &sj)

	// Compatibility: accept "Kind" if "kind" was not set.
	if sj.Kind == "" {
		var compat xmlStorageJSONCompat
		_ = json.Unmarshal(wrapper.Storage, &compat)
		if compat.Kind != "" {
			sj.Kind = compat.Kind
		}
	}

	// Build storage.Config for the storage repository.
	sc := storage.Config{
		Kind:       sj.Kind,
		DSN:        sj.DSN,
		Table:      sj.Table,
		Columns:    sj.Columns,
		KeyColumns: sj.KeyColumns,
		DateColumn: sj.DateColumn,
	}

	// Build a minimal config.Pipeline carrying only Storage.* for ddl.InferTableDef.
	var spec config.Pipeline
	spec.Storage.Kind = sj.Kind
	spec.Storage.Postgres.DSN = sj.DSN
	spec.Storage.Postgres.Table = sj.Table
	spec.Storage.Postgres.Columns = sj.Columns
	spec.Storage.Postgres.KeyColumns = sj.KeyColumns
	spec.Storage.Postgres.DateColumn = sj.DateColumn
	spec.Storage.Postgres.AutoCreateTable = sj.AutoCreateTable

	return sc, spec, nil
}

func generateReport(rep inspect.DiscoverReport, pretty bool) {
	type kv struct {
		Path string          `json:"path"`
		Data inspect.PathAgg `json:"data"`
	}
	paths := inspect.SortedPaths(rep)
	out := struct {
		RecordTag    string `json:"record_tag"`
		TotalRecords int    `json:"total_records"`
		Paths        []kv   `json:"paths"`
	}{
		RecordTag:    rep.RecordTag,
		TotalRecords: rep.TotalRecords,
	}
	for _, p := range paths {
		out.Paths = append(out.Paths, kv{Path: p, Data: rep.Paths[p]})
	}
	enc := json.NewEncoder(os.Stdout)
	if pretty {
		enc.SetIndent("", "  ")
	}
	if err := enc.Encode(out); err != nil {
		log.Fatalf("encode report: %v", err)
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// inferColumnsFromConfig builds a deterministic column list from the XML config.
// It uses all field and list names.
func inferColumnsFromConfig(cfg xmlparser.Config) []string {
	cols := make([]string, 0, len(cfg.Fields)+len(cfg.Lists))
	for name := range cfg.Fields {
		cols = append(cols, name)
	}
	for name := range cfg.Lists {
		cols = append(cols, name)
	}
	sort.Strings(cols)
	return cols
}

// recordToRow maps an xmlparser.Record (type Record map[string]any)
// into a []any in storage column order.
func recordToRow(rec xmlparser.Record, columns []string) []any {
	row := make([]any, len(columns))
	m := map[string]any(rec)
	for i, col := range columns {
		row[i] = m[col] // nil if missing
	}
	return row
}
