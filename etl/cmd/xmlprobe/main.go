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
	"strings"

	"etl/internal/inspect"
	xmlparser "etl/internal/parser/xml"
)

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
		schema      = flag.Bool("schema", false, "alias of -ultrafast (kept for parity)")
		preserveOrd = flag.Bool("preserve_order", false, "preserve record order (lower throughput)")
		orderWindow = flag.Int("order_window", 0, "bounded reordering window (0=unordered)")
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
			cfg := inspect.StarterConfigFrom(rep)
			b, err := xmlparser.MarshalConfigJSON(cfg, *pretty)
			if err != nil {
				log.Fatalf("marshal starter config: %v", err)
			}
			os.Stdout.Write(b)
			if *pretty {
				os.Stdout.Write([]byte("\n"))
			}
			return
		}
	}

	// ---- PARSE MODE (default when no discovery/gen flags) ----
	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "No action specified. Use -discover or -generate-config, or provide -config to parse.")
		flag.Usage()
		os.Exit(2)
	}

	// Load & compile config.
	cfg, err := readConfig(*configPath)
	if err != nil {
		log.Fatalf("read config: %v", err)
	}
	comp, err := xmlparser.Compile(cfg)
	if err != nil {
		log.Fatalf("compile config: %v", err)
	}

	// Build Options from flags.
	opts := xmlparser.Options{
		Workers:       *workers,
		Queue:         *queue,
		BufSize:       *bufSize,
		ZeroCopy:      *zeroCopy,
		UltraFast:     *ultraFast || *schema,
		Schema:        *schema,
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

	runParser(rdr, data, cfg.RecordTag, comp, opts)
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
