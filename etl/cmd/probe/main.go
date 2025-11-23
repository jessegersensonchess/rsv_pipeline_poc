package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"etl/internal/probe"
)

// main is the entrypoint for the unified probing CLI. It fetches a small sample
// from the given URL, auto-detects CSV vs XML vs JSON, infers a starter ETL
// pipeline configuration, and prints it as JSON.
//
// The resulting config is intended to be hand-edited and then used with
// cmd/etl.
func main() {
	var (
		flagURL = flag.String(
			"url",
			"",
			"URL of the source file (CSV, XML, or JSON)",
		)
		flagBytes = flag.Int(
			"bytes",
			20000,
			"Number of bytes to sample from the start of the file",
		)
		flagName = flag.String(
			"name",
			"dataset_name",
			"Logical dataset/connector name (used in storage.db.table, etc.)",
		)
		flagJob = flag.String(
			"job",
			"",
			"Logical job name for metrics/config; defaults to a normalized version of -name when empty",
		)
		flagSave = flag.Bool(
			"save",
			false,
			"Write sampled bytes to [name].{csv,xml,json} file next to the current directory",
		)
		flagPretty = flag.Bool(
			"pretty",
			true,
			"Pretty-print JSON output",
		)
		flagAllowInsecure = flag.Bool(
			"allow-insecure",
			true,
			"allow insecure certs",
		)
		flagBackend = flag.String(
			"backend",
			"postgres",
			"Storage backend to target in the generated config: postgres|mssql|sqlite",
		)
	)
	flag.Parse()

	if *flagURL == "" {
		fmt.Fprintln(os.Stderr, "missing -url")
		flag.Usage()
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg, err := probe.ProbeURL(ctx, probe.Options{
		URL:              *flagURL,
		MaxBytes:         *flagBytes,
		Name:             *flagName,
		SaveSample:       *flagSave,
		Backend:          *flagBackend,
		AllowInsecureTLS: *flagAllowInsecure,
		Job:              *flagJob, // may be empty; ProbeURL will derive from Name
	})
	if err != nil {
		log.Fatalf("probe: %v", err)
	}

	enc := json.NewEncoder(os.Stdout)
	if *flagPretty {
		enc.SetIndent("", "  ")
	}
	if err := enc.Encode(cfg); err != nil {
		log.Fatalf("encode config: %v", err)
	}
}
