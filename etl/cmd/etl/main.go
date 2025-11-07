package main

import (
	"context" // Add context import
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time" // Add time import

	"etl/internal/config"
)

func main() {
	var cfgPath string
	flag.StringVar(&cfgPath, "config", "configs/pipelines/file_to_postgres.json", "pipeline config JSON path")
	verbose := flag.Bool("v", true, "enable verbose logs") // default true as requested

	flag.Parse()

	if !*verbose {
		log.SetOutput(os.Stderr) // still stderr, just less chatter if you gate logs yourself
	}

	f, err := os.Open(cfgPath)
	if err != nil {
		fatalf("open config: %v", err)
	}
	defer f.Close()

	// Initialize the spec here, before using it
	var p config.Pipeline
	if err := json.NewDecoder(f).Decode(&p); err != nil {
		fatalf("decode config: %v", err)
	}

	// Initialize the context here, before passing it to run()
	ctx := context.Background()
	start := time.Now() // Initialize start time

	// Print the config information if verbose
	if *verbose {
		log.Printf("pipeline: source=%s parser=%s storage=%s table=%s",
			p.Source.Kind, p.Parser.Kind, p.Storage.Kind, p.Storage.Postgres.Table)
	}

	// Now call run with the correct context, spec, verbose flag, and start time
	if err := run(ctx, p, *verbose, start); err != nil {
		log.Fatalf("%v", err)
	}
}

func fatalf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(1)
}
