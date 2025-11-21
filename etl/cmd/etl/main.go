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
	// register all backends with the stroage factory.
	// config specifies which to use but we need to build in support for all of them
	_ "etl/internal/storage/all"
	// Register the "postgres" backend with the storage factory.
	//	_ "etl/internal/storage/postgres"
	//	_ "etl/internal/storage/mssql"
	//	_ "etl/internal/storage/sqlite"
)

// main is the entry point for the ETL binary. It loads the pipeline config,
// sets up optional profiling, and executes the streaming pipeline run.
func main() {
	var cfgPath string
	flag.StringVar(&cfgPath, "config", "configs/pipelines/file_to_postgres.json", "pipeline config JSON path")
	verbose := flag.Bool("v", false, "enable verbose logs") // default true as requested
	//	debug := flag.Bool("d", false, "enable debug mode")

	flag.Parse()

	//	if *debug {
	//		// profiling
	//		f, _ := os.Create("cpu.pprof")
	//		pprof.StartCPUProfile(f)
	//		defer pprof.StopCPUProfile()
	//
	//		hf, _ := os.Create("heap.pprof")
	//		defer func() {
	//			runtime.GC()
	//			pprof.WriteHeapProfile(hf)
	//			hf.Close()
	//		}()
	//		// end profiling
	//	}

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

	// Initialize the context here, before passing it to runStreamed()
	ctx := context.Background()
	start := time.Now() // Initialize start time

	// Print the config information if verbose
	if *verbose {
		log.Printf("pipeline: source=%s parser=%s storage=%s table=%s",
			p.Source.Kind, p.Parser.Kind, p.Storage.Kind, p.Storage.DB.Table)
	}

	// Execute the streaming pipeline
	if err := runStreamed(ctx, p); err != nil {
		log.Fatalf("%v", err)
	}

	// Optional: log total elapsed time when verbose
	if *verbose {
		log.Printf("completed in %s", time.Since(start).Truncate(time.Millisecond))
	}
}

func fatalf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(1)
}
