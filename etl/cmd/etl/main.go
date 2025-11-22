package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"etl/internal/config"
	"etl/internal/metrics"
	"etl/internal/metrics/prompush"

	//     metricsdatadog "etl/internal/metrics/datadog"

	// register all backends with the stroage factory.
	// config specifies which to use but we need to build in support for all of them
	_ "etl/internal/storage/all"
)

// main is the entry point for the ETL binary. It loads the pipeline config,
// sets up optional profiling, and executes the streaming pipeline run.
func main() {
	var cfgPath string
	flag.StringVar(&cfgPath, "config", "configs/pipelines/sample.json", "pipeline config JSON path")
	verbose := flag.Bool("v", false, "enable verbose logs") // default true as requested
	//	debug := flag.Bool("d", false, "enable debug mode")

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

	// Decide metrics backend based on env/config.
	// Example: METRICS_BACKEND=prom_push PUSHGATEWAY_URL=...
	metricsBackend := os.Getenv("METRICS_BACKEND")
	switch metricsBackend {
	case "pushgateway":
		gwURL := os.Getenv("PUSHGATEWAY_URL")
		b, err := prompush.NewBackend(p.Job, gwURL)
		if err != nil {
			log.Printf("metrics: failed to init prom push backend: %v; using nop", err)
		} else {
			metrics.SetBackend(b)
			defer func() {
				if err := metrics.Flush(); err != nil {
					log.Printf("metrics: flush error: %v", err)
				}
			}()
		}
	//	    case "datadog":
	//    ddCfg := metricsdatadog.Config{
	//        Addr:       os.Getenv("DD_AGENT_ADDR"),   // e.g. "127.0.0.1:8125"
	//        Namespace:  "etl.",
	//        GlobalTags: []string{"job:" + spec.Job},
	//    }
	//    if b, err := metricsdatadog.NewBackend(ddCfg); err != nil {
	//        log.Printf("metrics: datadog backend init failed: %v; using nop", err)
	//    } else {
	//        metrics.SetBackend(b)
	//        defer func() {
	//            if err := metrics.Flush(); err != nil {
	//                log.Printf("metrics: flush error: %v", err)
	//            }
	//        }()
	//    }

	case "", "none":
		// metrics disabled; nop backend remains

	default:
		log.Printf("no metrics configured")
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
