// cmd/get_url_from_list/main.go
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	fileds "etl/internal/datasource/file"
	"etl/internal/datasource/httpds"
)

// logRecord represents one JSON log line that we will print.
type logRecord struct {
	URL        string `json:"url"`
	DurationMs int64  `json:"duration_ms"`
	DownloadSz int64  `json:"download_size"`
	StatusCode int    `json:"http_response_code"`
	File       string `json:"file,omitempty"`
}

func main() {
	// =========================
	// Parse command line flags
	// =========================
	urlFile := flag.String("i", "", "Path to file containing URLs")
	threads := flag.Int("n", 4, "Number of concurrent workers")
	timeout := flag.Duration("t", 30*time.Second, "HTTP timeout per request, example 60s")
	outDir := flag.String("o", "out", "Directory to save downloaded bodies")
	flag.Parse()

	if *urlFile == "" {
		fmt.Fprintln(os.Stderr, "missing required -i <url_file>")
		os.Exit(1)
	}

	if *threads <= 0 {
		fmt.Fprintln(os.Stderr, "number of workers (-n) must be > 0")
		os.Exit(1)
	}

	// Create output directory if it does not exist.
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create output directory: %v\n", err)
		os.Exit(1)
	}

	// Read URLs from the file into memory.
	urls, err := fileds.ReadList(*urlFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error reading urls: %v\n", err)
		os.Exit(1)
	}
	if len(urls) == 0 {
		fmt.Fprintln(os.Stderr, "no URLs found in input file")
		return
	}

	// Context allows us to cancel all goroutines at once if needed.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// HTTP client with retry/backoff shared by all workers.
	client := httpds.NewClient(httpds.Config{
		Timeout:            *timeout,
		MaxRetries:         2,
		InitialBackoff:     200 * time.Millisecond,
		MaxBackoff:         5 * time.Second,
		InsecureSkipVerify: false,
	})

	// Channel of work: URLs to download.
	jobs := make(chan string)

	// Channel of log entries.
	logCh := make(chan logRecord, 200)

	// Flag used to track fatal errors across goroutines.
	var fatal int32

	// ======================================
	// Logger goroutine
	// ======================================
	var logWg sync.WaitGroup
	logWg.Add(1)
	go func() {
		defer logWg.Done()

		enc := json.NewEncoder(os.Stdout)
		for rec := range logCh {
			_ = enc.Encode(rec)
		}
	}()

	// ======================================
	// Worker goroutines
	// ======================================
	var wg sync.WaitGroup

	for i := 0; i < *threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker(ctx, client, jobs, logCh, *outDir, &fatal, cancel)
		}()
	}

	// Feed jobs in a separate goroutine.
	go func() {
		defer close(jobs)
		for _, u := range urls {
			select {
			case <-ctx.Done():
				return
			case jobs <- u:
			}
		}
	}()

	// =========================
	// Wait for work to finish
	// =========================
	wg.Wait()
	close(logCh)
	logWg.Wait()

	if atomic.LoadInt32(&fatal) == 1 {
		os.Exit(1)
	}
}

// worker runs in a goroutine and processes URLs from the jobs channel.
func worker(
	ctx context.Context,
	client *httpds.Client,
	jobs <-chan string,
	logCh chan<- logRecord,
	outDir string,
	fatal *int32,
	cancel context.CancelFunc,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case u, ok := <-jobs:
			if !ok {
				return
			}
			if !processURL(ctx, client, u, logCh, outDir) {
				atomic.StoreInt32(fatal, 1)
				cancel()
				return
			}
		}
	}
}

// processURL performs an HTTP GET for rawURL using the shared httpds.Client,
// logs metrics, writes the body to a file on success, and returns whether the
// operation should be considered successful.
//
// Retry and backoff behavior are handled inside httpds.Client.
func processURL(
	ctx context.Context,
	client *httpds.Client,
	rawURL string,
	logCh chan<- logRecord,
	outDir string,
) bool {
	start := time.Now()
	statusCode := 0
	var size int64
	var fileWritten string

	resp, err := client.Get(ctx, rawURL, nil)
	if err == nil && resp != nil {
		defer resp.Body.Close()

		body, readErr := io.ReadAll(resp.Body)
		if readErr == nil {
			statusCode = resp.StatusCode
			size = int64(len(body))

			if statusCode >= 200 && statusCode < 300 {
				// Build a deterministic, safe filename from the URL.
				name := httpds.SafeFilenameFromURL(rawURL)
				outputPath := filepath.Join(outDir, name)

				if writeErr := os.WriteFile(outputPath, body, 0o644); writeErr == nil {
					fileWritten = outputPath
				}
			}
		} else {
			// We did get a response but failed to read the body; treat as error.
			statusCode = resp.StatusCode
		}
	}

	duration := time.Since(start).Milliseconds()

	logCh <- logRecord{
		URL:        rawURL,
		DurationMs: duration,
		DownloadSz: size,
		StatusCode: statusCode,
		File:       fileWritten,
	}

	// Consider any 2xx response a success.
	if statusCode >= 200 && statusCode < 300 {
		return true
	}
	// Treat errors or non-2xx status codes as failure.
	return false
}
