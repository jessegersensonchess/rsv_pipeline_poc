// Command csvprobe-web starts a tiny web UI for the CSV sampler/inspector.
//
// Usage:
//
//	go run ./cmd/csvprobe-web -addr :8080
package main

import (
	"flag"
	"io"
	"log"
	"os"

	"etl/internal/webui"
)

// server is the minimal behavior we need from the web UI server.
type server interface {
	ListenAndServe() error
}

// newServer wraps webui.NewServer so tests can replace it.
var newServer = func(cfg webui.Config) server { return webui.NewServer(cfg) }

// run parses flags, constructs the server, logs, and runs it.
// It returns the error from ListenAndServe so callers (tests) can assert it.
func run(args []string, logger *log.Logger) error {
	fs := flag.NewFlagSet("csvprobe-web", flag.ContinueOnError)
	fs.SetOutput(io.Discard) // silence usage/errors to stderr during tests
	addr := fs.String("addr", ":8080", "listen address")
	if err := fs.Parse(args); err != nil {
		return err
	}

	srv := newServer(webui.Config{Addr: *addr})
	logger.Printf("listening on %s", *addr)
	return srv.ListenAndServe()
}

func main() {
	logger := log.New(os.Stderr, "", log.LstdFlags)
	if err := run(os.Args[1:], logger); err != nil {
		log.Fatal(err)
	}
}
