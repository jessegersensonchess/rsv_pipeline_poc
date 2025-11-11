package main

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"strings"
	"testing"

	"etl/internal/webui"
)

// fakeServer is a tiny test double implementing the server interface.
type fakeServer struct {
	err error
}

func (f *fakeServer) ListenAndServe() error { return f.err }

// TestRun covers flag parsing, defaulting, logging, and error propagation.
// Table-driven to make intended behaviors explicit and easy to extend.
func TestRun(t *testing.T) {
	t.Parallel()

	type tc struct {
		name       string
		args       []string
		listenErr  error
		wantAddr   string
		wantLogHas string
		wantErr    error // if non-nil, we expect run to return a non-nil error (not necessarily equal)
	}

	cases := []tc{
		{
			name:       "default address",
			args:       nil,
			listenErr:  errors.New("boom"),
			wantAddr:   ":8080",
			wantLogHas: "listening on :8080",
			wantErr:    errors.New("any"),
		},
		{
			name:       "custom address via flag",
			args:       []string{"-addr", "127.0.0.1:9999"},
			listenErr:  nil,
			wantAddr:   "127.0.0.1:9999",
			wantLogHas: "listening on 127.0.0.1:9999",
			wantErr:    nil,
		},
		{
			name:       "unknown flag returns error",
			args:       []string{"-bogus"},
			listenErr:  nil,
			wantAddr:   "", // not reached
			wantLogHas: "", // not reached
			wantErr:    errors.New("any"),
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			var gotAddr string
			orig := newServer
			defer func() { newServer = orig }()

			newServer = func(cfg webui.Config) server {
				gotAddr = cfg.Addr
				return &fakeServer{err: c.listenErr}
			}

			var buf bytes.Buffer
			logger := log.New(&buf, "", 0)

			err := run(c.args, logger)

			// Assert address propagation when applicable.
			if c.wantAddr != "" && gotAddr != c.wantAddr {
				t.Fatalf("addr mismatch: got %q, want %q", gotAddr, c.wantAddr)
			}

			// Assert log message when applicable.
			if c.wantLogHas != "" && !strings.Contains(buf.String(), c.wantLogHas) {
				t.Fatalf("log output %q does not contain %q", buf.String(), c.wantLogHas)
			}

			// Assert error presence/absence.
			if (c.wantErr == nil) != (err == nil) {
				t.Fatalf("error presence mismatch: got %v, wantErrNil=%v", err, c.wantErr == nil)
			}
		})
	}
}

// Example_run documents the happy path behavior and provides a godoc-style example.
func Example_run() {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	// Swap in a no-op server.
	orig := newServer
	newServer = func(cfg webui.Config) server { return &fakeServer{err: nil} }
	defer func() { newServer = orig }()

	_ = run([]string{"-addr", ":9090"}, logger)

	// Examples compare what is written to stdout with the Output: block,
	// so print the buffered log line.
	fmt.Print(buf.String())

	// Output:
	// listening on :9090
}

// BenchmarkRun exercises the flag parse + logger + no-op server path.
// These are micro-benchmarks (CLI startup path), not HTTP throughput.
func BenchmarkRun_NoFlags(b *testing.B) {
	benchRun(b, nil)
}

func BenchmarkRun_WithAddr(b *testing.B) {
	benchRun(b, []string{"-addr", "127.0.0.1:0"})
}

func benchRun(b *testing.B, args []string) {
	orig := newServer
	newServer = func(cfg webui.Config) server { return &fakeServer{err: nil} }
	defer func() { newServer = orig }()

	// Discard output to avoid lock contention in logging during the bench.
	logger := log.New(&bytes.Buffer{}, "", 0)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := run(args, logger); err != nil {
			b.Fatal(err)
		}
	}
}
