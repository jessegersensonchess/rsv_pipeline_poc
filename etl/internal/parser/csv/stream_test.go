package csv

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"etl/internal/config"
)

// TestStreamCSV_Basic verifies that StreamCSV reads a header and emits width-
// aligned rows, and that a malformed-width row is reported via onError without
// aborting the stream.
func TestStreamCSV_Basic(t *testing.T) {
	t.Parallel()

	// Arrange: temp CSV file with one malformed row.
	dir := t.TempDir()
	path := filepath.Join(dir, "basic.csv")
	content := "a,b,c\n1,2,3\nx,y\n4,5,6\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write temp csv: %v", err)
	}

	spec := config.Pipeline{
		Source: config.Source{
			Kind: "file",
			File: config.SourceFile{Path: path},
		},
		Parser: config.Parser{
			Kind: "csv",
			Options: config.Options{
				"has_header": true,
				"comma":      ",",
				"trim_space": true,
			},
		},
	}

	ctx := context.Background()
	out := make(chan []string, 4)

	var errors []string
	onErr := func(line int, err error) {
		errors = append(errors, err.Error())
	}

	if err := StreamCSV(ctx, spec, out, onErr); err != nil {
		t.Fatalf("StreamCSV: %v", err)
	}
	close(out)

	var rows [][]string
	for r := range out {
		rows = append(rows, r)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	if got, want := strings.Join(rows[0], ","), "1,2,3"; got != want {
		t.Errorf("row0 = %q, want %q", got, want)
	}
	if got, want := strings.Join(rows[1], ","), "4,5,6"; got != want {
		t.Errorf("row1 = %q, want %q", got, want)
	}

	// One width error should have been reported.
	found := false
	for _, e := range errors {
		if strings.Contains(e, "incorrect number of fields") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected incorrect number of fields error; got %v", errors)
	}
}

// TestStreamCSV_Scrub ensures that enabling stream_scrub_likvidaci fixes the
// broken sequence and allows the CSV to parse successfully.
func TestStreamCSV_Scrub(t *testing.T) {
	t.Parallel()

	// Arrange: a CSV row with the broken sequence.
	dir := t.TempDir()
	path := filepath.Join(dir, "scrub.csv")
	// Embedded unescaped quote before 'v likvidaci'—our scrubber fixes it.
	// Note: Keep the exact ` "v likvidaci` pattern to trigger replacement.
	content := "a,b\n1,\"text text text \"v likvidaci\"\"\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write temp csv: %v", err)
	}

	spec := config.Pipeline{
		Source: config.Source{Kind: "file", File: config.SourceFile{Path: path}},
		Parser: config.Parser{
			Kind: "csv",
			Options: config.Options{
				"has_header":             true,
				"stream_scrub_likvidaci": true,
				"comma":                  ",",
				"trim_space":             true,
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	out := make(chan []string, 2)
	var errs []string
	onErr := func(_ int, err error) { errs = append(errs, err.Error()) }

	if err := StreamCSV(ctx, spec, out, onErr); err != nil {
		t.Fatalf("StreamCSV: %v", err)
	}
	close(out)

	var rows [][]string
	for r := range out {
		rows = append(rows, r)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	if rows[0][0] != "1" {
		t.Errorf("first field = %q, want 1", rows[0][0])
	}
	if len(errs) != 0 {
		t.Errorf("unexpected per-row errors: %v", errs)
	}
}
