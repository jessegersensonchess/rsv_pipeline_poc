package skiplog

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// TestNewSkipStats_CreatesDirFileAndHeader verifies that NewSkipStats:
//  1. creates any missing parent directories,
//  2. creates the CSV file,
//  3. writes the fixed header row immediately.
func TestNewSkipStats_CreatesDirFileAndHeader(t *testing.T) {
	t.Parallel()

	// Arrange: a nested target path inside a temp directory.
	tmp := t.TempDir()
	target := filepath.Join(tmp, "skipped", "ownership.csv")

	// Act: construct a new skipStats and ensure cleanup is called at the end.
	ss, cleanup := NewSkipStats(target)
	defer cleanup()

	// Assert: directory exists.
	if _, err := os.Stat(filepath.Dir(target)); err != nil {
		t.Fatalf("parent dir should exist: %v", err)
	}
	// Assert: file exists.
	if _, err := os.Stat(target); err != nil {
		t.Fatalf("file should exist: %v", err)
	}

	// Flush/close and then re-open to verify content.
	cleanup()

	f, err := os.Open(target)
	if err != nil {
		t.Fatalf("open for read: %v", err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	rows, err := r.ReadAll()
	if err != nil {
		t.Fatalf("readall: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected exactly 1 row (header), got %d: %#v", len(rows), rows)
	}
	wantHeader := []string{"reason", "line_number", "pcv_field", "raw_line"}
	if !reflect.DeepEqual(rows[0], wantHeader) {
		t.Fatalf("header mismatch\ngot : %#v\nwant: %#v", rows[0], wantHeader)
	}

	// Sanity: internal state initialized correctly.
	if ss == nil || ss.reasons == nil || ss.w == nil {
		t.Fatalf("skipStats fields not initialized: %#v", ss)
	}
}

// TestSkipStats_Add_WritesRowsAndCounts ensures Add increments
// the per-reason counters and appends properly formatted CSV rows,
// including cases that need CSV quoting (commas in raw).
func TestSkipStats_Add_WritesRowsAndCounts(t *testing.T) {
	t.Parallel()

	// Arrange
	tmp := t.TempDir()
	target := filepath.Join(tmp, "skipped.csv")
	ss, cleanup := NewSkipStats(target)
	defer cleanup()

	// Act: add a few rows (with duplicates and with/without pcvField).
	type in struct {
		reason   string
		line     int
		pcvField string
		// raw includes commas to confirm csv. quoting.
		raw string
	}
	inputs := []in{
		{"parse_error", 2, "", `broken, row one`},
		{"field_parse_error", 3, "123", `bad "date", still row`},
		{"parse_error", 5, "456", `another,broken,row`},
	}
	for _, x := range inputs {
		ss.Add(x.reason, x.line, x.pcvField, x.raw)
	}

	// Flush and re-open to assert file contents.
	cleanup()

	f, err := os.Open(target)
	if err != nil {
		t.Fatalf("open for read: %v", err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	rows, err := r.ReadAll()
	if err != nil {
		t.Fatalf("readall: %v", err)
	}

	// Assert: header + 3 data rows.
	if len(rows) != 1+len(inputs) {
		t.Fatalf("want %d rows, got %d: %#v", 1+len(inputs), len(rows), rows)
	}
	header := rows[0]
	wantHeader := []string{"reason", "line_number", "pcv_field", "raw_line"}
	if !reflect.DeepEqual(header, wantHeader) {
		t.Fatalf("header mismatch\ngot : %#v\nwant: %#v", header, wantHeader)
	}

	// Assert each appended row matches the inputs after CSV parsing.
	for i, x := range inputs {
		got := rows[i+1]
		want := []string{x.reason, strconvItoa(x.line), x.pcvField, x.raw}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("row %d mismatch\ngot : %#v\nwant: %#v", i, got, want)
		}
	}

	// Assert counters were accumulated correctly.
	if ss.reasons["parse_error"] != 2 {
		t.Fatalf("parse_error count=%d want 2", ss.reasons["parse_error"])
	}
	if ss.reasons["field_parse_error"] != 1 {
		t.Fatalf("field_parse_error count=%d want 1", ss.reasons["field_parse_error"])
	}
	// No other reasons should be present.
	if len(ss.reasons) != 2 {
		t.Fatalf("unexpected reasons map: %#v", ss.reasons)
	}
}

// TestSkipStats_Add_EmptyValuesAreAccepted verifies that Add tolerates empty
// strings (pcvField/raw) and still writes valid CSV rows.
func TestSkipStats_Add_EmptyValuesAreAccepted(t *testing.T) {
	t.Parallel()

	// Arrange
	tmp := t.TempDir()
	target := filepath.Join(tmp, "empty.csv")
	ss, cleanup := NewSkipStats(target)
	defer cleanup()

	// Act
	ss.Add("empty_case", 42, "", "")
	cleanup()

	// Assert
	f, err := os.Open(target)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	rows, err := r.ReadAll()
	if err != nil {
		t.Fatalf("readall: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("want 2 rows (header + 1), got %d: %#v", len(rows), rows)
	}
	want := []string{"empty_case", "42", "", ""}
	if !reflect.DeepEqual(rows[1], want) {
		t.Fatalf("row mismatch\ngot : %#v\nwant: %#v", rows[1], want)
	}
	if ss.reasons["empty_case"] != 1 {
		t.Fatalf("counter mismatch: %+v", ss.reasons)
	}
}

// strconvItoa is a tiny local helper to make intent explicit in assertions.
// We could import strconv, but this keeps the test surface minimal.
func strconvItoa(n int) string {
	return fmt.Sprintf("%d", n)
}
