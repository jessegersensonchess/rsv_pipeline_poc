// Copyright 2025
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
//
// process_test.go contains unit tests and benchmarks for the CSV processing
// pipeline implemented in main.go. The tests focus on processCSV, which
// performs a single-pass scan over CSV records, applies filtering rules, and
// writes unique PCV identifiers to an abstract io.Writer.
//
// The tests in this file intentionally treat processCSV as a black box in
// order to validate the observable behavior of the pipeline, rather than
// tightly coupling to implementation details such as specific buffering or
// parsing strategies.
package main

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
)

/*
TestProcessCSV verifies the behavior of processCSV using a table-driven set
of scenarios.

Each subtest constructs an in-memory CSV payload, runs processCSV against that
payload with a fresh Bitmap instance and a bytes.Buffer as the output writer,
and then compares the buffer contents to the expected PCV IDs.

High-level expectations:

  - Only rows with at least 5 fields are considered for output.
  - The fifth field ("ico") must be a syntactically valid integer.
  - The first field ("pcv") must be a syntactically valid integer.
  - Whitespace at the edges of the pcv/ico fields is trimmed when present.
  - The first occurrence of each pcv integer is written once; subsequent
    occurrences of the same pcv are filtered using the Bitmap.
  - Malformed records are counted but do not terminate processing.

The tests cover common edge cases: invalid integers, missing fields, values
with surrounding whitespace, and IDs outside the bitmap range.

Note: Some expectations (such as the exact count of recordsProcessed for
blank lines) depend on encoding/csv internals; where that behavior is not
critical to the tool's semantics, the test uses -1 in wantProcessed to mean
"do not assert this field".
*/
func TestProcessCSV(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		input            string // CSV records, each line one record
		wantOut          string // Expected newline-terminated list of pcv IDs
		maxID            int    // Bitmap capacity for this test
		wantProcessed    int    // -1 means "do not assert"
		wantWritten      int
		wantMalformedMin int // minimum number of malformed/skipped rows expected
	}{
		{
			name: "basic_unique_first_occurrence",
			input: "" +
				"100,foo,bar,baz,200\n" +
				"101,x,y,z,300\n" +
				"102,a,b,c,400\n",
			wantOut:          "100\n101\n102\n",
			maxID:            1000,
			wantProcessed:    3,
			wantWritten:      3,
			wantMalformedMin: 0,
		},
		{
			name: "duplicate_pcv_suppressed",
			input: "" +
				"100,a,b,c,200\n" +
				"100,x,y,z,300\n" +
				"100,x,y,z,400\n",
			wantOut:          "100\n",
			maxID:            1000,
			wantProcessed:    3,
			wantWritten:      1,
			wantMalformedMin: 0,
		},
		{
			name: "invalid_ico_skipped",
			input: "" +
				"100,a,b,c,notint\n" + // invalid ICO; row is malformed
				"101,a,b,c,200\n", // valid ICO; row is kept
			wantOut:          "101\n",
			maxID:            1000,
			wantProcessed:    2,
			wantWritten:      1,
			wantMalformedMin: 1,
		},
		{
			name: "invalid_pcv_skipped",
			input: "" +
				"xxx,a,b,c,200\n" + // pcv is not an integer; malformed
				"200,a,b,c,300\n", // valid pcv; kept
			wantOut:          "200\n",
			maxID:            1000,
			wantProcessed:    2,
			wantWritten:      1,
			wantMalformedMin: 1,
		},
		{
			name: "edge_spaces_trimmed",
			input: "" +
				" 100 ,a,b,c, 200 \n" +
				" 101 ,a,b,c, 201 \n",
			// Leading/trailing spaces around pcv and ico are trimmed.
			wantOut:          "100\n101\n",
			maxID:            1000,
			wantProcessed:    2,
			wantWritten:      2,
			wantMalformedMin: 0,
		},
		{
			name: "records_with_less_than_5_fields_skipped",
			input: "" +
				"100,a,b,c\n" + // only 4 fields; malformed
				"101,a,b,c,300\n", // valid 5-field record
			wantOut:          "101\n",
			maxID:            1000,
			wantProcessed:    2,
			wantWritten:      1,
			wantMalformedMin: 1,
		},
		{
			name: "empty_lines_and_blank_pcv_ignored",
			input: "" +
				"\n" + // blank line; behavior depends on encoding/csv
				",a,b,c,200\n" + // blank pcv; malformed
				"300,a,b,c,200\n", // valid pcv + ico
			wantOut:          "300\n",
			maxID:            1000,
			wantProcessed:    -1, // Do not assert exact value; CSV may treat blank line specially.
			wantWritten:      1,
			wantMalformedMin: 1, // At least the blank pcv line is malformed.
		},
		{
			name: "pcv_outside_bitmap_range_still_written",
			input: "" +
				"1000,a,b,c,200\n" + // pcv=1000 larger than bitmap capacity
				"10,a,b,c,201\n", // pcv=10 is within bitmap
			// Bitmap.Has/Add safely ignore out-of-range bits internally, but
			// processCSV treats the first occurrence of any syntactically valid
			// PCV as unique. Therefore both 1000 and 10 are written.
			wantOut:          "1000\n10\n",
			maxID:            100, // deliberately small bitmap
			wantProcessed:    2,
			wantWritten:      2,
			wantMalformedMin: 0,
		},
	}

	for _, tt := range tests {
		tt := tt // capture range variable
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bitmap := NewBitmap(tt.maxID)

			var out bytes.Buffer
			reader := strings.NewReader(tt.input)

			gotProcessed, gotWritten, gotMalformed, err := processCSV(reader, bitmap, &out)
			if err != nil {
				t.Fatalf("processCSV error: %v", err)
			}

			gotOut := out.String()
			if gotOut != tt.wantOut {
				t.Errorf("unexpected output:\n  got:  %q\n  want: %q", gotOut, tt.wantOut)
			}

			// Only assert recordsProcessed if the test provided a concrete value.
			if tt.wantProcessed >= 0 && gotProcessed != tt.wantProcessed {
				t.Errorf("recordsProcessed = %d, want %d", gotProcessed, tt.wantProcessed)
			}
			if gotWritten != tt.wantWritten {
				t.Errorf("recordsWritten = %d, want %d", gotWritten, tt.wantWritten)
			}
			if gotMalformed < tt.wantMalformedMin {
				t.Errorf("malformedRecords = %d, want at least %d", gotMalformed, tt.wantMalformedMin)
			}
		})
	}
}

/*
BenchmarkProcessCSV measures the throughput of processCSV over a synthetic
CSV workload.

The benchmark constructs an in-memory CSV document containing a fixed number
of rows and repeatedly processes it into a bytes.Buffer. The goal is to
approximate end-to-end pipeline performance (parsing, bitmap lookups,
and writing) rather than isolate individual micro-operations.

The benchmark intentionally uses a reasonably sized dataset (e.g., 50k rows)
to better approximate real-world behavior without making the benchmark
unnecessarily slow.
*/
func BenchmarkProcessCSV(b *testing.B) {
	// Generate a synthetic CSV payload in memory. Each record uses an
	// incrementing pcv and a deterministic ico so that the first iteration
	// of processCSV will produce a maximal amount of output.
	var buf bytes.Buffer
	const numRows = 50_000
	for i := 0; i < numRows; i++ {
		pcv := strconv.Itoa(i)
		ico := strconv.Itoa(100_000 + i)
		buf.WriteString(pcv + ",a,b,c," + ico + "\n")
	}

	csvData := buf.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Each iteration uses a fresh bitmap and output buffer so that the
		// processing behavior remains consistent across iterations.
		bitmap := NewBitmap(200_000)
		var out bytes.Buffer
		reader := bytes.NewReader(csvData)

		if _, _, _, err := processCSV(reader, bitmap, &out); err != nil {
			b.Fatalf("processCSV: %v", err)
		}
	}
}

func BenchmarkProcessCSVWithFile(b *testing.B) {
	// Prepare a temp input file with synthetic CSV.
	var buf bytes.Buffer
	const numRows = 50_000
	for i := 0; i < numRows; i++ {
		pcv := strconv.Itoa(i)
		ico := strconv.Itoa(100_000 + i)
		buf.WriteString(pcv + ",a,b,c," + ico + "\n")
	}

	inFile, err := os.CreateTemp("", "pcvbench-input-*")
	if err != nil {
		b.Fatalf("CreateTemp input: %v", err)
	}
	defer os.Remove(inFile.Name())
	if _, err := inFile.Write(buf.Bytes()); err != nil {
		b.Fatalf("write input: %v", err)
	}
	if _, err := inFile.Seek(0, io.SeekStart); err != nil {
		b.Fatalf("seek input: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Rewind input each iteration.
		if _, err := inFile.Seek(0, io.SeekStart); err != nil {
			b.Fatalf("seek input: %v", err)
		}
		inReader := bufio.NewReaderSize(inFile, 256*1024)

		// Output goes to /dev/null-like sink.
		var out bytes.Buffer
		outWriter := bufio.NewWriterSize(&out, 256*1024)

		bitmap := NewBitmap(200_000)
		if _, _, _, err := processCSV(inReader, bitmap, outWriter); err != nil {
			b.Fatalf("processCSV: %v", err)
		}
		outWriter.Flush()
	}
}
