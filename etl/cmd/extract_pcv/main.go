// Package main implements a command-line tool for extracting unique PCV
// identifiers from a CSV dataset.
//
// The tool performs a single-pass scan over a CSV input, which may be sourced
// from a local file or from a remote HTTP endpoint. For each record, the tool:
//
//  1. Requires that the record contains at least five fields.
//  2. Interprets the first field as a PCV identifier and the fifth field as
//     an ICO identifier.
//  3. Requires both PCV and ICO to be syntactically valid integers.
//  4. Writes each distinct PCV identifier to the output exactly once.
//
// Membership checks for previously seen PCV identifiers are implemented via a
// bitmap-backed set, which allows O(1) membership tests with a compact memory
// footprint. The bitmap is sized by a configurable maximum ID, passed via
// the -maxid flag.
package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
)

// fetchCSVData issues an HTTP GET request to the provided URL and returns
// the response body as an io.ReadCloser.
//
// The caller is responsible for closing the returned ReadCloser.
//
// A non-200 HTTP status is treated as an error and results in a non-nil
// error being returned.
//
// This function is intentionally small and side-effect free to facilitate
// testing with net/http/httptest.
func fetchCSVData(url string) (io.ReadCloser, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		// Ensure the body is closed on error paths.
		defer resp.Body.Close()
		return nil, fmt.Errorf("failed to fetch data: %s", resp.Status)
	}
	return resp.Body, nil
}

// processCSV performs a single-pass scan over CSV data and writes the first
// occurrence of each unique PCV identifier to out.
//
// The CSV reader is constructed on top of r and configured to be tolerant of
// certain common irregularities (e.g., varying field counts, lazy quotes).
//
// Input semantics:
//
//   - Each record must contain at least 5 fields.
//   - record[0] (PCV) is trimmed of leading/trailing ASCII whitespace when
//     necessary and must parse as an int.
//   - record[4] (ICO) is similarly handled and must parse as an int.
//   - Only records for which both PCV and ICO are valid integers are eligible
//     for output.
//   - For each distinct PCV integer, only the first occurrence is written to
//     out; subsequent occurrences are filtered via bitmap.
//
// The function returns:
//   - recordsProcessed: number of CSV records read (including skipped ones),
//   - recordsWritten: number of unique PCV IDs written to out,
//   - malformedRecords: number of records that were skipped due to malformed
//     CSV or invalid field content,
//   - err: any fatal error encountered while processing or writing.
//
// No logging or printing is performed inside this function; the caller is
// responsible for interpreting the returned statistics and error.
func processCSV(r io.Reader, bitmap *Bitmap, out io.Writer) (recordsProcessed int, recordsWritten int, malformedRecords int, err error) {
	// Construct a CSV reader on top of r. The caller is expected to provide
	// an appropriately buffered reader if input performance is critical.
	csvReader := csv.NewReader(r)
	csvReader.LazyQuotes = true
	csvReader.FieldsPerRecord = -1 // Allow a variable number of fields.
	csvReader.ReuseRecord = true   // Reduce allocations by reusing the backing slice.

	for {
		record, readErr := csvReader.Read()
		if readErr == io.EOF {
			// Normal termination condition: all records consumed.
			break
		}
		if readErr != nil {
			// Treat CSV parse errors as non-fatal: count them and continue.
			malformedRecords++
			continue
		}

		recordsProcessed++

		// Require at least five fields before attempting to access indices.
		if len(record) < 5 {
			malformedRecords++
			continue
		}

		// Extract and normalize the ICO field (record[4]).
		icoField := record[4]
		if HasEdgeSpace(icoField) {
			icoField = strings.TrimSpace(icoField)
		}
		if icoField == "" {
			malformedRecords++
			continue
		}
		if _, parseErr := strconv.Atoi(icoField); parseErr != nil {
			// ICO must be a valid integer, even if not currently used for
			// bitmap membership. This enforces input quality.
			malformedRecords++
			continue
		}

		// Extract and normalize the PCV field (record[0]).
		pcvField := record[0]
		if HasEdgeSpace(pcvField) {
			pcvField = strings.TrimSpace(pcvField)
		}
		if pcvField == "" {
			malformedRecords++
			continue
		}
		pcvID, parseErr := strconv.Atoi(pcvField)
		if parseErr != nil {
			malformedRecords++
			continue
		}

		// Check bitmap membership on the PCV ID. IDs beyond the bitmap's
		// configured range are safely ignored by Has/Add.
		if bitmap.Has(pcvID) {
			// Already seen: no output for duplicate PCVs.
			continue
		}
		bitmap.Add(pcvID)

		// Emit the normalized PCV identifier, one per line. The caller is
		// expected to pass a buffered writer if disk throughput matters.
		if _, writeErr := io.WriteString(out, pcvField+"\n"); writeErr != nil {
			err = fmt.Errorf("write output: %w", writeErr)
			return
		}

		recordsWritten++
	}

	return
}

// HasEdgeSpace reports whether s starts or ends with common ASCII whitespace.
//
// The function is intentionally implemented using direct byte comparisons on
// the first and last bytes of the string in order to avoid scanning the entire
// string when checking for leading or trailing whitespace. This makes it a
// cheap precondition check before calling strings.TrimSpace.
//
// HasEdgeSpace does not consider non-ASCII whitespace; for those cases callers
// should fall back to strings.TrimSpace directly.
func HasEdgeSpace(s string) bool {
	n := len(s)
	if n == 0 {
		return false
	}
	b0, b1 := s[0], s[n-1]
	return b0 == ' ' || b0 == '\t' || b0 == '\n' || b0 == '\r' ||
		b1 == ' ' || b1 == '\t' || b1 == '\n' || b1 == '\r'
}

// main is the entry point for the extract_pcv tool.
//
// It is responsible for:
//   - Parsing command-line flags.
//   - Constructing the Bitmap with the requested maximum ID capacity.
//   - Opening an input source (file or URL).
//   - Opening and buffering the output file.
//   - Invoking processCSV with appropriate reader and writer abstractions.
//   - Printing high-level processing statistics.
//
// The core processing logic (parsing and filtering CSV) is delegated to
// processCSV to keep main small and testable.
func main() {
	// Define flags for input, output, and bitmap capacity.
	fileFlag := flag.String("file", "", "CSV file to process, example RSV_vlastnik_provozovatel_vozidla.csv")
	urlFlag := flag.String("url", "", "URL to fetch CSV data from")
	outputFlag := flag.String("output", "", "File to write the results to")
	maxIDFlag := flag.Int("maxid", 150_000_000, "Maximum ID value for the bitmap")
	flag.Parse()

	// Validate required flags. Exactly one of -file or -url must be supplied,
	// and -output must be non-empty.
	if (*fileFlag == "" && *urlFlag == "") || *outputFlag == "" {
		fmt.Println("Please provide either a CSV file using -file or a URL using -url, and an output file using -output.")
		flag.PrintDefaults()
		return
	}

	// Construct the bitmap used to track which PCV IDs have already been
	// observed. The bitmap capacity is determined by the -maxid flag.
	bitmap := NewBitmap(*maxIDFlag)

	// Set up the input reader. For files we open the path directly; for URLs
	// we delegate to fetchCSVData and ensure the HTTP body is closed.
	var (
		inputReader io.Reader
		closeFunc   func() error // optional closer for input source
	)

	switch {
	case *fileFlag != "":
		file, err := os.Open(*fileFlag)
		if err != nil {
			fmt.Printf("Error opening file %q: %v\n", *fileFlag, err)
			return
		}
		// Ensure the file is closed on exit.
		closeFunc = file.Close
		// Wrap the file in a buffered reader for better throughput.
		inputReader = bufio.NewReaderSize(file, 256*1024)

	case *urlFlag != "":
		body, err := fetchCSVData(*urlFlag)
		if err != nil {
			fmt.Printf("Error fetching data from URL %q: %v\n", *urlFlag, err)
			return
		}
		// Ensure the HTTP response body is closed on exit.
		closeFunc = body.Close
		// Wrap the body in a buffered reader; HTTP bodies may be slow streams.
		inputReader = bufio.NewReaderSize(body, 256*1024)
	}

	if closeFunc != nil {
		defer closeFunc()
	}

	// Open the output file and wrap it in a buffered writer. The writer is
	// passed into processCSV so that all formatting and flushing is under
	// the caller's control.
	outFile, err := os.Create(*outputFlag)
	if err != nil {
		fmt.Printf("Error creating output file %q: %v\n", *outputFlag, err)
		return
	}
	defer outFile.Close()

	outWriter := bufio.NewWriterSize(outFile, 256*1024)
	defer outWriter.Flush()

	// Execute the core CSV processing pipeline.
	recordsProcessed, recordsWritten, malformedRecords, err := processCSV(inputReader, bitmap, outWriter)
	if err != nil {
		fmt.Printf("Error processing CSV: %v\n", err)
		return
	}

	// Print a concise summary of the run. This is intentionally done in main
	// so that processCSV remains free of side effects and easy to test.
	fmt.Printf("Total records processed:   %d\n", recordsProcessed)
	fmt.Printf("Total unique PCVs written: %d\n", recordsWritten)
	fmt.Printf("Malformed or skipped rows: %d\n", malformedRecords)
	fmt.Printf("Results have been written to %s\n", *outputFlag)
}
