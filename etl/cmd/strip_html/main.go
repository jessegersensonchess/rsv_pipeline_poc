// Command strip_html removes HTML markup from text files, normalizes whitespace,
// and extracts only the content between optional start and end tags.
//
// The tool is designed to be used as a preprocessing step in ETL pipelines
// where large, semi-structured HTML-like documents need to be converted into
// clean, plain-text segments.
//
// Processing model:
//
//   - Files are processed in a streaming, line-by-line fashion to minimize
//     memory usage and allow safe handling of very large files.
//   - Each line is transformed by:
//     1. Stripping HTML/XML tags
//     2. Collapsing repeated whitespace into a single space
//   - Output is buffered and written to a new file with a configurable suffix.
//
// Example usage:
//
//	strip_html \
//	  -dir ./data \
//	  -pattern "*CASTECNY_SM" \
//	  -start-tag "<body>" \
//	  -end-tag "</body>" \
//	  -suffix ".processed"
//
// This command intentionally performs best-effort processing and does not
// attempt to validate HTML correctness.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"etl/internal/parser/html"
)

// processFile streams a single file, removes HTML tags, collapses whitespace,
// and writes only the text found between the configured start and end tags.
//
// Processing behavior:
//
//   - The file is read line-by-line using a buffered reader to keep memory
//     usage bounded.
//   - Once a line containing startTag is observed, all subsequent cleaned
//     lines are written to the output.
//   - Processing stops permanently after a line containing endTag is observed.
//   - Output is written to a new file whose name is derived by appending
//     the supplied suffix to the input path.
//
// The function is intentionally conservative and fail-fast:
// any filesystem or IO error immediately terminates processing and
// returns an error to the caller.
//
// Parameters:
//   - path:     Filesystem path of the input file.
//   - startTag: Marker text that enables output once observed.
//   - endTag:   Marker text that terminates processing once observed.
//   - suffix:   String appended to the original filename to form the output path.
//
// Returns:
//   - error: A non-nil error if any IO or filesystem operation fails.
func processFile(path, startTag, endTag, suffix string) error {
	in, err := os.Open(path)
	if err != nil {
		return err
	}
	defer in.Close()

	outPath := path + suffix
	out, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer out.Close()

	r := bufio.NewReader(in)
	w := bufio.NewWriter(out)
	defer w.Flush()

	printing := false

	for {
		line, err := r.ReadString('\n')
		if err != nil && err != io.EOF {
			return err
		}

		// Normalize content using shared HTML utilities.
		line = html.StripHTML(line)
		line = html.CollapseWhitespace(line)

		if line != "" {
			// Detect the beginning of the extraction window.
			if strings.Contains(line, startTag) {
				printing = true
			}

			// Write only when we are inside the extraction window.
			if printing {
				if _, err := w.WriteString(line + "\n"); err != nil {
					return err
				}
			}

			// Detect the end of the extraction window.
			if strings.Contains(line, endTag) {
				break
			}
		}

		if err == io.EOF {
			break
		}
	}

	return nil
}

// main parses command-line flags, discovers matching files, and
// invokes processFile for each one.
//
// The program exits with a non-zero status code when flag validation fails
// or when fatal filesystem errors occur. Individual file processing errors
// are reported to stderr, but processing of other files continues.
func main() {
	dir := flag.String("dir", ".", "Directory containing files")
	startTag := flag.String("start-tag", "", "Tag marking start of extraction (required)")
	endTag := flag.String("end-tag", "", "Tag marking end of extraction (required)")
	pattern := flag.String("pattern", "*.html", "Glob pattern for selecting input files")
	suffix := flag.String("suffix", ".processed", "Suffix appended to output files")

	flag.Parse()

	if *startTag == "" {
		fmt.Fprintln(os.Stderr, "error: -start-tag is required")
		os.Exit(1)
	}
	if *endTag == "" {
		fmt.Fprintln(os.Stderr, "error: -end-tag is required")
		os.Exit(1)
	}

	matches, err := filepath.Glob(filepath.Join(*dir, *pattern))
	if err != nil {
		fmt.Fprintf(os.Stderr, "glob error: %v\n", err)
		os.Exit(1)
	}

	if len(matches) == 0 {
		fmt.Fprintln(os.Stderr, "no files matched pattern")
		return
	}

	for _, path := range matches {
		if err := processFile(path, *startTag, *endTag, *suffix); err != nil {
			fmt.Fprintf(os.Stderr, "error processing %s: %v\n", path, err)
		}
	}
}
