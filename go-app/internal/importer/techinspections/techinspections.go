// Package techinspections provides an importer for the "technicky_prohlidky"
// CSV dataset. It parses rows and writes them to the database using the
// application's SmallDB bulk-copy interface.
//
// Concurrency model (mirrors the ownership importer):
//   - One producer goroutine streams parsed CSV records (after the header)
//     using an in-memory resynchronizer over physical lines (no temp files).
//   - N worker goroutines validate/convert fields, batch, and COPY/INSERT.
//   - Each worker writes its own "skipped" CSV with reasons.
//
// Why in-memory resynchronization instead of a plain csv.Reader?
//
//	Real-world files sometimes contain malformed quoted fields that merge multiple
//	logical rows. The producer below reads physical lines and accumulates them
//	until a tolerant CSV parse yields exactly 9 fields. This recovers rows without
//	touching disk and keeps memory use bounded. It also correctly handles inputs
//	like ""ADMINISTRATIVNÍ …"" (literal quotes) and embedded CRLF inside quoted fields.
package techinspections

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"csvloader/internal/config"
	"csvloader/internal/db"
)

// Constants describing the CSV format and I/O behavior.
const (
	expectedColumns   = 9            // Number of columns required by the CSV.
	dateLayout        = "02.01.2006" // CZ date format, e.g., 28.12.2011.
	readBufferBytes   = 4 << 20      // 4 MiB line buffer for robust CSV reads.
	skippedDir        = "skipped"    // Directory for per-worker skipped rows.
	skippedFileFormat = "skipped_techinspections_w%d.csv"
)

// misquotedFieldRE matches fields of the form ,""TEXT"" where TEXT has no inner
// quotes/commas/newlines. It captures the trailing delimiter (comma or EOL)
// so we can reinsert it in the replacement.
// Example:  ,""ADMINISTRATIVNÍ OMEZENÍ - NOVÁ VOZIDLA"",
// becomes:  ,"ADMINISTRATIVNÍ OMEZENÍ - NOVÁ VOZIDLA",
var misquotedFieldRE = regexp.MustCompile(`,""([^\"",\r\n]+)""([,]|$)`)

// normalizeMisquotedFields fixes importer-specific malformed fields:
// ,""TEXT""  ->  ,"TEXT"
// Safety: only rewrites when TEXT contains no inner quotes, commas, or newlines.
func normalizeMisquotedFields(rec string) string {
	return misquotedFieldRE.ReplaceAllString(rec, `,"$1"$2`)
}

// job is one parsed CSV record with its 1-based line number (including header).
// rawCSV is a faithful CSV-encoded representation of the fields for audit logs.
type job struct {
	fields  []string
	lineNum int
	rawCSV  string
}

// workerResult aggregates the outcome of a single worker.
type workerResult struct {
	inserted int
	skipped  int
	seen     int            // number of jobs this worker consumed (good + skipped)
	reasons  map[string]int // reason -> count
	err      error
}

// inOpenQuotedField reports whether s ends with an unclosed quoted field,
// accounting for doubled quotes ("") inside quoted text.
// It toggles state on a single " and ignores a doubled "" sequence.
//
// Example:
//
//	inOpenQuotedField(`a,"b`)        -> true
//	inOpenQuotedField(`a,"b""c"`)    -> false
func inOpenQuotedField(s string) bool {
	in := false
	for i := 0; i < len(s); i++ {
		if s[i] != '"' {
			continue
		}
		// Doubled quote inside a quoted field -> skip both characters.
		if i+1 < len(s) && s[i+1] == '"' {
			i++
			continue
		}
		in = !in
	}
	return in
}

// ImportTechInspectionsParallel imports the "technicky_prohlidky" CSV at path
// using cfg.Workers parallel workers and cfg.BatchSize row batches for DB COPY.
//
// It ensures the destination table exists by calling
// SmallDB.CreateTechInspectionsTable, then processes the CSV:
//
//   - Header row is consumed (BOM stripped).
//   - The producer resynchronizes malformed records in-memory without temp files,
//     and applies an importer-specific scrubber for fields like ""TEXT"".
//   - Each worker writes a CSV of skipped rows to "skipped/skipped_techinspections_w{N}.csv"
//     with columns: reason,line_number,pcv_field,raw_line.
//
// Returns the first non-nil error encountered by any worker, or nil on success.
func ImportTechInspectionsParallel(ctx context.Context, cfg *config.Config, smallFactory db.SmallDBFactory, path string) error {
	start := time.Now() // track total import duration

	// Ensure target table exists before any worker starts.
	ctrl, err := smallFactory(ctx)
	if err != nil {
		return fmt.Errorf("tech_inspections: open small db (ensure table): %w", err)
	}
	if err := ctrl.CreateTechInspectionsTable(ctx); err != nil {
		_ = ctrl.Close(ctx)
		return fmt.Errorf("tech_inspections: create table: %w", err)
	}
	_ = ctrl.Close(ctx)

	// Open file for streaming.
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("tech_inspections: open csv: %w", err)
	}
	defer f.Close()
	br := bufio.NewReaderSize(f, readBufferBytes)

	// Read physical header line; strip BOM on first cell.
	headerLine, err := readPhysicalLine(br)
	if err != nil && err != io.EOF {
		return fmt.Errorf("tech_inspections: read header: %w", err)
	}
	if headerLine == "" {
		log.Printf("tech_inspections: empty csv (no header)")
		return nil
	}
	hdr := strings.Split(headerLine, ",")
	if len(hdr) > 0 {
		hdr[0] = trimBOM(hdr[0])
	}

	// Producer goroutine: resynchronize records from physical lines and emit jobs.
	jobs := make(chan job, 32768)

	// Instrumentation for debugging visibility.
	var (
		produced     int64 // successfully emitted logical records
		repairWarns  int64 // forced boundaries / unrecoverable fragments
		lineNum      = 1   // physical line counter; header is line 1
		currentStart = 2   // current logical record starting at this physical line
	)

	go func() {
		defer close(jobs)

		var buf strings.Builder

		appendLine := func(s string) {
			if buf.Len() > 0 {
				buf.WriteString("\r\n") // preserve logical separation for audit
			}
			buf.WriteString(s)
		}

		// Attempt to parse buffer into exactly 9 fields via tolerant CSV reader.
		parseToNine := func(rec string) ([]string, bool) {
			cr := csv.NewReader(strings.NewReader(rec))
			cr.Comma = ','
			cr.FieldsPerRecord = -1
			cr.LazyQuotes = true
			fields, err := cr.Read()
			if err != nil || len(fields) != expectedColumns {
				return nil, false
			}
			return fields, true
		}

		flushCurrent := func(force bool) {
			if buf.Len() == 0 {
				return
			}
			rec := buf.String()

			// First try as-is.
			if fields, ok := parseToNine(rec); ok {
				jobs <- job{fields: copyStrings(fields), lineNum: currentStart, rawCSV: encodeCSV(fields)}
				atomic.AddInt64(&produced, 1)
				buf.Reset()
				return
			}

			// Importer-specific scrub: normalize fields of the form ,""TEXT""
			// (no inner quotes/commas/newlines) into ,"TEXT" before parsing again.
			recFixed := normalizeMisquotedFields(rec)
			if recFixed != rec {
				if fields, ok := parseToNine(recFixed); ok {
					jobs <- job{fields: copyStrings(fields), lineNum: currentStart, rawCSV: encodeCSV(fields)}
					atomic.AddInt64(&produced, 1)
					buf.Reset()
					return
				}
			}

			if force {
				// Best-effort repair failed: log and drop the fragment.
				atomic.AddInt64(&repairWarns, 1)
				log.Printf("tech_inspections: repair drop; cannot parse record starting at physical line %d", currentStart)
				buf.Reset()
			}
		}

		for {
			line, rerr := readPhysicalLine(br)
			if rerr == io.EOF {
				// finalize last buffered record
				flushCurrent(true)
				break
			}
			if rerr != nil {
				// I/O error: finalize what we have and return
				flushCurrent(true)
				log.Printf("tech_inspections: read error at ~line %d: %v", lineNum+1, rerr)
				break
			}
			lineNum++

			// If the current buffer ends inside an open quoted field, the next line
			// MUST be treated as a continuation even if it "looks like" a record start.
			inQuoted := inOpenQuotedField(buf.String())
			startsNew := !inQuoted && looksLikeRecordStart(line)

			if buf.Len() == 0 {
				// Start a new potential record.
				currentStart = lineNum
				appendLine(line)
				// Try to emit immediately.
				flushCurrent(false)
				continue
			}

			// We already have a buffer; decide whether to append or force a cut.
			if startsNew {
				// If we hit a new probable record start but the current buffer
				// still doesn't parse to 9 fields even after normalization, force a boundary.
				before := buf.String()
				if _, ok := parseToNine(before); !ok {
					if _, ok2 := parseToNine(normalizeMisquotedFields(before)); !ok2 {
						atomic.AddInt64(&repairWarns, 1)
						log.Printf("tech_inspections: repair forced boundary before physical line %d (prev start at %d)", lineNum, currentStart)
						flushCurrent(true)
						currentStart = lineNum
						appendLine(line)
						// Try to emit immediately.
						flushCurrent(false)
						continue
					}
				}
				// Current buffer parses; emit it and start a new one.
				flushCurrent(false)
				currentStart = lineNum
				appendLine(line)
				flushCurrent(false)
				continue
			}

			// Continuation of the current record.
			appendLine(line)
			// Try to emit if it became valid.
			flushCurrent(false)
		}
	}()

	workers := cfg.Workers
	if workers <= 0 {
		workers = 1
	}
	results := make(chan workerResult, workers)

	// Start N workers.
	for i := 0; i < workers; i++ {
		go runWorker(ctx, i+1, cfg, smallFactory, jobs, results)
	}

	// Collect results.
	var (
		totalInserted int
		totalSkipped  int
		totalSeen     int
		firstErr      error
		reasonAgg     = map[string]int{}
	)
	for i := 0; i < workers; i++ {
		r := <-results
		totalInserted += r.inserted
		totalSkipped += r.skipped
		totalSeen += r.seen
		if r.err != nil && firstErr == nil {
			firstErr = r.err
		}
		for k, v := range r.reasons {
			reasonAgg[k] += v
		}
	}

	if firstErr != nil {
		return firstErr
	}

	duration := time.Since(start)

	processingRate := float64(totalInserted) / duration.Seconds()

	log.Printf(
		"tech_inspections: parallel=%d produced=%d seen=%d inserted=%d skipped=%d repair_warnings=%d reasons=%v, duration=%s, rate_per_second=%.0f",
		workers,
		atomic.LoadInt64(&produced),
		totalSeen,
		totalInserted,
		totalSkipped,
		atomic.LoadInt64(&repairWarns),
		reasonAgg,
		duration.Round(time.Millisecond),
		processingRate,
	)
	return nil
}

// runWorker consumes jobs, parses rows, batches them, and copies batches
// into the database. Each worker writes its own "skipped" report CSV.
//
// The function sends a workerResult on completion (success or failure).
func runWorker(
	ctx context.Context,
	id int,
	cfg *config.Config,
	smallFactory db.SmallDBFactory,
	jobs <-chan job,
	results chan<- workerResult,
) {
	res := workerResult{reasons: make(map[string]int)}
	defer func() { results <- res }()

	sdb, err := smallFactory(ctx)
	if err != nil {
		res.err = fmt.Errorf("worker %d: connect small db: %w", id, err)
		return
	}
	defer sdb.Close(ctx)

	if err := os.MkdirAll(skippedDir, 0o755); err != nil {
		res.err = fmt.Errorf("worker %d: ensure skipped dir: %w", id, err)
		return
	}

	skippedPath := filepath.Join(skippedDir, fmt.Sprintf(skippedFileFormat, id))
	skf, err := os.Create(skippedPath)
	if err != nil {
		res.err = fmt.Errorf("worker %d: create skipped file: %w", id, err)
		return
	}
	defer skf.Close()

	skw := csv.NewWriter(skf)
	defer skw.Flush()

	// Header for skipped rows file.
	_ = skw.Write([]string{"reason", "line_number", "pcv_field", "raw_line"})

	addSkip := func(reason string, ln int, pcvField, raw string) {
		res.reasons[reason]++
		res.skipped++
		_ = skw.Write([]string{reason, strconv.Itoa(ln), pcvField, raw})
	}

	batch := make([][]interface{}, 0, cfg.BatchSize)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := sdb.CopyTechInspections(ctx, batch); err != nil {
			return err
		}
		res.inserted += len(batch)
		batch = batch[:0]
		log.Printf("tech_inspections[w%d]: inserted=%d skipped=%d", id, res.inserted, res.skipped)
		return nil
	}

	for j := range jobs {
		res.seen++

		fields := j.fields
		if len(fields) != expectedColumns {
			addSkip("column_mismatch", j.lineNum, "", j.rawCSV)
			continue
		}

		row, err := parseRow(fields)
		if err != nil {
			// fields[0] is PČV; safe to include as a hint in the skipped file.
			pcvField := ""
			if len(fields) > 0 {
				pcvField = fields[0]
			}
			addSkip("field_parse_error", j.lineNum, pcvField, j.rawCSV)
			continue
		}

		batch = append(batch, row)
		if len(batch) >= cfg.BatchSize {
			if err := flush(); err != nil {
				res.err = fmt.Errorf("worker %d: copy batch: %w", id, err)
				return
			}
		}
	}

	// Final flush.
	if err := flush(); err != nil {
		res.err = fmt.Errorf("worker %d: copy final: %w", id, err)
		return
	}
}

// parseRow converts one CSV record (exactly 9 columns) into a []interface{}
// suitable for SmallDB.CopyTechInspections. Column mapping is:
//
//	0: PČV               -> INT (required)
//	1: Typ               -> TEXT (nullable)
//	2: Stav              -> TEXT (nullable)
//	3: Kód STK           -> INT  (nullable)
//	4: Název STK         -> TEXT (nullable)
//	5: Platnost od       -> DATE (nullable; "02.01.2006")
//	6: Platnost do       -> DATE (nullable; "02.01.2006")
//	7: Číslo protokolu   -> TEXT (nullable)
//	8: Aktuální          -> BOOLEAN (case-insensitive "true" => true; else false)
//
// Returns an error if the column count is not 9, PČV is invalid, or any date
// fails to parse when non-empty.
func parseRow(fields []string) ([]interface{}, error) {
	if len(fields) != expectedColumns {
		return nil, fmt.Errorf("want %d columns, got %d", expectedColumns, len(fields))
	}

	trim := func(s string) string { return strings.TrimSpace(s) }

	// 0: PČV (required int)
	pcv, err := strconv.Atoi(trim(fields[0]))
	if err != nil {
		return nil, fmt.Errorf("invalid PČV: %w", err)
	}

	// 1: Typ (nullable)
	var typ *string
	if s := trim(fields[1]); s != "" {
		typ = &s
	}

	// 2: Stav (nullable)
	var stav *string
	if s := trim(fields[2]); s != "" {
		stav = &s
	}

	// 3: Kód STK (nullable int). If non-empty but unparsable, keep behavior: treat as NULL.
	var kodSTK *int
	if s := trim(fields[3]); s != "" {
		if v, err := strconv.Atoi(s); err == nil {
			kodSTK = &v
		}
	}

	// 4: Název STK (nullable)
	var nazevSTK *string
	if s := trim(fields[4]); s != "" {
		nazevSTK = &s
	}

	// 5/6: Platnost od / do (nullable dates)
	parseDate := func(s string) (*time.Time, error) {
		s = trim(s)
		if s == "" {
			return nil, nil
		}
		t, err := time.Parse(dateLayout, s)
		if err != nil {
			return nil, err
		}
		return &t, nil
	}

	dFrom, err := parseDate(fields[5])
	if err != nil {
		return nil, fmt.Errorf("invalid Platnost od: %w", err)
	}
	dTo, err := parseDate(fields[6])
	if err != nil {
		return nil, fmt.Errorf("invalid Platnost do: %w", err)
	}

	// 7: Číslo protokolu (nullable)
	var cisloProt *string
	if s := trim(fields[7]); s != "" {
		cisloProt = &s
	}

	// 8: Aktuální (bool). Accept case-insensitive "true" only.
	aktualni := strings.EqualFold(trim(fields[8]), "true")

	return []interface{}{
		pcv,
		typ,
		stav,
		kodSTK,
		nazevSTK,
		dFrom,
		dTo,
		cisloProt,
		aktualni,
	}, nil
}

// readPhysicalLine reads a single physical line, tolerating both LF and CRLF.
func readPhysicalLine(r *bufio.Reader) (string, error) {
	var b bytes.Buffer
	for {
		part, isPrefix, err := r.ReadLine()
		if err != nil {
			if err == io.EOF && b.Len() > 0 {
				return b.String(), nil
			}
			return "", err
		}
		b.Write(part)
		if !isPrefix {
			return b.String(), nil
		}
	}
}

// looksLikeRecordStart returns true if the physical line likely starts a new record:
// first non-BOM rune is a digit and the line contains at least one comma.
func looksLikeRecordStart(s string) bool {
	s = strings.TrimLeftFunc(s, func(r rune) bool {
		return r == '\uFEFF' || unicode.IsSpace(r)
	})
	if s == "" {
		return false
	}
	r, _ := utf8.DecodeRuneInString(s)
	return unicode.IsDigit(r) && strings.ContainsRune(s, ',')
}

// encodeCSV returns a single-line CSV encoding of fields using encoding/csv.
func encodeCSV(fields []string) string {
	var b bytes.Buffer
	w := csv.NewWriter(&b)
	_ = w.Write(fields)
	w.Flush()
	return strings.TrimRight(b.String(), "\r\n")
}

// copyStrings makes a stable copy of a reused slice.
func copyStrings(in []string) []string {
	out := make([]string, len(in))
	copy(out, in)
	return out
}

// trimBOM removes a leading UTF-8 BOM (if present) from s.
func trimBOM(s string) string {
	if s == "" {
		return s
	}
	// UTF-8 BOM: 0xEF 0xBB 0xBF
	if strings.HasPrefix(s, "\uFEFF") {
		_, i := utf8.DecodeRuneInString(s)
		return s[i:]
	}
	return s
}
