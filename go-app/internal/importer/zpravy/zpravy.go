// Package rsvzpravy provides an importer for the "RSV_zpravy_vyrobce_zastupce"
// CSV dataset. It follows the same parallel import model as the techinspections
// importer, but expects exactly two columns per record:
//
//	0: PČV           (required int)
//	1: Krátký text   (nullable string)
//
// Each worker writes a CSV of skipped rows to "skipped/skipped_rsvzpravy_w{N}.csv".
package rsvzpravy

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
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"csvloader/internal/config"
	"csvloader/internal/db"
)

const (
	expectedColumns   = 2
	readBufferBytes   = 4 << 20
	skippedDir        = "skipped"
	skippedFileFormat = "skipped_rsvzpravy_w%d.csv"
)

// job represents one logical CSV record.
type job struct {
	fields  []string
	lineNum int
	rawCSV  string
}

// workerResult aggregates one worker’s stats.
type workerResult struct {
	inserted int
	skipped  int
	seen     int
	reasons  map[string]int
	err      error
}

// ImportRSVZpravyParallel imports the RSV_zpravy_vyrobce_zastupce dataset
// using the same parallel/concurrent pattern as techinspections.
func ImportRSVZpravyParallel(ctx context.Context, cfg *config.Config, smallFactory db.SmallDBFactory, path string) error {
	start := time.Now()

	ctrl, err := smallFactory(ctx)
	if err != nil {
		return fmt.Errorf("rsv_zpravy: open small db: %w", err)
	}
	if err := ctrl.CreateRSVZpravyTable(ctx); err != nil {
		_ = ctrl.Close(ctx)
		return fmt.Errorf("rsv_zpravy: create table: %w", err)
	}
	_ = ctrl.Close(ctx)

	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("rsv_zpravy: open csv: %w", err)
	}
	defer f.Close()
	br := bufio.NewReaderSize(f, readBufferBytes)

	// Consume header (and strip BOM)
	headerLine, err := readPhysicalLine(br)
	if err != nil && err != io.EOF {
		return fmt.Errorf("rsv_zpravy: read header: %w", err)
	}
	if headerLine == "" {
		log.Printf("rsv_zpravy: empty csv")
		return nil
	}
	hdr := strings.Split(headerLine, ",")
	if len(hdr) > 0 {
		hdr[0] = trimBOM(hdr[0])
	}

	jobs := make(chan job, 32768)
	var produced int64

	go func() {
		defer close(jobs)
		lineNum := 1
		cr := csv.NewReader(br)
		cr.FieldsPerRecord = -1
		cr.LazyQuotes = true

		for {
			fields, err := cr.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("rsv_zpravy: csv read error at line %d: %v", lineNum, err)
				continue
			}
			lineNum++
			if len(fields) != expectedColumns {
				log.Printf("rsv_zpravy: wrong column count at line %d", lineNum)
				continue
			}
			jobs <- job{fields: copyStrings(fields), lineNum: lineNum, rawCSV: encodeCSV(fields)}
			atomic.AddInt64(&produced, 1)
		}
	}()

	workers := cfg.Workers
	if workers <= 0 {
		workers = 1
	}
	results := make(chan workerResult, workers)

	for i := 0; i < workers; i++ {
		go runWorker(ctx, i+1, cfg, smallFactory, jobs, results)
	}

	var (
		totalInserted int
		totalSkipped  int
		totalSeen     int
		firstErr      error
	)
	for i := 0; i < workers; i++ {
		r := <-results
		totalInserted += r.inserted
		totalSkipped += r.skipped
		totalSeen += r.seen
		if r.err != nil && firstErr == nil {
			firstErr = r.err
		}
	}

	if firstErr != nil {
		return firstErr
	}

	dur := time.Since(start)
	log.Printf("rsv_zpravy: inserted=%d skipped=%d seen=%d duration=%s rate=%.0f/s",
		totalInserted, totalSkipped, totalSeen, dur.Round(time.Millisecond),
		float64(totalInserted)/dur.Seconds(),
	)
	return nil
}

// runWorker consumes jobs and inserts batches into DB.
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
		res.err = fmt.Errorf("worker %d: db connect: %w", id, err)
		return
	}
	defer sdb.Close(ctx)

	if err := os.MkdirAll(skippedDir, 0o755); err != nil {
		res.err = fmt.Errorf("worker %d: skipped dir: %w", id, err)
		return
	}

	skippedPath := filepath.Join(skippedDir, fmt.Sprintf(skippedFileFormat, id))
	skf, err := os.Create(skippedPath)
	if err != nil {
		res.err = fmt.Errorf("worker %d: create skipped: %w", id, err)
		return
	}
	defer skf.Close()

	skw := csv.NewWriter(skf)
	defer skw.Flush()
	_ = skw.Write([]string{"reason", "line_number", "pcv_field", "raw_line"})

	addSkip := func(reason string, j job) {
		res.reasons[reason]++
		res.skipped++
		pcv := ""
		if len(j.fields) > 0 {
			pcv = j.fields[0]
		}
		_ = skw.Write([]string{reason, strconv.Itoa(j.lineNum), pcv, j.rawCSV})
	}

	batch := make([][]interface{}, 0, cfg.BatchSize)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := sdb.CopyRSVZpravy(ctx, batch); err != nil {
			return err
		}
		res.inserted += len(batch)
		batch = batch[:0]
		return nil
	}

	for j := range jobs {
		res.seen++
		fields := j.fields
		if len(fields) != expectedColumns {
			addSkip("column_mismatch", j)
			continue
		}
		row, err := parseRow(fields)
		if err != nil {
			addSkip("parse_error", j)
			continue
		}
		batch = append(batch, row)
		if len(batch) >= cfg.BatchSize {
			if err := flush(); err != nil {
				res.err = fmt.Errorf("worker %d: batch copy: %w", id, err)
				return
			}
		}
	}

	if err := flush(); err != nil {
		res.err = fmt.Errorf("worker %d: final flush: %w", id, err)
	}
}

func parseRow(fields []string) ([]interface{}, error) {
	if len(fields) != expectedColumns {
		return nil, fmt.Errorf("expected %d columns, got %d", expectedColumns, len(fields))
	}
	pcvStr := strings.TrimSpace(fields[0])
	text := strings.TrimSpace(fields[1])

	pcv, err := strconv.Atoi(pcvStr)
	if err != nil {
		return nil, fmt.Errorf("invalid PČV: %w", err)
	}

	var kratkyText *string
	if text != "" {
		kratkyText = &text
	}

	return []interface{}{pcv, kratkyText}, nil
}

// --- utilities (same as techinspections) ---

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

func trimBOM(s string) string {
	if s == "" {
		return s
	}
	if strings.HasPrefix(s, "\uFEFF") {
		_, i := utf8.DecodeRuneInString(s)
		return s[i:]
	}
	return s
}

func encodeCSV(fields []string) string {
	var b bytes.Buffer
	w := csv.NewWriter(&b)
	_ = w.Write(fields)
	w.Flush()
	return strings.TrimRight(b.String(), "\r\n")
}

func copyStrings(in []string) []string {
	out := make([]string, len(in))
	copy(out, in)
	return out
}
