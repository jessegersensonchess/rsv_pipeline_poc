package importer

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"csvloader/internal/config"
	"csvloader/internal/csvutil"
	"csvloader/internal/db"
	"csvloader/internal/domain"
)

/*
	========================================
	SECTION: Ownership parsing & construction
	========================================
*/

func parseRecord(fields []string) (*domain.Record, error) {
	pcv, err := strconv.Atoi(strings.TrimSpace(fields[0]))
	if err != nil {
		return nil, fmt.Errorf("invalid PCV: %v", err)
	}
	typSubjektu, err := strconv.Atoi(strings.TrimSpace(fields[1]))
	if err != nil {
		return nil, fmt.Errorf("invalid Typ subjektu: %v", err)
	}
	vztahKVozidlu, err := strconv.Atoi(strings.TrimSpace(fields[2]))
	if err != nil {
		return nil, fmt.Errorf("invalid Vztah k vozidlu: %v", err)
	}
	aktualni := strings.TrimSpace(fields[3]) == "True"

	var ico *int
	if s := strings.TrimSpace(fields[4]); s != "" {
		if val, err := strconv.Atoi(s); err == nil {
			ico = &val
		}
	}

	var nazev *string
	if s := strings.TrimSpace(fields[5]); s != "" {
		nazev = &s
	}
	var adresa *string
	if s := strings.TrimSpace(fields[6]); s != "" {
		adresa = &s
	}

	var datumOd *time.Time
	if s := strings.TrimSpace(fields[7]); s != "" {
		t, err := time.Parse(domain.Layout, s)
		if err != nil {
			return nil, fmt.Errorf("invalid Datum od: %v", err)
		}
		datumOd = &t
	}

	var datumDo *time.Time
	if s := strings.TrimSpace(fields[8]); s != "" {
		if t, err := time.Parse(domain.Layout, s); err == nil {
			datumDo = &t
		}
	}

	return &domain.Record{
		PCV:           pcv,
		TypSubjektu:   typSubjektu,
		VztahKVozidlu: vztahKVozidlu,
		Aktualni:      aktualni,
		ICO:           ico,
		Nazev:         nazev,
		Adresa:        adresa,
		DatumOd:       datumOd,
		DatumDo:       datumDo,
	}, nil
}

// ImportOwnershipParallel ingests ownership CSV using N workers and SmallDB batches.
// It remains storage-agnostic by depending only on SmallDBFactory/SmallDB.
func ImportOwnershipParallel(ctx context.Context, cfg *config.Config, smallFactory db.SmallDBFactory, path string) error {
	start := time.Now()
	// Ensure table using a short-lived control instance
	ctrl, err := smallFactory(ctx)
	if err != nil {
		return fmt.Errorf("open small db (ensure table): %w", err)
	}
	if err := ctrl.CreateOwnershipTable(ctx); err != nil {
		_ = ctrl.Close(ctx)
		return fmt.Errorf("create ownership table: %w", err)
	}
	_ = ctrl.Close(ctx)

	// Open file
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open ownership csv: %w", err)
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 4<<20)

	// Skip header
	if _, err := csvutil.ReadLogicalCSVLine(r); err != nil && err != io.EOF {
		return fmt.Errorf("read header: %w", err)
	}

	type job struct {
		line    string
		lineNum int
	}
	jobs := make(chan job, 32_768)

	// Reader goroutine
	go func() {
		lineNum := 1 // header consumed
		for {
			l, err := csvutil.ReadLogicalCSVLine(r)
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("⚠️ ownership read error: %v", err)
				break
			}
			lineNum++
			jobs <- job{line: l, lineNum: lineNum}
		}
		close(jobs)
	}()

	type workerResult struct {
		inserted int
		skipped  int
		err      error
		reasons  map[string]int
	}

	workers := cfg.Workers
	results := make(chan workerResult, workers)

	workerFn := func(id int) {
		res := workerResult{reasons: map[string]int{}}
		defer func() { results <- res }()

		// Fresh SmallDB instance per worker (own connection/transaction lifecycle hidden inside adapter)
		sdb, err := smallFactory(ctx)
		if err != nil {
			res.err = fmt.Errorf("worker %d connect: %w", id, err)
			return
		}
		defer sdb.Close(ctx)

		// per-worker skipped CSV
		if err := os.MkdirAll("skipped", 0o755); err != nil {
			res.err = fmt.Errorf("worker %d create skipped dir: %w", id, err)
			return
		}
		skf, err := os.Create(filepath.Join("skipped", fmt.Sprintf("skipped_ownership_w%d.csv", id)))
		if err != nil {
			res.err = fmt.Errorf("worker %d skipped file: %w", id, err)
			return
		}
		defer skf.Close()
		skw := csv.NewWriter(skf)
		defer skw.Flush()
		_ = skw.Write([]string{"reason", "line_number", "pcv_field", "raw_line"})
		addSkip := func(reason string, ln int, pcvField, raw string) {
			res.reasons[reason]++
			res.skipped++
			_ = skw.Write([]string{reason, strconv.Itoa(ln), pcvField, raw})
		}

		batchSize := cfg.BatchSize
		batch := make([][]interface{}, 0, batchSize)

		flush := func() error {
			if len(batch) == 0 {
				return nil
			}
			// Adapter handles its own tx/COPY/INSERT details
			if err := sdb.CopyOwnership(ctx, batch); err != nil {
				return err
			}
			res.inserted += len(batch)
			batch = batch[:0]
			log.Printf("ownership[w%d]: inserted=%d skipped=%d so far", id, res.inserted, res.skipped)
			return nil
		}

		for j := range jobs {
			fields, perr := csvutil.ParseCSVLineLoose(j.line)
			if perr != nil {
				addSkip("parse_error", j.lineNum, "", j.line)
				continue
			}
			// Expect 9 fields; try repairs like in the original
			if len(fields) != 9 {
				if fixed, ok := csvutil.RepairOverlongCommaFields(fields); ok {
					fields = fixed
				} else if fixed2, ok2 := csvutil.ParseSpaceSeparatedRow(j.line); ok2 {
					fields = fixed2
				} else {
					addSkip("column_mismatch", j.lineNum, "", j.line)
					continue
				}
			}

			rec, err := parseRecord(fields)
			if err != nil {
				addSkip("field_parse_error", j.lineNum, fields[0], j.line)
				continue
			}

			batch = append(batch, []interface{}{
				rec.PCV, rec.TypSubjektu, rec.VztahKVozidlu, rec.Aktualni,
				rec.ICO, rec.Nazev, rec.Adresa, rec.DatumOd, rec.DatumDo,
			})

			if len(batch) >= batchSize {
				if err := flush(); err != nil {
					res.err = fmt.Errorf("worker %d copy: %w", id, err)
					return
				}
			}
		}

		// final batch
		if err := flush(); err != nil {
			res.err = fmt.Errorf("worker %d copy final: %w", id, err)
			return
		}
	}

	// Launch workers
	for i := 0; i < workers; i++ {
		go workerFn(i + 1)
	}

	// Gather
	totalInserted, totalSkipped := 0, 0
	reasonAgg := map[string]int{}
	var firstErr error
	for i := 0; i < workers; i++ {
		r := <-results
		totalInserted += r.inserted
		totalSkipped += r.skipped
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

	var parts []string
	for k, v := range reasonAgg {
		parts = append(parts, fmt.Sprintf("%s=%d", k, v))
	}
	duration := time.Since(start)
	processingRate := float64(totalInserted) / duration.Seconds()

	log.Printf("ownership (parallel %d): inserted=%d skipped=%d (%s), duration=%s, rate_per_second=%.0f",
		workers, totalInserted, totalSkipped, strings.Join(parts, ", "), duration.Round(time.Millisecond), processingRate)

	return nil
}

// ensureOwnershipTable creates the ownership table for the selected driver.
func ensureOwnershipTable(ctx context.Context, d db.DB, unlogged bool, driver string) error {
	switch strings.ToLower(driver) {
	case "postgres":
		if err := d.Exec(ctx, `
			CREATE TABLE IF NOT EXISTS ownership (
				pcv INT,
				typ_subjektu INT,
				vztah_k_vozidlu INT,
				aktualni BOOLEAN,
				ico INT,
				nazev TEXT,
				adresa TEXT,
				datum_od DATE,
				datum_do DATE
			);
		`); err != nil {
			return fmt.Errorf("create ownership (pg): %w", err)
		}
		if unlogged {
			_ = d.Exec(ctx, `ALTER TABLE ownership SET UNLOGGED`)
		}
		_ = d.Exec(ctx, `CREATE INDEX IF NOT EXISTS ownership_pcv_idx ON ownership(pcv)`)
		return nil

	case "mssql":
		if err := d.Exec(ctx, `
			IF OBJECT_ID(N'ownership', N'U') IS NULL
			BEGIN
				CREATE TABLE ownership (
					pcv INT,
					typ_subjektu INT,
					vztah_k_vozidlu INT,
					aktualni BIT,
					ico INT,
					nazev NVARCHAR(MAX),
					adresa NVARCHAR(MAX),
					datum_od DATE,
					datum_do DATE
				);
			END
		`); err != nil {
			return fmt.Errorf("create ownership (mssql): %w", err)
		}
		_ = d.Exec(ctx, `IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ownership_pcv_idx')
		  CREATE INDEX ownership_pcv_idx ON ownership(pcv)`)
		return nil
	default:
		return fmt.Errorf("unknown driver: %s", driver)
	}
}
