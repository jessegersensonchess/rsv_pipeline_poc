package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"etl/internal/config"
	_ "etl/internal/storage/sqlite" // register "sqlite" backend for tests
)

// makeTempCSV creates a CSV with the given header and rows.
func makeTempCSV(tb testing.TB, header []string, rows [][]string) string {
	tb.Helper()
	dir := tb.TempDir()
	p := filepath.Join(dir, "data.csv")
	var b strings.Builder
	b.WriteString(strings.Join(header, ","))
	b.WriteByte('\n')
	for _, r := range rows {
		b.WriteString(strings.Join(r, ","))
		b.WriteByte('\n')
	}
	if err := os.WriteFile(p, []byte(b.String()), 0o644); err != nil {
		tb.Fatalf("write csv: %v", err)
	}
	return p
}

// openSQL opens a raw *sql.DB to the same DSN so we can verify inserted rows.
// The sqlite adapter blank-import ensures the driver is available.
func openSQL(tb testing.TB, dsn string) *sql.DB {
	tb.Helper()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		tb.Fatalf("sql open: %v", err)
	}
	tb.Cleanup(func() { _ = db.Close() })
	return db
}

// buildPipeline is a minimal working pipeline config for runStreamed.
func buildPipeline(dsn, table string, cols []string, csvPath string, autoCreate bool) config.Pipeline {
	return config.Pipeline{
		Source: config.Source{
			Kind: "file",
			File: config.SourceFile{
				Path: csvPath,
			},
		},
		Parser: config.Parser{
			Kind:    "csv",
			Options: config.Options{}, // zero value → defaults in csv streamer
		},
		Storage: config.Storage{
			Kind: "sqlite",
			DB: config.DBConfig{ // shape compatibility
				DSN:             dsn,
				Table:           table,
				Columns:         cols,
				AutoCreateTable: autoCreate,
			},
		},
		// No transforms; pipeline should still load as TEXT columns.
	}
}

/*
End-to-end test: runs the full streaming pipeline reading a CSV and loading into
SQLite (shared in-memory DB). Uses AutoCreateTable=true to exercise the DDL path.
*/
func TestRunStreamed_E2E_SQLite_AutoCreate(t *testing.T) {
	t.Parallel()

	// Use a file: URI with mode=rwc so multiple handles see the same DB reliably.
	dbPath := filepath.Join(t.TempDir(), "e2e_auto.sqlite")
	dsn := "file:" + url.PathEscape(dbPath) + "?mode=rwc"

	table := "items_e2e" // SQLite has no schemas; use a flat table name
	cols := []string{"id", "name"}

	// Prepare CSV file (header must match columns).
	csv := makeTempCSV(t, cols, [][]string{
		{"1", "a"},
		{"2", "b"},
	})

	// Build pipeline and run.
	p := buildPipeline(dsn, table, cols, csv, true)

	if err := runStreamed(context.Background(), p); err != nil {
		t.Fatalf("runStreamed: %v", err)
	}

	// Verify DB contents via direct SQL.
	db := openSQL(t, dsn)
	var got int
	if err := db.QueryRow(`SELECT COUNT(*) FROM items_e2e`).Scan(&got); err != nil {
		t.Fatalf("verify count: %v", err)
	}
	if got != 2 {
		t.Fatalf("row count mismatch: got %d want 2", got)
	}
}

/*
Integration test: focuses on batch flushing behavior and parameter wiring.

We force small batch size via environment to ensure multiple COPY calls happen.
We don't assert logs; instead we assert the total row count at the end.
*/
func TestRunStreamed_Integration_BatchesAndEnv(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "integ_batch.sqlite")
	dsn := "file:" + url.PathEscape(dbPath) + "?mode=rwc"
	table := "items_integ" // SQLite has no schemas; use a flat table name
	cols := []string{"id", "name"}

	// 5 rows, batch size 2 → expect 3 flushes internally (2,2,1)
	var rows [][]string
	for i := 1; i <= 5; i++ {
		rows = append(rows, []string{fmt.Sprintf("%d", i), fmt.Sprintf("n%d", i)})
	}
	csv := makeTempCSV(t, cols, rows)

	// Force a small batch and modest buffers via env (picked up by runStreamed).
	t.Setenv("ETL_BATCH_SIZE", "2")
	t.Setenv("ETL_READER_WORKERS", "1")
	t.Setenv("ETL_TRANSFORM_WORKERS", "2")
	t.Setenv("ETL_LOADER_WORKERS", "1")
	t.Setenv("ETL_CH_BUFFER", "8")

	p := buildPipeline(dsn, table, cols, csv, true)

	if err := runStreamed(context.Background(), p); err != nil {
		t.Fatalf("runStreamed: %v", err)
	}

	db := openSQL(t, dsn)
	var got int
	if err := db.QueryRow(`SELECT COUNT(*) FROM items_integ`).Scan(&got); err != nil {
		t.Fatalf("verify count: %v", err)
	}
	if got != 5 {
		t.Fatalf("row count mismatch: got %d want 5", got)
	}
}
