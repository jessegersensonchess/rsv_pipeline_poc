package main

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"etl/internal/config"
	_ "etl/internal/storage/sqlite" // register backend
)

// buildBenchCSV creates N rows with simple integer+string payloads.
func buildBenchCSV(b *testing.B, n int, cols []string) string {
	b.Helper()
	dir := b.TempDir()
	p := filepath.Join(dir, "bench.csv")

	// Write header + rows.
	f, err := os.Create(p)
	if err != nil {
		b.Fatalf("create csv: %v", err)
	}
	defer f.Close()

	if _, err := f.WriteString(cols[0] + "," + cols[1] + "\n"); err != nil {
		b.Fatalf("write header: %v", err)
	}
	for i := 0; i < n; i++ {
		if _, err := f.WriteString(strconv.Itoa(i) + ",n" + strconv.Itoa(i) + "\n"); err != nil {
			b.Fatalf("write row: %v", err)
		}
	}
	return p
}

// prepareTable creates the target table once (avoids DDL in the hot path).
func prepareTable(b *testing.B, dsn, table string) {
	b.Helper()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS "items" (id TEXT, name TEXT)`)
	if err != nil {
		b.Fatalf("create table: %v", err)
	}
}

/*
End-to-end benchmark: runs runStreamed over a CSV with N rows into SQLite.
We avoid DDL inside the loop to reduce noise; COPY batching and row transforms
remain in play.
*/
func BenchmarkRunStreamed_E2E_SQLite(b *testing.B) {
	const (
		table = "items" // SQLite has no schemas; use a flat table name
		nrows = 5000
	)
	dbPath := filepath.Join(b.TempDir(), "bench.sqlite")
	dsn := "file:" + url.PathEscape(dbPath) + "?mode=rwc"
	cols := []string{"id", "name"}

	csv := buildBenchCSV(b, nrows, cols)
	prepareTable(b, dsn, table)

	// Keep batch size reasonable for throughput; env is read during runStreamed.
	b.Setenv("ETL_BATCH_SIZE", "512")
	b.Setenv("ETL_READER_WORKERS", "1")
	b.Setenv("ETL_TRANSFORM_WORKERS", "4")
	b.Setenv("ETL_LOADER_WORKERS", "1")
	b.Setenv("ETL_CH_BUFFER", "1024")

	p := config.Pipeline{
		Source: config.Source{
			Kind: "file",
			File: config.SourceFile{Path: csv},
		},
		Parser: config.Parser{Kind: "csv", Options: config.Options{}},
		Storage: config.Storage{
			Kind: "sqlite",
			Postgres: config.StoragePostgres{
				DSN:             dsn,
				Table:           table,
				Columns:         cols,
				AutoCreateTable: false, // already created
			},
		},
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// Use a fresh table each iteration to avoid PK/conflicts; easiest way:
		// truncate by recreating the table (SQLite: DELETE is fine).
		db, err := sql.Open("sqlite", dsn)
		if err != nil {
			b.Fatalf("open db: %v", err)
		}
		if _, err := db.Exec(`DELETE FROM "items"`); err != nil {
			b.Fatalf("truncate: %v", err)
		}
		db.Close()

		if err := runStreamed(context.Background(), p); err != nil {
			b.Fatal(err)
		}
	}
}
