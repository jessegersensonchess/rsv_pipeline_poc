// Package sqlite implements a SQLite-backed storage.Repository using
// database/sql. It performs batched INSERTs inside a transaction; SQLite does
// not have a dedicated bulk-load API like Postgres COPY, but transactions keep
// performance acceptable for moderate volumes.
package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	// SQLite driver; replace with your preferred driver if desired.
	_ "modernc.org/sqlite" // alternative: github.com/mattn/go-sqlite3
)

// Repository is a SQLite-backed implementation of storage.Repository.
type Repository struct {
	db  *sql.DB
	cfg Config
}

// NewRepository opens a SQLite connection using the provided DSN and returns
// a Repository plus a Close function for cleanup.
//
// DSN is passed directly to database/sql; for example:
//
//	"file:etl.db?cache=shared&_fk=1"
//	"etl.db"
func NewRepository(ctx context.Context, cfg Config) (*Repository, func(), error) {
	if strings.TrimSpace(cfg.DSN) == "" {
		return nil, nil, fmt.Errorf("sqlite: DSN must not be empty")
	}

	db, err := sql.Open("sqlite", cfg.DSN)
	if err != nil {
		return nil, nil, fmt.Errorf("sqlite: open: %w", err)
	}

	// Apply a basic ping with context to fail fast on invalid DSNs.
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, nil, fmt.Errorf("sqlite: ping: %w", err)
	}

	// Enable foreign keys by default; ignore error if driver doesn't support it.
	_, _ = db.ExecContext(ctx, "PRAGMA foreign_keys = ON;")

	closeFn := func() { db.Close() }
	return &Repository{db: db, cfg: cfg}, closeFn, nil
}

// CopyFrom inserts the given rows into the configured table using a single
// transaction and a prepared multi-value INSERT statement.
//
// It returns the number of rows successfully inserted or an error. The
// columns slice must match the configured table columns, and len(row) must
// equal len(columns) for every row.
func (r *Repository) CopyFrom(
	ctx context.Context,
	columns []string,
	rows [][]any,
) (int64, error) {
	if len(columns) == 0 {
		return 0, fmt.Errorf("sqlite: CopyFrom: columns must not be empty")
	}
	if len(rows) == 0 {
		return 0, nil
	}

	// Build INSERT INTO <table> (<cols>) VALUES (?, ?, ...).
	colList := strings.Join(columns, ", ")
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	stmtSQL := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		r.cfg.Table,
		colList,
		strings.Join(placeholders, ", "),
	)

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("sqlite: begin tx: %w", err)
	}
	stmt, err := tx.PrepareContext(ctx, stmtSQL)
	if err != nil {
		_ = tx.Rollback()
		return 0, fmt.Errorf("sqlite: prepare insert: %w", err)
	}
	defer stmt.Close()

	var inserted int64
	for _, row := range rows {
		if len(row) != len(columns) {
			_ = tx.Rollback()
			return inserted, fmt.Errorf("sqlite: CopyFrom: row length %d != columns length %d", len(row), len(columns))
		}
		if _, err := stmt.ExecContext(ctx, row...); err != nil {
			_ = tx.Rollback()
			return inserted, fmt.Errorf("sqlite: insert: %w", err)
		}
		inserted++
	}

	if err := tx.Commit(); err != nil {
		return inserted, fmt.Errorf("sqlite: commit: %w", err)
	}
	return inserted, nil
}

// Exec executes an arbitrary SQL statement (typically DDL) using the underlying
// database/sql connection.
func (r *Repository) Exec(ctx context.Context, sql string) error {
	if strings.TrimSpace(sql) == "" {
		return nil
	}
	if _, err := r.db.ExecContext(ctx, sql); err != nil {
		return fmt.Errorf("sqlite: exec: %w", err)
	}
	return nil
}
