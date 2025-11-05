// Package db provides database adapter implementations for Postgres (pgx)
// and MSSQL via standardized DB and Tx interfaces. This file contains
// the Postgres adapter, which wraps pgx.Conn/pgx.Tx while remaining testable
// via lightweight seams.
//
// Design goals:
//   - Allow mocking via the pgConnLike interface (for hermetic unit tests).
//   - Keep behavior minimal and predictable—no implicit retries.
//   - Surface errors directly; avoid wrapping for clarity.
//   - Maintain parity with the MSSQL adapter where possible.
package db

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

//
// ===========================
//  Interface seam for testing
// ===========================
//
// pgConnLike defines the minimal subset of methods used from *pgx.Conn.
// This seam allows injecting a test double that mimics *pgx.Conn behavior,
// enabling hermetic (non-networked) testing of the adapter.
//

type pgConnLike interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Begin(ctx context.Context) (pgx.Tx, error)
	Close(ctx context.Context) error
}

//
// ===============
//  Core pgDB type
// ===============
//
// pgDB is the concrete Postgres adapter implementing the DB interface.
// It is intentionally minimal: it wraps Exec, BeginTx, and Close around
// pgx.Conn (via pgConnLike). This makes it both production-usable and
// trivially testable using a fake connection.
//

type pgDB struct{ conn pgConnLike }

// NewPgDB connects to Postgres using pgx.Connect and wraps the connection
// in a pgDB. Callers are responsible for closing it via Close().
func NewPgDB(ctx context.Context, dsn string) (DB, error) {
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &pgDB{conn: c}, nil
}

// Exec delegates to pgx.Conn.Exec, executing the provided SQL statement
// with the given arguments. It returns only the error for simplicity.
func (p *pgDB) Exec(ctx context.Context, q string, args ...any) error {
	_, err := p.conn.Exec(ctx, q, args...)
	return err
}

// BeginTx starts a transaction by calling pgx.Conn.Begin.
// It returns a pgTx wrapper that satisfies the Tx interface.
func (p *pgDB) BeginTx(ctx context.Context) (Tx, error) {
	tx, err := p.conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &pgTx{tx: tx}, nil
}

// Close closes the underlying connection.
func (p *pgDB) Close(ctx context.Context) error {
	return p.conn.Close(ctx)
}

//
// =====================
//  Transaction wrapper
// =====================
//
// pgTx wraps pgx.Tx to implement our Tx interface. It provides uniform
// methods for Exec, CopyInto, Commit, and Rollback.
//

type pgTx struct {
	tx pgx.Tx
}

// Exec executes a SQL statement within the current transaction context.
// It discards the returned CommandTag, returning only error.
func (t *pgTx) Exec(ctx context.Context, q string, args ...any) error {
	_, err := t.tx.Exec(ctx, q, args...)
	return err
}

// CopyInto performs a bulk insert using Postgres's native COPY FROM mechanism.
// This is the fast path for high-throughput imports.
func (t *pgTx) CopyInto(ctx context.Context, table string, columns []string, rows [][]interface{}) (int64, error) {
	n, err := t.tx.CopyFrom(ctx, pgx.Identifier{table}, columns, pgx.CopyFromRows(rows))
	return n, err
}

// Commit commits the active transaction.
func (t *pgTx) Commit(ctx context.Context) error { return t.tx.Commit(ctx) }

// Rollback aborts the active transaction.
func (t *pgTx) Rollback(ctx context.Context) error { return t.tx.Rollback(ctx) }

//
// ==================
//  smallPg (utility)
// ==================
//
// smallPg provides a narrow implementation of SmallDB used by
// smaller ingestion pipelines. To keep it testable without a live DB,
// we introduce a minimal interface seam (smallPgConnLike) mirroring the
// subset of *pgx.Conn methods we actually use.
//

// smallPgConnLike is a tiny seam around *pgx.Conn for unit testing smallPg.
type smallPgConnLike interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Begin(ctx context.Context) (pgx.Tx, error)
	Close(ctx context.Context) error
}

type smallPg struct{ conn smallPgConnLike }

// NewSmallPg establishes a new *pgx.Conn for lightweight pipelines.
func NewSmallPg(ctx context.Context, dsn string) (SmallDB, error) {
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &smallPg{conn: c}, nil
}

// CreateOwnershipTable ensures the "ownership" table exists using
// proper Postgres DDL semantics ("IF NOT EXISTS").
func (p *smallPg) CreateOwnershipTable(ctx context.Context) error {
	ddl := `
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
	)`
	_, err := p.conn.Exec(ctx, ddl)
	return err
}

// CopyOwnership performs a COPY FROM operation for bulk inserts into the
// "ownership" table. It runs within a transaction to ensure atomicity.
func (p *smallPg) CopyOwnership(ctx context.Context, rows [][]interface{}) error {
	tx, err := p.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	n, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"ownership"},
		[]string{
			"pcv", "typ_subjektu", "vztah_k_vozidlu", "aktualni",
			"ico", "nazev", "adresa", "datum_od", "datum_do",
		},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return err
	}

	// Log if fewer rows were inserted than expected.
	if n != int64(len(rows)) {
		log.Printf("⚠️ postgres CopyFrom inserted %d of %d rows", n, len(rows))
	}
	return tx.Commit(ctx)
}

// CreateTechInspectionsTable ensures the "tech_inspections" table exists.
func (p *smallPg) CreateTechInspectionsTable(ctx context.Context) error {
	ddl := `
	CREATE TABLE IF NOT EXISTS tech_inspections (
		pcv INT,
		typ TEXT,
		stav TEXT,
		kod_stk INT,
		nazev_stk TEXT,
		platnost_od DATE,
		platnost_do DATE,
		cislo_protokolu TEXT,
		aktualni BOOLEAN
	);
	CREATE INDEX IF NOT EXISTS tech_inspections_pcv_idx ON tech_inspections(pcv);`
	_, err := p.conn.Exec(ctx, ddl)
	return err
}

// CopyTechInspections performs COPY FROM into the tech_inspections table.
//
// It uses pgx.CopyFrom for high performance bulk inserts. If fewer rows are
// reported as inserted than provided, a warning is logged (but the import
// continues). This usually indicates one or more rows were rejected by
// PostgreSQL due to type conversion issues (e.g., integer overflow or invalid date).
func (p *smallPg) CopyTechInspections(ctx context.Context, rows [][]interface{}) error {
	tx, err := p.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	n, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"tech_inspections"},
		[]string{
			"pcv",
			"typ",
			"stav",
			"kod_stk",
			"nazev_stk",
			"platnost_od",
			"platnost_do",
			"cislo_protokolu",
			"aktualni",
		},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("postgres CopyFrom tech_inspections: %w", err)
	}

	if n != int64(len(rows)) {
		log.Printf("⚠️  postgres CopyFrom(tech_inspections): inserted %d of %d rows — some rows were rejected", n, len(rows))
		// Print offending records for debugging.
		start := len(rows) - int(len(rows)-int(n))
		if start < 0 {
			start = 0
		}
		for i := start; i < len(rows); i++ {
			log.Printf("⚠️  rejected row #%d: %+v", i, rows[i])
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres commit tech_inspections: %w", err)
	}
	return nil
}

// CreateRSVZpravyTable ensures the "rsv_zpravy_vyrobce_zastupce" table exists.
func (p *smallPg) CreateRSVZpravyTable(ctx context.Context) error {
	ddl := `
	CREATE TABLE IF NOT EXISTS rsv_zpravy_vyrobce_zastupce (
		pcv INT,
		kratky_text TEXT
	);
	CREATE INDEX IF NOT EXISTS rsv_zpravy_vyrobce_zastupce_pcv_idx ON rsv_zpravy_vyrobce_zastupce(pcv);`
	_, err := p.conn.Exec(ctx, ddl)
	return err
}

// CopyRSVZpravy performs a bulk COPY INTO the "rsv_zpravy_vyrobce_zastupce" table.
func (p *smallPg) CopyRSVZpravy(ctx context.Context, rows [][]interface{}) error {
	tx, err := p.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	n, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"rsv_zpravy_vyrobce_zastupce"},
		[]string{"pcv", "kratky_text"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("postgres CopyFrom rsv_zpravy_vyrobce_zastupce: %w", err)
	}

	if n != int64(len(rows)) {
		log.Printf("⚠️  postgres CopyFrom(rsv_zpravy_vyrobce_zastupce): inserted %d of %d rows — some were rejected", n, len(rows))
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres commit rsv_zpravy_vyrobce_zastupce: %w", err)
	}
	return nil
}

// Close closes the underlying pgx.Conn.
func (p *smallPg) Close(ctx context.Context) error { return p.conn.Close(ctx) }

//
// ==============================
//  Adapter Introspection Helpers
// ==============================
//
// AsPgConn extracts the underlying *pgx.Conn when available.
// This is used by components that need to use native pgx features
// (e.g., CopyFrom) directly while remaining adapter-agnostic.
//

func AsPgConn(d DB) (*pgx.Conn, bool) {
	p, ok := d.(*pgDB)
	if !ok {
		return nil, false
	}
	if real, ok := p.conn.(*pgx.Conn); ok {
		return real, true
	}
	return nil, false
}

//
// =======================
//  Test-only constructors
// =======================
//
// These helpers allow injection of fakes and test doubles for hermetic tests.
// They are no-ops in production builds.
//

// newPgDBFromConn constructs a pgDB from a pgConnLike fake.
// Used exclusively in unit tests.
func newPgDBFromConn(c pgConnLike) *pgDB { return &pgDB{conn: c} }

// newPgTxForTest wraps a pgx.Tx fake into a pgTx for testing.
func newPgTxForTest(t pgx.Tx) *pgTx { return &pgTx{tx: t} }

// newSmallPgFromConn constructs a smallPg from a smallPgConnLike fake.
// Used exclusively in unit tests.
func newSmallPgFromConn(c smallPgConnLike) *smallPg { return &smallPg{conn: c} }
