// Package vehicletech contains the ingestion pipeline and writer backends for
// streaming vehicle-tech CSV rows into the database. This file provides the
// production writer backends for Postgres (COPY) and MSSQL (TVP/CopyIn), as
// well as small seams to make those paths hermetic and testable without
// networked dependencies.
package vehicletech

import (
	"context"
	"csvloader/internal/db"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"

	"github.com/jackc/pgx/v5"
	mssql "github.com/microsoft/go-mssqldb"
)

//
// ========================
//  Postgres writer backend
// ========================
//

// pgCopyConn is the minimal subset of *pgx.Conn used for COPY FROM.
// It exists as an interface seam to allow tests to inject a fake that
// counts rows without a real network connection.
//
// Implementations:
//   - Production: *pgx.Conn
//   - Tests: small fakes that satisfy CopyFrom
type pgCopyConn interface {
	CopyFrom(ctx context.Context, table pgx.Identifier, columns []string, src pgx.CopyFromSource) (int64, error)
}

// pgWriterBackend implements WriterBackend for Postgres using COPY FROM.
// It resolves a pgx connection either from the provided DB adapter (via
// db.AsPgConn) or from the optional test seam (conn) when non-nil.
type pgWriterBackend struct {
	dbh db.DB
	// conn is an optional test seam. When nil, Write() uses db.AsPgConn to
	// obtain the underlying *pgx.Conn from dbh. Tests can set this directly
	// to avoid touching the real network/driver.
	conn pgCopyConn
}

// newPgWriterBackend constructs a Postgres writer backend bound to the DB
// adapter d. The adapter is expected to expose a *pgx.Conn via db.AsPgConn
// at runtime.
func newPgWriterBackend(d db.DB) *pgWriterBackend { return &pgWriterBackend{dbh: d} }

// Write streams encoded jobs into Postgres using COPY FROM. Each job is
// validated by the upstream encoder; invalid rows must be rejected by the
// copy source (pgxEncodedSource) via addSkip. The method returns the number
// of inserted rows (according to pgx.CopyFrom) and a non-nil error on failure.
//
// Parameters:
//   - ctx: request-scoped context; cancellation aborts COPY.
//   - encodedCh: channel of encodedJob; the source closes it when done.
//   - addSkip: callback to record rejected rows; invoked by the source.
//   - logEvery: cadence in rows for progress logs (>=1 recommended).
//
// Behavior:
//   - Resolves *pgx.Conn via db.AsPgConn unless a test seam is present.
//   - Uses a pgxEncodedSource that logs delta inserts at the requested cadence.
//   - COPY FROM target: table "vehicle_tech" with columns (pcv, payload).
func (b *pgWriterBackend) Write(
	ctx context.Context,
	encodedCh <-chan encodedJob,
	addSkip func(string, int, string, string),
	logEvery int,
) (int, error) {
	// Resolve the pg copy connection. In production we call db.AsPgConn;
	// tests can inject b.conn directly to avoid real pgx.
	pgconn := b.conn
	if pgconn == nil {
		var ok bool
		pgconn, ok = db.AsPgConn(b.dbh)
		if !ok {
			return 0, fmt.Errorf("not a Postgres-backed DB")
		}
	}

	// Track delta since last log to keep progress human-readable.
	insertedAtLastLog := 0
	src := &pgxEncodedSource{
		ch:       encodedCh,
		addSkip:  addSkip,
		isPG:     true,
		logEvery: logEvery,
		// logf is invoked by the copy source whenever "ins" has advanced by
		// logEvery. We compute the delta and update the watermark. Avoids
		// spamming logs with monotonically increasing totals.
		logf: func(ins, _ int) {
			delta := ins - insertedAtLastLog
			insertedAtLastLog = ins
			log.Printf("vehicle_tech: streaming COPY; +%d", delta)
		},
	}

	n, err := pgconn.CopyFrom(
		ctx,
		pgx.Identifier{"vehicle_tech"},
		[]string{"pcv", "payload"},
		src,
	)
	return int(n), err
}

//
// =======================
//  MSSQL writer backend
// =======================
//

// stmtCore abstracts *sql.Stmt to enable hermetic tests (no driver needed).
// Production uses realStmt (adapts *sql.Stmt); tests use a fake with the
// same ExecContext/Close shape.
type stmtCore interface {
	ExecContext(ctx context.Context, args ...any) (sql.Result, error)
	Close() error
}

// sqlPreparer abstracts *sql.DB to a PrepareContext that returns stmtCore.
// Production uses realPreparer (adapts *sql.DB); tests inject a fake.
type sqlPreparer interface {
	PrepareContext(ctx context.Context, query string) (stmtCore, error)
}

// realPreparer adapts *sql.DB to sqlPreparer. It constructs a realStmt wrapper
// around the driver-owned *sql.Stmt, keeping the writer backend agnostic of
// database/sql concrete types.
type realPreparer struct{ db *sql.DB }

// PrepareContext implements sqlPreparer by delegating to (*sql.DB).PrepareContext
// and wrapping the result in realStmt.
func (p realPreparer) PrepareContext(ctx context.Context, query string) (stmtCore, error) {
	st, err := p.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return realStmt{st: st}, nil
}

// realStmt adapts *sql.Stmt to stmtCore.
type realStmt struct{ st *sql.Stmt }

// ExecContext delegates to (*sql.Stmt).ExecContext.
func (r realStmt) ExecContext(ctx context.Context, args ...any) (sql.Result, error) {
	return r.st.ExecContext(ctx, args...)
}

// Close delegates to (*sql.Stmt).Close.
func (r realStmt) Close() error { return r.st.Close() }

// mssqlWriterBackend implements WriterBackend for MSSQL. It uses the TDS
// TVP/CopyIn helper (microsoft/go-mssqldb) to prepare a bulk-insert statement
// and then streams per-row Exec calls followed by a finalizing Exec.
type mssqlWriterBackend struct {
	dbh db.DB
	// prep is an optional test seam. When nil, Write() resolves *sql.DB via
	// db.AsSQLDB and adapts it with realPreparer. Tests set this to a fake
	// that returns a fake stmtCore to avoid real drivers.
	prep sqlPreparer
}

// newMSSQLWriterBackend constructs an MSSQL writer backend bound to the DB
// adapter d. The adapter is expected to expose a *sql.DB via db.AsSQLDB at
// runtime.
func newMSSQLWriterBackend(d db.DB) *mssqlWriterBackend { return &mssqlWriterBackend{dbh: d} }

// Write streams encoded jobs into MSSQL via TVP/CopyIn. Each valid job results
// in a single ExecContext(pcv, payload). Invalid jobs are skipped and reported
// via addSkip. After the channel is drained, a final ExecContext() call with
// no args finalizes the bulk operation.
//
// Parameters:
//   - ctx: request-scoped context; cancellation aborts the batch.
//   - encodedCh: channel of encodedJob; producer closes it when done.
//   - addSkip: callback to record rejected rows with context.
//   - logEvery: cadence in rows for progress logs (>=1 recommended).
//
// Behavior:
//   - Resolves *sql.DB via db.AsSQLDB unless a test seam is provided.
//   - Constructs a CopyIn statement for ("vehicle_tech", "pcv", "payload").
//   - Skips invalid rows (pcv==0, nil payload, or invalid JSON).
//   - Increments counts and logs progress every logEvery rows.
//   - Finalizes the batch with a no-args ExecContext() call.
//
// Returns the number of successfully inserted rows and a non-nil error on
// prepare/exec/finalize failure.
func (b *mssqlWriterBackend) Write(
	ctx context.Context,
	encodedCh <-chan encodedJob,
	addSkip func(string, int, string, string),
	logEvery int,
) (int, error) {
	// Resolve preparer. In production we ask db.AsSQLDB (which returns *sql.DB),
	// in tests we inject b.prep to avoid real DBs.
	prep := b.prep
	if prep == nil {
		var ok bool
		raw, ok := db.AsSQLDB(b.dbh)
		if !ok {
			return 0, fmt.Errorf("not a SQL-backed DB")
		}
		prep = realPreparer{db: raw}
	}

	// Build a TVP/CopyIn statement for fast batched inserts.
	stmtText := mssql.CopyIn("vehicle_tech", mssql.BulkOptions{}, "pcv", "payload")
	stmt, err := prep.PrepareContext(ctx, stmtText)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	ins := 0
	for job := range encodedCh {
		// Reject rows with empty PCV, nil payload, or syntactically invalid JSON.
		if job.pcv == 0 || job.payload == nil || !json.Valid(job.payload) {
			pcvField := job.pcvField
			// Preserve observability: if the row had a numeric PCV but an empty
			// pcvField string, synthesize it for logging/CSV via strconv.
			if pcvField == "" && job.pcv > 0 {
				pcvField = strconv.FormatInt(job.pcv, 10)
			}
			addSkip("invalid_row", job.lineNum, pcvField, job.raw)
			continue
		}

		// Normal case: bind PCV and JSON payload (as text) and execute.
		if _, err := stmt.ExecContext(ctx, job.pcv, string(job.payload)); err != nil {
			return ins, err
		}
		ins++

		// Periodic progress logging to avoid noisy per-row logs.
		if ins%logEvery == 0 {
			log.Printf("vehicle_tech: streaming TVP %d", ins)
		}
	}

	// Finalize the batch; no-arg Exec signals end of input for TVP CopyIn.
	if _, err := stmt.ExecContext(ctx); err != nil {
		return ins, err
	}
	return ins, nil
}
