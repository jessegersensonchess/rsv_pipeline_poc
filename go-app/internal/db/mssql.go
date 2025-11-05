// Package db provides a portable SQL adapter (e.g., MSSQL via database/sql).
// The adapter favors portability over engine-specific bulk paths, so COPY-like
// operations fall back to prepared INSERT statements executed in batches.
// This is slower than engine-native COPY but keeps import code database-agnostic.
package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

//
// =======================
//  Testability-first seams
// =======================
//
// We keep sqlDBCore compatible with *sql.DB (BeginTx returns *sql.Tx) so callers
// and legacy tests can still inject a real *sql.DB. Internally, we adapt *sql.Tx
// to a smaller sqlTxCore that returns a stmtCore from PrepareContext so unit
// tests can inject light fakes—no sockets required.
//

// stmtCore is the minimal subset of *sql.Stmt we use.
type stmtCore interface {
	ExecContext(ctx context.Context, args ...any) (sql.Result, error)
	Close() error
}

// sqlTxCore is the subset of a transaction that sqlTx uses.
type sqlTxCore interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	PrepareContext(ctx context.Context, query string) (stmtCore, error)
	Commit() error
	Rollback() error
}

// sqlDBCore is the minimal subset of *sql.DB we use. It must match *sql.DB.
type sqlDBCore interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	Close() error
}

//
// ============================
//  Real wrappers for production
// ============================
//
// These wrap real *sql.DB/*sql.Tx/*sql.Stmt to satisfy our seams.
//

type realStmt struct{ s *sql.Stmt }

func (r realStmt) ExecContext(ctx context.Context, args ...any) (sql.Result, error) {
	return r.s.ExecContext(ctx, args...)
}
func (r realStmt) Close() error { return r.s.Close() }

type realSQLTx struct{ tx *sql.Tx }

func (r realSQLTx) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	return r.tx.ExecContext(ctx, q, args...)
}
func (r realSQLTx) PrepareContext(ctx context.Context, q string) (stmtCore, error) {
	st, err := r.tx.PrepareContext(ctx, q)
	if err != nil {
		return nil, err
	}
	return realStmt{st}, nil
}
func (r realSQLTx) Commit() error   { return r.tx.Commit() }
func (r realSQLTx) Rollback() error { return r.tx.Rollback() }

type realSQLDB struct{ db *sql.DB }

func (r realSQLDB) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	return r.db.ExecContext(ctx, q, args...)
}
func (r realSQLDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return r.db.BeginTx(ctx, opts)
}
func (r realSQLDB) Close() error { return r.db.Close() }

// RawDB exposes the underlying *sql.DB for AsSQLDB.
func (r realSQLDB) RawDB() *sql.DB { return r.db }

//
// ===================
//  sqlDB (DB adapter)
// ===================
//
// This is the portable SQL adapter used for engines behind database/sql.
//

type sqlDB struct{ db sqlDBCore }

// NewSQLDB opens a database connection and pings to confirm connectivity.
func NewSQLDB(driver, dsn string) (DB, error) {
	d, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	if err := d.Ping(); err != nil {
		_ = d.Close()
		return nil, err
	}
	return &sqlDB{db: realSQLDB{db: d}}, nil
}

// Exec forwards a statement to the underlying database.
func (s *sqlDB) Exec(ctx context.Context, q string, args ...any) error {
	_, err := s.db.ExecContext(ctx, q, args...)
	return err
}

// BeginTx starts a transaction and returns a Tx adapter. We wrap the *sql.Tx
// returned by sqlDBCore into realSQLTx (sqlTxCore) so sqlTx can remain mockable.
func (s *sqlDB) BeginTx(ctx context.Context) (Tx, error) {
	raw, err := s.db.BeginTx(ctx, nil) // *sql.Tx
	if err != nil {
		return nil, err
	}
	return &sqlTx{tx: realSQLTx{tx: raw}}, nil
}

// Close closes the underlying database connection.
func (s *sqlDB) Close(ctx context.Context) error { return s.db.Close() }

//
// ==============
//  sqlTx (Tx adapter)
// ==============
//
// sqlTx wraps sqlTxCore to implement the portable Tx interface.
//

type sqlTx struct{ tx sqlTxCore }

// Exec forwards execution to the transaction and returns any error.
func (t *sqlTx) Exec(ctx context.Context, q string, args ...any) error {
	_, err := t.tx.ExecContext(ctx, q, args...)
	return err
}

// CopyInto emulates bulk insert by preparing an INSERT and executing once per row.
// Example: INSERT INTO table (c1,c2) VALUES (@p1,@p2)
func (t *sqlTx) CopyInto(ctx context.Context, table string, columns []string, rows [][]interface{}) (int64, error) {
	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = fmt.Sprintf("@p%d", i+1) // SQL Server style
	}
	stmtText := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		table,
		strings.Join(columns, ","),
		strings.Join(placeholders, ","),
	)

	stmt, err := t.tx.PrepareContext(ctx, stmtText)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	var inserted int64
	for _, row := range rows {
		if _, err := stmt.ExecContext(ctx, row...); err != nil {
			return inserted, err
		}
		inserted++
	}
	return inserted, nil
}

// Commit commits the active transaction.
func (t *sqlTx) Commit(ctx context.Context) error { return t.tx.Commit() }

// Rollback aborts the active transaction.
func (t *sqlTx) Rollback(ctx context.Context) error { return t.tx.Rollback() }

//
// ========
//  helpers
// ========

// join concatenates a slice of strings using sep without allocating a builder.
func join(ss []string, sep string) string {
	if len(ss) == 0 {
		return ""
	}
	out := ss[0]
	for i := 1; i < len(ss); i++ {
		out += sep + ss[i]
	}
	return out
}

//
// ==================
//  smallMSSQL utility
// ==================
//
// smallMSSQL is a narrow adapter used by smaller ingestion pipelines. We keep
// the same sqlDBCore seam so tests can inject fakes or a real *sql.DB via
// the realSQLDB wrapper.
//

type smallMSSQL struct {
	db sqlDBCore
}

// NewSmallMSSQL opens a SQL Server connection using the "sqlserver" driver.
func NewSmallMSSQL(dsn string) (SmallDB, error) {
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &smallMSSQL{db: realSQLDB{db: db}}, nil
}

// CreateOwnershipTable ensures the "ownership" table exists with SQL Server types.
func (m *smallMSSQL) CreateOwnershipTable(ctx context.Context) error {
	ddl := `
	IF OBJECT_ID(N'ownership', N'U') IS NULL
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
	)`
	_, err := m.db.ExecContext(ctx, ddl)
	return err
}

// CopyOwnership inserts records into ownership via a prepared INSERT inside a tx.
func (m *smallMSSQL) CopyOwnership(ctx context.Context, records [][]interface{}) error {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO ownership(
			pcv, typ_subjektu, vztah_k_vozidlu, aktualni, ico, nazev, adresa, datum_od, datum_do
		) VALUES (@p1,@p2,@p3,@p4,@p5,@p6,@p7,@p8,@p9)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, r := range records {
		if _, err := stmt.ExecContext(ctx, r...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// CreateTechInspectionsTable ensures the "tech_inspections" table exists.
func (m *smallMSSQL) CreateTechInspectionsTable(ctx context.Context) error {
	ddl := `
	IF OBJECT_ID(N'tech_inspections', N'U') IS NULL
	BEGIN
	  CREATE TABLE tech_inspections (
	    pcv INT,
	    typ NVARCHAR(100),
	    stav NVARCHAR(100),
	    kod_stk INT,
	    nazev_stk NVARCHAR(MAX),
	    platnost_od DATE,
	    platnost_do DATE,
	    cislo_protokolu NVARCHAR(100),
	    aktualni BIT
	  );
	END
	IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'tech_inspections_pcv_idx' AND object_id = OBJECT_ID(N'tech_inspections'))
	  CREATE INDEX tech_inspections_pcv_idx ON tech_inspections(pcv);`
	_, err := m.db.ExecContext(ctx, ddl)
	return err
}

// CopyTechInspections inserts rows into tech_inspections in a single transaction.
func (m *smallMSSQL) CopyTechInspections(ctx context.Context, records [][]interface{}) error {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO tech_inspections(
			pcv, typ, stav, kod_stk, nazev_stk,
			platnost_od, platnost_do, cislo_protokolu, aktualni
		) VALUES (@p1,@p2,@p3,@p4,@p5,@p6,@p7,@p8,@p9)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, r := range records {
		if _, err := stmt.ExecContext(ctx, r...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// CreateRSVZpravyTable ensures the "rsv_zpravy_vyrobce_zastupce" table exists.
func (m *smallMSSQL) CreateRSVZpravyTable(ctx context.Context) error {
	ddl := `
	IF OBJECT_ID(N'rsv_zpravy_vyrobce_zastupce', N'U') IS NULL
	BEGIN
	  CREATE TABLE rsv_zpravy_vyrobce_zastupce (
	    pcv INT,
	    kratky_text NVARCHAR(MAX)
	  );
	END
	IF NOT EXISTS (
	  SELECT 1
	  FROM sys.indexes
	  WHERE name = 'rsv_zpravy_vyrobce_zastupce_pcv_idx'
	    AND object_id = OBJECT_ID(N'rsv_zpravy_vyrobce_zastupce')
	)
	CREATE INDEX rsv_zpravy_vyrobce_zastupce_pcv_idx ON rsv_zpravy_vyrobce_zastupce(pcv);`
	_, err := m.db.ExecContext(ctx, ddl)
	return err
}

// CopyRSVZpravy inserts rows into the rsv_zpravy_vyrobce_zastupce table.
func (m *smallMSSQL) CopyRSVZpravy(ctx context.Context, records [][]interface{}) error {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO rsv_zpravy_vyrobce_zastupce (pcv, kratky_text)
		VALUES (@p1, @p2)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, r := range records {
		if _, err := stmt.ExecContext(ctx, r...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Close closes the underlying connection.
func (m *smallMSSQL) Close(ctx context.Context) error { return m.db.Close() }

//
// ==============================
//  Adapter Introspection Helpers
// ==============================
//
// AsSQLDB exposes the underlying *sql.DB for callers that need raw access.
// We handle both sqlDB{db: *sql.DB} and sqlDB{db: realSQLDB{db: *sql.DB}}.
//

func AsSQLDB(d DB) (*sql.DB, bool) {
	s, ok := d.(*sqlDB)
	if !ok {
		return nil, false
	}
	switch core := any(s.db).(type) {
	case *sql.DB:
		return core, true
	case realSQLDB:
		return core.RawDB(), true
	default:
		return nil, false
	}
}

//
// =======================
//  Test-only constructors
// =======================
//
// These helpers allow injection of fakes and test doubles for hermetic tests.
//

// newSQLTxForTest wraps a fake sqlTxCore as a Tx.
func newSQLTxForTest(core sqlTxCore) *sqlTx { return &sqlTx{tx: core} }

// newSQLDBForTest wraps a fake sqlDBCore as a DB.
func newSQLDBForTest(core sqlDBCore) *sqlDB { return &sqlDB{db: core} }
