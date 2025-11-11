// Package sqlite implements a SQLite repository mirroring the Postgres shape.
package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "modernc.org/sqlite"
)

// Config matches the Postgres one for symmetry.
type Config struct {
	DSN        string   // path to .sqlite file, e.g. "data.sqlite"
	Table      string   // table name (no schema)
	Columns    []string // ordered columns
	KeyColumns []string // conflict keys (used for delete-then-insert strategy)
}

type Repository struct {
	db  *sql.DB
	cfg Config
}

func Open(path string) (*sql.DB, error) { return sql.Open("sqlite", path) }
func New(db *sql.DB) *Repository        { return &Repository{db: db} }

// Column is a tiny helper so web ETL can request table creation.
type Column struct {
	Name    string
	SQLType string // TEXT|INTEGER|REAL
	NotNull bool
}

// CreateTable is used by the web ETL path (not the factory path).
func (r *Repository) CreateTable(ctx context.Context, table string, cols []Column, pk string) error {
	parts := make([]string, 0, len(cols)+1)
	for _, c := range cols {
		nn := ""
		if c.NotNull {
			nn = " NOT NULL"
		}
		parts = append(parts, fmt.Sprintf(`%q %s%s`, c.Name, c.SQLType, nn))
	}
	pkClause := ""
	if pk != "" {
		pkClause = fmt.Sprintf(", PRIMARY KEY(%q)", pk)
	}
	sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %q (%s%s);`, table, strings.Join(parts, ", "), pkClause)
	_, err := r.db.ExecContext(ctx, sql)
	return err
}

func (r *Repository) InsertRows(ctx context.Context, table string, columns []string, rows [][]any) error {
	ph := make([]string, len(columns))
	for i := range columns {
		ph[i] = "?"
	}
	stmt := fmt.Sprintf(`INSERT INTO %q (%s) VALUES (%s)`, table, joinIdents(columns), strings.Join(ph, ","))
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	ps, err := tx.PrepareContext(ctx, stmt)
	if err != nil {
		return err
	}
	defer ps.Close()
	for _, row := range rows {
		if _, err := ps.ExecContext(ctx, row...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (r *Repository) Schema(ctx context.Context, table string) (string, error) {
	var ddl string
	err := r.db.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&ddl)
	return ddl, err
}

func (r *Repository) SampleField(ctx context.Context, table, field string, limit int) ([]any, error) {
	q := fmt.Sprintf(`SELECT %q FROM %q LIMIT %d`, field, table, limit)
	rows, err := r.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []any
	for rows.Next() {
		var v any
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// Search runs a simple LIKE-based fuzzy search across the provided columns.
// If cols is empty, it falls back to SELECT * with only LIMIT applied.
func (r *Repository) Search(ctx context.Context, table string, cols []string, q string, limit int) (*sql.Rows, error) {
	if limit <= 0 {
		limit = 50
	}
	base := fmt.Sprintf(`SELECT * FROM %s`, sqlIdent(table))
	if len(cols) == 0 || q == "" {
		// No columns or empty query: just return some rows.
		stmt := fmt.Sprintf(`%s LIMIT %d`, base, limit)
		return r.db.QueryContext(ctx, stmt)
	}
	like := "%" + q + "%"
	ors := make([]string, 0, len(cols))
	args := make([]any, 0, len(cols))
	for _, c := range cols {
		ors = append(ors, fmt.Sprintf(`%s LIKE ?`, sqlIdent(c)))
		args = append(args, like)
	}
	stmt := fmt.Sprintf(`%s WHERE %s LIMIT %d`, base, strings.Join(ors, " OR "), limit)
	return r.db.QueryContext(ctx, stmt, args...)
}

// === Factory-facing methods (mirror Postgres adapter expectations) ===

func NewRepository(_ context.Context, cfg Config) (*Repository, func(), error) {
	db, err := sql.Open("sqlite", cfg.DSN)
	if err != nil {
		return nil, nil, err
	}
	close := func() { _ = db.Close() }
	return &Repository{db: db, cfg: cfg}, close, nil
}

// CopyFrom: emulate COPY using a single prepared statement in a transaction.
func (r *Repository) CopyFrom(ctx context.Context, columns []string, rows [][]any) (int64, error) {
	ph := make([]string, len(columns))
	for i := range columns {
		ph[i] = "?"
	}
	stmt := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)`,
		sqlIdent(r.cfg.Table), joinIdents(columns), strings.Join(ph, ","))
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	ps, err := tx.PrepareContext(ctx, stmt)
	if err != nil {
		return 0, err
	}
	defer ps.Close()
	var n int64
	for _, row := range rows {
		res, err := ps.ExecContext(ctx, row...)
		if err != nil {
			return n, err
		}
		if c, _ := res.RowsAffected(); c > 0 {
			n += c
		}
	}
	if err := tx.Commit(); err != nil {
		return n, err
	}
	return n, nil
}

// BulkUpsert: simple delete-then-insert using key columns.
func (r *Repository) BulkUpsert(ctx context.Context, recs []map[string]any, keyCols []string, _ string) (int64, error) {
	if len(recs) == 0 {
		return 0, nil
	}
	cols := r.cfg.Columns
	if len(cols) == 0 {
		return 0, fmt.Errorf("no columns configured")
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	// delete duplicates if keys provided
	if len(keyCols) > 0 {
		where := make([]string, len(keyCols))
		for i, k := range keyCols {
			where[i] = fmt.Sprintf(`%s = ?`, sqlIdent(k))
		}
		del := fmt.Sprintf(`DELETE FROM %s WHERE %s`, sqlIdent(r.cfg.Table), strings.Join(where, " AND "))
		psDel, err := tx.PrepareContext(ctx, del)
		if err != nil {
			return 0, err
		}

		// Reuse args slice to avoid per-row allocs.
		args := make([]any, len(keyCols))
		for _, m := range recs {
			for i, k := range keyCols {
				args[i] = m[k]
			}
			if _, err := psDel.ExecContext(ctx, args...); err != nil {
				psDel.Close()
				return 0, err
			}
		}
		psDel.Close()
	}

	// insert
	ph := make([]string, len(cols))
	for i := range cols {
		ph[i] = "?"
	}
	ins := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)`, sqlIdent(r.cfg.Table), joinIdents(cols), strings.Join(ph, ","))
	psIns, err := tx.PrepareContext(ctx, ins)
	if err != nil {
		return 0, err
	}
	var n int64
	args := make([]any, len(cols))
	for _, m := range recs {
		for i, c := range cols {
			args[i] = m[c]
		}
		res, err := psIns.ExecContext(ctx, args...)
		if err != nil {
			psIns.Close()
			return n, err
		}
		if c, _ := res.RowsAffected(); c > 0 {
			n += c
		}
	}
	psIns.Close()

	if err := tx.Commit(); err != nil {
		return n, err
	}
	return n, nil
}

func (r *Repository) Exec(ctx context.Context, sql string) error {
	_, err := r.db.ExecContext(ctx, sql)
	return err
}

func sqlIdent(id string) string {
	// simple quoting for single-segment names
	return `"` + strings.ReplaceAll(id, `"`, `""`) + `"`
}
func joinIdents(cols []string) string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = sqlIdent(c)
	}
	return strings.Join(out, ",")
}
