// Package mssql implements a Microsoft SQL Server repository using the
// go-mssqldb bulk copy API. It performs a bulk insert into a temporary table
// (#temp) followed by a delete+insert into the target table.
package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	mssql "github.com/microsoft/go-mssqldb"
	"github.com/microsoft/go-mssqldb/msdsn"
)

// Config holds MSSQL repository configuration.
type Config struct {
	DSN        string
	Table      string
	Columns    []string
	KeyColumns []string
}

// Repository is an MSSQL-backed implementation of storage.Repository.
type Repository struct {
	db  *sql.DB
	cfg Config
}

// NewRepository constructs a Repository and returns a Close function for cleanup.
func NewRepository(ctx context.Context, cfg Config) (*Repository, func(), error) {
	// Validate DSN early to fail fast on obvious mistakes.
	if _, err := msdsn.Parse(cfg.DSN); err != nil {
		return nil, nil, fmt.Errorf("mssql dsn: %w", err)
	}
	db, err := sql.Open("sqlserver", cfg.DSN)
	if err != nil {
		return nil, nil, fmt.Errorf("sql.Open: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("ping: %w", err)
	}
	close := func() { _ = db.Close() }
	return &Repository{db: db, cfg: cfg}, close, nil
}

// depreciated: confirm it's not used, then remove
// // BulkUpsert performs a delete-then-insert via a session-scoped temporary table.
// func (r *Repository) BulkUpsert(
// 	ctx context.Context,
// 	recs []map[string]any,
// 	keyColumns []string,
// 	_ string, // dateColumn unused (parity with interface)
// ) (int64, error) {
// 	if len(recs) == 0 {
// 		return 0, nil
// 	}
// 	cols := r.cfg.Columns
// 	if len(cols) == 0 {
// 		return 0, fmt.Errorf("no columns configured")
// 	}
//
// 	tmp := "#tmp_" + strings.ReplaceAll(r.cfg.Table, ".", "_")
// 	fqTable := msFQN(r.cfg.Table)
//
// 	tx, err := r.db.BeginTx(ctx, nil)
// 	if err != nil {
// 		return 0, fmt.Errorf("begin tx: %w", err)
// 	}
// 	rollback := func() { _ = tx.Rollback() }
//
// 	// 1) Create temp table with same shape as target.
// 	create := fmt.Sprintf(
// 		"SELECT TOP 0 %s INTO %s FROM %s WITH (HOLDLOCK)",
// 		strings.Join(mapIdent(cols), ","),
// 		msIdent(tmp),
// 		fqTable,
// 	)
// 	if _, err := tx.ExecContext(ctx, create); err != nil {
// 		rollback()
// 		return 0, fmt.Errorf("create temp: %w", err)
// 	}
//
// 	// 2) Add diagnostic column.
// 	if _, err := tx.ExecContext(ctx,
// 		fmt.Sprintf("ALTER TABLE %s ADD %s INT NULL", msIdent(tmp), msIdent("__rownum")),
// 	); err != nil {
// 		rollback()
// 		return 0, fmt.Errorf("alter temp: %w", err)
// 	}
//
// 	// 3) Bulk copy rows into #tmp.
// 	colsWithRow := append(append([]string{}, cols...), "__rownum")
// 	stmt, err := tx.PrepareContext(ctx, mssql.CopyIn(tmp, mssql.BulkOptions{}, colsWithRow...))
// 	if err != nil {
// 		rollback()
// 		return 0, fmt.Errorf("prepare bulk copy: %w", err)
// 	}
//
// 	const rownumBase = 2 // CSV data starts on line 2
// 	for i, rec := range recs {
// 		row := make([]any, len(colsWithRow))
// 		for j, c := range cols {
// 			row[j] = toCopyVal(rec[c])
// 		}
// 		row[len(colsWithRow)-1] = i + rownumBase
// 		if _, err := stmt.ExecContext(ctx, row...); err != nil {
// 			_ = stmt.Close()
// 			rollback()
// 			return 0, fmt.Errorf("bulk row %d: %w", i, err)
// 		}
// 	}
// 	res, err := stmt.ExecContext(ctx) // flush
// 	if cerr := stmt.Close(); cerr != nil && err == nil {
// 		err = cerr
// 	}
// 	if err != nil {
// 		rollback()
// 		return 0, fmt.Errorf("bulk finalize: %w", err)
// 	}
// 	copied, err := res.RowsAffected()
// 	if err != nil {
// 		rollback()
// 		return 0, fmt.Errorf("rows affected: %w", err)
// 	}
// 	log.Printf("Successfully bulk-copied %d rows into %s", copied, tmp)
//
// 	// 4) Delete matching rows if keys provided.
// 	if len(keyColumns) > 0 {
// 		cond := buildDeleteCondition(keyColumns)
// 		del := fmt.Sprintf(
// 			`DELETE T
// 			   FROM %s AS T
// 			   INNER JOIN %s AS S
// 			       ON %s`,
// 			fqTable, msIdent(tmp), cond,
// 		)
// 		if _, err := tx.ExecContext(ctx, del); err != nil {
// 			rollback()
// 			return 0, fmt.Errorf("delete matching rows: %w", err)
// 		}
// 	}
//
// 	// 5) Insert into target excluding "id".
// 	var colsWithoutID []string
// 	for _, c := range cols {
// 		if c != "id" {
// 			colsWithoutID = append(colsWithoutID, c)
// 		}
// 	}
// 	insert := fmt.Sprintf(
// 		`INSERT INTO %s (%s)
// 		  SELECT %s FROM %s`,
// 		fqTable,
// 		strings.Join(mapIdent(colsWithoutID), ","),
// 		strings.Join(mapIdent(colsWithoutID), ","),
// 		msIdent(tmp),
// 	)
// 	if _, err := tx.ExecContext(ctx, insert); err != nil {
// 		rollback()
// 		return 0, fmt.Errorf("insert phase: %w", err)
// 	}
//
// 	if err := tx.Commit(); err != nil {
// 		return 0, fmt.Errorf("commit: %w", err)
// 	}
// 	return copied, nil
// }

// CopyFrom performs a bulk insert directly into the configured target table.
func (r *Repository) CopyFrom(ctx context.Context, columns []string, rows [][]any) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	rollback := func() { _ = tx.Rollback() }

	stmt, err := tx.PrepareContext(ctx, mssql.CopyIn(r.cfg.Table, mssql.BulkOptions{}, columns...))
	if err != nil {
		rollback()
		return 0, fmt.Errorf("prepare bulk: %w", err)
	}
	for i := range rows {
		if _, err := stmt.ExecContext(ctx, rows[i]...); err != nil {
			_ = stmt.Close()
			rollback()
			return 0, fmt.Errorf("bulk row %d: %w", i, err)
		}
	}
	res, err := stmt.ExecContext(ctx)
	if cerr := stmt.Close(); cerr != nil && err == nil {
		err = cerr
	}
	if err != nil {
		rollback()
		return 0, fmt.Errorf("bulk finalize: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		rollback()
		return 0, fmt.Errorf("rows affected: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return n, nil
}

// Exec executes a SQL statement against the pool.
func (r *Repository) Exec(ctx context.Context, sqlText string) error {
	_, err := r.db.ExecContext(ctx, sqlText)
	return err
}

// buildDeleteCondition builds the T=S equality join for the provided key columns.
func buildDeleteCondition(keyColumns []string) string {
	conds := make([]string, 0, len(keyColumns))
	for _, col := range keyColumns {
		conds = append(conds, fmt.Sprintf("T.%s = S.%s", msIdent(col), msIdent(col)))
	}
	return strings.Join(conds, " AND ")
}

// filterConflictKeys mirrors helper semantics used in the Postgres adapter.
func filterConflictKeys(setParts []string, keys []string) []string {
	keySet := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		keySet[k] = struct{}{}
	}
	var out []string
	for _, part := range setParts {
		col := strings.Split(part, " = ")[0]
		if _, isKey := keySet[col]; !isKey {
			out = append(out, part)
		}
	}
	return out
}

// toCopyVal passes values through as-is; nil stays nil.
func toCopyVal(v any) any {
	if v == nil {
		return nil
	}
	return v
}

// msIdent safely quotes a SQL Server identifier using [brackets], escaping ].
func msIdent(id string) string { return `[` + strings.ReplaceAll(id, `]`, `]]`) + `]` }

// msFQN quotes a possibly schema-qualified name like "dbo.hr_events" to
// "[dbo].[hr_events]". If no dot is present, returns a single quoted ident.
func msFQN(name string) string {
	parts := strings.Split(name, ".")
	for i, p := range parts {
		parts[i] = msIdent(p)
	}
	return strings.Join(parts, ".")
}

// mapIdent maps a list of column names to their bracket-quoted forms.
func mapIdent(cols []string) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = msIdent(c)
	}
	return out
}
