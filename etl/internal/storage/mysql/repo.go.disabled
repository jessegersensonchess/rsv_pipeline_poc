// Package mysql implements a MySQL repository using database/sql and the
// go-sql-driver/mysql driver. It performs a bulk-style insert into a
// session-scoped TEMPORARY TABLE followed by a delete+insert into the target
// table. This mirrors the behavior of the other adapters to keep callers
// backend-agnostic.
package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	mysql "github.com/go-sql-driver/mysql"
)

// Config holds MySQL repository configuration.
type Config struct {
	DSN        string   // DSN for database/sql (e.g., user:pass@tcp(host:3306)/db?parseTime=true)
	Table      string   // fully qualified target table name, e.g., "hr.hr_events"
	Columns    []string // ordered columns for inserts
	KeyColumns []string // join condition columns for delete-matching-rows
}

// Repository is a MySQL-backed implementation of storage.Repository.
type Repository struct {
	db  *sql.DB
	cfg Config
}

// NewRepository constructs a Repository and returns a Close function for cleanup.
func NewRepository(ctx context.Context, cfg Config) (*Repository, func(), error) {
	// Validate DSN early to fail fast.
	if _, err := mysql.ParseDSN(cfg.DSN); err != nil {
		return nil, nil, fmt.Errorf("mysql dsn: %w", err)
	}
	db, err := sql.Open("mysql", cfg.DSN)
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

// depreciated
// // BulkUpsert performs a delete-then-insert via a session-scoped TEMPORARY TABLE.
// // Steps:
// //  1. CREATE TEMPORARY TABLE tmp AS SELECT <cols> FROM target WHERE 1=0
// //  2. ALTER TABLE tmp ADD COLUMN `__rownum` INT NULL
// //  3. Multi-row INSERT INTO tmp
// //  4. If key columns provided: DELETE T FROM target T INNER JOIN tmp S ON ...
// //  5. INSERT INTO target(<cols-without-id>) SELECT <cols-without-id> FROM tmp
// //
// // Returns the number of rows inserted into the temp table.
// func (r *Repository) BulkUpsert(
// 	ctx context.Context,
// 	recs []map[string]any,
// 	keyColumns []string,
// 	_ string, // dateColumn unused; kept for parity with interface
// ) (int64, error) {
// 	if len(recs) == 0 {
// 		return 0, nil
// 	}
// 	cols := r.cfg.Columns
// 	if len(cols) == 0 {
// 		return 0, fmt.Errorf("no columns configured")
// 	}
//
// 	tmp := "tmp_" + strings.ReplaceAll(r.cfg.Table, ".", "_")
// 	fq := myFQN(r.cfg.Table)
//
// 	tx, err := r.db.BeginTx(ctx, nil)
// 	if err != nil {
// 		return 0, fmt.Errorf("begin tx: %w", err)
// 	}
// 	rollback := func() { _ = tx.Rollback() }
//
// 	// 1) Create temp table with the same selected columns.
// 	create := fmt.Sprintf(
// 		"CREATE TEMPORARY TABLE %s AS SELECT %s FROM %s WHERE 1=0",
// 		myIdent(tmp), strings.Join(mapIdent(cols), ","), fq,
// 	)
// 	if _, err := tx.ExecContext(ctx, create); err != nil {
// 		rollback()
// 		return 0, fmt.Errorf("create temp: %w", err)
// 	}
//
// 	// 2) Add diagnostic column.
// 	if _, err := tx.ExecContext(ctx,
// 		fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s INT NULL", myIdent(tmp), myIdent("__rownum")),
// 	); err != nil {
// 		rollback()
// 		return 0, fmt.Errorf("alter temp: %w", err)
// 	}
//
// 	// 3) Insert rows into the temp table (multi-row insert).
// 	colsWithRow := append(append([]string{}, cols...), "__rownum")
// 	if _, err := insertMulti(ctx, tx, tmp, colsWithRow, recs); err != nil {
// 		rollback()
// 		return 0, fmt.Errorf("insert into temp: %w", err)
// 	}
// 	copied := int64(len(recs))
//
// 	// 4) Delete matching rows if keys provided.
// 	if len(keyColumns) > 0 {
// 		cond := buildDeleteCondition(keyColumns)
// 		del := fmt.Sprintf(
// 			`DELETE T FROM %s AS T
// 			 INNER JOIN %s AS S
// 			    ON %s`,
// 			fq, myIdent(tmp), cond,
// 		)
// 		if _, err := tx.ExecContext(ctx, del); err != nil {
// 			rollback()
// 			return 0, fmt.Errorf("delete matching rows: %w", err)
// 		}
// 	}
//
// 	// 5) Insert into target excluding "id".
// 	var colsNoID []string
// 	for _, c := range cols {
// 		if c != "id" {
// 			colsNoID = append(colsNoID, c)
// 		}
// 	}
// 	insert := fmt.Sprintf(
// 		`INSERT INTO %s (%s)
// 		  SELECT %s FROM %s`,
// 		fq,
// 		strings.Join(mapIdent(colsNoID), ","),
// 		strings.Join(mapIdent(colsNoID), ","),
// 		myIdent(tmp),
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

// CopyFrom performs a batch INSERT into the configured target table. It is used
// by the streaming loader. Rows are grouped into a single multi-row INSERT.
func (r *Repository) CopyFrom(ctx context.Context, columns []string, rows [][]any) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	rollback := func() { _ = tx.Rollback() }

	// Build values and args for multi-row insert.
	values, args := buildValuesAndArgs(columns, rows)
	stmt := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		myFQN(r.cfg.Table), strings.Join(mapIdent(columns), ","), values,
	)

	res, err := tx.ExecContext(ctx, stmt, args...)
	if err != nil {
		rollback()
		return 0, fmt.Errorf("exec insert: %w", err)
	}
	aff, err := res.RowsAffected()
	if err != nil {
		rollback()
		return 0, fmt.Errorf("rows affected: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return aff, nil
}

// Exec executes a SQL statement against the pool.
func (r *Repository) Exec(ctx context.Context, sqlText string) error {
	_, err := r.db.ExecContext(ctx, sqlText)
	return err
}

// insertMulti inserts recs into a given table name with the provided columns.
// It adds a 1-based CSV row number into the trailing `__rownum` column.
func insertMulti(ctx context.Context, tx *sql.Tx, table string, cols []string, recs []map[string]any) (sql.Result, error) {
	if len(recs) == 0 {
		return dummyResult(0), nil
	}
	// Prepare rows → [][]any to share the same builder as CopyFrom.
	rows := make([][]any, 0, len(recs))
	const rownumBase = 2
	for i, rec := range recs {
		row := make([]any, len(cols))
		// All but the last column map from rec
		for j := 0; j < len(cols)-1; j++ {
			row[j] = toCopyVal(rec[cols[j]])
		}
		// Last column is __rownum
		row[len(cols)-1] = i + rownumBase
		rows = append(rows, row)
	}

	values, args := buildValuesAndArgs(cols, rows)
	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", myIdent(table), strings.Join(mapIdent(cols), ","), values)
	return tx.ExecContext(ctx, stmt, args...)
}

// buildValuesAndArgs builds a "(?, ?, ?),(...)" VALUES clause and a flat args slice.
func buildValuesAndArgs(columns []string, rows [][]any) (string, []any) {
	var (
		sb    strings.Builder
		args  = make([]any, 0, len(rows)*len(columns))
		tuple = "(" + strings.TrimRight(strings.Repeat("?,", len(columns)), ",") + ")"
	)
	for i := range rows {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(tuple)
		args = append(args, rows[i]...)
	}
	return sb.String(), args
}

// buildDeleteCondition builds the T=S equality join for the provided key columns.
func buildDeleteCondition(keyColumns []string) string {
	conds := make([]string, 0, len(keyColumns))
	for _, col := range keyColumns {
		conds = append(conds, fmt.Sprintf("T.%s = S.%s", myIdent(col), myIdent(col)))
	}
	return strings.Join(conds, " AND ")
}

// filterConflictKeys mirrors helper semantics in other adapters.
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

// myIdent safely quotes a MySQL identifier using backticks, escaping backticks.
func myIdent(id string) string { return "`" + strings.ReplaceAll(id, "`", "``") + "`" }

// myFQN quotes "schema.table" to "`schema`.`table`". If no dot, returns single.
func myFQN(name string) string {
	parts := strings.Split(name, ".")
	for i, p := range parts {
		parts[i] = myIdent(p)
	}
	return strings.Join(parts, ".")
}

// mapIdent maps a list of column names to their backtick-quoted forms.
func mapIdent(cols []string) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = myIdent(c)
	}
	return out
}

// dummyResult implements sql.Result for zero-row operations in tests/helpers.
type dummyResult int64

func (d dummyResult) LastInsertId() (int64, error) { return 0, nil }
func (d dummyResult) RowsAffected() (int64, error) { return int64(d), nil }
