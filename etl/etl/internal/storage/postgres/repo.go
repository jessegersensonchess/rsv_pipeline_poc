// Package postgres implements a Postgres repository using pgx v5. It performs
// a COPY into a temporary table followed by an upsert into the target table.
package postgres

import (
	"context"
	"errors"
	"etl/pkg/records"
	"fmt"
	"log"
	"strings"

	// Correct import for v5
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Config holds Postgres repository configuration.
type Config struct {
	DSN        string   // connection string for pgxpool
	Table      string   // fully qualified target table name, e.g., "public.hr_events"
	Columns    []string // ordered columns for COPY and INSERT
	KeyColumns []string // conflict target columns
}

// Repository is a Postgres-backed implementation of storage.Repository.
type Repository struct {
	pool *pgxpool.Pool
	cfg  Config
}

// NewRepository constructs a Repository and returns a Close function for cleanup.
func NewRepository(ctx context.Context, cfg Config) (*Repository, func(), error) {
	pool, err := pgxpool.New(ctx, cfg.DSN)
	if err != nil {
		return nil, nil, fmt.Errorf("pgxpool: %w", err)
	}
	close := func() { pool.Close() }
	return &Repository{pool: pool, cfg: cfg}, close, nil
}

// updateColumns generates a list of column updates in the format: "col = EXCLUDED.col"
func updateColumns(cols []string) []string {
	var updates []string
	for _, col := range cols {
		updates = append(updates, fmt.Sprintf("%s = EXCLUDED.%s", pgIdent(col), pgIdent(col)))
	}
	return updates
}

// DEPRECATED: Prefer the streaming path (CopyFrom + LoadBatches).
// BulkUpsert will be removed after migration is complete.

func (r *Repository) BulkUpsert(ctx context.Context, recs []records.Record, keyColumns []string, dateColumn string) (int64, error) {
	// Check if there's any record to process
	if len(recs) == 0 {
		return 0, nil
	}

	cols := r.cfg.Columns
	if len(cols) == 0 {
		return 0, fmt.Errorf("no columns configured")
	}

	tmp := "tmp_" + strings.ReplaceAll(r.cfg.Table, ".", "_")
	fqTable := pgFQN(r.cfg.Table)

	// Acquire the connection from the pool
	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Release()

	// Step 1: Create a temporary staging table (if it doesn't exist)
	create := fmt.Sprintf(
		"CREATE TEMP TABLE %s AS SELECT %s FROM %s WHERE false",
		pgIdent(tmp), strings.Join(mapIdent(cols), ","), fqTable,
	)
	if _, err := conn.Exec(ctx, create); err != nil {
		return 0, fmt.Errorf("create temp: %w", err)
	}
	defer func() { _, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+pgIdent(tmp)) }()

	// Add a diagnostic column for CSV row numbers (1-based; header at 1)
	if _, err := conn.Exec(ctx, fmt.Sprintf(
		"ALTER TABLE %s ADD COLUMN __rownum integer", pgIdent(tmp),
	)); err != nil {
		return 0, fmt.Errorf("alter temp: %w", err)
	}

	// Step 3: Delete rows that match the key columns in the target table.
	// If no keyColumns were provided, skip this step entirely.
	if len(keyColumns) > 0 {
		cond := buildDeleteCondition(keyColumns)
		del := fmt.Sprintf(
			`DELETE FROM %s AS T
             USING %s AS S
             WHERE %s`,
			fqTable, pgIdent(tmp), cond,
		)
		if _, err := conn.Exec(ctx, del); err != nil {
			return 0, fmt.Errorf("delete matching rows: %w", err)
		}
	}
	// Step 4: Insert records into the staging table
	colsWithRow := append(append([]string{}, cols...), "__rownum")
	rows := make([][]any, 0, len(recs))
	for i, rec := range recs {
		row := make([]any, len(colsWithRow))
		for j, c := range cols {
			row[j] = rec[c]
		}
		row[len(colsWithRow)-1] = i + 2 // CSV data starts on line 2
		rows = append(rows, row)
	}

	if len(recs) == 0 {
		log.Println("No records to insert.")
	} else {
		log.Printf("Preparing to insert %d records.", len(recs))
	}
	//	var countRecords int64
	countRecords, err := conn.CopyFrom(ctx, pgx.Identifier{tmp}, colsWithRow, pgx.CopyFromRows(rows))
	log.Printf("DEBUG: countRecords %d rows", len(rows))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Detail != "" {
			return 0, fmt.Errorf("copy into temp: %s (%s)", pgErr.Detail, pgErr.SQLState())
		}
		return 0, fmt.Errorf("copy into temp: %w", err)
	} else {
		log.Printf("Successfully inserted %d rows into the temporary table.", len(rows))
		log.Printf("Successfully inserted %d rows into DB", countRecords)
	}

	// Step 5: Simple Insert without ON CONFLICT clause
	// Remove "id" from the columns and keyColumns list
	colsWithoutID := []string{}
	for _, col := range cols {
		if col != "id" {
			colsWithoutID = append(colsWithoutID, col)
		}
	}

	// Remove "id" from keyColumns as well
	keyColumnsWithoutID := []string{}
	for _, keyCol := range keyColumns {
		if keyCol != "id" {
			keyColumnsWithoutID = append(keyColumnsWithoutID, keyCol)
		}
	}

	// Update the insert query to exclude "id"
	insert := fmt.Sprintf(
		`INSERT INTO %s (%s)
     SELECT %s FROM %s`,
		fqTable,
		strings.Join(mapIdent(colsWithoutID), ","), // Exclude "id" here
		strings.Join(mapIdent(colsWithoutID), ","), // Exclude "id" from values as well
		pgIdent(tmp),
	)

	log.Printf("Executing insert query: %s", insert)

	_, err = conn.Exec(ctx, insert)
	if err != nil {
		return 0, fmt.Errorf("insert phase: %w", err)
	}

	return countRecords, nil
}

// buildDeleteCondition builds the condition for deleting matching rows based on key_columns and date_column.
func buildDeleteCondition(keyColumns []string) string {
	conds := make([]string, 0, len(keyColumns)+1)
	for _, col := range keyColumns {
		conds = append(conds, fmt.Sprintf("T.%s = S.%s", pgIdent(col), pgIdent(col)))
	}
	return strings.Join(conds, " AND ")
}

// filterConflictKeys ensures that the columns in the conflict condition are not included in the update statement.
func filterConflictKeys(setParts []string, keys []string) []string {
	keySet := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		keySet[k] = struct{}{}
	}

	var result []string
	for _, part := range setParts {
		// Skip parts that are part of the conflict key (e.g., pcv, date_from)
		col := strings.Split(part, " = ")[0]
		if _, isKey := keySet[col]; !isKey {
			result = append(result, part)
		}
	}
	return result
}

// toCopyVal passes values to pgx as-is so it can encode them properly.
// nil stays nil; everything else returns unchanged.
func toCopyVal(v any) any {
	if v == nil {
		return nil
	}
	return v
}

// pgIdent safely quotes a single identifier segment for Postgres.
func pgIdent(id string) string { return `"` + strings.ReplaceAll(id, `"`, `""`) + `"` }

// pgFQN quotes a possibly schema-qualified name like "public.hr_events" to
// "public"."hr_events". If no dot is present, returns a single quoted ident.
func pgFQN(name string) string {
	parts := strings.Split(name, ".")
	for i, p := range parts {
		parts[i] = pgIdent(p)
	}
	return strings.Join(parts, ".")
}

// mapIdent maps a list of column names to their quoted forms.
func mapIdent(cols []string) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = pgIdent(c)
	}
	return out
}

// toString converts values to their string representation suitable for TEXT.
func toString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	default:
		return fmt.Sprint(t)
	}
}

func derefStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func (r *Repository) CopyFrom(ctx context.Context, columns []string, rows [][]any) (int64, error) {
	//nRows := len(rows)
	//log.Printf("DEBUG: repo.CopyFrom: begin table=%s rows=%d", r.cfg.Table, nRows)

	return r.pool.CopyFrom(ctx, splitFQN(r.cfg.Table), columns, pgx.CopyFromRows(rows))
}

// splitFQN converts "schema.table" into a pgx.Identifier {"schema","table"}.
// If no dot is present, returns {"table"}.
func splitFQN(fqn string) pgx.Identifier {
	parts := strings.Split(fqn, ".")
	id := make(pgx.Identifier, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			id = append(id, p)
		}
	}
	return id
}

// Exec implements storage.Repository.Exec for Postgres.
func (r *Repository) Exec(ctx context.Context, sql string) error {
	_, err := r.pool.Exec(ctx, sql)
	return err
}
