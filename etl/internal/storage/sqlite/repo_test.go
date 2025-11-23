package sqlite

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

// TestNewRepositoryEmptyDSN verifies that NewRepository rejects empty DSNs.
func TestNewRepositoryEmptyDSN(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cfg := Config{DSN: "   "}

	repo, closeFn, err := NewRepository(ctx, cfg)
	if err == nil {
		t.Fatalf("NewRepository() error = nil, want non-nil for empty DSN")
	}
	if !strings.Contains(err.Error(), "DSN must not be empty") {
		t.Fatalf("NewRepository() error = %q, want to contain %q", err.Error(), "DSN must not be empty")
	}
	if repo != nil || closeFn != nil {
		t.Fatalf("NewRepository() returned non-nil repo or closeFn on error")
	}
}

// TestNewRepositoryInMemory verifies that NewRepository can open an in-memory
// SQLite database and that the returned Close function works.
func TestNewRepositoryInMemory(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cfg := Config{DSN: ":memory:"}

	repo, closeFn, err := NewRepository(ctx, cfg)
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	if repo == nil {
		t.Fatalf("NewRepository() repo = nil, want non-nil")
	}
	if closeFn == nil {
		t.Fatalf("NewRepository() closeFn = nil, want non-nil")
	}

	// Basic smoke test: Exec should succeed for a simple statement.
	if err := repo.Exec(ctx, "SELECT 1;"); err != nil {
		t.Fatalf("repo.Exec(SELECT 1) error = %v", err)
	}

	closeFn() // should not panic
}

// TestCopyFromErrors verifies error conditions in CopyFrom such as empty
// columns and row length mismatches.
func TestCopyFromErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Empty columns.
	r := &Repository{
		db:  nil,
		cfg: Config{Table: "events"},
	}
	if _, err := r.CopyFrom(ctx, nil, nil); err == nil {
		t.Fatalf("CopyFrom(nil columns) error = nil, want non-nil")
	}

	// Empty rows should be a no-op and not require a live DB.
	if n, err := r.CopyFrom(ctx, []string{"id"}, nil); err != nil || n != 0 {
		t.Fatalf("CopyFrom(empty rows) = (%d, %v), want (0, nil)", n, err)
	}

	// Row length mismatch.
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open error = %v", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, `CREATE TABLE events (id INTEGER, name TEXT);`); err != nil {
		t.Fatalf("create table error = %v", err)
	}

	r = &Repository{
		db:  db,
		cfg: Config{Table: "events"},
	}

	cols := []string{"id", "name"}
	rows := [][]any{
		{1}, // length 1 instead of 2
	}

	n, err := r.CopyFrom(ctx, cols, rows)
	if err == nil {
		t.Fatalf("CopyFrom(row length mismatch) error = nil, want non-nil")
	}
	if n != 0 {
		t.Fatalf("CopyFrom(row length mismatch) inserted = %d, want 0", n)
	}
	if !strings.Contains(err.Error(), "row length") {
		t.Fatalf("CopyFrom(row length mismatch) error = %q, want to contain %q", err.Error(), "row length")
	}
}

// TestCopyFromSuccess verifies that CopyFrom inserts rows into the configured
// table using an in-memory SQLite database.
func TestCopyFromSuccess(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open error = %v", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, `CREATE TABLE events (id INTEGER, name TEXT);`); err != nil {
		t.Fatalf("create table error = %v", err)
	}

	r := &Repository{
		db:  db,
		cfg: Config{Table: "events"},
	}

	cols := []string{"id", "name"}
	rows := [][]any{
		{1, "alice"},
		{2, "bob"},
	}

	n, err := r.CopyFrom(ctx, cols, rows)
	if err != nil {
		t.Fatalf("CopyFrom() error = %v", err)
	}
	if n != int64(len(rows)) {
		t.Fatalf("CopyFrom() inserted = %d, want %d", n, len(rows))
	}

	// Verify rows actually landed in the table.
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events;`).Scan(&count); err != nil {
		t.Fatalf("count query error = %v", err)
	}
	if count != len(rows) {
		t.Fatalf("COUNT(*) = %d, want %d", count, len(rows))
	}
}

// TestExecNoopAndError verifies Exec behavior for empty SQL and error paths.
func TestExecNoopAndError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open error = %v", err)
	}
	defer db.Close()

	r := &Repository{
		db:  db,
		cfg: Config{Table: "events"},
	}

	// Empty / whitespace-only SQL should be a no-op.
	if err := r.Exec(ctx, "   "); err != nil {
		t.Fatalf("Exec(whitespace) error = %v, want nil", err)
	}

	// Invalid SQL should surface a wrapped error.
	err = r.Exec(ctx, "THIS IS NOT SQL")
	if err == nil {
		t.Fatalf("Exec(invalid SQL) error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "sqlite: exec:") {
		t.Fatalf("Exec(invalid SQL) error = %q, want prefix %q", err.Error(), "sqlite: exec:")
	}
}

// BenchmarkCopyFromSmall measures the performance of CopyFrom for a small batch.
func BenchmarkCopyFromSmall(b *testing.B) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		b.Fatalf("sql.Open error = %v", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, `CREATE TABLE events (id INTEGER, name TEXT);`); err != nil {
		b.Fatalf("create table error = %v", err)
	}

	r := &Repository{
		db:  db,
		cfg: Config{Table: "events"},
	}

	cols := []string{"id", "name"}
	rows := [][]any{
		{1, "alice"},
		{2, "bob"},
		{3, "carol"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.CopyFrom(ctx, cols, rows); err != nil {
			b.Fatalf("CopyFrom() error = %v", err)
		}
		// Clean up between iterations.
		if _, err := db.ExecContext(ctx, `DELETE FROM events;`); err != nil {
			b.Fatalf("delete rows error = %v", err)
		}
	}
}

// BenchmarkExecNoop measures the cost of Exec when given whitespace-only SQL.
func BenchmarkExecNoop(b *testing.B) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		b.Fatalf("sql.Open error = %v", err)
	}
	defer db.Close()

	r := &Repository{
		db:  db,
		cfg: Config{Table: "events"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := r.Exec(ctx, "   "); err != nil {
			b.Fatalf("Exec() error = %v", err)
		}
	}
}
