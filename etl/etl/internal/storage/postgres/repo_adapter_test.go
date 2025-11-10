package postgres

import (
	"context"
	"os"
	"sync/atomic"
	"testing"

	"etl/internal/storage"
)

// Test that init() registration works and that storage.New constructs the repo
// via our adapter. We stub newRepository to avoid a real DB connection.
func TestAdapterRegistrationAndClose(t *testing.T) {
	t.Parallel()

	// Save and restore the hook.
	orig := newRepository
	defer func() { newRepository = orig }()

	// Capture the config passed to newRepository and count Close calls.
	var gotCfg Config
	var closed int32

	newRepository = func(ctx context.Context, cfg Config) (*Repository, func(), error) {
		gotCfg = cfg
		// Return a zero-value Repository; tests won't invoke its DB methods.
		return &Repository{}, func() { atomic.AddInt32(&closed, 1) }, nil
	}

	// storage.New should route to our adapter via init() registration.
	want := storage.Config{
		Kind:       "postgres",
		DSN:        "postgresql://user:pass@localhost:5432/db?sslmode=disable",
		Table:      "public.some_table",
		Columns:    []string{"a", "b"},
		KeyColumns: []string{"id"},
		// DateColumn is intentionally ignored by Postgres adapter wiring.
	}

	repo, err := storage.New(context.Background(), want)
	if err != nil {
		t.Fatalf("storage.New error: %v", err)
	}
	if repo == nil {
		t.Fatalf("storage.New returned nil repo")
	}

	// Verify adapter mapped fields into postgres.Config.
	if gotCfg.DSN != want.DSN {
		t.Errorf("cfg.DSN = %q, want %q", gotCfg.DSN, want.DSN)
	}
	if gotCfg.Table != want.Table {
		t.Errorf("cfg.Table = %q, want %q", gotCfg.Table, want.Table)
	}
	if len(gotCfg.Columns) != len(want.Columns) || gotCfg.Columns[0] != "a" || gotCfg.Columns[1] != "b" {
		t.Errorf("cfg.Columns = %#v, want %#v", gotCfg.Columns, want.Columns)
	}
	if len(gotCfg.KeyColumns) != len(want.KeyColumns) || gotCfg.KeyColumns[0] != "id" {
		t.Errorf("cfg.KeyColumns = %#v, want %#v", gotCfg.KeyColumns, want.KeyColumns)
	}

	// The wrapped Close must invoke the closeFn from newRepository.
	repo.Close()
	if atomic.LoadInt32(&closed) != 1 {
		t.Fatalf("Close() did not invoke closeFn")
	}
}

// TestWrappedRepoCopyFrom_Delegates is an integration-style test that verifies
// wrappedRepo.CopyFrom delegates to the inner *Repository.CopyFrom. We avoid
// pgx mocking by running only when TEST_PG_DSN is present (e.g., via your
// docker-compose Postgres). This pattern is common in Google-style test suites:
//
//   - Fast, hermetic unit tests always run.
//   - Optional integration tests run when env/flags are provided.
//
// To run this test:
//
//	TEST_PG_DSN='postgresql://user:password@0.0.0.0:5432/testdb?sslmode=disable' go test ./internal/storage/postgres -run CopyFrom_Delegates
func TestWrappedRepoCopyFrom_Delegates(t *testing.T) {
	t.Parallel()

	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("skipping integration test: set TEST_PG_DSN to run")
	}

	ctx := context.Background()

	// Build a real repo using the adapter path through the storage factory.
	// (The adapter is registered in init(); we use storage.New to remain
	// backend-agnostic at the call site.)
	repo, closeFn, err := NewRepository(ctx, Config{
		DSN:   dsn,
		Table: "public.__etl_copyfrom_test",
		Columns: []string{
			"a", "b",
		},
		KeyColumns: nil,
	})
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer closeFn()

	// Create a small temp table. We use Exec via a one-off connection obtained
	// from the repository to keep the test simple; in a larger suite you might
	// expose a helper for admin DDL in test code.
	conn, err := repo.pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("pool acquire: %v", err)
	}
	_, _ = conn.Exec(ctx, `DROP TABLE IF EXISTS public.__etl_copyfrom_test`)
	_, err = conn.Exec(ctx, `CREATE TABLE public.__etl_copyfrom_test (a int, b text)`)
	conn.Release()
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	w := &wrappedRepo{Repository: repo, closeFn: func() { repo.pool.Close() }}

	rows := [][]any{
		{1, "x"},
		{2, "y"},
	}
	n, err := w.CopyFrom(ctx, []string{"a", "b"}, rows)
	if err != nil {
		t.Fatalf("CopyFrom delegate error: %v", err)
	}
	if n != int64(len(rows)) {
		t.Fatalf("CopyFrom affected=%d, want=%d", n, len(rows))
	}
}
