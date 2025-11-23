//go:build integration

package mssql

import (
	"context"
	"os"
	"testing"
	"time"
)

// getTestDSN reads the MSSQL_TEST_DSN environment variable.
// If it is empty, the caller should skip the test.
func getTestDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("MSSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("MSSQL_TEST_DSN not set; skipping MSSQL integration tests")
	}
	return dsn
}

// TestNewRepositoryIntegration verifies that NewRepository can successfully
// connect to a real SQL Server and that the returned Close function works.
func TestNewRepositoryIntegration(t *testing.T) {
	dsn := getTestDSN(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := Config{
		DSN:   dsn,
		Table: "tempdb.dbo.repo_integration_test", // table name is arbitrary; not used here
	}

	repo, closeFn, err := NewRepository(ctx, cfg)
	if err != nil {
		t.Fatalf("NewRepository() error = %v, want nil", err)
	}
	if repo == nil {
		t.Fatalf("NewRepository() repo = nil, want non-nil")
	}
	if closeFn == nil {
		t.Fatalf("NewRepository() closeFn = nil, want non-nil")
	}

	// Close should not panic or error.
	closeFn()
}

// TestCopyFromAndExecIntegration verifies that Exec and CopyFrom work together
// against a real SQL Server by creating a temp table, inserting data, and
// verifying the number of inserted rows.
func TestCopyFromAndExecIntegration(t *testing.T) {
	dsn := getTestDSN(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := Config{
		DSN:   dsn,
		Table: "tempdb.dbo.repo_copyfrom_test",
	}

	repo, closeFn, err := NewRepository(ctx, cfg)
	if err != nil {
		t.Fatalf("NewRepository() error = %v, want nil", err)
	}
	defer closeFn()

	// Ensure a clean table in tempdb.
	_ = repo.Exec(ctx, "IF OBJECT_ID('tempdb..repo_copyfrom_test', 'U') IS NOT NULL DROP TABLE tempdb..repo_copyfrom_test;")

	if err := repo.Exec(ctx, `
		CREATE TABLE tempdb..repo_copyfrom_test (
			id INT NOT NULL,
			name NVARCHAR(100) NOT NULL
		);`); err != nil {
		t.Fatalf("Exec(CREATE TABLE) error = %v", err)
	}

	// Configure the repository columns to match.
	repo.cfg.Columns = []string{"id", "name"}

	rows := [][]any{
		{1, "alice"},
		{2, "bob"},
		{3, "carol"},
	}

	n, err := repo.CopyFrom(ctx, []string{"id", "name"}, rows)
	if err != nil {
		t.Fatalf("CopyFrom() error = %v, want nil", err)
	}
	if n != int64(len(rows)) {
		t.Fatalf("CopyFrom() inserted = %d, want %d", n, len(rows))
	}
}
