package mssql

import (
	"context"
	"testing"

	"etl/internal/storage"
)

// TestMSSQLStorageRegistrationUsesNewRepositoryHook verifies that the "mssql"
// storage backend registered in init() uses the newRepository hook and that
// the wrappedRepo correctly propagates configuration and close behavior.
func TestMSSQLStorageRegistrationUsesNewRepositoryHook(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Save and restore global hook.
	origNewRepository := newRepository
	defer func() { newRepository = origNewRepository }()

	var (
		called   bool
		gotCfg   Config
		closed   bool
		fakeRepo = &Repository{}
	)

	newRepository = func(ctx context.Context, cfg Config) (*Repository, func(), error) {
		called = true
		gotCfg = cfg
		return fakeRepo, func() { closed = true }, nil
	}

	cfg := storage.Config{
		Kind:       "mssql",
		DSN:        "sqlserver://example",
		Table:      "dbo.target",
		Columns:    []string{"id", "name"},
		KeyColumns: []string{"id"},
	}

	repo, err := storage.New(ctx, cfg)
	if err != nil {
		t.Fatalf("storage.New() error = %v, want nil", err)
	}

	if !called {
		t.Fatalf("newRepository hook was not called")
	}

	// Assert that we received the expected config from storage.Config.
	if gotCfg.DSN != cfg.DSN {
		t.Errorf("hook cfg.DSN = %q, want %q", gotCfg.DSN, cfg.DSN)
	}
	if gotCfg.Table != cfg.Table {
		t.Errorf("hook cfg.Table = %q, want %q", gotCfg.Table, cfg.Table)
	}
	if len(gotCfg.Columns) != len(cfg.Columns) {
		t.Errorf("hook cfg.Columns length = %d, want %d", len(gotCfg.Columns), len(cfg.Columns))
	}
	if len(gotCfg.KeyColumns) != len(cfg.KeyColumns) {
		t.Errorf("hook cfg.KeyColumns length = %d, want %d", len(gotCfg.KeyColumns), len(cfg.KeyColumns))
	}

	// Verify the dynamic type and that the wrapped Repository is exactly the
	// fakeRepo instance returned by our hook.
	w, ok := repo.(*wrappedRepo)
	if !ok {
		t.Fatalf("storage.New() type = %T, want *wrappedRepo", repo)
	}
	if w.Repository != fakeRepo {
		t.Fatalf("wrappedRepo.Repository = %p, want %p", w.Repository, fakeRepo)
	}
	if w.closeFn == nil {
		t.Fatalf("wrappedRepo.closeFn is nil, want non-nil")
	}

	// Close should invoke our closeFn.
	repo.Close()
	if !closed {
		t.Fatalf("wrappedRepo.Close() did not invoke closeFn")
	}
}

// BenchmarkMSSQLStorageNew measures the overhead of constructing an MSSQL
// storage.Repository via storage.New using the newRepository hook. The hook
// is overridden to avoid real database connections.
func BenchmarkMSSQLStorageNew(b *testing.B) {
	ctx := context.Background()

	origNewRepository := newRepository
	defer func() { newRepository = origNewRepository }()

	newRepository = func(ctx context.Context, cfg Config) (*Repository, func(), error) {
		// Fast fake: no DB, trivial close.
		return &Repository{cfg: cfg}, func() {}, nil
	}

	cfg := storage.Config{
		Kind:       "mssql",
		DSN:        "sqlserver://example",
		Table:      "dbo.target",
		Columns:    []string{"id", "name", "created_at"},
		KeyColumns: []string{"id"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		repo, err := storage.New(ctx, cfg)
		if err != nil {
			b.Fatalf("storage.New() error = %v", err)
		}
		repo.Close()
	}
}
