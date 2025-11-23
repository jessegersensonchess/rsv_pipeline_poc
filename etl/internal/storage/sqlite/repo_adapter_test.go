package sqlite

import (
	"context"
	"testing"

	"etl/internal/storage"
)

// TestSQLiteStorageRegistrationUsesNewRepositoryHook verifies that the
// "sqlite" storage backend registered in init() uses the newRepository hook
// and that wrappedRepo correctly delegates Close.
func TestSQLiteStorageRegistrationUsesNewRepositoryHook(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	origNewRepository := newRepository
	defer func() { newRepository = origNewRepository }()

	var (
		called bool
		gotCfg Config
		closed bool

		fakeRepo = &Repository{}
	)

	newRepository = func(ctx context.Context, cfg Config) (*Repository, func(), error) {
		called = true
		gotCfg = cfg
		return fakeRepo, func() { closed = true }, nil
	}

	cfg := storage.Config{
		Kind:       "sqlite",
		DSN:        "file:test.db?mode=memory&cache=shared",
		Table:      "events",
		Columns:    []string{"id", "name"},
		KeyColumns: []string{"id"},
	}

	repo, err := storage.New(ctx, cfg)
	if err != nil {
		t.Fatalf("storage.New() error = %v", err)
	}

	if !called {
		t.Fatalf("newRepository hook was not called")
	}

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

	w, ok := repo.(*wrappedRepo)
	if !ok {
		t.Fatalf("storage.New() type = %T, want *wrappedRepo", repo)
	}
	if w.Repository != fakeRepo {
		t.Fatalf("wrappedRepo.Repository = %p, want %p", w.Repository, fakeRepo)
	}
	if w.closeFn == nil {
		t.Fatalf("wrappedRepo.closeFn = nil, want non-nil")
	}

	repo.Close()
	if !closed {
		t.Fatalf("wrappedRepo.Close() did not invoke closeFn")
	}
}

// BenchmarkSQLiteStorageNew measures the overhead of constructing a SQLite
// storage.Repository via storage.New using the newRepository hook.
func BenchmarkSQLiteStorageNew(b *testing.B) {
	ctx := context.Background()

	origNewRepository := newRepository
	defer func() { newRepository = origNewRepository }()

	newRepository = func(ctx context.Context, cfg Config) (*Repository, func(), error) {
		return &Repository{cfg: cfg}, func() {}, nil
	}

	cfg := storage.Config{
		Kind:       "sqlite",
		DSN:        "file:test.db?mode=memory&cache=shared",
		Table:      "events",
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
