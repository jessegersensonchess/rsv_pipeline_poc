// Package sqlite wires the SQLite backend into the storage factory. It exposes
// a storage.Repository implementation without forcing callers to import this
// package directly; registration happens in init.
package sqlite

import (
	"context"
	"etl/internal/config"
	sqliteddl "etl/internal/storage/sqlite/ddl"
	"fmt"

	"etl/internal/storage"
)

// newRepository is a test hook that points to NewRepository by default.
// Tests may replace this variable to avoid real DB connections.
var newRepository = NewRepository

// wrappedRepo adapts *sqlite.Repository to the storage.Repository interface,
// adding a Close method that calls the cleanup function returned by
// NewRepository.
type wrappedRepo struct {
	*Repository
	closeFn func()
}

// Close implements storage.Repository.Close.
func (w *wrappedRepo) Close() {
	if w.closeFn != nil {
		w.closeFn()
	}
}

// Ensure wrappedRepo satisfies the interface at compile time.
var _ storage.Repository = (*wrappedRepo)(nil)

func init() {
	storage.Register("sqlite", func(ctx context.Context, cfg storage.Config) (storage.Repository, error) {
		r, closeFn, err := newRepository(ctx, Config{
			DSN:        cfg.DSN,
			Table:      cfg.Table,
			Columns:    cfg.Columns,
			KeyColumns: cfg.KeyColumns,
		})
		if err != nil {
			return nil, err
		}
		return &wrappedRepo{Repository: r, closeFn: closeFn}, nil
	})

	// DDL bootstrap registration.
	storage.RegisterDDL("sqlite",
		func(ctx context.Context, repo storage.Repository, spec config.Pipeline) error {
			td, err := sqliteddl.FromPipeline(spec)
			if err != nil {
				return fmt.Errorf("infer table definition: %w", err)
			}
			return sqliteddl.EnsureTable(ctx, repo, td)
		})
}
