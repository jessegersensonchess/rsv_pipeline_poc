// Package postgres provides a Postgres-backed storage.Repository implementation.
// This adapter wires the Postgres backend into the storage-agnostic factory by
// registering a constructor at init time. The CLI (cmd/etl) and other callers
// can then obtain a Repository via storage.New(...) without importing this
// package directly.
//
// The adapter also reconciles method signatures between the storage.Repository
// interface (which uses []map[string]any for legacy/buffered upserts) and the
// concrete *postgres.Repository (which uses []records.Record).
package postgres

import (
	"context"

	"etl/internal/storage"
)

// newRepository is a test hook that points to NewRepository by default.
// Tests may replace this variable to avoid real DB connections.
var newRepository = NewRepository

var _ storage.Repository = (*wrappedRepo)(nil)

// init registers the "postgres" backend with the storage factory. This keeps
// the wiring in one place and allows callers to remain backend-agnostic.
func init() {
	storage.Register("postgres", func(ctx context.Context, cfg storage.Config) (storage.Repository, error) {
		// Adapt storage.Config → postgres.Config.
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
}

// wrappedRepo implements storage.Repository by delegating to the concrete
// *postgres.Repository while providing a Close method that calls the close
// function returned by NewRepository.
type wrappedRepo struct {
	*Repository
	closeFn func()
}

// Close implements storage.Repository.Close.
func (w *wrappedRepo) Close() { w.closeFn() }

// CopyFrom implements storage.Repository.CopyFrom by delegating directly.
func (w *wrappedRepo) CopyFrom(
	ctx context.Context,
	columns []string,
	rows [][]any,
) (int64, error) {
	return w.Repository.CopyFrom(ctx, columns, rows)
}
