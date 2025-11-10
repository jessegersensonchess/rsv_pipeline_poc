// Package mysql provides a MySQL-backed storage.Repository implementation.
// This adapter wires the MySQL backend into the storage-agnostic factory.
package mysql

import (
	"context"

	"etl/internal/storage"
)

// newRepository is a test hook that points to NewRepository by default.
// Tests may replace this variable to avoid real DB connections.
var newRepository = NewRepository

var _ storage.Repository = (*wrappedRepo)(nil)

// init registers the "mysql" backend with the factory.
func init() {
	storage.Register("mysql", func(ctx context.Context, cfg storage.Config) (storage.Repository, error) {
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

// wrappedRepo adapts *mysql.Repository to storage.Repository and provides Close.
type wrappedRepo struct {
	*Repository
	closeFn func()
}

// Close closes the underlying connection pool.
func (w *wrappedRepo) Close() { w.closeFn() }

// BulkUpsert adapts the legacy signature (map[string]any) to our implementation.
func (w *wrappedRepo) BulkUpsert(
	ctx context.Context,
	rows []map[string]any,
	keyColumns []string,
	dateColumn string,
) (int64, error) {
	return w.Repository.BulkUpsert(ctx, rows, keyColumns, dateColumn)
}

// CopyFrom delegates to the underlying repository.
func (w *wrappedRepo) CopyFrom(
	ctx context.Context,
	columns []string,
	rows [][]any,
) (int64, error) {
	return w.Repository.CopyFrom(ctx, columns, rows)
}
