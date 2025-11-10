// Package mssql provides an MSSQL-backed storage.Repository implementation.
// This adapter wires the MSSQL backend into the storage-agnostic factory.
package mssql

import (
	"context"

	"etl/internal/storage"
)

// newRepository is a test hook that points to NewRepository by default.
// Tests may replace this variable to avoid real DB connections.
var newRepository = NewRepository

var _ storage.Repository = (*wrappedRepo)(nil)

func init() {
	storage.Register("mssql", func(ctx context.Context, cfg storage.Config) (storage.Repository, error) {
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

// wrappedRepo adapts *mssql.Repository to storage.Repository and provides Close.
type wrappedRepo struct {
	*Repository
	closeFn func()
}

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

func (w *wrappedRepo) CopyFrom(
	ctx context.Context,
	columns []string,
	rows [][]any,
) (int64, error) {
	return w.Repository.CopyFrom(ctx, columns, rows)
}
