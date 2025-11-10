// Package sqlite registers the SQLite backend with the storage factory.
package sqlite

import (
	"context"

	"etl/internal/storage"
)

var newRepository = NewRepository // test hook

type wrappedRepo struct {
	*Repository
	closeFn func()
}

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
}

func (w *wrappedRepo) Close() { w.closeFn() }

func (w *wrappedRepo) BulkUpsert(ctx context.Context, rows []map[string]any, keyColumns []string, dateColumn string) (int64, error) {
	return w.Repository.BulkUpsert(ctx, rows, keyColumns, dateColumn)
}

func (w *wrappedRepo) CopyFrom(ctx context.Context, columns []string, rows [][]any) (int64, error) {
	return w.Repository.CopyFrom(ctx, columns, rows)
}
