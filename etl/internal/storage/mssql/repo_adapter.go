package mssql

import (
	"context"
	"fmt"

	"etl/internal/config"
	"etl/internal/storage"
	mssqlddl "etl/internal/storage/mssql/ddl"
)

// newRepository is a test hook;
// default points to your real constructor (NewRepository).
var newRepository = NewRepository

func init() {
	storage.Register("mssql", func(ctx context.Context, cfg storage.Config) (storage.Repository, error) {
		// Convert storage.Config → mssql.Config
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
	storage.RegisterDDL("mssql",
		func(ctx context.Context, repo storage.Repository, spec config.Pipeline) error {
			td, err := mssqlddl.FromPipeline(spec)
			if err != nil {
				return fmt.Errorf("infer table definition: %w", err)
			}
			return mssqlddl.EnsureTable(ctx, repo, td)
		})
}

// wrappedRepo satisfies storage.Repository.
type wrappedRepo struct {
	*Repository // your concrete MSSQL repo implementation
	closeFn     func()
}

func (w *wrappedRepo) Close() {
	if w.closeFn != nil {
		w.closeFn()
	}
}

// Delegates directly to the real implementation.
func (w *wrappedRepo) CopyFrom(ctx context.Context, cols []string, rows [][]any) (int64, error) {
	return w.Repository.CopyFrom(ctx, cols, rows)
}

func (w *wrappedRepo) Exec(ctx context.Context, sql string) error {
	return w.Repository.Exec(ctx, sql)
}
