// Package postgres provides a Postgres-backed storage.Repository implementation.
// This adapter wires the Postgres backend into the storage-agnostic factory by
// registering a constructor at init time. The CLI (cmd/etl) and other callers
// can then obtain a Repository via storage.New(...) without importing this
// package directly.
//
// The adapter also reconciles method signatures between the storage.Repository
// interface and the concrete *postgres.Repository type, and registers a DDL
// bootstrapper so that callers can apply backend-specific DDL based only on
// storage.Kind, without branching on the backend themselves.
package postgres

import (
	"context"
	"fmt"

	"etl/internal/config"
	"etl/internal/storage"
	pgddl "etl/internal/storage/postgres/ddl"
)

// newRepository is a test hook that points to NewRepository by default.
// Tests may replace this variable to avoid real DB connections.
var newRepository = NewRepository

// wrappedRepo implements storage.Repository by delegating to the concrete
// *postgres.Repository while providing a Close method that calls the close
// function returned by NewRepository.
type wrappedRepo struct {
	*Repository
	closeFn func()
}

// Ensure wrappedRepo satisfies storage.Repository at compile time.
var _ storage.Repository = (*wrappedRepo)(nil)

// Close implements storage.Repository.Close.
func (w *wrappedRepo) Close() {
	if w.closeFn != nil {
		w.closeFn()
	}
}

// CopyFrom implements storage.Repository.CopyFrom by delegating directly to
// the underlying *Repository. This wrapper exists to keep the adapter free to
// evolve independently of the concrete implementation's method set.
func (w *wrappedRepo) CopyFrom(
	ctx context.Context,
	columns []string,
	rows [][]any,
) (int64, error) {
	return w.Repository.CopyFrom(ctx, columns, rows)
}

// init registers the "postgres" backend with the storage factory and also
// registers a DDL bootstrapper for storage.Kind == "postgres". This keeps the
// wiring in one place and allows callers to remain backend-agnostic.
//
// Typical usage:
//
//	repo, err := storage.New(ctx, storage.Config{Kind: "postgres", ...})
//	defer repo.Close()
//
//	if spec.Storage.DB.AutoCreateTable {
//	    if err := storage.EnsureTableFromPipeline(ctx, spec, repo); err != nil {
//	        // handle DDL error
//	    }
//	}
func init() {
	// Repository factory registration.
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

	// DDL bootstrap registration: infer table definition from the pipeline and
	// apply backend-specific DDL via the generic Repository.Exec method.
	storage.RegisterDDL("postgres",
		func(ctx context.Context, repo storage.Repository, spec config.Pipeline) error {
			td, err := pgddl.FromPipeline(spec)
			if err != nil {
				return fmt.Errorf("infer table definition: %w", err)
			}
			if err := pgddl.EnsureTable(ctx, repo, td); err != nil {
				return fmt.Errorf("apply DDL: %w", err)
			}
			return nil
		})
}
