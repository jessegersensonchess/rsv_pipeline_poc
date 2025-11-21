package storage

import (
	"context"
	"fmt"
	"sync"

	"etl/internal/config"
)

// DDLBootstrapper is a backend-specific function that:
//   - infers a table definition from the pipeline spec, and
//   - applies the appropriate DDL via repo.Exec (typically CREATE TABLE).
//
// Backends (postgres, mssql, sqlite, etc.) register their implementation for
// a given storage kind (e.g., "postgres") at init time.
type DDLBootstrapper func(ctx context.Context, repo Repository, spec config.Pipeline) error

var (
	ddlMu  sync.RWMutex
	ddlFns = map[string]DDLBootstrapper{}
)

// RegisterDDL registers (or replaces) a DDLBootstrapper for the given storage
// kind. It is typically called from backend packages' init() functions.
func RegisterDDL(kind string, fn DDLBootstrapper) {
	ddlMu.Lock()
	defer ddlMu.Unlock()
	ddlFns[kind] = fn
}

// EnsureTableFromPipeline locates the DDLBootstrapper for spec.Storage.Kind
// and invokes it. Callers do not need to know which backend they are using;
// they simply pass the pipeline and the already-open Repository.
//
// If no DDL bootstrapper has been registered for the storage kind, an error
// is returned.
func EnsureTableFromPipeline(ctx context.Context, spec config.Pipeline, repo Repository) error {
	ddlMu.RLock()
	fn, ok := ddlFns[spec.Storage.Kind]
	ddlMu.RUnlock()
	if !ok {
		return fmt.Errorf("no DDL bootstrapper registered for storage.kind=%q", spec.Storage.Kind)
	}
	return fn(ctx, repo, spec)
}
