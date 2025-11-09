// Package storage defines storage-agnostic contracts and a factory to obtain a
// concrete Repository implementation. Backends (e.g., Postgres, MySQL, MSSQL)
// register themselves via Register, allowing the CLI to remain decoupled from
// driver-specific packages.
//
// Typical usage (in container/runner):
//
//	repo, err := storage.New(ctx, storage.Config{Kind: "postgres", ...})
//	defer repo.Close()
package storage

import (
	"context"
	"fmt"
	"sync"
)

// Repository is a backend-agnostic contract used by the container.
// Implementations (postgres, mysql, mssql, etc.) satisfy this interface.
type Repository interface {
	// BulkUpsert is the legacy/buffered path for moderate input sizes.
	// Implementations should perform an upsert (or insert) of the provided rows.
	BulkUpsert(ctx context.Context, rows []map[string]any, keyColumns []string, dateColumn string) (int64, error)

	// CopyFrom inserts rows in batches (streaming path). Implementations that
	// do not support native COPY can use multi-row INSERTs or bulk APIs.
	CopyFrom(ctx context.Context, columns []string, rows [][]any) (int64, error)

	// Exec runs an arbitrary statement (used for optional DDL).
	Exec(ctx context.Context, sql string) error

	// Close releases any held resources (connections, clients).
	Close()
}

// Config contains the minimal, backend-agnostic settings to open a repository.
type Config struct {
	// Kind selects the backend implementation, e.g., "postgres", "mysql", "mssql".
	Kind string
	// DSN is the connection string or endpoint for the backend.
	DSN string
	// Table is the fully-qualified destination table name.
	Table string
	// Columns is the ordered list of destination columns.
	Columns []string
	// KeyColumns lists conflict-target / key columns for upserts, if applicable.
	KeyColumns []string
	// DateColumn is an optional column name used by some upsert strategies.
	DateColumn string
}

// factory is the function signature backends must provide to construct a repo.
type factory func(ctx context.Context, cfg Config) (Repository, error)

var (
	mu        sync.RWMutex
	factories = map[string]factory{}
)

// Register adds (or replaces) a backend factory for the given kind.
// It is typically called from a backend package's init() function:
//
//	func init() {
//	    storage.Register("postgres", func(ctx context.Context, cfg storage.Config) (storage.Repository, error) {
//	        return newPostgres(ctx, cfg) // construct and return a *postgres.Repository adapter
//	    })
//	}
//
// Register is safe for concurrent use. Re-registering a kind overwrites the
// previous factory to make testing and dynamic wiring straightforward.
func Register(kind string, f factory) {
	mu.Lock()
	defer mu.Unlock()
	factories[kind] = f
}

// New constructs a Repository for the given Config.Kind using the most recently
// registered factory for that kind. It returns an error if no factory exists.
func New(ctx context.Context, cfg Config) (Repository, error) {
	mu.RLock()
	f, ok := factories[cfg.Kind]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unsupported storage.kind=%s", cfg.Kind)
	}
	return f(ctx, cfg)
}

// ListKinds returns the set of registered backend kinds. It is intended for
// diagnostics and tests.
func ListKinds() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(factories))
	for k := range factories {
		out = append(out, k)
	}
	return out
}
