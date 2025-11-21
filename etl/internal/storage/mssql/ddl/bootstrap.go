// Package ddl provides convenience helpers for applying MSSQL DDL using a
// storage.Repository. It offers a thin abstraction over Exec so that callers
// can create tables in a backend-agnostic fashion.
package ddl

import (
	"context"

	gddl "etl/internal/ddl"
	"etl/internal/storage"
)

// EnsureTable creates the target SQL Server table if it does not already exist.
//
// It uses BuildCreateTableSQL to generate a CREATE script guarded by an
// IF OBJECT_ID(...) check, then executes that script via repo.Exec. The
// operation is idempotent and safe to call multiple times for the same table.
func EnsureTable(ctx context.Context, repo storage.Repository, def gddl.TableDef) error {
	sql, err := BuildCreateTableSQL(def)
	if err != nil {
		return err
	}
	return repo.Exec(ctx, sql)
}
