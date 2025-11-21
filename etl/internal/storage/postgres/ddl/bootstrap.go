package ddl

import (
	"context"

	"etl/internal/ddl"
	"etl/internal/storage"
)

// EnsureTable creates the target Postgres table if it does not exist.
// It is idempotent and simply issues the CREATE TABLE IF NOT EXISTS via the
// repository's Exec method.
func EnsureTable(ctx context.Context, repo storage.Repository, def ddl.TableDef) error {
	sql, err := BuildCreateTableSQL(def)
	if err != nil {
		return err
	}
	return repo.Exec(ctx, sql)
}
