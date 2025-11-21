// Package all wires all built-in storage backends into the storage factory.
//
// This package exists purely for side effects: importing it (even as a blank
// import) causes the init functions of each concrete storage backend to run,
// which in turn register their factories and DDL bootstrappers with the
// storage package.
//
// In other words, importing this package makes the following storage kinds
// available at runtime:
//
//   - "postgres" (etl/internal/storage/postgres)
//   - "mssql"    (etl/internal/storage/mssql)
//   - "sqlite"   (etl/internal/storage/sqlite)
//
// Typical usage (in cmd/etl/main.go or a similar wiring layer):
//
//	package main
//
//	import (
//	    "context"
//
//	    _ "etl/internal/storage/all" // enable all built-in backends
//
//	    "etl/internal/config"
//	    "etl/internal/storage"
//	    // ... other imports ...
//	)
//
//	func main() {
//	    ctx := context.Background()
//
//	    // Load pipeline spec (config.Pipeline) from disk, flags, etc.
//	    var spec config.Pipeline
//	    // ... decode spec ...
//
//	    // Construct a storage.Config from the pipeline and open a Repository.
//	    repo, err := storage.New(ctx, storage.Config{
//	        Kind:       spec.Storage.Kind,
//	        DSN:        spec.Storage.DB.DSN,
//	        Table:      spec.Storage.DB.Table,
//	        Columns:    spec.Storage.DB.Columns,
//	        KeyColumns: spec.Storage.DB.KeyColumns,
//	        DateColumn: spec.Storage.DB.DateColumn,
//	    })
//	    if err != nil {
//	        // handle error
//	    }
//	    defer repo.Close()
//
//	    // Optionally create the destination table if requested by the pipeline.
//	    if spec.Storage.DB.AutoCreateTable {
//	        if err := storage.EnsureTableFromPipeline(ctx, spec, repo); err != nil {
//	            // handle DDL error
//	        }
//	    }
//
//	    // From this point on, the caller can remain fully backend-agnostic.
//	    // Reads, transforms, and writes all go through the storage.Repository
//	    // interface, regardless of whether the underlying backend is Postgres,
//	    // MSSQL, or SQLite.
//	}
//
// This pattern keeps backend-specific wiring in a single, small package and
// allows the rest of the application (container/runner, transforms, CLI) to
// depend only on the storage abstraction rather than individual backends.
//
// Note: if you want a binary that supports only a subset of backends, you can
// define alternative wiring packages (e.g., storage/postgresonly, storage/sqliteonly)
// that import only the required backends instead of this package.
package all

import (
	_ "etl/internal/storage/mssql"
	_ "etl/internal/storage/postgres"
	_ "etl/internal/storage/sqlite"
)
