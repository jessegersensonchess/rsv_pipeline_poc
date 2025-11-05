package db

import "context"

// DB is a connection capable of starting transactions and executing DDL/DML.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) error
	BeginTx(ctx context.Context) (Tx, error)
	Close(ctx context.Context) error
}

// Tx (transaction) supports Exec, bulk inserts, and lifecycle.
type Tx interface {
	Exec(ctx context.Context, sql string, args ...any) error
	CopyInto(ctx context.Context, table string, columns []string, rows [][]interface{}) (int64, error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type SmallDB interface {
	Close(ctx context.Context) error
	CreateOwnershipTable(ctx context.Context) error
	CopyOwnership(ctx context.Context, records [][]interface{}) error

	// tech inspections
	CreateTechInspectionsTable(ctx context.Context) error
	CopyTechInspections(ctx context.Context, rows [][]interface{}) error

	// RSV zpravy importer
	CreateRSVZpravyTable(ctx context.Context) error
	CopyRSVZpravy(ctx context.Context, rows [][]interface{}) error
}

// DBFactory can mint a new DB connection per worker (for parallel ingestion).
type DBFactory func(ctx context.Context) (DB, error)

// SmallDBFactory can mint a new SmallDB (fresh connection) per worker.
type SmallDBFactory func(ctx context.Context) (SmallDB, error)
