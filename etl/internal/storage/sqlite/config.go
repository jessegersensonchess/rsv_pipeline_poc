// Package sqlite implements a SQLite-backed storage.Repository.
package sqlite

// Config holds SQLite repository configuration derived from storage.Config.
type Config struct {
	// DSN is a SQLite connection string or file path, e.g.:
	//   "file:etl.db?cache=shared&_fk=1"
	//   "etl.db" (interpreted by the driver)
	DSN string

	// Table is the target table name for inserts, e.g. "events".
	// SQLite does not use schemas in the same way as Postgres/MSSQL; FQN
	// values such as "main.events" are still accepted and passed through.
	Table string

	// Columns is the ordered list of destination columns.
	Columns []string

	// KeyColumns, DateColumn are included for parity with other backends.
	// They are not currently used by the SQLite repository itself but are
	// carried for future upsert/merge logic.
	KeyColumns []string
}
