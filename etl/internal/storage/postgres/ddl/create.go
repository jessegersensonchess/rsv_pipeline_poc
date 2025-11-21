package ddl

import (
	"etl/internal/ddl" // generic package
)

// BuildCreateTableSQL returns a Postgres CREATE TABLE IF NOT EXISTS statement
// for the given table definition. It is a thin wrapper over the generic ddl
// implementation, which already uses Postgres-compatible quoting and syntax.
var BuildCreateTableSQL = ddl.BuildCreateTableSQL
