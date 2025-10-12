package db

import (
	"database/sql"
	"testing"
)

// These tests live in the db package to reference unexported concrete types.

// TestAsPgConn_NonPg ensures we don't falsely downcast non-pg adapters.
//func TestAsPgConn_NonPg(t *testing.T) {
//	t.Parallel()
//	if _, ok := AsPgConn(&sqlDB{db: &sql.DB{}}); ok {
//		t.Fatalf("AsPgConn should be false for sqlDB")
//	}
//}

//// TestAsSQLDB_NonSQL ensures we don't falsely downcast non-sql adapters.
//func TestAsSQLDB_NonSQL(t *testing.T) {
//	t.Parallel()
//	if _, ok := AsSQLDB(&pgDB{}); ok {
//		t.Fatalf("AsSQLDB should be false for pgDB")
//	}
//}

// TestAsSQLDB_TruePath validates the true branch for sql adapters.
func TestAsSQLDB_TruePath(t *testing.T) {
	t.Parallel()
	s := &sql.DB{}
	got, ok := AsSQLDB(&sqlDB{db: s})
	if !ok || got != s {
		t.Fatalf("AsSQLDB failed true path: ok=%v got=%v", ok, got)
	}
}
