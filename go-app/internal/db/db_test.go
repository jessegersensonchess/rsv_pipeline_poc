package db

import (
	"database/sql"
	"testing"
)

func TestAsPgConn_NonPg(t *testing.T) {
	_, ok := AsPgConn(&sqlDB{db: &sql.DB{}})
	if ok {
		t.Fatalf("expected ok=false for non-pg")
	}
}

func TestAsSQLDB_NonSQL(t *testing.T) {
	_, ok := AsSQLDB(&pgDB{}) // non-sql adapter
	if ok {
		t.Fatalf("expected ok=false for non-sql")
	}
}

// Sanity: AsSQLDB true path
func TestAsSQLDB_True(t *testing.T) {
	s := &sql.DB{}
	got, ok := AsSQLDB(&sqlDB{db: s})
	if !ok || got != s {
		t.Fatalf("expected ok and same pointer, got %v %v", ok, got)
	}
}
