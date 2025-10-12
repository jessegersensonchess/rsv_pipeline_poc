package db

import (
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestAsPgConn_FalseBranch
// Ensures AsPgConn returns (nil,false) for non-pg adapters (e.g., sqlDB).
func TestAsPgConn_FalseBranch(t *testing.T) {
	t.Parallel()

	s := &sqlDB{} // anything that's not *pgDB
	got, ok := AsPgConn(s)
	if ok || got != nil {
		t.Fatalf("AsPgConn should return (nil,false) for non-pg adapters, got (%v,%v)", got, ok)
	}
}

// TestAsPgConn_TrueBranch
// Ensures AsPgConn returns the wrapped *pgx.Conn (which may be nil) and true
// for the pg adapter type. Using a nil *pgx.Conn is safe here; we never call it.
func TestAsPgConn_TrueBranch(t *testing.T) {
	t.Parallel()

	var underlying *pgx.Conn = nil
	ad := &pgDB{conn: underlying}

	got, ok := AsPgConn(ad)
	if !ok {
		t.Fatalf("AsPgConn should return ok=true for pgDB adapters")
	}
	if got != underlying {
		t.Fatalf("AsPgConn returned unexpected *pgx.Conn pointer: got=%p want=%p", got, underlying)
	}
}
