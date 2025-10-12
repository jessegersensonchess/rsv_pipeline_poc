package db

import (
	"context"
	"testing"
)

// TestNewPgDB_InvalidDSN exercises the constructor’s error path by providing
// a clearly invalid connection string that fails during config parsing.
// This covers NewPgDB without requiring a live database.
func TestNewPgDB_InvalidDSN(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	if _, err := NewPgDB(ctx, "not-a-valid-dsn"); err == nil {
		t.Fatalf("expected error for invalid DSN")
	}
}

// TestPgDB_Close verifies the adapter delegates Close to the underlying
// pgConnLike. We reuse the fakePgConn defined in postgres_adapter_test.go.
func TestPgDB_Close(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	f := &fakePgConn{}      // defined in postgres_adapter_test.go
	p := newPgDBFromConn(f) // test seam to inject the fake

	if err := p.Close(ctx); err != nil {
		t.Fatalf("Close err: %v", err)
	}
	if !f.closed {
		t.Fatalf("expected underlying connection to be closed")
	}
}
