package postgres

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// -----------------------------------------------------------------------------
// Pure helper tests (hermetic, fast).
// Each test documents the contract being validated and why it matters.
// -----------------------------------------------------------------------------

// TestToCopyVal verifies that toCopyVal preserves non-nil values as-is and
// leaves nil as nil so pgx can encode values correctly without unintended
// stringification or zero-value conversions.
func TestToCopyVal(t *testing.T) {
	t.Parallel()

	if toCopyVal(nil) != nil {
		t.Fatalf("toCopyVal(nil) != nil")
	}
	if got := toCopyVal(123); got != 123 {
		t.Fatalf("toCopyVal(123) = %#v, want 123", got)
	}
	if got := toCopyVal("x"); got != "x" {
		t.Fatalf("toCopyVal(\"x\") = %#v, want \"x\"", got)
	}
}

// TestPgFQN checks that schema-qualified names are safely quoted per-segment
// (e.g., public.table → "public"."table") and unqualified names are still
// quoted ("table"). This is important to avoid SQL injection via identifiers
// and to support reserved words or mixed-case identifiers.
func TestPgFQN(t *testing.T) {
	t.Parallel()

	if got, want := pgFQN("public.table"), `"public"."table"`; got != want {
		t.Fatalf("pgFQN = %q, want %q", got, want)
	}
	if got, want := pgFQN("table"), `"table"`; got != want {
		t.Fatalf("pgFQN = %q, want %q", got, want)
	}
}

// TestMapIdent ensures each identifier in the slice is quoted individually,
// returns a new slice (no aliasing), and preserves order. This prevents
// accidental mutation and ensures column order is stable.
func TestMapIdent(t *testing.T) {
	t.Parallel()

	in := []string{"a", "b", "c"}
	got := mapIdent(in)
	if len(got) != 3 || got[0] != `"a"` || got[1] != `"b"` || got[2] != `"c"` {
		t.Fatalf("mapIdent = %#v, want [\"a\",\"b\",\"c\"]", got)
	}
	// Verify no aliasing: mutating input does not affect output.
	in[0] = "z"
	if got[0] != `"a"` {
		t.Fatalf("mapIdent output appears aliased to input")
	}
}

// TestToString validates conversion semantics used in diagnostics and
// text-based encodings: nil → "", strings pass through, and numbers are
// formatted with fmt.Sprint.
func TestToString(t *testing.T) {
	t.Parallel()

	if got := toString(nil); got != "" {
		t.Fatalf("toString(nil) = %q, want empty", got)
	}
	if got := toString("x"); got != "x" {
		t.Fatalf("toString(\"x\") = %q, want \"x\"", got)
	}
	if got := toString(42); got != "42" {
		t.Fatalf("toString(42) = %q, want \"42\"", got)
	}
}

// TestDerefStr ensures derefStr safely handles nil pointers (returns "") and
// returns the pointed-to value when non-nil. This avoids nil-deref panics in
// logging/formatting paths.
func TestDerefStr(t *testing.T) {
	t.Parallel()

	if got := derefStr(nil); got != "" {
		t.Fatalf("derefStr(nil) = %q, want empty", got)
	}
	s := "hi"
	if got := derefStr(&s); got != "hi" {
		t.Fatalf("derefStr(&\"hi\") = %q, want \"hi\"", got)
	}
}

// TestSplitFQN verifies that splitFQN produces a pgx.Identifier suitable
// for pgx methods: "public.table" → {"public","table"} and "table" → {"table"}.
// This is used by CopyFrom to address schema-qualified tables correctly.
func TestSplitFQN(t *testing.T) {
	t.Parallel()

	id := splitFQN("public.table")
	if len(id) != 2 || id[0] != "public" || id[1] != "table" {
		t.Fatalf("splitFQN(public.table) = %#v", []string(id))
	}
	id = splitFQN("table")
	if len(id) != 1 || id[0] != "table" {
		t.Fatalf("splitFQN(table) = %#v", []string(id))
	}
}

// -----------------------------------------------------------------------------
// NewRepository negative-path test (hermetic).
// -----------------------------------------------------------------------------

// TestNewRepository_InvalidDSN asserts that an invalid DSN propagates a
// descriptive error (prefixed with "pgxpool:") so callers can reliably
// distinguish wiring/config failures from runtime I/O failures.
func TestNewRepository_InvalidDSN(t *testing.T) {
	t.Parallel()

	_, _, err := NewRepository(context.Background(), Config{DSN: "not-a-dsn"})
	if err == nil {
		t.Fatal("expected error for invalid DSN, got nil")
	}
	if !strings.Contains(err.Error(), "pgxpool:") {
		t.Fatalf("error prefix mismatch: %v", err)
	}
}

// -----------------------------------------------------------------------------
// Optional integration test (requires TEST_PG_DSN).
// This covers the success path of NewRepository and Repository.CopyFrom using
// a real Postgres. It is skipped unless TEST_PG_DSN is provided.
// -----------------------------------------------------------------------------

// To run:
//
//	TEST_PG_DSN='postgresql://user:password@0.0.0.0:5432/testdb?sslmode=disable' \
//	  go test ./internal/storage/postgres -run Integration
func TestIntegration_NewRepositoryAndCopyFrom(t *testing.T) {
	t.Parallel()

	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("skipping integration test: set TEST_PG_DSN to run")
	}

	ctx := context.Background()
	repo, closeFn, err := NewRepository(ctx, Config{
		DSN:   dsn,
		Table: "public.__etl_copyfrom_repo_test",
		Columns: []string{
			"a", "b",
		},
	})
	if err != nil {
		t.Fatalf("NewRepository failed: %v", err)
	}
	defer closeFn()

	// DDL for a scratch table. We acquire a connection from the pool to keep
	// the test self-contained without exposing admin helpers in the API.
	conn, err := repo.pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("pool acquire: %v", err)
	}
	_, _ = conn.Exec(ctx, `DROP TABLE IF EXISTS public.__etl_copyfrom_repo_test`)
	_, err = conn.Exec(ctx, `CREATE TABLE public.__etl_copyfrom_repo_test (a int, b text)`)
	conn.Release()
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Exercise CopyFrom via a small batch; verify affected row count.
	rows := [][]any{{1, "x"}, {2, "y"}}
	n, err := repo.CopyFrom(ctx, []string{"a", "b"}, rows)
	if err != nil {
		t.Fatalf("repo.CopyFrom: %v", err)
	}
	if n != 2 {
		t.Fatalf("rows affected=%d, want 2", n)
	}

	// Compile-time sanity: splitFQN returns a pgx.Identifier.
	var _ pgx.Identifier = splitFQN("public.__etl_copyfrom_repo_test")
}
