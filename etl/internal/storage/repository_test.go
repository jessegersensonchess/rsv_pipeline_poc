package storage

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

// fakeRepo is a minimal Repository implementation for tests.
type fakeRepo struct {
	closed bool
}

func (f *fakeRepo) BulkUpsert(ctx context.Context, rows []map[string]any, keyCols []string, dateCol string) (int64, error) {
	return int64(len(rows)), nil
}
func (f *fakeRepo) CopyFrom(ctx context.Context, columns []string, rows [][]any) (int64, error) {
	return int64(len(rows)), nil
}
func (f *fakeRepo) Close() { f.closed = true }

func (f *fakeRepo) Exec(ctx context.Context, sql string) error { return nil }

// TestRegisterAndNew_Success verifies that registering a backend enables New()
// to return the corresponding repository.
func TestRegisterAndNew_Success(t *testing.T) {
	t.Parallel()

	kind := "fake"
	Register(kind, func(ctx context.Context, cfg Config) (Repository, error) {
		return &fakeRepo{}, nil
	})

	repo, err := New(context.Background(), Config{Kind: kind})
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	if repo == nil {
		t.Fatalf("New returned nil repo")
	}

	// Ensure ListKinds contains the registered kind.
	kinds := ListKinds()
	found := false
	for _, k := range kinds {
		if k == kind {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("registered kind %q not present in ListKinds: %v", kind, kinds)
	}
}

// TestNew_Unsupported verifies that unsupported kinds return a helpful error.
func TestNew_Unsupported(t *testing.T) {
	t.Parallel()

	_, err := New(context.Background(), Config{Kind: "does-not-exist"})
	if err == nil {
		t.Fatalf("expected error for unsupported kind")
	}
	if got, want := err.Error(), "unsupported storage.kind=does-not-exist"; got != want {
		t.Fatalf("error = %q, want %q", got, want)
	}
}

// TestRegister_Override verifies that re-registering a kind overrides the
// previous factory (useful for tests and dynamic wiring).
func TestRegister_Override(t *testing.T) {
	t.Parallel()

	kind := "override"
	calls := 0

	Register(kind, func(ctx context.Context, cfg Config) (Repository, error) {
		calls++
		return &fakeRepo{}, nil
	})
	Register(kind, func(ctx context.Context, cfg Config) (Repository, error) {
		calls += 10
		return &fakeRepo{}, nil
	})

	_, err := New(context.Background(), Config{Kind: kind})
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	if calls != 10 { // only the second factory should have been used
		t.Fatalf("factory call count = %d, want 10", calls)
	}
}

// TestListKinds_Snapshot performs a shallow sanity check that ListKinds returns
// a copy (mutations by caller do not affect internal registry).
func TestListKinds_Snapshot(t *testing.T) {
	t.Parallel()

	k := "snap"
	Register(k, func(ctx context.Context, cfg Config) (Repository, error) { return &fakeRepo{}, nil })

	a := ListKinds()
	if len(a) == 0 {
		t.Fatalf("ListKinds empty after registration")
	}
	// Mutate the returned slice; registry should be unaffected.
	a[0] = "mutated"

	b := ListKinds()
	if reflect.DeepEqual(a, b) {
		t.Fatalf("ListKinds returned same slice; want snapshot copy")
	}
}

// TestRegister_AllowsErrors shows factories can return errors that bubble up.
func TestRegister_AllowsErrors(t *testing.T) {
	t.Parallel()

	kind := "errkind"
	want := errors.New("boom")

	Register(kind, func(ctx context.Context, cfg Config) (Repository, error) {
		return nil, want
	})

	_, err := New(context.Background(), Config{Kind: kind})
	if !errors.Is(err, want) {
		t.Fatalf("want %v, got %v", want, err)
	}
}
