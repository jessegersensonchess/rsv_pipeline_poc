package ddl

import (
	"context"
	"strings"
	"testing"

	gddl "etl/internal/ddl"
	"etl/internal/storage"
)

// fakeRepository is a test double for storage.Repository used to verify
// EnsureTable behavior without hitting a real database.
type fakeRepository struct {
	storage.Repository
	execCalls int
	lastSQL   string
	err       error
}

func (f *fakeRepository) Exec(ctx context.Context, sql string) error {
	f.execCalls++
	f.lastSQL = sql
	return f.err
}

// TestEnsureTableExecutesSQL verifies that EnsureTable builds a CREATE TABLE
// statement and passes it to the repository's Exec method.
func TestEnsureTableExecutesSQL(t *testing.T) {
	t.Parallel()

	def := gddl.TableDef{
		FQN: "events",
		Columns: []gddl.ColumnDef{
			{Name: "id", SQLType: "INTEGER", PrimaryKey: true},
		},
	}

	var repo fakeRepository
	ctx := context.Background()

	if err := EnsureTable(ctx, &repo, def); err != nil {
		t.Fatalf("EnsureTable() error = %v", err)
	}

	if repo.execCalls != 1 {
		t.Fatalf("repo.Exec called %d times, want 1", repo.execCalls)
	}
	if repo.lastSQL == "" {
		t.Fatalf("repo.Exec was called with empty SQL")
	}
	if !strings.HasPrefix(repo.lastSQL, "CREATE TABLE IF NOT EXISTS") {
		t.Fatalf("repo.Exec SQL does not start with CREATE TABLE IF NOT EXISTS:\n%s", repo.lastSQL)
	}
}

// TestEnsureTablePropagatesBuildError verifies that EnsureTable propagates
// BuildCreateTableSQL errors and does not call Exec.
func TestEnsureTablePropagatesBuildError(t *testing.T) {
	t.Parallel()

	def := gddl.TableDef{
		FQN:     "", // triggers BuildCreateTableSQL error
		Columns: []gddl.ColumnDef{{Name: "id", SQLType: "INTEGER"}},
	}

	var repo fakeRepository
	ctx := context.Background()

	err := EnsureTable(ctx, &repo, def)
	if err == nil {
		t.Fatalf("EnsureTable() error = nil, want non-nil")
	}
	if repo.execCalls != 0 {
		t.Fatalf("repo.Exec called %d times, want 0 when build fails", repo.execCalls)
	}
}
