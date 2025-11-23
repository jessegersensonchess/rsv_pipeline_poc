package ddl

import (
	"context"
	"strconv"
	"strings"
	"testing"

	gddl "etl/internal/ddl"
	"etl/internal/storage"
)

// TestQuoteIdent verifies SQL Server identifier quoting and escaping behavior
// for single identifier segments in quoteIdent.
func TestQuoteIdent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		id   string
		want string
	}{
		{name: "simple", id: "name", want: "[name]"},
		{name: "empty", id: "", want: "[]"},
		{name: "with space", id: "order id", want: "[order id]"},
		// Note: quoteIdent does not attempt to detect existing brackets; it just
		// wraps and escapes closing brackets.
		{name: "already bracketed", id: "[name]", want: "[[name]]]"},
		{name: "escape closing bracket", id: "weird]id", want: "[weird]]id]"},
		{name: "multiple closing brackets", id: "a]]b]", want: "[a]]]]b]]]"},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := quoteIdent(tt.id)
			if got != tt.want {
				t.Fatalf("quoteIdent(%q) = %q, want %q", tt.id, got, tt.want)
			}
		})
	}
}

// TestQuoteFQN verifies quoting and splitting behavior for schema-qualified
// table names in quoteFQN.
func TestQuoteFQN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fqn  string
		want string
	}{
		{name: "simple table", fqn: "Users", want: "[Users]"},
		{name: "schema and table", fqn: "dbo.Users", want: "[dbo].[Users]"},
		{name: "three segments", fqn: "a.b.c", want: "[a].[b].[c]"},
		{name: "with spaces", fqn: " dbo . Users ", want: "[dbo].[Users]"},
		{name: "extra dots", fqn: ".dbo..Users.", want: "[dbo].[Users]"},
		{name: "empty", fqn: "", want: ""},
		{name: "with closing bracket", fqn: "dbo.weird]name", want: "[dbo].[weird]]name]"},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := quoteFQN(tt.fqn)
			if got != tt.want {
				t.Fatalf("quoteFQN(%q) = %q, want %q", tt.fqn, got, tt.want)
			}
		})
	}
}

// TestBuildCreateTableSQLErrors validates error handling and basic input
// validation in BuildCreateTableSQL.
func TestBuildCreateTableSQLErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		def  gddl.TableDef
	}{
		{
			name: "empty FQN",
			def: gddl.TableDef{
				FQN:     "   ",
				Columns: []gddl.ColumnDef{{Name: "id", SQLType: "BIGINT"}},
			},
		},
		{
			name: "no columns",
			def: gddl.TableDef{
				FQN:     "dbo.Users",
				Columns: nil,
			},
		},
		{
			name: "column empty name",
			def: gddl.TableDef{
				FQN: "dbo.Users",
				Columns: []gddl.ColumnDef{
					{Name: "id", SQLType: "BIGINT"},
					{Name: "   ", SQLType: "INT"},
				},
			},
		},
		{
			name: "column missing SQLType",
			def: gddl.TableDef{
				FQN: "dbo.Users",
				Columns: []gddl.ColumnDef{
					{Name: "id", SQLType: ""},
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := BuildCreateTableSQL(tt.def)
			if err == nil {
				t.Fatalf("BuildCreateTableSQL(%+v) error = nil, want non-nil", tt.def)
			}
			if got != "" {
				t.Fatalf("BuildCreateTableSQL(%+v) SQL = %q, want empty string on error", tt.def, got)
			}
		})
	}
}

// TestBuildCreateTableSQLBasic verifies that BuildCreateTableSQL renders a
// simple table with primary key, nullable column, and default expression.
func TestBuildCreateTableSQLBasic(t *testing.T) {
	t.Parallel()

	def := gddl.TableDef{
		FQN: "dbo.Users",
		Columns: []gddl.ColumnDef{
			{
				Name:       "id",
				SQLType:    "BIGINT",
				Nullable:   false,
				PrimaryKey: true,
			},
			{
				Name:     "name",
				SQLType:  "NVARCHAR(100)",
				Nullable: true,
				Default:  "N''",
			},
		},
	}

	got, err := BuildCreateTableSQL(def)
	if err != nil {
		t.Fatalf("BuildCreateTableSQL() error = %v", err)
	}

	want := "" +
		"IF OBJECT_ID(N'[dbo].[Users]', N'U') IS NULL\n" +
		"BEGIN\n" +
		"  CREATE TABLE [dbo].[Users] (\n" +
		"    [id] BIGINT NOT NULL,\n" +
		"    [name] NVARCHAR(100) DEFAULT N'',\n" +
		"    PRIMARY KEY ([id])\n" +
		"  );\n" +
		"END;"

	if got != want {
		t.Fatalf("BuildCreateTableSQL() =\n%s\nwant:\n%s", got, want)
	}
}

// TestBuildCreateTableSQLPrimaryKeyVariants verifies rendering of
// multi-column primary keys and absence of PRIMARY KEY when not requested.
func TestBuildCreateTableSQLPrimaryKeyVariants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		def        gddl.TableDef
		wantPKLine string
		wantHasPK  bool
	}{
		{
			name: "multi-column primary key",
			def: gddl.TableDef{
				FQN: "sales.OrderItems",
				Columns: []gddl.ColumnDef{
					{Name: "order_id", SQLType: "BIGINT", PrimaryKey: true},
					{Name: "item_id", SQLType: "INT", PrimaryKey: true},
					{Name: "qty", SQLType: "INT"},
				},
			},
			wantPKLine: "PRIMARY KEY ([order_id], [item_id])",
			wantHasPK:  true,
		},
		{
			name: "no primary key at all",
			def: gddl.TableDef{
				FQN: "dbo.Logs",
				Columns: []gddl.ColumnDef{
					{Name: "id", SQLType: "BIGINT"},
					{Name: "message", SQLType: "NVARCHAR(MAX)"},
				},
			},
			wantHasPK: false,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := BuildCreateTableSQL(tt.def)
			if err != nil {
				t.Fatalf("BuildCreateTableSQL() error = %v", err)
			}

			hasPK := strings.Contains(got, "PRIMARY KEY")
			if hasPK != tt.wantHasPK {
				t.Fatalf("PRIMARY KEY presence = %v, want %v; SQL:\n%s", hasPK, tt.wantHasPK, got)
			}
			if tt.wantHasPK && tt.wantPKLine != "" && !strings.Contains(got, tt.wantPKLine) {
				t.Fatalf("SQL does not contain expected PK line %q; SQL:\n%s", tt.wantPKLine, got)
			}
		})
	}
}

// TestBuildCreateTableSQLDefaultRaw verifies that default expressions are
// emitted as raw SQL without additional quoting or escaping.
func TestBuildCreateTableSQLDefaultRaw(t *testing.T) {
	t.Parallel()

	def := gddl.TableDef{
		FQN: "dbo.Events",
		Columns: []gddl.ColumnDef{
			{
				Name:     "created_at",
				SQLType:  "DATETIME2",
				Nullable: false,
				Default:  "SYSUTCDATETIME()",
			},
		},
	}

	got, err := BuildCreateTableSQL(def)
	if err != nil {
		t.Fatalf("BuildCreateTableSQL() error = %v", err)
	}

	if !strings.Contains(got, "DEFAULT SYSUTCDATETIME()") {
		t.Fatalf("SQL does not contain expected raw DEFAULT expression; SQL:\n%s", got)
	}
}

// fakeRepository is a test double for storage.Repository used to verify
// EnsureTable behavior without hitting a real database.
type fakeRepository struct {
	storage.Repository // embed to satisfy interface if it grows
	execCalls          int
	lastSQL            string
	err                error
}

// Exec records the executed SQL and returns the configured error.
func (f *fakeRepository) Exec(ctx context.Context, sql string) error {
	f.execCalls++
	f.lastSQL = sql
	return f.err
}

// TestEnsureTableExecutesSQL verifies that EnsureTable generates a CREATE TABLE
// statement and passes it to the repository's Exec method.
func TestEnsureTableExecutesSQL(t *testing.T) {
	t.Parallel()

	def := gddl.TableDef{
		FQN: "dbo.Users",
		Columns: []gddl.ColumnDef{
			{Name: "id", SQLType: "BIGINT", PrimaryKey: true},
		},
	}

	var repo fakeRepository
	ctx := context.Background()

	err := EnsureTable(ctx, &repo, def)
	if err != nil {
		t.Fatalf("EnsureTable() error = %v", err)
	}

	if repo.execCalls != 1 {
		t.Fatalf("repo.Exec called %d times, want 1", repo.execCalls)
	}
	if repo.lastSQL == "" {
		t.Fatalf("repo.Exec was called with empty SQL")
	}
	if !strings.Contains(repo.lastSQL, "CREATE TABLE") {
		t.Fatalf("repo.Exec SQL does not contain CREATE TABLE; SQL:\n%s", repo.lastSQL)
	}
}

// TestEnsureTablePropagatesErrors verifies that EnsureTable returns errors
// from BuildCreateTableSQL and from repo.Exec.
func TestEnsureTablePropagatesErrors(t *testing.T) {
	t.Parallel()

	t.Run("build error prevents exec", func(t *testing.T) {
		t.Parallel()

		// Missing FQN triggers BuildCreateTableSQL error.
		def := gddl.TableDef{
			FQN:     "",
			Columns: []gddl.ColumnDef{{Name: "id", SQLType: "BIGINT"}},
		}
		repo := &fakeRepository{
			err: nil,
		}

		defer func() {
			if repo.execCalls != 0 {
				t.Fatalf("repo.Exec was called %d times, want 0 when BuildCreateTableSQL fails", repo.execCalls)
			}
		}()

		if err := EnsureTable(context.Background(), repo, def); err == nil {
			t.Fatalf("EnsureTable() error = nil, want non-nil for invalid TableDef")
		}
	})

	t.Run("exec error is returned", func(t *testing.T) {
		t.Parallel()

		def := gddl.TableDef{
			FQN: "dbo.Users",
			Columns: []gddl.ColumnDef{
				{Name: "id", SQLType: "BIGINT"},
			},
		}
		repo := &fakeRepository{
			err: context.Canceled, // arbitrary non-nil error
		}

		err := EnsureTable(context.Background(), repo, def)
		if err == nil {
			t.Fatalf("EnsureTable() error = nil, want non-nil")
		}
		if err != repo.err {
			t.Fatalf("EnsureTable() error = %v, want %v", err, repo.err)
		}
		if repo.execCalls != 1 {
			t.Fatalf("repo.Exec called %d times, want 1", repo.execCalls)
		}
	})
}

// BenchmarkBuildCreateTableSQLSmall measures the performance of
// BuildCreateTableSQL for a small table definition.
func BenchmarkBuildCreateTableSQLSmall(b *testing.B) {
	def := gddl.TableDef{
		FQN: "dbo.Users",
		Columns: []gddl.ColumnDef{
			{Name: "id", SQLType: "BIGINT", PrimaryKey: true},
			{Name: "name", SQLType: "NVARCHAR(100)", Nullable: true},
			{Name: "created_at", SQLType: "DATETIME2", Nullable: false, Default: "SYSUTCDATETIME()"},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := BuildCreateTableSQL(def); err != nil {
			b.Fatalf("BuildCreateTableSQL() error = %v", err)
		}
	}
}

// BenchmarkBuildCreateTableSQLWide measures the performance of
// BuildCreateTableSQL for a table with many columns, approximating a
// wide fact table.
func BenchmarkBuildCreateTableSQLWide(b *testing.B) {
	const numCols = 64

	cols := make([]gddl.ColumnDef, 0, numCols)
	for i := 0; i < numCols; i++ {
		cols = append(cols, gddl.ColumnDef{
			Name:     "col_" + strconv.Itoa(i),
			SQLType:  "NVARCHAR(255)",
			Nullable: i%2 == 0,
		})
	}
	// First column primary key just to exercise PK code path.
	cols[0].PrimaryKey = true

	def := gddl.TableDef{
		FQN:     "dbo.WideTable",
		Columns: cols,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := BuildCreateTableSQL(def); err != nil {
			b.Fatalf("BuildCreateTableSQL() error = %v", err)
		}
	}
}

// BenchmarkEnsureTableNoopRepo measures the overhead of EnsureTable when
// using a repository whose Exec method is effectively a no-op.
func BenchmarkEnsureTableNoopRepo(b *testing.B) {
	def := gddl.TableDef{
		FQN: "dbo.Users",
		Columns: []gddl.ColumnDef{
			{Name: "id", SQLType: "BIGINT", PrimaryKey: true},
			{Name: "name", SQLType: "NVARCHAR(100)", Nullable: true},
		},
	}

	repo := &fakeRepository{}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := EnsureTable(ctx, repo, def); err != nil {
			b.Fatalf("EnsureTable() error = %v", err)
		}
	}
}
