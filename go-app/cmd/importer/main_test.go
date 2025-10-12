package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"csvloader/internal/config"
	"csvloader/internal/db"
)

//
// ======================
//  Test fakes (no I/O)
// ======================
//
// These satisfy db.DB and db.SmallDB without touching real sockets or files.
// Keep them minimal and deterministic; we only assert call ordering/values.

type fakeDB struct{}

func (f *fakeDB) Exec(context.Context, string, ...any) error { return nil }
func (f *fakeDB) BeginTx(context.Context) (db.Tx, error)     { return nil, nil }
func (f *fakeDB) Close(context.Context) error                { return nil }

type fakeSmallDB struct{}

func (f *fakeSmallDB) CreateOwnershipTable(context.Context) error           { return nil }
func (f *fakeSmallDB) CopyOwnership(context.Context, [][]interface{}) error { return nil }
func (f *fakeSmallDB) Close(context.Context) error                          { return nil }

// testCfg returns a baseline config used by most tests; individual tests
// tweak fields (e.g., DBDriver/DSN) to exercise branches.
func testCfg() *config.Config {
	return &config.Config{
		DBDriver:     "postgres",
		DBUser:       "u",
		DBPassword:   "p",
		DBHost:       "h",
		DBPort:       "5432",
		DBName:       "n",
		OwnershipCSV: "own.csv",
		VehicleCSV:   "veh.csv",
		BatchSize:    1000,
		Workers:      4,
	}
}

// TestDefaultDeps_ProvidesNonNilProductionWiring
// Sanity check that defaultDeps() returns non-nil function pointers.
// We don't execute them here—just validate the production wiring exists.
func TestDefaultDeps_ProvidesNonNilProductionWiring(t *testing.T) {
	d := defaultDeps()
	if d.NewPgDB == nil || d.NewSmallPg == nil || d.NewSQLDB == nil || d.NewSmallMSSQL == nil {
		t.Fatalf("DB constructors must be non-nil")
	}
	if d.ImportOwnership == nil || d.ImportVehicleTech == nil {
		t.Fatalf("importers must be non-nil")
	}
	if d.Sleep == nil {
		t.Fatalf("sleep must be non-nil")
	}
}

// TestRun_Postgres_Path_OK_BuildsDSN_AndCallsImporters
// Verifies DSN building for Postgres when --dsn is empty and that both importers
// are invoked with factories that construct DB/SmallDB successfully.
func TestRun_Postgres_Path_OK_BuildsDSN_AndCallsImporters(t *testing.T) {
	cfg := testCfg()
	cfg.DSN = "" // force DSN build

	calledPg := false
	calledSmallPg := false
	calledOwn := false
	calledVeh := false

	deps := Deps{
		NewPgDB: func(ctx context.Context, dsn string) (db.DB, error) {
			calledPg = true
			want := "postgres://u:p@h:5432/n"
			if dsn != want {
				t.Fatalf("pg dsn got %q want %q", dsn, want)
			}
			return &fakeDB{}, nil
		},
		NewSmallPg: func(ctx context.Context, dsn string) (db.SmallDB, error) {
			calledSmallPg = true
			return &fakeSmallDB{}, nil
		},
		NewSQLDB:      func(driver, dsn string) (db.DB, error) { t.Fatalf("should not be called"); return nil, nil },
		NewSmallMSSQL: func(dsn string) (db.SmallDB, error) { t.Fatalf("should not be called"); return nil, nil },
		ImportOwnership: func(ctx context.Context, cfg *config.Config, smallFactory db.SmallDBFactory, path string) error {
			calledOwn = true
			if path != "own.csv" {
				t.Fatalf("ownership path got %q", path)
			}
			s, err := smallFactory(ctx)
			if err != nil || s == nil {
				t.Fatalf("small factory failed: %v %T", err, s)
			}
			return nil
		},
		ImportVehicleTech: func(ctx context.Context, cfg *config.Config, factory db.DBFactory, path string) error {
			calledVeh = true
			if path != "veh.csv" {
				t.Fatalf("vehicle path got %q", path)
			}
			d, err := factory(ctx)
			if err != nil || d == nil {
				t.Fatalf("db factory failed: %v %T", err, d)
			}
			return nil
		},
		Sleep: func(time.Duration) {}, // skip real sleep
	}

	if err := run(context.Background(), cfg, deps); err != nil {
		t.Fatalf("run error: %v", err)
	}
	if !calledPg || !calledSmallPg || !calledOwn || !calledVeh {
		t.Fatalf("expected all paths called: pg=%v smallPg=%v own=%v veh=%v",
			calledPg, calledSmallPg, calledOwn, calledVeh)
	}
}

// TestRun_Postgres_Path_UsesProvidedDSN
// Ensures that when --dsn is provided, run() uses it verbatim and the DB
// constructor (NewPgDB) is actually invoked via the factory path.
//
// Note: run() only calls constructors through the factories that are handed
// to the importers. Therefore, this test's ImportVehicleTech stub MUST call
// the provided factory to exercise NewPgDB.
func TestRun_Postgres_Path_UsesProvidedDSN(t *testing.T) {
	cfg := testCfg()
	cfg.DSN = "postgres://explicit:dsn@host:5433/db"

	calledNewPg := false

	deps := Deps{
		NewPgDB: func(ctx context.Context, dsn string) (db.DB, error) {
			calledNewPg = true
			if dsn != cfg.DSN {
				t.Fatalf("dsn mismatch: got %q want %q", dsn, cfg.DSN)
			}
			return &fakeDB{}, nil
		},
		NewSmallPg: func(ctx context.Context, dsn string) (db.SmallDB, error) {
			// Ownership importer uses SmallDB; keep it simple and return a fake.
			return &fakeSmallDB{}, nil
		},
		ImportOwnership: func(context.Context, *config.Config, db.SmallDBFactory, string) error {
			// Not relevant to this assertion; returning nil is fine.
			return nil
		},
		ImportVehicleTech: func(ctx context.Context, _ *config.Config, factory db.DBFactory, _ string) error {
			// CRITICAL: call the factory so NewPgDB is exercised.
			d, err := factory(ctx)
			if err != nil || d == nil {
				t.Fatalf("db factory failed: %v %T", err, d)
			}
			return nil
		},
		Sleep: func(time.Duration) {}, // avoid real sleep
	}

	if err := run(context.Background(), cfg, deps); err != nil {
		t.Fatalf("run error: %v", err)
	}
	if !calledNewPg {
		t.Fatalf("expected NewPgDB to be called")
	}
}

// TestRun_MSSQL_RequiresDSN
// Validates argument checking for MSSQL: --dsn must be provided.
func TestRun_MSSQL_RequiresDSN(t *testing.T) {
	cfg := testCfg()
	cfg.DBDriver = "mssql"
	cfg.DSN = "" // missing

	deps := Deps{
		Sleep: func(time.Duration) {},
		// ensure no constructors/importers are called
		NewSQLDB:      func(driver, dsn string) (db.DB, error) { t.Fatalf("unexpected"); return nil, nil },
		NewSmallMSSQL: func(dsn string) (db.SmallDB, error) { t.Fatalf("unexpected"); return nil, nil },
		ImportOwnership: func(context.Context, *config.Config, db.SmallDBFactory, string) error {
			t.Fatalf("unexpected")
			return nil
		},
		ImportVehicleTech: func(context.Context, *config.Config, db.DBFactory, string) error { t.Fatalf("unexpected"); return nil },
	}

	err := run(context.Background(), cfg, deps)
	if err == nil || err.Error() != "--dsn required for mssql" {
		t.Fatalf("want dsn-required error, got %v", err)
	}
}

// TestRun_MSSQL_Path_OK_CallsConstructorsAndImporters
// Happy path for MSSQL: provided DSN is passed through to both DB constructors;
// both importers are called with usable factories.
func TestRun_MSSQL_Path_OK_CallsConstructorsAndImporters(t *testing.T) {
	cfg := testCfg()
	cfg.DBDriver = "mssql"
	cfg.DSN = "sqlserver://u:p@h:1433?database=n"

	calledSQL := false
	calledSmall := false
	calledOwn := false
	calledVeh := false

	deps := Deps{
		NewSQLDB: func(driver, dsn string) (db.DB, error) {
			calledSQL = true
			if driver != "sqlserver" || dsn != cfg.DSN {
				t.Fatalf("driver/dsn mismatch: %q %q", driver, dsn)
			}
			return &fakeDB{}, nil
		},
		NewSmallMSSQL: func(dsn string) (db.SmallDB, error) {
			calledSmall = true
			if dsn != cfg.DSN {
				t.Fatalf("small dsn mismatch: %q", dsn)
			}
			return &fakeSmallDB{}, nil
		},
		ImportOwnership: func(ctx context.Context, cfg *config.Config, smallFactory db.SmallDBFactory, path string) error {
			calledOwn = true
			if _, err := smallFactory(ctx); err != nil {
				t.Fatalf("small factory failed: %v", err)
			}
			return nil
		},
		ImportVehicleTech: func(ctx context.Context, cfg *config.Config, factory db.DBFactory, path string) error {
			calledVeh = true
			if _, err := factory(ctx); err != nil {
				t.Fatalf("db factory failed: %v", err)
			}
			return nil
		},
		Sleep: func(time.Duration) {},
	}

	if err := run(context.Background(), cfg, deps); err != nil {
		t.Fatalf("run error: %v", err)
	}
	if !calledSQL || !calledSmall || !calledOwn || !calledVeh {
		t.Fatalf("expected all paths called: sql=%v small=%v own=%v veh=%v", calledSQL, calledSmall, calledOwn, calledVeh)
	}
}

// TestRun_UnsupportedDriver
// Ensures run() rejects unknown drivers with a clear error message.
func TestRun_UnsupportedDriver(t *testing.T) {
	cfg := testCfg()
	cfg.DBDriver = "oracle"

	deps := Deps{
		Sleep: func(time.Duration) {}, // no-op
	}

	err := run(context.Background(), cfg, deps)
	if err == nil || err.Error() != `unsupported --db_driver="oracle"` {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestRun_ImporterErrorsBubbleUp
// Verifies that importer errors are propagated with context.
func TestRun_ImporterErrorsBubbleUp(t *testing.T) {
	cfg := testCfg()

	want := errors.New("boom")
	deps := Deps{
		NewPgDB:           func(ctx context.Context, dsn string) (db.DB, error) { return &fakeDB{}, nil },
		NewSmallPg:        func(ctx context.Context, dsn string) (db.SmallDB, error) { return &fakeSmallDB{}, nil },
		ImportOwnership:   func(context.Context, *config.Config, db.SmallDBFactory, string) error { return want },
		ImportVehicleTech: func(context.Context, *config.Config, db.DBFactory, string) error { return nil },
		Sleep:             func(time.Duration) {},
	}

	err := run(context.Background(), cfg, deps)
	if err == nil || err.Error() != "ownership import failed: boom" {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestRun_VehicleTechErrorBubblesUp
// Mirrors the previous test but for the vehicle tech importer.
func TestRun_VehicleTechErrorBubblesUp(t *testing.T) {
	cfg := testCfg()

	want := errors.New("vboom")
	deps := Deps{
		NewPgDB:           func(ctx context.Context, dsn string) (db.DB, error) { return &fakeDB{}, nil },
		NewSmallPg:        func(ctx context.Context, dsn string) (db.SmallDB, error) { return &fakeSmallDB{}, nil },
		ImportOwnership:   func(context.Context, *config.Config, db.SmallDBFactory, string) error { return nil },
		ImportVehicleTech: func(context.Context, *config.Config, db.DBFactory, string) error { return want },
		Sleep:             func(time.Duration) {},
	}

	err := run(context.Background(), cfg, deps)
	if err == nil || err.Error() != "vehicle tech import failed: vboom" {
		t.Fatalf("unexpected error: %v", err)
	}
}
