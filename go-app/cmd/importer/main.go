// Command importer wires configuration, database adapters, and import pipelines.
// It serves as a thin composition layer with minimal logic and clear seams to
// enable hermetic tests. All side effects (sleep, DB constructors, and import
// entrypoints) are injected via Deps.
//
// Design goals:
//   - Keep main() tiny and delegate to run() for testability.
//   - Avoid hidden globals and make behavior obvious from Deps.
//   - Prefer explicit, readable control flow over cleverness.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"csvloader/internal/config"
	"csvloader/internal/db"
	imp "csvloader/internal/importer"
	ti "csvloader/internal/importer/techinspections"
	vt "csvloader/internal/importer/vehicletech"
)

// Deps holds injectable dependencies so run() is fully testable. Each field
// represents a boundary that would otherwise be “hard-coded” in main(). In
// tests, we pass fakes here; in production, defaultDeps() provides real funcs.
type Deps struct {
	// Constructors (DB adapters)
	NewPgDB       func(ctx context.Context, dsn string) (db.DB, error)
	NewSmallPg    func(ctx context.Context, dsn string) (db.SmallDB, error)
	NewSQLDB      func(driver, dsn string) (db.DB, error)
	NewSmallMSSQL func(dsn string) (db.SmallDB, error)

	// Importers (top-level entrypoints for data loading)
	ImportOwnership      func(ctx context.Context, cfg *config.Config, smallFactory db.SmallDBFactory, path string) error
	ImportVehicleTech    func(ctx context.Context, cfg *config.Config, factory db.DBFactory, path string) error
	ImportTechInspection func(ctx context.Context, cfg *config.Config, smallFactory db.SmallDBFactory, path string) error

	// Misc side-effects
	Sleep func(d time.Duration)
}

// defaultDeps wires production implementations. Tests should inject fakes.
func defaultDeps() Deps {
	return Deps{
		// Real DB constructors
		NewPgDB:       db.NewPgDB,
		NewSmallPg:    db.NewSmallPg,
		NewSQLDB:      db.NewSQLDB,
		NewSmallMSSQL: db.NewSmallMSSQL,

		// Real importers
		ImportOwnership:      imp.ImportOwnershipParallel,
		ImportVehicleTech:    vt.ImportVehicleTech,
		ImportTechInspection: ti.ImportTechInspectionsParallel,

		// Real sleep
		Sleep: time.Sleep,
	}
}

// run executes the main program logic given a config and injected Deps.
// It:
//
//  1. Waits briefly (configurable via Deps.Sleep) to allow DB containers to start.
//  2. Builds DB factories for the selected driver (postgres|mssql).
//  3. Executes the ownership and vehicle tech importers in sequence.
//  4. Propagates any error with clear context.
func run(ctx context.Context, cfg *config.Config, deps Deps) error {
	// Small grace period to accommodate “DB just started” scenarios. Tests inject a no-op.
	deps.Sleep(5 * time.Second)

	// Helper to build a Postgres DSN when one isn’t provided via --dsn.
	buildPgDSN := func() string {
		return "postgres://" + cfg.DBUser + ":" + cfg.DBPassword + "@" + cfg.DBHost + ":" + cfg.DBPort + "/" + cfg.DBName
	}

	var factory db.DBFactory
	var smallFactory db.SmallDBFactory

	switch cfg.DBDriver {
	case "postgres":
		dsn := cfg.DSN
		if dsn == "" {
			dsn = buildPgDSN()
		}
		factory = func(ctx context.Context) (db.DB, error) { return deps.NewPgDB(ctx, dsn) }
		smallFactory = func(ctx context.Context) (db.SmallDB, error) { return deps.NewSmallPg(ctx, dsn) }

	case "mssql":
		if cfg.DSN == "" {
			return fmt.Errorf("--dsn required for mssql")
		}
		factory = func(ctx context.Context) (db.DB, error) { return deps.NewSQLDB("sqlserver", cfg.DSN) }
		smallFactory = func(ctx context.Context) (db.SmallDB, error) { return deps.NewSmallMSSQL(cfg.DSN) }

	default:
		return fmt.Errorf("unsupported --db_driver=%q", cfg.DBDriver)
	}

	// Ownership import (small DB interface)
	if err := deps.ImportOwnership(ctx, cfg, smallFactory, cfg.OwnershipCSV); err != nil {
		return fmt.Errorf("ownership import failed: %w", err)
	}

	// Vehicle tech import (streaming DB interface)
	if err := deps.ImportVehicleTech(ctx, cfg, factory, cfg.VehicleCSV); err != nil {
		return fmt.Errorf("vehicle tech import failed: %w", err)
	}

	// Technical inspections import (small DB interface)
	if cfg.TechInspectionsCSV != "" {
		if err := deps.ImportTechInspection(ctx, cfg, smallFactory, cfg.TechInspectionsCSV); err != nil {
			return fmt.Errorf("tech inspections import failed: %w", err)
		}
	}

	return nil
}

// main is intentionally tiny. It loads config, builds real deps, and runs.
// Any error is fatal; we log once and exit non-zero.
func main() {
	cfg := config.Load()
	if err := run(context.Background(), cfg, defaultDeps()); err != nil {
		log.Fatal(err)
	}
}
