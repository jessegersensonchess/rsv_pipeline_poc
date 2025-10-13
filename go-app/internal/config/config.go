// Package config centralizes application configuration. It follows a
// "clean" configuration pattern where all tunables live outside the
// code and are sourced from command-line flags with environment-variable
// fallbacks (12-factor friendly). Flags are defined first so that
// `-help` shows all available knobs and their defaults.
//
// Typical usage:
//
//	cfg := config.Load() // reads os.Args and os.Environ
//
// For tests, prefer LoadFromArgs to keep them hermetic:
//
//	fs := flag.NewFlagSet("test", flag.ContinueOnError)
//	getenv := func(k string) string { return testEnv[k] }
//	cfg := config.LoadFromArgs(fs, getenv, []string{"-workers=4"})
package config

import (
	"flag"
	"os"
	"strconv"
	"strings"
)

// Config holds all process configuration derived from flags and
// environment variables. All fields are plain values so the struct
// can be safely copied and used across goroutines after construction.
type Config struct {
	// IO controls input/diagnostic file locations.
	OwnershipCSV       string // Path to ownership CSV data.
	VehicleCSV         string // Path to vehicle technology CSV data.
	TechInspectionsCSV string `env:"TECH_CSV"` // path to technicky_prohlidky CSV
	SkippedDir         string // Directory for writing skipped-row CSV logs.

	// DB describes the target database. For MSSQL a full DSN is required.
	// For Postgres, DSN is optional (it can be built from discrete parts).
	DBDriver   string // Database driver: "postgres" or "mssql".
	DSN        string // Full DSN (required for MSSQL; optional for Postgres).
	DBUser     string // Database username (Postgres convenience).
	DBPassword string // Database password (Postgres convenience).
	DBHost     string // Database host (Postgres convenience).
	DBPort     string // Database port (Postgres convenience).
	DBName     string // Database name (Postgres convenience).

	// Import tunables control ingestion throughput.
	BatchSize int // Number of rows per batch/COPY.
	Workers   int // Number of parallel worker goroutines.

	// Misc contains database-specific toggles.
	UnloggedTables bool // Postgres-only: create UNLOGGED tables for speed.
}

// LoadFromArgs builds a Config by defining flags on fs, wiring each flag
// to an environment-variable fallback via getenv, and then parsing args.
// This is the most testable entry point: callers supply a private FlagSet,
// a getenv func (often backed by a map), and a synthetic arg slice.
//
// Precedence:
//  1. Environment values seed each flag's default.
//  2. Explicit CLI flags (in args) override the seeded defaults.
//
// The returned Config is fully populated; no further mutation occurs.
func LoadFromArgs(fs *flag.FlagSet, getenv func(string) string, args []string) *Config {
	cfg := &Config{}

	// Inline helpers use the provided getenv to avoid touching process env.
	envOrDefaultFn := func(k, d string) string {
		if v := getenv(k); v != "" {
			return v
		}
		return d
	}
	intEnvOrDefaultFn := func(k string, d int) int {
		if v := getenv(k); v != "" {
			if i, err := strconv.Atoi(v); err == nil {
				return i
			}
		}
		return d
	}
	boolEnvOrDefaultFn := func(k string, d bool) bool {
		if v := strings.ToLower(getenv(k)); v != "" {
			switch v {
			case "1", "true", "yes", "on":
				return true
			case "0", "false", "no", "off":
				return false
			}
		}
		return d
	}

	// IO paths
	fs.StringVar(&cfg.OwnershipCSV, "ownership_csv", envOrDefaultFn("OWNERSHIP_CSV", "RSV_vlastnik_provozovatel_vozidla_20250901.csv"), "Path to ownership CSV")
	fs.StringVar(&cfg.VehicleCSV, "vehicle_csv", envOrDefaultFn("VEHICLE_CSV", "RSV_vypis_vozidel_20250902.csv"), "Path to vehicle tech CSV")
	fs.StringVar(&cfg.TechInspectionsCSV, "tech_csv", envOrDefaultFn("TECH_CSV", "RSV_technicke_prohlidky_20250902.csv"), "Path to technical inspections CSV")
	fs.StringVar(&cfg.SkippedDir, "skipped_dir", envOrDefaultFn("SKIPPED_DIR", "./skipped"), "Directory for writing skipped-rows CSV logs.")

	// DB connectivity
	fs.StringVar(&cfg.DBDriver, "db_driver", envOrDefaultFn("DB_DRIVER", "postgres"), "Database driver: 'postgres' or 'mssql'.")
	fs.StringVar(&cfg.DSN, "dsn", getenv("DB_DSN"), "Full DSN (required for mssql).")

	fs.StringVar(&cfg.DBUser, "db_user", envOrDefaultFn("DB_USER", "user"), "DB user")
	fs.StringVar(&cfg.DBPassword, "db_password", envOrDefaultFn("DB_PASSWORD", "password"), "DB password")
	fs.StringVar(&cfg.DBHost, "db_host", envOrDefaultFn("DB_HOST", "localhost"), "DB host")
	fs.StringVar(&cfg.DBPort, "db_port", envOrDefaultFn("DB_PORT", "5432"), "DB port")
	fs.StringVar(&cfg.DBName, "db_name", envOrDefaultFn("DB_NAME", "testdb"), "DB name")

	// Throughput & toggles
	fs.IntVar(&cfg.BatchSize, "batch_size", intEnvOrDefaultFn("BATCH_SIZE", 5000), "Number of rows per batch/COPY")
	fs.IntVar(&cfg.Workers, "workers", intEnvOrDefaultFn("WORKERS", 8), "Number of parallel workers")
	fs.BoolVar(&cfg.UnloggedTables, "pg_unlogged", boolEnvOrDefaultFn("PG_UNLOGGED", true), "Postgres only: set UNLOGGED for speed")

	// Parse the provided args (nil means no extra args).
	if args == nil {
		args = []string{}
	}
	_ = fs.Parse(args)
	return cfg
}

// LoadFrom is a compatibility wrapper around LoadFromArgs for call-sites
// that don't need to pass args explicitly (useful in some tests).
func LoadFrom(fs *flag.FlagSet, getenv func(string) string) *Config {
	return LoadFromArgs(fs, getenv, nil)
}

// Load is the production entry point. It wires the loader to the process
// flag set (flag.CommandLine), reads environment variables via os.Getenv,
// and parses os.Args[1:] as the CLI arguments.
func Load() *Config {
	return LoadFromArgs(flag.CommandLine, os.Getenv, os.Args[1:])
}

// envOrDefault returns the value of environment variable k if set,
// otherwise it returns the provided default d. It reads from the
// real process environment and is intended for package-internal use.
func envOrDefault(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// intEnvOrDefault returns the integer value parsed from environment
// variable k when set to a valid decimal string; otherwise it returns d.
func intEnvOrDefault(k string, d int) int {
	if v := os.Getenv(k); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return d
}

// boolEnvOrDefault interprets environment variable k as a boolean,
// accepting common truthy/falsey forms ("1/0", "true/false", "yes/no",
// "on/off", case-insensitive). If k is unset or unrecognized, d is returned.
func boolEnvOrDefault(k string, d bool) bool {
	if v := strings.ToLower(os.Getenv(k)); v != "" {
		switch v {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off":
			return false
		}
	}
	return d
}
