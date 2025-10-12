package config

import (
	"flag"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// TestLoadFrom_EnvDefaultsAndFlags validates the basic precedence model for
// LoadFromArgs: environment seeds defaults, explicit flags override env.
//
// This exercises multiple types (string, int, bool) and ensures a user-supplied
// flag (`-workers`) wins over env.
func TestLoadFrom_EnvDefaultsAndFlags(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	env := map[string]string{
		"DB_DRIVER":   "mssql",
		"DB_DSN":      "sqlserver://u:p@h:1433?database=d",
		"BATCH_SIZE":  "12",
		"PG_UNLOGGED": "false",
	}
	getenv := func(k string) string { return env[k] }

	// Pass args so flags are defined first, then parsed.
	cfg := LoadFromArgs(fs, getenv, []string{"-workers=3"})

	if cfg.DBDriver != "mssql" || cfg.DSN == "" {
		t.Fatalf("env not applied: %+v", cfg)
	}
	if cfg.BatchSize != 12 {
		t.Fatalf("batch env not applied: %d", cfg.BatchSize)
	}
	if cfg.UnloggedTables != false {
		t.Fatalf("bool env not applied: %v", cfg.UnloggedTables)
	}
	if cfg.Workers != 3 {
		t.Fatalf("flag override not applied: %d", cfg.Workers)
	}
}

// TestLoad_Defaults ensures that when no environment or flags are present,
// default values are populated to sensible non-zero settings.
func TestLoad_Defaults(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg := LoadFrom(fs, func(string) string { return "" }) // no env
	if cfg.DBDriver != "postgres" {
		t.Fatalf("want postgres default, got %s", cfg.DBDriver)
	}
	if cfg.BatchSize == 0 || cfg.Workers == 0 {
		t.Fatalf("defaults not set: %+v", cfg)
	}
}

// TestLoad_DefaultsSane verifies that Load() reads process flags/env without
// mutating them here. It checks only structural sanity to keep the test hermetic.
func TestLoad_DefaultsSane(t *testing.T) {
	t.Parallel()

	// Arrange / Act
	cfg := Load()

	// Assert: defaults are populated to sensible values
	if cfg.DBDriver == "" {
		t.Fatalf("DBDriver should have a default")
	}
	if cfg.BatchSize <= 0 || cfg.Workers <= 0 {
		t.Fatalf("BatchSize/Workers must have positive defaults: batch=%d workers=%d", cfg.BatchSize, cfg.Workers)
	}
}

// Test_envOrDefault ensures envOrDefault returns env value when set,
// otherwise falls back to the provided default string. These helpers are
// package-private, so tests in package config can call them directly.
func Test_envOrDefault(t *testing.T) {
	t.Parallel()

	// Save and restore environment key used in test.
	const key = "TEST_ENV_OR_DEFAULT"
	restore := snapshotEnv(key)
	defer restore()

	// 1) empty env -> default used
	_ = os.Unsetenv(key)
	if got := envOrDefault(key, "DEF"); got != "DEF" {
		t.Fatalf("want DEF, got %q", got)
	}

	// 2) env present -> env used
	_ = os.Setenv(key, "VAL")
	if got := envOrDefault(key, "DEF"); got != "VAL" {
		t.Fatalf("want VAL, got %q", got)
	}
}

// Test_intEnvOrDefault verifies that when the environment contains a
// valid integer string it is parsed; invalid values fall back to the default.
func Test_intEnvOrDefault(t *testing.T) {
	t.Parallel()

	const key = "TEST_INT_ENV"
	restore := snapshotEnv(key)
	defer restore()

	type tc struct {
		env  string
		def  int
		want int
	}
	cases := []tc{
		{"", 10, 10},      // empty -> default
		{"123", 10, 123},  // valid int
		{"-5", 10, -5},    // negative valid int
		{"not-int", 7, 7}, // invalid -> default
	}
	for i, c := range cases {
		if c.env == "" {
			_ = os.Unsetenv(key)
		} else {
			_ = os.Setenv(key, c.env)
		}
		if got := intEnvOrDefault(key, c.def); got != c.want {
			t.Fatalf("case %d: env=%q def=%d -> got=%d want=%d", i, c.env, c.def, got, c.want)
		}
	}
}

// Test_boolEnvOrDefault checks the accepted truthy/falsey forms and
// default behavior. Case insensitivity is required.
func Test_boolEnvOrDefault(t *testing.T) {
	t.Parallel()

	const key = "TEST_BOOL_ENV"
	restore := snapshotEnv(key)
	defer restore()

	type tc struct {
		env  string
		def  bool
		want bool
	}
	truthy := []string{"1", "true", "True", "YES", "On"}
	falsy := []string{"0", "false", "False", "no", "OFF"}

	// Empty -> default preserved
	_ = os.Unsetenv(key)
	if got := boolEnvOrDefault(key, true); !got {
		t.Fatalf("empty env should preserve default=true")
	}
	if got := boolEnvOrDefault(key, false); got {
		t.Fatalf("empty env should preserve default=false")
	}

	// Truthy forms
	for _, v := range truthy {
		_ = os.Setenv(key, v)
		if got := boolEnvOrDefault(key, false); !got {
			t.Fatalf("env=%q should be true", v)
		}
	}

	// Falsy forms
	for _, v := range falsy {
		_ = os.Setenv(key, v)
		if got := boolEnvOrDefault(key, true); got {
			t.Fatalf("env=%q should be false", v)
		}
	}
}

// TestLoadFromArgs_EnvAndFlagPrecedence validates the injectable loader:
//  1. environment seeds defaults,
//  2. flags override env where present,
//  3. types are parsed as expected.
func TestLoadFromArgs_EnvAndFlagPrecedence(t *testing.T) {
	t.Parallel()

	// Arrange a private FlagSet to avoid polluting global flags.
	fs := flag.NewFlagSet("test", flag.ContinueOnError)

	// Provide a getenv func backed by a local map for hermeticity.
	env := map[string]string{
		"DB_DRIVER":   "mssql",
		"DB_DSN":      "sqlserver://user:pass@localhost:1433?database=db",
		"BATCH_SIZE":  "42",
		"PG_UNLOGGED": "false",
	}
	getenv := func(k string) string { return env[k] }

	// Act: flags override env; workers=3 should beat env default.
	cfg := LoadFromArgs(fs, getenv, []string{"-workers=3", "-db_host=myhost"})

	// Assert: env applied
	if cfg.DBDriver != "mssql" || cfg.DSN == "" {
		t.Fatalf("env not applied: %+v", cfg)
	}
	// Assert: flags override env or defaults
	if cfg.Workers != 3 {
		t.Fatalf("flag override failed for workers: %d", cfg.Workers)
	}
	if cfg.DBHost != "myhost" {
		t.Fatalf("flag override failed for db_host: %s", cfg.DBHost)
	}
	// Types
	if cfg.BatchSize != 42 {
		t.Fatalf("int env parse failed: batch_size=%d", cfg.BatchSize)
	}
	if cfg.UnloggedTables != false {
		t.Fatalf("bool env parse failed: unlogged=%v", cfg.UnloggedTables)
	}
}

// TestLoadFromArgs_PgUnloggedTruthy ensures the inner boolEnvOrDefaultFn path
// returns true when PG_UNLOGGED is set to any accepted truthy value. This
// specifically exercises the loader's inline helper (distinct from the
// package-level boolEnvOrDefault) by observing the parsed flag default.
func TestLoadFromArgs_PgUnloggedTruthy(t *testing.T) {
	t.Parallel()

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	env := map[string]string{
		// Set to a truthy variant to ensure the inline helper yields true.
		"PG_UNLOGGED": "on",
	}
	getenv := func(k string) string { return env[k] }

	// No explicit -pg_unlogged flag provided; value should come from env.
	cfg := LoadFromArgs(fs, getenv, nil)

	if cfg.UnloggedTables != true {
		t.Fatalf("PG_UNLOGGED truthy env should set UnloggedTables=true, got %v", cfg.UnloggedTables)
	}
}

// snapshotEnv returns a restore func that resets key to its prior value.
// This is the standard pattern used internally to keep tests hermetic.
func snapshotEnv(key string) func() {
	old, had := os.LookupEnv(key)
	return func() {
		if had {
			_ = os.Setenv(key, old)
		} else {
			_ = os.Unsetenv(key)
		}
	}
}

// TestPackageInit_NoSideEffects is a tiny sanity check to ensure the package
// initializes cleanly under the race detector and on different platforms.
// It doesn't assert functional behavior—only that imports and init paths are safe.
func TestPackageInit_NoSideEffects(t *testing.T) {
	t.Parallel()
	_ = strings.Contains(runtime.GOOS, "") // touch runtime to silence unused import in some editors
	_ = strconv.IntSize                    // and strconv for parity with int parsing
}
