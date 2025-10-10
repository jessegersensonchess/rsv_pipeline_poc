// Package main
//
// WHAT CHANGED (high level):
//  1. Comments: This file is now extensively annotated for engineers coming from Python/C#.
//  2. Flags: Hardcoded filenames moved to CLI flags with sensible env fallbacks (see --help).
//  3. Clean Code for importVehicleTech: The function no longer owns DB connection strings or calls pgx directly.
//     It depends on a small DB interface injected from main (dependency inversion).
//  4. DB abstraction: We define a storage-agnostic interface (DB/TX) and provide two adapters:
//     - Postgres (pgx) with efficient COPY.
//     - Generic SQL (works with MSSQL) using prepared batched INSERTs (portable, not as fast as COPY).
//     This keeps storage concerns separate from parsing/business logic.
//  5. Config moved out: batch size, worker count, paths, etc. controlled via flags and environment variables.
//
// QUICK START (Postgres):
//
//	go build -o importer .
//	./importer \
//	  --db_driver=postgres \
//	  --db_user=user --db_password=password --db_host=localhost --db_port=5432 --db_name=testdb \
//	  --ownership_csv=RSV_vlastnik_provozovatel_vozidla_20250901.csv \
//	  --vehicle_csv=RSV_vypis_vozidel_20250902.csv
//
// QUICK START (MSSQL example):
//
//	go build -o importer .
//	./importer \
//	  --db_driver=mssql \
//	  --dsn="sqlserver://user:password@localhost:1433?database=MyDb"
package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	// Postgres driver (used by the Postgres adapter)
	"github.com/jackc/pgx/v5"

	// Optional MSSQL driver (used by the generic SQL adapter when --db_driver=mssql)
	_ "github.com/microsoft/go-mssqldb"
)

// -------------------------
// Small DB interface (per request)
// -------------------------
type SmallDB interface {
	CreateOwnershipTable(ctx context.Context) error
	CopyOwnership(ctx context.Context, records [][]interface{}) error
	Close(ctx context.Context) error
}

// SmallDBFactory can mint a new SmallDB (fresh connection) per worker.
type SmallDBFactory func(ctx context.Context) (SmallDB, error)

// -------------------------
// Postgres implementation
// -------------------------
type smallPg struct {
	conn *pgx.Conn
}

func newSmallPg(ctx context.Context, dsn string) (SmallDB, error) {
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &smallPg{conn: c}, nil
}

func (p *smallPg) CreateOwnershipTable(ctx context.Context) error {
	// Native PG: IF NOT EXISTS + proper types
	ddl := `
	CREATE TABLE IF NOT EXISTS ownership (
		pcv INT,
		typ_subjektu INT,
		vztah_k_vozidlu INT,
		aktualni BOOLEAN,
		ico INT,
		nazev TEXT,
		adresa TEXT,
		datum_od DATE,
		datum_do DATE
	)`
	_, err := p.conn.Exec(ctx, ddl)
	return err
}

func (p *smallPg) CopyOwnership(ctx context.Context, records [][]interface{}) error {
	tx, err := p.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	n, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"ownership"},
		[]string{"pcv", "typ_subjektu", "vztah_k_vozidlu", "aktualni", "ico", "nazev", "adresa", "datum_od", "datum_do"},
		pgx.CopyFromRows(records),
	)
	if err != nil {
		return err
	}
	if n != int64(len(records)) {
		log.Printf("⚠️ postgres CopyFrom inserted %d of %d rows", n, len(records))
	}
	return tx.Commit(ctx)
}

func (p *smallPg) Close(ctx context.Context) error { return p.conn.Close(ctx) }

// -------------------------
// MSSQL implementation (via database/sql)
// -------------------------
type smallMSSQL struct {
	db *sql.DB
}

func newSmallMSSQL(dsn string) (SmallDB, error) {
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &smallMSSQL{db: db}, nil
}

func (m *smallMSSQL) CreateOwnershipTable(ctx context.Context) error {
	// T-SQL "create if missing" + SQL Server types
	ddl := `
	IF OBJECT_ID(N'ownership', N'U') IS NULL
	CREATE TABLE ownership (
		pcv INT,
		typ_subjektu INT,
		vztah_k_vozidlu INT,
		aktualni BIT,
		ico INT,
		nazev NVARCHAR(MAX),
		adresa NVARCHAR(MAX),
		datum_od DATE,
		datum_do DATE
	)`
	_, err := m.db.ExecContext(ctx, ddl)
	return err
}

func (m *smallMSSQL) CopyOwnership(ctx context.Context, records [][]interface{}) error {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO ownership(
			pcv, typ_subjektu, vztah_k_vozidlu, aktualni, ico, nazev, adresa, datum_od, datum_do
		) VALUES (@p1,@p2,@p3,@p4,@p5,@p6,@p7,@p8,@p9)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, r := range records {
		if _, err := stmt.ExecContext(ctx, r...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (m *smallMSSQL) Close(ctx context.Context) error { return m.db.Close() }

/*
===========================
SECTION: Domain structures
===========================
Think of Record as your business object for the "ownership" CSV.
*/
type Record struct {
	PCV           int
	TypSubjektu   int
	VztahKVozidlu int
	Aktualni      bool
	ICO           *int
	Nazev         *string
	Adresa        *string
	DatumOd       *time.Time
	DatumDo       *time.Time
}

const layout = "02.01.2006"

/*
	=====================================================
	SECTION: "Clean" configuration (flags + env fallback)
	=====================================================
	We keep ALL tunables out of the code. This is the most standard Go approach:
	- First define flags (so --help shows all knobs).
	- Each flag consults an env var fallback (12-factor friendly).
*/

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

type Config struct {
	// IO
	OwnershipCSV string
	VehicleCSV   string
	SkippedDir   string

	// DB
	DBDriver   string // "postgres" | "mssql"
	DSN        string // preferred for mssql; optional for postgres (we can build from parts)
	DBUser     string
	DBPassword string
	DBHost     string
	DBPort     string
	DBName     string

	// Import tunables
	BatchSize int
	Workers   int

	// Misc
	UnloggedTables bool // Postgres-only speed-up toggle
}

// loadConfig wires flags to env defaults.
// For env names, keep them intuitive and documented via --help.
func loadConfig() *Config {
	cfg := &Config{}

	flag.StringVar(&cfg.OwnershipCSV, "ownership_csv",
		envOrDefault("OWNERSHIP_CSV", "RSV_vlastnik_provozovatel_vozidla_20250901.csv"),
		"Path to ownership CSV (default is the provided filename).")

	flag.StringVar(&cfg.VehicleCSV, "vehicle_csv",
		envOrDefault("VEHICLE_CSV", "RSV_vypis_vozidel_20250902.csv"),
		"Path to vehicle tech CSV (default is the provided filename).")

	flag.StringVar(&cfg.SkippedDir, "skipped_dir",
		envOrDefault("SKIPPED_DIR", "./skipped"),
		"Directory for writing skipped-rows CSV logs.")

	flag.StringVar(&cfg.DBDriver, "db_driver",
		envOrDefault("DB_DRIVER", "postgres"),
		"Database driver: 'postgres' (pgx COPY) or 'mssql' (generic SQL batch inserts).")

	flag.StringVar(&cfg.DSN, "dsn",
		os.Getenv("DB_DSN"),
		"Full DSN (recommended for MSSQL, optional for Postgres). If empty and postgres, DSN built from parts.")

	flag.StringVar(&cfg.DBUser, "db_user", envOrDefault("DB_USER", "user"), "DB user (postgres DSN builder).")
	flag.StringVar(&cfg.DBPassword, "db_password", envOrDefault("DB_PASSWORD", "password"), "DB password (postgres DSN builder).")
	flag.StringVar(&cfg.DBHost, "db_host", envOrDefault("DB_HOST", "localhost"), "DB host (postgres DSN builder).")
	flag.StringVar(&cfg.DBPort, "db_port", envOrDefault("DB_PORT", "5432"), "DB port (postgres DSN builder).")
	flag.StringVar(&cfg.DBName, "db_name", envOrDefault("DB_NAME", "testdb"), "DB name (postgres DSN builder).")

	flag.IntVar(&cfg.BatchSize, "batch_size",
		intEnvOrDefault("BATCH_SIZE", 5_000),
		"Number of rows per batch/COPY. Lower if memory constrained.")

	flag.IntVar(&cfg.Workers, "workers",
		intEnvOrDefault("WORKERS", 8),
		"Number of parallel workers for vehicle tech import.")

	flag.BoolVar(&cfg.UnloggedTables, "pg_unlogged",
		boolEnvOrDefault("PG_UNLOGGED", true),
		"Postgres only: SET UNLOGGED for load speed (unsafe during crash).")

	flag.Parse()
	return cfg
}

func intEnvOrDefault(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}

func boolEnvOrDefault(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		switch strings.ToLower(v) {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off":
			return false
		}
	}
	return def
}

/*
	====================================
	SECTION: Skip statistics (unchanged)
	====================================
*/

type skipStats struct {
	reasons map[string]int
	w       *csv.Writer
}

func newSkipStats(path string) (*skipStats, func()) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		log.Fatalf("create dir %s: %v", filepath.Dir(path), err)
	}
	f, err := os.Create(path)
	if err != nil {
		log.Fatalf("open %s: %v", path, err)
	}
	w := csv.NewWriter(f)
	_ = w.Write([]string{"reason", "line_number", "pcv_field", "raw_line"})
	return &skipStats{reasons: make(map[string]int), w: w}, func() {
		w.Flush()
		_ = f.Close()
	}
}

func (s *skipStats) add(reason string, lineNum int, pcvField string, raw string) {
	s.reasons[reason]++
	_ = s.w.Write([]string{reason, strconv.Itoa(lineNum), pcvField, raw})
}

var dateRe = regexp.MustCompile(`^\d{2}\.\d{2}\.\d{4}$`)

/*
	============================================================
	SECTION: CSV parsing helpers (unchanged, with clarifying docs)
	============================================================
	Go's stdlib CSV is strict; your data is "dirty". We keep resilient parsers that:
	- Read logical rows even when quoted fields contain CRLF.
	- Tolerate unbalanced quotes and embedded quotes via loose parsing.
	- Attempt repairs for rows broken by stray commas or space-delimited fallbacks.
*/

// Read one logical CSV row (may span multiple physical lines if a quoted field contains CRLF)
func readLogicalCSVLine(r *bufio.Reader) (string, error) {
	var sb strings.Builder
	inQuotes := false
	atStartOfField := true
	firstChunk := true

	for {
		part, err := r.ReadString('\n') // CRLF -> "\r\n"
		if err != nil && err != io.EOF && !(err == io.EOF && part != "") {
			return "", err
		}
		part = strings.TrimRight(part, "\r\n")

		if !firstChunk && inQuotes {
			sb.WriteString("\r\n")
		}
		sb.WriteString(part)
		firstChunk = false

		i := 0
		for i < len(part) {
			ch := part[i]
			switch ch {
			case ',':
				if !inQuotes {
					atStartOfField = true
				}
				i++
			case '"':
				if inQuotes {
					if i+1 < len(part) && part[i+1] == '"' {
						i += 2
						continue
					}
					j := i + 1
					for j < len(part) && (part[j] == ' ' || part[j] == '\t') {
						j++
					}
					if j >= len(part) || part[j] == ',' {
						inQuotes = false
						atStartOfField = false
						i++
						continue
					}
					i++
				} else {
					if atStartOfField {
						inQuotes = true
						atStartOfField = false
						i++
					} else {
						i++
					}
				}
			default:
				if !inQuotes {
					atStartOfField = false
				}
				i++
			}
		}

		if !inQuotes || err == io.EOF {
			if sb.Len() == 0 && err == io.EOF {
				return "", io.EOF
			}
			return sb.String(), nil
		}
	}
}

// Loose CSV splitter – tolerant of inner quotes + `""` + closing quotes before delimiter
func parseCSVLineLoose(line string) ([]string, error) {
	var fields []string
	var sb strings.Builder
	inQuotes := false
	atStartOfField := true
	i := 0

	for i < len(line) {
		ch := line[i]
		switch ch {
		case ',':
			if inQuotes {
				sb.WriteByte(',')
			} else {
				fields = append(fields, sb.String())
				sb.Reset()
				atStartOfField = true
			}
			i++
		case '"':
			if inQuotes {
				if i+1 < len(line) && line[i+1] == '"' {
					j := i + 2
					for j < len(line) && (line[j] == ' ' || line[j] == '\t') {
						j++
					}
					if j >= len(line) || line[j] == ',' {
						sb.WriteByte('"')
						inQuotes = false
						atStartOfField = false
						i += 2
						continue
					}
					sb.WriteByte('"')
					i += 2
					continue
				}
				j := i + 1
				for j < len(line) && (line[j] == ' ' || line[j] == '\t') {
					j++
				}
				if j >= len(line) || line[j] == ',' {
					inQuotes = false
					atStartOfField = false
					i++
					continue
				}
				sb.WriteByte('"')
				i++
			} else {
				if atStartOfField {
					inQuotes = true
					atStartOfField = false
					i++
				} else {
					sb.WriteByte('"')
					i++
				}
			}
		default:
			sb.WriteByte(ch)
			if !inQuotes {
				atStartOfField = false
			}
			i++
		}
	}
	fields = append(fields, sb.String())
	return fields, nil
}

// When a name field was split by a stray comma, glue middle pieces back
func repairOverlongCommaFields(fields []string) ([]string, bool) {
	if len(fields) <= 9 {
		return nil, false
	}
	last := len(fields) - 1
	d2 := fields[last]
	d1 := fields[last-1]
	if !(d2 == "" || dateRe.MatchString(strings.TrimSpace(d2))) {
		return nil, false
	}
	if !(d1 == "" || dateRe.MatchString(strings.TrimSpace(d1))) {
		return nil, false
	}
	addrIdx := last - 2
	if addrIdx < 6 {
		return nil, false
	}
	head := fields[:5]
	name := strings.Join(fields[5:addrIdx], ",")
	addr := fields[addrIdx]

	out := make([]string, 0, 9)
	out = append(out, head...)
	out = append(out, name, addr, d1, d2)
	if len(out) != 9 {
		return nil, false
	}
	return out, true
}

// Tokenizer + fallback for space-delimited dirty rows
func lexBySpaceWithQuotes(line string) []string {
	tokens := []string{}
	var sb strings.Builder
	inQuotes := false
	i := 0
	flush := func() {
		if sb.Len() > 0 {
			tokens = append(tokens, sb.String())
			sb.Reset()
		}
	}
	for i < len(line) {
		ch := line[i]
		switch ch {
		case '"':
			if inQuotes {
				if i+1 < len(line) && line[i+1] == '"' {
					sb.WriteByte('"')
					i += 2
					continue
				}
				inQuotes = false
				i++
			} else {
				inQuotes = true
				i++
			}
		case ' ', '\t':
			if inQuotes {
				sb.WriteByte(ch)
				i++
			} else {
				flush()
				for i < len(line) && (line[i] == ' ' || line[i] == '\t') {
					i++
				}
			}
		default:
			sb.WriteByte(ch)
			i++
		}
	}
	flush()
	return tokens
}

func parseSpaceSeparatedRow(line string) ([]string, bool) {
	toks := lexBySpaceWithQuotes(line)
	if len(toks) < 6 {
		return nil, false
	}
	var d2, d1 string
	if len(toks) >= 1 && dateRe.MatchString(toks[len(toks)-1]) {
		d2 = toks[len(toks)-1]
		toks = toks[:len(toks)-1]
	}
	if len(toks) >= 1 && dateRe.MatchString(toks[len(toks)-1]) {
		d1 = toks[len(toks)-1]
		toks = toks[:len(toks)-1]
	}
	if len(toks) < 5 {
		return nil, false
	}
	pcv, typ, vztah, aktualni, ico := toks[0], toks[1], toks[2], toks[3], toks[4]
	rest := toks[5:]
	if len(rest) == 0 {
		return nil, false
	}
	name := rest[0]
	rest = rest[1:]
	addr := strings.Join(rest, " ")
	out := []string{pcv, typ, vztah, aktualni, ico, name, addr}
	if d1 != "" {
		out = append(out, d1)
	} else {
		out = append(out, "")
	}
	if d2 != "" {
		out = append(out, d2)
	} else {
		out = append(out, "")
	}
	if len(out) != 9 {
		return nil, false
	}
	return out, true
}

/*
	========================================
	SECTION: Ownership parsing & construction
	========================================
*/

func parseRecord(fields []string) (*Record, error) {
	pcv, err := strconv.Atoi(strings.TrimSpace(fields[0]))
	if err != nil {
		return nil, fmt.Errorf("invalid PCV: %v", err)
	}
	typSubjektu, err := strconv.Atoi(strings.TrimSpace(fields[1]))
	if err != nil {
		return nil, fmt.Errorf("invalid Typ subjektu: %v", err)
	}
	vztahKVozidlu, err := strconv.Atoi(strings.TrimSpace(fields[2]))
	if err != nil {
		return nil, fmt.Errorf("invalid Vztah k vozidlu: %v", err)
	}
	aktualni := strings.TrimSpace(fields[3]) == "True"

	var ico *int
	if s := strings.TrimSpace(fields[4]); s != "" {
		if val, err := strconv.Atoi(s); err == nil {
			ico = &val
		}
	}

	var nazev *string
	if s := strings.TrimSpace(fields[5]); s != "" {
		nazev = &s
	}
	var adresa *string
	if s := strings.TrimSpace(fields[6]); s != "" {
		adresa = &s
	}

	var datumOd *time.Time
	if s := strings.TrimSpace(fields[7]); s != "" {
		t, err := time.Parse(layout, s)
		if err != nil {
			return nil, fmt.Errorf("invalid Datum od: %v", err)
		}
		datumOd = &t
	}

	var datumDo *time.Time
	if s := strings.TrimSpace(fields[8]); s != "" {
		if t, err := time.Parse(layout, s); err == nil {
			datumDo = &t
		}
	}

	return &Record{
		PCV:           pcv,
		TypSubjektu:   typSubjektu,
		VztahKVozidlu: vztahKVozidlu,
		Aktualni:      aktualni,
		ICO:           ico,
		Nazev:         nazev,
		Adresa:        adresa,
		DatumOd:       datumOd,
		DatumDo:       datumDo,
	}, nil
}

/*
	==========================================================
	SECTION: Storage abstraction (DB/TX interfaces + adapters)
	==========================================================
	We apply dependency inversion: parsing/import code depends on the abstraction,
	not on a specific driver. The adapter hides driver-specific stuff like COPY.
*/

// DB is a connection capable of starting transactions and executing DDL/DML.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) error
	BeginTx(ctx context.Context) (Tx, error)
	Close(ctx context.Context) error
}

// Tx (transaction) supports Exec, bulk inserts, and lifecycle.
type Tx interface {
	Exec(ctx context.Context, sql string, args ...any) error
	// CopyInto abstracts "bulk load". Implementations may use COPY (postgres) or batched INSERTs (generic).
	CopyInto(ctx context.Context, table string, columns []string, rows [][]interface{}) (int64, error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

/*** Postgres adapter (pgx) ***/

type pgDB struct {
	conn *pgx.Conn
}

func newPgDB(ctx context.Context, dsn string) (DB, error) {
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &pgDB{conn: c}, nil
}

func (p *pgDB) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := p.conn.Exec(ctx, sql, args...)
	return err
}

func (p *pgDB) BeginTx(ctx context.Context) (Tx, error) {
	tx, err := p.conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &pgTx{tx: tx}, nil
}

func (p *pgDB) Close(ctx context.Context) error {
	return p.conn.Close(ctx)
}

type pgTx struct {
	tx pgx.Tx
}

func (t *pgTx) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := t.tx.Exec(ctx, sql, args...)
	return err
}

func (t *pgTx) CopyInto(ctx context.Context, table string, columns []string, rows [][]interface{}) (int64, error) {
	n, err := t.tx.CopyFrom(ctx, pgx.Identifier{table}, columns, pgx.CopyFromRows(rows))
	return n, err
}

func (t *pgTx) Commit(ctx context.Context) error   { return t.tx.Commit(ctx) }
func (t *pgTx) Rollback(ctx context.Context) error { return t.tx.Rollback(ctx) }

/*** Generic SQL adapter (works with MSSQL, Postgres via database/sql, etc.) ***/
/*
   This adapter favors portability. There is no native COPY abstraction in database/sql,
   so CopyInto falls back to a prepared INSERT with batched Execs.
   It's slower than native COPY but keeps the import code database-agnostic.
*/

type sqlDB struct {
	db *sql.DB
}

func newSQLDB(driver, dsn string) (DB, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	// Connection ping here is pragmatic; production code might add timeouts.
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &sqlDB{db: db}, nil
}

func (s *sqlDB) Exec(ctx context.Context, q string, args ...any) error {
	_, err := s.db.ExecContext(ctx, q, args...)
	return err
}

func (s *sqlDB) BeginTx(ctx context.Context) (Tx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &sqlTx{tx: tx}, nil
}

func (s *sqlDB) Close(ctx context.Context) error { return s.db.Close() }

type sqlTx struct {
	tx *sql.Tx
}

func (t *sqlTx) Exec(ctx context.Context, q string, args ...any) error {
	_, err := t.tx.ExecContext(ctx, q, args...)
	return err
}

func (t *sqlTx) CopyInto(ctx context.Context, table string, columns []string, rows [][]interface{}) (int64, error) {
	// Build INSERT statement: INSERT INTO table (col1, col2) VALUES (@p1, @p2)
	placeholders := make([]string, len(columns))
	for i := range columns {
		// SQL Server uses @pN style. For portability, name them @p1..@pN and bind by order.
		placeholders[i] = fmt.Sprintf("@p%d", i+1)
	}
	stmtText := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		table,
		strings.Join(columns, ","),
		strings.Join(placeholders, ","),
	)

	stmt, err := t.tx.PrepareContext(ctx, stmtText)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	var inserted int64
	for _, row := range rows {
		if _, err := stmt.ExecContext(ctx, row...); err != nil {
			return inserted, err
		}
		inserted++
	}
	return inserted, nil
}

func (t *sqlTx) Commit(ctx context.Context) error   { return t.tx.Commit() }
func (t *sqlTx) Rollback(ctx context.Context) error { return t.tx.Rollback() }

/*
	=================================================
	SECTION: Importers (depend on the DB abstraction)
	=================================================
	NOTE FOR PYTHON/C# ENGINEERS:
	- imports accept DB (not a concrete driver), enabling easy swapping between Postgres & MSSQL.
	- no connection strings or driver-specific logic inside the importers.
*/

// importOwnershipParallel ingests ownership CSV using N workers and SmallDB batches.
// It remains storage-agnostic by depending only on SmallDBFactory/SmallDB.
func importOwnershipParallel(ctx context.Context, cfg *Config, smallFactory SmallDBFactory, path string) error {
	// Ensure table using a short-lived control instance
	ctrl, err := smallFactory(ctx)
	if err != nil {
		return fmt.Errorf("open small db (ensure table): %w", err)
	}
	if err := ctrl.CreateOwnershipTable(ctx); err != nil {
		_ = ctrl.Close(ctx)
		return fmt.Errorf("create ownership table: %w", err)
	}
	_ = ctrl.Close(ctx)

	// Open file
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open ownership csv: %w", err)
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 4<<20)

	// Skip header
	if _, err := readLogicalCSVLine(r); err != nil && err != io.EOF {
		return fmt.Errorf("read header: %w", err)
	}

	type job struct {
		line    string
		lineNum int
	}
	jobs := make(chan job, 32_768)

	// Reader goroutine
	go func() {
		lineNum := 1 // header consumed
		for {
			l, err := readLogicalCSVLine(r)
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("⚠️ ownership read error: %v", err)
				break
			}
			lineNum++
			jobs <- job{line: l, lineNum: lineNum}
		}
		close(jobs)
	}()

	type workerResult struct {
		inserted int
		skipped  int
		err      error
		reasons  map[string]int
	}

	workers := cfg.Workers
	results := make(chan workerResult, workers)

	workerFn := func(id int) {
		res := workerResult{reasons: map[string]int{}}
		defer func() { results <- res }()

		// Fresh SmallDB instance per worker (own connection/transaction lifecycle hidden inside adapter)
		sdb, err := smallFactory(ctx)
		if err != nil {
			res.err = fmt.Errorf("worker %d connect: %w", id, err)
			return
		}
		defer sdb.Close(ctx)

		// per-worker skipped CSV
		if err := os.MkdirAll("skipped", 0o755); err != nil {
			res.err = fmt.Errorf("worker %d create skipped dir: %w", id, err)
			return
		}
		skf, err := os.Create(filepath.Join("skipped", fmt.Sprintf("skipped_ownership_w%d.csv", id)))
		if err != nil {
			res.err = fmt.Errorf("worker %d skipped file: %w", id, err)
			return
		}
		defer skf.Close()
		skw := csv.NewWriter(skf)
		defer skw.Flush()
		_ = skw.Write([]string{"reason", "line_number", "pcv_field", "raw_line"})
		addSkip := func(reason string, ln int, pcvField, raw string) {
			res.reasons[reason]++
			res.skipped++
			_ = skw.Write([]string{reason, strconv.Itoa(ln), pcvField, raw})
		}

		batchSize := cfg.BatchSize
		batch := make([][]interface{}, 0, batchSize)

		flush := func() error {
			if len(batch) == 0 {
				return nil
			}
			// Adapter handles its own tx/COPY/INSERT details
			if err := sdb.CopyOwnership(ctx, batch); err != nil {
				return err
			}
			res.inserted += len(batch)
			batch = batch[:0]
			log.Printf("ownership[w%d]: inserted=%d skipped=%d so far", id, res.inserted, res.skipped)
			return nil
		}

		for j := range jobs {
			fields, perr := parseCSVLineLoose(j.line)
			if perr != nil {
				addSkip("parse_error", j.lineNum, "", j.line)
				continue
			}
			// Expect 9 fields; try repairs like in the original
			if len(fields) != 9 {
				if fixed, ok := repairOverlongCommaFields(fields); ok {
					fields = fixed
				} else if fixed2, ok2 := parseSpaceSeparatedRow(j.line); ok2 {
					fields = fixed2
				} else {
					addSkip("column_mismatch", j.lineNum, "", j.line)
					continue
				}
			}

			rec, err := parseRecord(fields)
			if err != nil {
				addSkip("field_parse_error", j.lineNum, fields[0], j.line)
				continue
			}

			batch = append(batch, []interface{}{
				rec.PCV, rec.TypSubjektu, rec.VztahKVozidlu, rec.Aktualni,
				rec.ICO, rec.Nazev, rec.Adresa, rec.DatumOd, rec.DatumDo,
			})

			if len(batch) >= batchSize {
				if err := flush(); err != nil {
					res.err = fmt.Errorf("worker %d copy: %w", id, err)
					return
				}
			}
		}

		// final batch
		if err := flush(); err != nil {
			res.err = fmt.Errorf("worker %d copy final: %w", id, err)
			return
		}
	}

	// Launch workers
	for i := 0; i < workers; i++ {
		go workerFn(i + 1)
	}

	// Gather
	totalInserted, totalSkipped := 0, 0
	reasonAgg := map[string]int{}
	var firstErr error
	for i := 0; i < workers; i++ {
		r := <-results
		totalInserted += r.inserted
		totalSkipped += r.skipped
		if r.err != nil && firstErr == nil {
			firstErr = r.err
		}
		for k, v := range r.reasons {
			reasonAgg[k] += v
		}
	}
	if firstErr != nil {
		return firstErr
	}

	var parts []string
	for k, v := range reasonAgg {
		parts = append(parts, fmt.Sprintf("%s=%d", k, v))
	}
	log.Printf("ownership (parallel %d): inserted=%d skipped=%d (%s)",
		workers, totalInserted, totalSkipped, strings.Join(parts, ", "))

	return nil
}

// Finds index of the "PČV" column (exact match). Returns -1 if not found.
func findPCVIndex(headers []string) int {
	for i, h := range headers {
		if strings.TrimSpace(h) == "PČV" {
			return i
		}
	}
	return -1
}

// Build JSON payload: map[header]value for the row
func rowToJSON(headers, fields []string) ([]byte, error) {
	m := make(map[string]string, len(headers))
	for i := 0; i < len(headers) && i < len(fields); i++ {
		m[headers[i]] = fields[i]
	}
	return json.Marshal(m)
}

/*
	CLEAN-CODE REFACTOR of importVehicleTech
	----------------------------------------
	- No DSN building here.
	- No direct pgx usage.
	- No direct connection creation.
	- Pure IO + parsing + batching + concurrency. DB specifics live behind DBFactory/Tx.
*/

// DBFactory can mint a new DB connection per worker (for parallel ingestion).
type DBFactory func(ctx context.Context) (DB, error)

// todo: refactor with small db interface to remove inline db specifics
func ensureVehicleTechTable(ctx context.Context, db DB, unlogged bool, driver string) error {
	switch strings.ToLower(driver) {
	case "postgres":
		// Use JSONB for indexing speed; JSON also fine if you prefer.
		if err := db.Exec(ctx, `
			CREATE TABLE IF NOT EXISTS vehicle_tech (
				pcv BIGINT,
				payload JSONB
			);
		`); err != nil {
			return fmt.Errorf("create vehicle_tech (pg): %w", err)
		}
		if unlogged {
			_ = db.Exec(ctx, `ALTER TABLE vehicle_tech SET UNLOGGED`)
		}
		// Helpful index
		_ = db.Exec(ctx, `CREATE INDEX IF NOT EXISTS vehicle_tech_pcv_idx ON vehicle_tech(pcv)`)
		return nil

	case "mssql":
		// T-SQL: create-if-missing + enforce valid JSON
		if err := db.Exec(ctx, `
			IF OBJECT_ID(N'vehicle_tech', N'U') IS NULL
			BEGIN
				CREATE TABLE vehicle_tech (
					pcv BIGINT,
					payload NVARCHAR(MAX) CHECK (ISJSON(payload) = 1)
				);
			END
		`); err != nil {
			return fmt.Errorf("create vehicle_tech (mssql): %w", err)
		}
		// Helpful index
		_ = db.Exec(ctx, `IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'vehicle_tech_pcv_idx')
			CREATE INDEX vehicle_tech_pcv_idx ON vehicle_tech(pcv)`)
		return nil

	default:
		return fmt.Errorf("unknown driver: %s", driver)
	}
}

func importVehicleTech(ctx context.Context, cfg *Config, factory DBFactory, path string) error {
	// Open once to create table / probe header
	// We deliberately use a short-lived conn to avoid sharing tx across workers.
	db, err := factory(ctx)
	if err != nil {
		return fmt.Errorf("open db (ensure table): %w", err)
	}
	if err := ensureVehicleTechTable(ctx, db, cfg.UnloggedTables, cfg.DBDriver); err != nil {
		_ = db.Close(ctx)
		return err
	}
	_ = db.Close(ctx)

	// Open CSV file & parse header to discover columns + PČV index
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open tech csv: %w", err)
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 4<<20) // 4 MB

	headerLine, err := readLogicalCSVLine(r)
	if err != nil {
		return fmt.Errorf("read tech header: %w", err)
	}
	headers, err := parseCSVLineLoose(headerLine)
	if err != nil {
		return fmt.Errorf("parse tech header: %w", err)
	}
	pcvIdx := findPCVIndex(headers)
	if pcvIdx < 0 {
		return fmt.Errorf("PČV column not found in header")
	}
	statusIdx := -1
	for i, h := range headers {
		if strings.TrimSpace(h) == "Status" {
			statusIdx = i
			break
		}
	}

	type job struct {
		line    string
		lineNum int
	}
	jobs := make(chan job, 32_768) // big buffer to keep workers busy

	// Reader goroutine: stream logical rows into jobs
	go func() {
		lineNum := 1 // header consumed
		for {
			l, err := readLogicalCSVLine(r)
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("⚠️ tech read error: %v", err)
				break
			}
			lineNum++
			jobs <- job{line: l, lineNum: lineNum}
		}
		close(jobs)
	}()

	type workerResult struct {
		inserted int
		skipped  int
		err      error
		reasons  map[string]int
	}

	workers := cfg.Workers
	results := make(chan workerResult, workers)

	workerFn := func(id int) {
		res := workerResult{reasons: map[string]int{}}
		defer func() { results <- res }()

		dbc, err := factory(ctx)
		if err != nil {
			res.err = fmt.Errorf("worker %d connect: %w", id, err)
			return
		}
		defer dbc.Close(ctx)

		tx, err := dbc.BeginTx(ctx)
		if err != nil {
			res.err = fmt.Errorf("worker %d begin: %w", id, err)
			return
		}
		defer tx.Rollback(ctx)

		// per-worker skipped CSV
		if err := os.MkdirAll("skipped", 0o755); err != nil {
			res.err = fmt.Errorf("worker %d create skipped dir: %w", id, err)
			return
		}
		skf, err := os.Create(filepath.Join("skipped", fmt.Sprintf("skipped_vehicle_tech_w%d.csv", id)))
		if err != nil {
			res.err = fmt.Errorf("worker %d skipped file: %w", id, err)
			return
		}
		defer skf.Close()
		skw := csv.NewWriter(skf)
		defer skw.Flush()
		_ = skw.Write([]string{"reason", "line_number", "pcv_field", "raw_line"})
		addSkip := func(reason string, ln int, pcvField, raw string) {
			res.reasons[reason]++
			res.skipped++
			_ = skw.Write([]string{reason, strconv.Itoa(ln), pcvField, raw})
		}

		batchSize := cfg.BatchSize
		batch := make([][]interface{}, 0, batchSize)
		digitsOnly := regexp.MustCompile(`^\d+$`)

		for j := range jobs {
			fields, perr := parseCSVLineLoose(j.line)
			if perr != nil {
				addSkip("parse_error", j.lineNum, "", j.line)
				continue
			}
			origFields := fields

			// PČV extraction (int64 / BIGINT)
			pcv, pcvField := extractPCV(headers, origFields, pcvIdx, statusIdx, digitsOnly)
			if pcv == 0 {
				addSkip("pcv_not_numeric", j.lineNum, pcvField, j.line)
				continue
			}

			// Normalize fields to header length for predictable JSON
			if len(fields) > len(headers) {
				fields = fields[:len(headers)]
			} else if len(fields) < len(headers) {
				fields = append(fields, make([]string, len(headers)-len(fields))...)
			}

			js, jerr := rowToJSON(headers, fields)
			if jerr != nil {
				addSkip("json_marshal_error", j.lineNum, strconv.FormatInt(pcv, 10), j.line)
				continue
			}

			batch = append(batch, []interface{}{pcv, string(js)})

			if len(batch) >= batchSize {
				n, err := tx.CopyInto(ctx,
					"vehicle_tech",
					[]string{"pcv", "payload"},
					batch,
				)
				if err != nil {
					res.err = fmt.Errorf("worker %d copy: %w", id, err)
					return
				}
				res.inserted += int(n)
				batch = batch[:0]

				log.Printf("vehicle_tech[w%d]: inserted=%d skipped=%d so far", id, res.inserted, res.skipped)
			}
		}

		// final batch
		if len(batch) > 0 {
			n, err := tx.CopyInto(ctx,
				"vehicle_tech",
				[]string{"pcv", "payload"},
				batch,
			)
			if err != nil {
				res.err = fmt.Errorf("worker %d copy final: %w", id, err)
				return
			}
			res.inserted += int(n)
		}

		if err := tx.Commit(ctx); err != nil {
			res.err = fmt.Errorf("worker %d commit: %w", id, err)
			return
		}
	}

	// Launch workers
	for i := 0; i < workers; i++ {
		go workerFn(i + 1)
	}

	// Gather results
	totalInserted, totalSkipped := 0, 0
	reasonAgg := map[string]int{}
	var firstErr error
	for i := 0; i < workers; i++ {
		r := <-results
		totalInserted += r.inserted
		totalSkipped += r.skipped
		if r.err != nil && firstErr == nil {
			firstErr = r.err
		}
		for k, v := range r.reasons {
			reasonAgg[k] += v
		}
	}
	if firstErr != nil {
		return firstErr
	}

	// Log summary
	var parts []string
	for k, v := range reasonAgg {
		parts = append(parts, fmt.Sprintf("%s=%d", k, v))
	}
	log.Printf("vehicle_tech (parallel %d): inserted=%d skipped=%d (%s)",
		workers, totalInserted, totalSkipped, strings.Join(parts, ", "))

	return nil
}

/*
	=====================================
	SECTION: Utility for PCV field locate
	=====================================
*/

// digitsOnly and headers come from the caller.
// Returns pcv (int64) and the string it parsed from (for logging).
func extractPCV(headers, fields []string, pcvIdx, statusIdx int, digitsOnly *regexp.Regexp) (int64, string) {
	parse := func(s string) (int64, error) { return strconv.ParseInt(s, 10, 64) }

	// 1) Direct by header index (if aligned)
	if pcvIdx >= 0 && pcvIdx < len(fields) {
		s := strings.TrimSpace(fields[pcvIdx])
		if digitsOnly.MatchString(s) {
			if v, err := parse(s); err == nil {
				return v, s
			}
		}
	}

	// 2) Tail-aligned index when row length != header length
	if len(fields) != len(headers) && pcvIdx >= 0 {
		tailOffset := (len(headers) - 1) - pcvIdx
		idx := (len(fields) - 1) - tailOffset
		if idx >= 0 && idx < len(fields) {
			s := strings.TrimSpace(fields[idx])
			if digitsOnly.MatchString(s) {
				if v, err := parse(s); err == nil {
					return v, s
				}
			}
		}
	}

	// 3) If we know Status, look right after it (typically Status -> PČV)
	if statusIdx >= 0 {
		tryFrom := func(si int) (int64, string, bool) {
			if si < 0 || si >= len(fields) {
				return 0, "", false
			}
			for i := si + 1; i < len(fields) && i <= si+10; i++ {
				s := strings.TrimSpace(fields[i])
				if digitsOnly.MatchString(s) && len(s) >= 5 {
					if v, err := parse(s); err == nil {
						return v, s, true
					}
				}
			}
			return 0, "", false
		}
		if v, s, ok := tryFrom(statusIdx); ok {
			return v, s
		}
		if len(fields) != len(headers) {
			tailOffsetS := (len(headers) - 1) - statusIdx
			si := (len(fields) - 1) - tailOffsetS
			if v, s, ok := tryFrom(si); ok {
				return v, s
			}
		}
	}

	// 4) Last-resort: scan tail
	start := len(fields) - 16
	if start < 0 {
		start = 0
	}
	for i := len(fields) - 1; i >= start; i-- {
		s := strings.TrimSpace(fields[i])
		if digitsOnly.MatchString(s) && len(s) >= 6 {
			if v, err := parse(s); err == nil {
				return v, s
			}
		}
	}

	return 0, ""
}

// ensureOwnershipTable creates the ownership table for the selected driver.
func ensureOwnershipTable(ctx context.Context, db DB, unlogged bool, driver string) error {
	switch strings.ToLower(driver) {
	case "postgres":
		if err := db.Exec(ctx, `
			CREATE TABLE IF NOT EXISTS ownership (
				pcv INT,
				typ_subjektu INT,
				vztah_k_vozidlu INT,
				aktualni BOOLEAN,
				ico INT,
				nazev TEXT,
				adresa TEXT,
				datum_od DATE,
				datum_do DATE
			);
		`); err != nil {
			return fmt.Errorf("create ownership (pg): %w", err)
		}
		if unlogged {
			_ = db.Exec(ctx, `ALTER TABLE ownership SET UNLOGGED`)
		}
		_ = db.Exec(ctx, `CREATE INDEX IF NOT EXISTS ownership_pcv_idx ON ownership(pcv)`)
		return nil

	case "mssql":
		if err := db.Exec(ctx, `
			IF OBJECT_ID(N'ownership', N'U') IS NULL
			BEGIN
				CREATE TABLE ownership (
					pcv INT,
					typ_subjektu INT,
					vztah_k_vozidlu INT,
					aktualni BIT,
					ico INT,
					nazev NVARCHAR(MAX),
					adresa NVARCHAR(MAX),
					datum_od DATE,
					datum_do DATE
				);
			END
		`); err != nil {
			return fmt.Errorf("create ownership (mssql): %w", err)
		}
		_ = db.Exec(ctx, `IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ownership_pcv_idx')
		  CREATE INDEX ownership_pcv_idx ON ownership(pcv)`)
		return nil
	default:
		return fmt.Errorf("unknown driver: %s", driver)
	}
}

/*
	==================
	SECTION: main()
	==================
	- Build configuration.
	- Initialize the proper DB adapter (postgres or mssql).
	- Run imports using the DB abstraction.
*/

/*
	==================
	SECTION: main()
	==================
	- Build configuration.
	- Initialize the proper DB adapter (postgres or mssql).
	- Run imports using the DB abstraction.
*/

func main() {
	cfg := loadConfig()

	// Build DSN if using Postgres and DSN not explicitly provided.
	buildPgDSN := func() string {
		return fmt.Sprintf("postgres://%s:%s@%s:%s/%s",
			cfg.DBUser, cfg.DBPassword, cfg.DBHost, cfg.DBPort, cfg.DBName)
	}

	ctx := context.Background()

	// Small grace sleep in case DB just started via docker-compose, etc.
	time.Sleep(5 * time.Second)

	// DB factory for vehicle_tech (DB/TX abstraction)
	var factory DBFactory
	// SmallDB factory for ownership (keeps importer DB-agnostic)
	var smallFactory SmallDBFactory

	switch strings.ToLower(cfg.DBDriver) {
	case "postgres":
		dsn := cfg.DSN
		if dsn == "" {
			dsn = buildPgDSN()
		}
		// The DBFactory returns a fresh connection per caller (main + workers).
		factory = func(ctx context.Context) (DB, error) { return newPgDB(ctx, dsn) }
		// The SmallDBFactory returns a fresh SmallDB (own connection) per worker.
		smallFactory = func(ctx context.Context) (SmallDB, error) { return newSmallPg(ctx, dsn) }

	case "mssql":
		// For MSSQL we require a DSN (e.g. sqlserver://user:pass@host:1433?database=Db)
		if cfg.DSN == "" {
			log.Fatal("For --db_driver=mssql please provide --dsn (e.g. sqlserver://user:pass@host:1433?database=Db)")
		}
		factory = func(ctx context.Context) (DB, error) { return newSQLDB("sqlserver", cfg.DSN) }
		smallFactory = func(ctx context.Context) (SmallDB, error) { return newSmallMSSQL(cfg.DSN) }

	default:
		log.Fatalf("unsupported --db_driver=%q (use 'postgres' or 'mssql')", cfg.DBDriver)
	}

	start := time.Now()

	// 1) Ownership (vlastník) via new parallel importer using SmallDBFactory
	if err := importOwnershipParallel(ctx, cfg, smallFactory, cfg.OwnershipCSV); err != nil {
		log.Fatalf("ownership import failed: %v", err)
	}

	// 2) Vehicle tech via existing parallel path + DBFactory
	if err := importVehicleTech(ctx, cfg, factory, cfg.VehicleCSV); err != nil {
		log.Fatalf("vehicle tech import failed: %v", err)
	}

	log.Printf("✅ All imports complete in %s", time.Since(start))
}
