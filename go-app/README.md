# csvloader

A fast, testable CSV ingestion tool for importing Czech vehicle ownership and vehicle tech datasets into Postgres or MSSQL.

- **12-factor config:** flags with environment fallbacks (see `internal/config`).
- **Clean adapters:** database implementations behind small seams (see `internal/db`).
- **Resilient parsing:** tolerant CSV parsing for “dirty” data (see `internal/csvutil`).
- **Pipelines:** streaming, back-pressure aware importers with autoscaling writers.

> This repo emphasizes hermetic unit tests and practical seams so code can be exercised without a live database. Integration points are kept minimal and explicit.

---

## Table of Contents

- [Layout](#layout)
- [Configuration](#configuration)
- [Build and Run](#build-and-run)
- [Testing](#testing)
- [Coverage](#coverage)
- [Design Notes](#design-notes)
- [Contributing](#contributing)
- [License](#license)

---

## Layout

```text
cmd/
  importer/           # CLI entry point (imports ownership + vehicle tech)
internal/
  config/             # Flag/env config; injectable loader for tests
  csvutil/            # Resilient CSV readers/splitters for dirty data
  db/                 # DB adapters: Postgres (pgx) and MSSQL (database/sql)
  importer/
    vehicletech/      # Vehicle tech pipeline and writer backends
  jsonutil/           # Minimal JSON row encoder
  pcv/                # PCV column resolver/extractor heuristics
```



# csvloader

A fast, testable CSV ingestion tool for importing Czech vehicle ownership and vehicle tech datasets into Postgres or MSSQL.

- **12-factor config:** flags with environment fallbacks (see `internal/config`).
- **Clean adapters:** database implementations behind small seams (see `internal/db`).
- **Resilient parsing:** tolerant CSV parsing for “dirty” data (see `internal/csvutil`).
- **Pipelines:** streaming, back-pressure aware importers with autoscaling writers.

> This repo emphasizes hermetic unit tests and practical seams so code can be exercised without a live database. Integration points are kept minimal and explicit.

---

## Table of contents

- [Layout](#layout)
- [Configuration](#configuration)
- [Build and run](#build-and-run)
- [Testing](#testing)
- [Coverage](#coverage)
- [Design notes](#design-notes)
- [Contributing](#contributing)
- [License](#license)

---

## Layout

cmd/
importer/ # CLI entry point (imports ownership + vehicle tech)
internal/
config/ # Flag/env config; injectable loader for tests
csvutil/ # Resilient CSV readers/splitters for dirty data
db/ # DB adapters: Postgres (pgx) and MSSQL (database/sql)
importer/
vehicletech/ # Vehicle tech pipeline and writer backends
jsonutil/ # Minimal JSON row encoder
pcv/ # PCV column resolver/extractor heuristics


---

## Configuration

All tunables can be set via flags or environment variables (12-factor style). Flags show the full set of knobs via `--help`.

Common variables/flags:

- `--db_driver` / `DB_DRIVER`: `postgres` (default) or `mssql`
- `--dsn` / `DB_DSN`: full connection string (required for MSSQL)
- `--db_user`, `--db_password`, `--db_host`, `--db_port`, `--db_name`: Postgres parts (used if `--dsn` empty)
- `--ownership_csv` / `OWNERSHIP_CSV`: path to ownership CSV
- `--vehicle_csv` / `VEHICLE_CSV`: path to vehicle tech CSV
- `--batch_size` / `BATCH_SIZE`: copy/insert batch size
- `--workers` / `WORKERS`: parser concurrency; writers autoscale
- `--pg_unlogged` / `PG_UNLOGGED`: Postgres UNLOGGED table for speed (import-only)


| Flag / Env Var                                                      | Description                                                       |
| ------------------------------------------------------------------- | ----------------------------------------------------------------- |
| `--db_driver` / `DB_DRIVER`                                         | Database type: `postgres` (default) or `mssql`                    |
| `--dsn` / `DB_DSN`                                                  | Full connection string (required for MSSQL)                       |
| `--db_user`, `--db_password`, `--db_host`, `--db_port`, `--db_name` | Postgres components (used if `--dsn` is empty)                    |
| `--ownership_csv` / `OWNERSHIP_CSV`                                 | Path to ownership CSV                                             |
| `--vehicle_csv` / `VEHICLE_CSV`                                     | Path to vehicle tech CSV                                          |
| `--batch_size` / `BATCH_SIZE`                                       | COPY/INSERT batch size                                            |
| `--workers` / `WORKERS`                                             | Parser concurrency; writers autoscale                             |
| `--pg_unlogged` / `PG_UNLOGGED`                                     | Use UNLOGGED tables for Postgres imports (faster but non-durable) |


Example:

```bash
# Postgres via DSN parts
./importer \
  --db_driver=postgres \
  --db_host=localhost \
  --db_port=5432 \
  --db_user=user \
  --db_password=pass \
  --db_name=vehdb \
  --ownership_csv=RSV_vlastnik_provozovatel_vozidla_20250901.csv \
  --vehicle_csv=RSV_vypis_vozidel_20250902.csv \
  --workers=8 \
  --batch_size=5000

# MSSQL requires an explicit DSN
./importer \
  --db_driver=mssql \
  --dsn="sqlserver://user:pass@host:1433?database=vehdb" \
  --ownership_csv=... --vehicle_csv=...
```
# Build and run
```go build -o importer ./cmd/importer

./importer --help

```
```
# Build and run
```go build -o importer ./cmd/importer

./importer --help

```
```
# Build and run
```go build -o importer ./cmd/importer

./importer --help

```
```
# Build and run
```go build -o importer ./cmd/importer

./importer --help

```
```
# Build and run
```go build -o importer ./cmd/importer

./importer --help

```
```
# Build and run
```go build -o importer ./cmd/importer

./importer --help

```
```
# Build and run
```go build -o importer ./cmd/importer

./importer --help

```
```
# Build and run
```go build -o importer ./cmd/importer

./importer --help

```
```
# Build and run
```go build -o importer ./cmd/importer

./importer --help

```
```
# Build and run
```go build -o importer ./cmd/importer

./importer --help

```
```
# Build and run
```go build -o importer ./cmd/importer

./importer --help

```
Containerized builds are also supported (see docker-compose.yml).


Testing

The codebase favors hermetic tests:

DB adapters have small interfaces (pgConnLike, sqlDBCore, etc.) and fakes.

Importer pipelines can be injected with no-op or fake backends.

CSV utilities are deterministic and table-driven.

Run tests:
go test ./... -count=1

Common test tips

Tests use per-case temp dirs and override skippedRoot to avoid path collisions.

“Networkless” by design: unit tests do not require a live DB.

# Coverage
Per-package coverage:
go test ./... -cover

### HTML coverage for drill-down:
go test ./... -coverprofile=coverage.out
go tool cover -html=coverage.out

### coverage per function
go tool cover -func=cover.out

# Design notes

Postgres path uses native COPY FROM via pgx for throughput.

MSSQL path uses mssql.CopyIn (TVP) with batched Exec for portability.

The writer backends expose small seams (pgCopyConn, sqlPreparer, stmtCore) so tests can replace networked components with fakes while exercising real logic paths (skips, logging, finalize, error propagation).

Autoscaling writers react to encoder queue fill to add capacity without over-provisioning.

# Contributing

Keep tests hermetic; prefer fakes over network dependencies.

Use table-driven tests and add comments that explain what a test covers and why the assertions matter.

Maintain consistent doc comments suitable for GoDoc (start with the identifier name, describe behavior and parameters).

Run go fmt / go vet before submitting a PR.
```
    go fmt ./...
    go vet ./...
```
