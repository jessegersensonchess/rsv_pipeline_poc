# csvloader

A fast, testable CSV ingestion tool for importing [Czech vehicle ownership](https://download.dataovozidlech.cz/vypiszregistru/vlastnikprovozovatelvozidla) and [vehicle tech](https://download.dataovozidlech.cz/vypiszregistru/technickeprohlidky) datasets into Postgres or MSSQL.

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

---

## Configuration

All tunables can be set via flags or environment variables (12-factor style). Flags show the full set of knobs via `--help`.

Common variables/flags:

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
Containerized builds are also supported (see docker-compose.yml).


## Testing

Tests are designed to be hermetic:

 - "networkless" by design
 - no live DB needed (DB adapters have small interfaces and fakes)
 - Importer pipelines can be injected with no-op or fake backends
 - CSV utilities are deterministic and table-driven
 - Tests use per-case temp dirs to avoid path collisions

### Run tests:
go test ./... -count=1  
docker compose run go-tests  

# Coverage
go test ./... -cover  
go test ./... -coverprofile=coverage.out  
go tool cover -html=coverage.out  
### coverage per function
go tool cover -func=cover.out

# Design notes

Postgres path uses native COPY FROM via pgx for throughput.<br>  

MSSQL path uses mssql.CopyIn (TVP) with batched Exec for portability.<br>

The writer backends expose small seams (pgCopyConn, sqlPreparer, stmtCore) so tests can replace networked components with fakes while exercising real logic paths (skips, logging, finalize, error propagation).<br>

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
