# ETL POC (stdlib-first)

Run web UI locally:  
go run ./cmd/csvprobe-web -addr :8080

# ETL POC

Run locally:

```bash
export POSTGRES_DSN="postgres://user:pass@localhost:5432/etl?sslmode=disable"
go run ./cmd/etl run -c configs/pipelines/file_to_postgres.yaml

Design highlights:

* **No Cobra/Viper**: stdlib `flag` + `encoding/json` for configs.
* **Flexible registries**: parser/transformer/storage chosen by `kind` with per-kind options.
* **Type safety before load**: `normalize` + `coerce` transformers convert strings → `int`/`date`.
* **Dynamic Postgres loader**: `columns` and `key_columns` fully configurable; COPY → temp → UPSERT.


