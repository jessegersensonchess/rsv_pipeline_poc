# Generic ETL
go run ./cmd/csvprobe-web -addr :8080

# TODO
move ddl.create -> storage/[db]/ddl/create.go -- so mssql can create tables

add metrics (something usable by both prometheus and datadog)

add documentation
    - xmlprobe
    - etl
        - config structure
        - example configs
