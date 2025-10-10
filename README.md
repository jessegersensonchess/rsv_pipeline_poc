# What is this?
Proof of concept scripts which import csv data into a DB. The golang script can use either a MSSQL or Postgres DB. The C# version has only been tested with MSSQL. 

# Performance
Very basic performance benchmarks were done on a local system, importing 500k vehicle_tech records and 500k million ownership records.
Total time includes DB startup, import, DB shutdown. 

Go + Postgres == 0m17.628s
Go + MSSQL == 1m10.269s

C# + Postgres == untested
C# + MSSQL == 3m49.629s


# Build & run
C# + Postgres
docker compose --profile postgres up --build cs-app-postgres

C# + MSSQL
docker compose --profile mssql up --build cs-app-mssql

Go + Postgres
scripts/run_import_postgres.sh

Go + MSSQL
scripts/run_import_mssql.sh



// get data
wget -O data/RSV_vlastnik_provozovatel_vozidla_20250901.csv https://download.dataovozidlech.cz/vypiszregistru/vlastnikprovozovatelvozidla
wget -O data/RSV_vypis_vozidel_20250902.csv https://download.dataovozidlech.cz/vypiszregistru/vypisvozidel

// run unit tests
docker compose --profile test run --rm go-tests



// import data into postgres
export DB_HOST=localhost
export DB_PORT=5432
export DB_USER=user
export DB_PASSWORD=password
export DB_NAME=testdb
go run main.go


