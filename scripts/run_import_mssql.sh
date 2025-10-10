#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."  # go to project root

PROJECT_NAME="rsv-import-mssql"

# 1. Start MSSQL (background)
docker compose --project-name "$PROJECT_NAME" --profile mssql up -d mssql

# 2. Run one-shot initializer
docker compose --project-name "$PROJECT_NAME" --profile mssql run --rm mssql-init

# 3. Run importer
docker compose --project-name "$PROJECT_NAME" --profile mssql run --rm go-app-mssql

# 4. Clean up everything
docker compose --project-name "$PROJECT_NAME" --profile mssql down

