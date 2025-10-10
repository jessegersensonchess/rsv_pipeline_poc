#!/usr/bin/env bash
set -euo pipefail

# Optional: fixed project name so concurrent runs don’t collide
PROJECT_NAME="rsv-import-postgres"

# Build + run; stop all when go-app-postgres exits; propagate its exit code
docker compose \
  --project-name "$PROJECT_NAME" \
  --profile postgres \
  up --build --abort-on-container-exit --exit-code-from go-app-postgres

# Always bring the stack down (containers stop either way, this cleans networks, etc.)
# Remove the trailing "-v" if you want to keep named volumes between runs.
docker compose \
  --project-name "$PROJECT_NAME" \
  --profile postgres \
  down

