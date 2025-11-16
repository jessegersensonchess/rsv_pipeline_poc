#!/usr/bin/env bash
set -euo pipefail

# Prefer sqlcmd v18; fallback to classic path
if [ -x /opt/mssql-tools18/bin/sqlcmd ]; then
  CMD=/opt/mssql-tools18/bin/sqlcmd
  TRUST_FLAG=-C            # v18 supports -C (Trust Server Certificate)
elif [ -x /opt/mssql-tools/bin/sqlcmd ]; then
  CMD=/opt/mssql-tools/bin/sqlcmd
  TRUST_FLAG=              # older clients may not support -C
else
  echo "sqlcmd not found in tools image" >&2
  exit 127
fi

echo "Waiting for ${MSSQL_HOST}:${MSSQL_PORT}..."
for i in {1..120}; do
  (echo > /dev/tcp/${MSSQL_HOST}/${MSSQL_PORT}) >/dev/null 2>&1 && break || true
  sleep 1
done

runsql() {
  "$CMD" -S tcp:${MSSQL_HOST},${MSSQL_PORT} -U sa -P "${SA_PASSWORD}" ${TRUST_FLAG:-} -b -l 30 "$@"
}

# Sanity ping (fail fast if creds/host wrong)
runsql -Q "SELECT 1"

# 1) Create DB if missing (idempotent)
runsql -Q "IF DB_ID(N'${MSSQL_DB}') IS NULL CREATE DATABASE [${MSSQL_DB}];"

# 2) Wait until DB is ONLINE
runsql -Q "
  DECLARE @deadline DATETIME2 = DATEADD(SECOND, 120, SYSDATETIME());
  WHILE DB_ID(N'${MSSQL_DB}') IS NULL
     OR DATABASEPROPERTYEX(N'${MSSQL_DB}', 'Status') <> 'ONLINE'
  BEGIN
    IF SYSDATETIME() > @deadline
      BEGIN RAISERROR('Database ${MSSQL_DB} not ONLINE in time', 16, 1); BREAK; END
    WAITFOR DELAY '00:00:01';
  END;"

# 3) Create login/user (idempotent) and grant db_owner
runsql -Q "
  IF NOT EXISTS (SELECT * FROM sys.sql_logins WHERE name = N'${APP_LOGIN}')
    CREATE LOGIN [${APP_LOGIN}] WITH PASSWORD = N'${APP_PASSWORD}', CHECK_POLICY = OFF;
  USE [${MSSQL_DB}];
  IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'${APP_LOGIN}')
    CREATE USER [${APP_LOGIN}] FOR LOGIN [${APP_LOGIN}];
  IF NOT EXISTS (
    SELECT 1
    FROM sys.database_role_members rm
    JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id AND r.name = N'db_owner'
    JOIN sys.database_principals m ON rm.member_principal_id = m.principal_id AND m.name = N'${APP_LOGIN}'
  )
    ALTER ROLE [db_owner] ADD MEMBER [${APP_LOGIN}];
"

echo "MSSQL init done."

