#!/bin/sh
# Runs once, on the first start of an empty Postgres volume (docker-entrypoint-initdb.d).
# Creates the application roles with passwords from .env (guardrail S4 groundwork):
#   pipeline_writer  producer / sink: writes telemetry and topology
#   readonly         Grafana and, later, the ops agent: SELECT only, read-only transactions
# Passwords are passed as psql variables, never echoed.
set -eu

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -v writer_pw="$PIPELINE_WRITER_PASSWORD" \
  -v readonly_pw="$READONLY_PASSWORD" <<'SQL'
CREATE ROLE pipeline_writer LOGIN PASSWORD :'writer_pw';
CREATE ROLE readonly LOGIN PASSWORD :'readonly_pw';
ALTER ROLE readonly SET default_transaction_read_only = on;
SQL
