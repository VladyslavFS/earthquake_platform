#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${DB_HOST:-}" || -z "${DB_PORT:-}" || -z "${DB_NAME:-}" || -z "${DB_USER:-}" ]]; then
  echo "Missing DB_* environment variables."
  echo "Expected: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD (optional if auth allows)."
  exit 1
fi

psql "host=${DB_HOST} port=${DB_PORT} dbname=${DB_NAME} user=${DB_USER} password=${DB_PASSWORD:-}" \
  -f sql/001_create_schemas.sql \
  -f sql/002_create_ods_fct_earthquake.sql \
  -f sql/003_create_dm_fct_count_day_earthquake.sql \
  -f sql/004_create_dm_fct_avg_day_earthquake.sql
