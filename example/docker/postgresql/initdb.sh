#!/bin/bash -e

pg_ctl restart

DB_EXISTS=$(psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT 1 FROM pg_database WHERE datname='example'")
TEST_DB_EXISTS=$(psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT 1 FROM pg_database WHERE datname='example-test'")

if [ "$DB_EXISTS" != "1" ]; then
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE DATABASE "example";
EOSQL
else
  echo "Database 'example' already exists"
fi

if [ "$TEST_DB_EXISTS" != "1" ]; then
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE DATABASE "example-test";
EOSQL
else
  echo "Database 'example-test' already exists"
fi

echo "shared_preload_libraries = 'pg_stat_statements'" >> "$PGDATA/postgresql.conf"

pg_ctl restart

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
EOSQL