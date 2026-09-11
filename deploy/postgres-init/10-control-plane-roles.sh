#!/bin/sh
set -eu

: "${PLATFORM_DB_USER:=trpc_platform}"
: "${PLATFORM_DB_PASSWORD:?set PLATFORM_DB_PASSWORD in .env}"

psql --set=ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" \
  --set=database="${POSTGRES_DB}" \
  --set=platform_user="${PLATFORM_DB_USER}" \
  --set=platform_password="${PLATFORM_DB_PASSWORD}" <<'SQL'
CREATE EXTENSION IF NOT EXISTS vector;
CREATE ROLE :"platform_user" LOGIN BYPASSRLS PASSWORD :'platform_password';
CREATE ROLE trpc_tenant NOLOGIN NOBYPASSRLS;
GRANT trpc_tenant TO :"platform_user";
GRANT CONNECT ON DATABASE :"database" TO :"platform_user";
GRANT USAGE, CREATE ON SCHEMA public TO :"platform_user";
GRANT USAGE ON SCHEMA public TO trpc_tenant;
ALTER DEFAULT PRIVILEGES FOR ROLE :"platform_user" IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO trpc_tenant;
ALTER DEFAULT PRIVILEGES FOR ROLE :"platform_user" IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO trpc_tenant;
SQL
