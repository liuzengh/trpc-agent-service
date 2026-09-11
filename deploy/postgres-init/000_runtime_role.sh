#!/bin/sh
set -eu

# Local Compose bootstrap only. Production roles should be provisioned by the
# database platform and supplied through separate migration/runtime secrets.
runtime_user="${POSTGRES_RUNTIME_USER:?POSTGRES_RUNTIME_USER is required}"
runtime_password="${POSTGRES_RUNTIME_PASSWORD:?POSTGRES_RUNTIME_PASSWORD is required}"

psql --set ON_ERROR_STOP=1 \
  --username "${POSTGRES_USER}" \
  --dbname "${POSTGRES_DB}" \
  --set runtime_user="${runtime_user}" \
  --set runtime_password="${runtime_password}" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'runtime_user', :'runtime_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'runtime_user')
\gexec
SELECT format(
  'ALTER ROLE %I WITH LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS',
  :'runtime_user', :'runtime_password'
)
\gexec

-- Remove grants left by older local images before rebuilding the explicit
-- runtime allowlist. The migration owner retains ownership and DDL rights.
-- PostgreSQL ACLs have no per-role deny: revoke legacy PUBLIC schema/temp
-- creation too, otherwise the runtime login could inherit those defaults.
SELECT format('REVOKE TEMPORARY ON DATABASE %I FROM PUBLIC', current_database())
\gexec
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
SELECT format('REVOKE ALL PRIVILEGES ON DATABASE %I FROM %I', current_database(), :'runtime_user')
\gexec
SELECT format('REVOKE ALL PRIVILEGES ON SCHEMA public FROM %I', :'runtime_user')
\gexec
SELECT format('REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM %I', :'runtime_user')
\gexec
SELECT format('REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM %I', :'runtime_user')
\gexec
SELECT format(
  'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLES FROM %I',
  current_user, :'runtime_user'
)
\gexec
SELECT format(
  'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public REVOKE USAGE, SELECT, UPDATE ON SEQUENCES FROM %I',
  current_user, :'runtime_user'
)
\gexec

SELECT format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), :'runtime_user')
\gexec
SELECT format('GRANT USAGE ON SCHEMA public TO %I', :'runtime_user')
\gexec

-- Startup verification needs only this read. Raw control-plane tables from
-- migration 001 are intentionally not exposed to the runtime account.
SELECT format(
  'GRANT SELECT ON TABLE public.schema_migrations TO %I', :'runtime_user'
)
WHERE to_regclass('public.schema_migrations') IS NOT NULL
\gexec

-- Data-plane tables are named by immutable migrations. Re-running this file
-- after the migrator grants newly added allowlisted objects without broad
-- default privileges or schema DDL capability.
SELECT format(
  'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.%I TO %I',
  tablename, :'runtime_user'
)
FROM pg_tables
WHERE schemaname = 'public'
  AND (
    tablename LIKE 'runtime\_%' ESCAPE '\'
    OR tablename = 'session_turns'
    OR tablename LIKE 'session\_turn\_%' ESCAPE '\'
    OR tablename LIKE 'tool\_operation%' ESCAPE '\'
	    OR tablename = 'artifact_blob_versions'
	    OR tablename LIKE 'config\_%' ESCAPE '\'
	    OR tablename LIKE 'budget\_%' ESCAPE '\'
	    OR tablename LIKE 'model\_usage\_%' ESCAPE '\'
	    OR tablename LIKE 'data\_migration\_%' ESCAPE '\'
	    OR tablename LIKE 'session\_turn\_summar%'
	    OR tablename LIKE 'session\_summary\_%' ESCAPE '\'
	    OR tablename LIKE 'memory\_visibility\_%' ESCAPE '\'
	    OR tablename LIKE 'audit\_%' ESCAPE '\'
	    OR tablename LIKE 'content\_safety\_%' ESCAPE '\'
  )
\gexec

SELECT format(
  'GRANT USAGE, SELECT ON SEQUENCE public.%I TO %I',
  c.relname, :'runtime_user'
)
FROM pg_class AS c
JOIN pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND c.relkind = 'S'
  AND (
    c.relname LIKE 'runtime\_%' ESCAPE '\'
    OR c.relname LIKE 'session\_turn\_%' ESCAPE '\'
	    OR c.relname LIKE 'tool\_operation%' ESCAPE '\'
	    OR c.relname LIKE 'config\_%' ESCAPE '\'
	    OR c.relname LIKE 'budget\_%' ESCAPE '\'
	    OR c.relname LIKE 'model\_usage\_%' ESCAPE '\'
	    OR c.relname LIKE 'data\_migration\_%' ESCAPE '\'
	    OR c.relname LIKE 'session\_turn\_summar%'
	    OR c.relname LIKE 'session\_summary\_%' ESCAPE '\'
	    OR c.relname LIKE 'memory\_visibility\_%' ESCAPE '\'
	    OR c.relname LIKE 'audit\_%' ESCAPE '\'
	    OR c.relname LIKE 'content\_safety\_%' ESCAPE '\'
  )
\gexec
SQL
