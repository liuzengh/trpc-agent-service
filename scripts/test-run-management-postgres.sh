#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
name="run-management-postgres-$RANDOM-$$"
cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT
docker run -d --name "$name" -e POSTGRES_PASSWORD=fixture-admin -e POSTGRES_DB=agent_platform -p 127.0.0.1::5432 postgres:17 >/dev/null
ready=0
for _ in $(seq 1 90); do
  if docker exec "$name" psql -U postgres -d agent_platform -Atqc 'SELECT 1' 2>/dev/null | grep -qx 1; then ready=1; break; fi
  sleep 1
done
if [ "$ready" != 1 ]; then docker logs "$name"; exit 1; fi
port=$(docker port "$name" 5432/tcp | sed 's/.*://')
docker exec -i "$name" psql -U postgres -d agent_platform -v ON_ERROR_STOP=1 <<'SQL'
CREATE ROLE worker_migrator LOGIN PASSWORD 'fixture-migrator' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
CREATE ROLE worker_runtime LOGIN PASSWORD 'fixture-runtime' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
CREATE SCHEMA worker AUTHORIZATION worker_migrator;
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA worker TO worker_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE worker_migrator IN SCHEMA worker GRANT SELECT,INSERT,UPDATE,DELETE ON TABLES TO worker_runtime;
ALTER ROLE worker_migrator IN DATABASE agent_platform SET search_path=worker,pg_temp;
ALTER ROLE worker_runtime IN DATABASE agent_platform SET search_path=worker,pg_temp;
SQL
export WORKER_MANAGEMENT_TEST_MIGRATION_URL="postgres://worker_migrator:fixture-migrator@127.0.0.1:$port/agent_platform?sslmode=disable"
export WORKER_MANAGEMENT_TEST_RUNTIME_URL="postgres://worker_runtime:fixture-runtime@127.0.0.1:$port/agent_platform?sslmode=disable"
export CONTROL_AUDIT_TEST_DATABASE_URL="postgres://postgres:fixture-admin@127.0.0.1:$port/agent_platform?sslmode=disable"
cd "$root"
GOCACHE=${GOCACHE:-/tmp/run-management-gocache} go test -race ./services/agent-worker/internal/management -run '^TestManagementProjectionPostgres$' -count=1 -v
GOCACHE=${GOCACHE:-/tmp/run-management-gocache} go test -race ./services/control-api/internal/runmanagement/adapter/outbound/postgres -run '^TestAuditProjectionPostgres$' -count=1 -v
