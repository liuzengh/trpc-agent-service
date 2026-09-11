#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/../../.." && pwd)
name="worker-memory-test-$RANDOM-$$"
cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT
docker run -d --name "$name" -e POSTGRES_PASSWORD=fixture-admin -e POSTGRES_DB=memory_test -p 127.0.0.1::5432 postgres:17 >/dev/null
for i in $(seq 1 90); do if docker exec "$name" pg_isready -h 127.0.0.1 -U postgres -d memory_test >/dev/null 2>&1; then break; fi; sleep 1; done
port=$(docker port "$name" 5432/tcp | sed 's/.*://')
docker exec -i "$name" psql -U postgres -d memory_test -v ON_ERROR_STOP=1 <<'SQL'
CREATE ROLE memory_migrator LOGIN PASSWORD 'fixture-migrator';
CREATE ROLE memory_runtime LOGIN PASSWORD 'fixture-runtime';
CREATE SCHEMA runtime_memory AUTHORIZATION memory_migrator;
REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA runtime_memory FROM PUBLIC;
GRANT USAGE ON SCHEMA runtime_memory TO memory_runtime;
ALTER ROLE memory_migrator SET search_path=runtime_memory,pg_temp;
ALTER ROLE memory_runtime SET search_path=runtime_memory,pg_temp;
SQL
cd "$root"
export WORKER_MEMORY_TEST_ADMIN_URL="postgres://postgres:fixture-admin@127.0.0.1:$port/memory_test?sslmode=disable"
export WORKER_MEMORY_TEST_MIGRATION_URL="postgres://memory_migrator:fixture-migrator@127.0.0.1:$port/memory_test?sslmode=disable"
export WORKER_MEMORY_TEST_RUNTIME_URL="postgres://memory_runtime:fixture-runtime@127.0.0.1:$port/memory_test?sslmode=disable"
export WORKER_MEMORY_TEST_ALLOW_RESET=1
go test -race ./services/agent-worker/internal/execution/adapter/outbound/memorystore -count=1 -v
