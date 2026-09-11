#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/../../.." && pwd)
work=$(mktemp -d)
postgres_name="worker-backend-migration-postgres-$RANDOM-$$"
redis_name="worker-backend-migration-redis-$RANDOM-$$"
cleanup() {
  docker rm -f "$postgres_name" "$redis_name" >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT

cat >"$work/users.acl" <<'ACL'
user default off
user fixture_admin on >fixture-admin-password ~* +@all
user session_runtime on >session-runtime-password ~runtime_session:* +@connection +get +type +pttl +eval +set
user memory_runtime on >memory-runtime-password ~runtime_memory:* +@connection +mget +get +type +pttl +eval +mset
ACL
chmod 600 "$work/users.acl"

docker run -d --name "$postgres_name" \
  -e POSTGRES_PASSWORD=fixture-admin \
  -e POSTGRES_DB=backend_migration \
  -p 127.0.0.1::5432 postgres:17 >/dev/null
docker run -d --name "$redis_name" --user 0 \
  -p 127.0.0.1::6379 \
  -v "$work:/fixture:ro" redis:7.4-alpine \
  redis-server --aclfile /fixture/users.acl --appendonly yes \
  --appendfsync always --save '' >/dev/null

for _ in $(seq 1 90); do
  if docker exec "$postgres_name" pg_isready -h 127.0.0.1 \
    -U postgres -d backend_migration >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
for _ in $(seq 1 60); do
  if docker exec "$redis_name" redis-cli --user fixture_admin \
    --no-auth-warning -a fixture-admin-password ping 2>/dev/null | \
    grep -qx PONG; then
    break
  fi
  sleep 1
done

docker exec -i "$postgres_name" psql -U postgres \
  -d backend_migration -v ON_ERROR_STOP=1 <<'SQL'
CREATE ROLE session_migrator LOGIN PASSWORD 'session-migrator';
CREATE ROLE session_runtime LOGIN PASSWORD 'session-runtime';
CREATE ROLE memory_migrator LOGIN PASSWORD 'memory-migrator';
CREATE ROLE memory_runtime LOGIN PASSWORD 'memory-runtime';
CREATE SCHEMA runtime_session AUTHORIZATION session_migrator;
CREATE SCHEMA runtime_memory AUTHORIZATION memory_migrator;
REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA runtime_session,runtime_memory FROM PUBLIC;
GRANT USAGE ON SCHEMA runtime_session TO session_runtime;
GRANT USAGE ON SCHEMA runtime_memory TO memory_runtime;
ALTER ROLE session_migrator SET search_path=runtime_session,pg_temp;
ALTER ROLE session_runtime SET search_path=runtime_session,pg_temp;
ALTER ROLE memory_migrator SET search_path=runtime_memory,pg_temp;
ALTER ROLE memory_runtime SET search_path=runtime_memory,pg_temp;
SQL

postgres_port=$(docker port "$postgres_name" 5432/tcp | sed 's/.*://')
redis_port=$(docker port "$redis_name" 6379/tcp | sed 's/.*://')
export WORKER_BACKEND_MIGRATION_SESSION_POSTGRES_URL="postgres://session_runtime:session-runtime@127.0.0.1:$postgres_port/backend_migration?sslmode=disable"
export WORKER_BACKEND_MIGRATION_MEMORY_POSTGRES_URL="postgres://memory_runtime:memory-runtime@127.0.0.1:$postgres_port/backend_migration?sslmode=disable"
export WORKER_BACKEND_MIGRATION_SESSION_MIGRATION_URL="postgres://session_migrator:session-migrator@127.0.0.1:$postgres_port/backend_migration?sslmode=disable"
export WORKER_BACKEND_MIGRATION_MEMORY_MIGRATION_URL="postgres://memory_migrator:memory-migrator@127.0.0.1:$postgres_port/backend_migration?sslmode=disable"
export WORKER_BACKEND_MIGRATION_REDIS_ADDR="127.0.0.1:$redis_port"
export WORKER_BACKEND_MIGRATION_SESSION_REDIS_PASSWORD=session-runtime-password
export WORKER_BACKEND_MIGRATION_MEMORY_REDIS_PASSWORD=memory-runtime-password

cd "$root"
GOCACHE=${GOCACHE:-/tmp/worker-backend-migration-gocache} \
  go test -race \
  ./services/agent-worker/internal/execution/adapter/outbound/backendmigration \
  -run '^TestPostgresRedisRoundTrip$' -count=1 -v
printf 'POSTGRES_REDIS_SESSION_MEMORY_MIGRATION=PASS\n'
