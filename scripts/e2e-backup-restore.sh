#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

RUN_ID="$(date +%s)-$$"
DB_NAME="backup_drill_${RUN_ID//-/_}"
REDIS_KEY="backup:drill:$RUN_ID"
MARKER="restored-$RUN_ID"
RESTORE_CONTAINER="trpc-agent-redis-restore-$RUN_ID"
TEMP_DIR="$(mktemp -d)"

cleanup() {
  docker rm -f "$RESTORE_CONTAINER" >/dev/null 2>&1 || true
  docker compose exec -T postgres \
    dropdb -U trpc_agent --if-exists "$DB_NAME" >/dev/null 2>&1 || true
  docker compose exec -T redis redis-cli DEL "$REDIS_KEY" >/dev/null 2>&1 || true
  docker compose stop redis postgres >/dev/null 2>&1 || true
  rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

docker compose up -d postgres redis
for _ in $(seq 1 60); do
  pg_health="$(docker inspect --format '{{.State.Health.Status}}' trpc-agent-service-postgres-1 2>/dev/null || true)"
  redis_health="$(docker inspect --format '{{.State.Health.Status}}' trpc-agent-service-redis-1 2>/dev/null || true)"
  [[ "$pg_health" == "healthy" && "$redis_health" == "healthy" ]] && break
  sleep 1
done
if [[ "$pg_health" != "healthy" || "$redis_health" != "healthy" ]]; then
  echo "PostgreSQL or Redis did not become healthy" >&2
  exit 1
fi

docker compose exec -T postgres createdb -U trpc_agent "$DB_NAME"
docker compose exec -T postgres psql -U trpc_agent -d "$DB_NAME" \
  -v ON_ERROR_STOP=1 -c "
    CREATE TABLE restore_probe(id INTEGER PRIMARY KEY, value TEXT NOT NULL);
    INSERT INTO restore_probe(id, value) VALUES (1, '$MARKER');" >/dev/null
docker compose exec -T postgres pg_dump -U trpc_agent -Fc "$DB_NAME" \
  >"$TEMP_DIR/postgres.dump"
if [[ ! -s "$TEMP_DIR/postgres.dump" ]]; then
  echo "PostgreSQL dump is empty" >&2
  exit 1
fi

docker compose exec -T postgres dropdb -U trpc_agent "$DB_NAME"
docker compose exec -T postgres createdb -U trpc_agent "$DB_NAME"
docker compose exec -T postgres pg_restore -U trpc_agent -d "$DB_NAME" \
  <"$TEMP_DIR/postgres.dump"
postgres_marker="$(docker compose exec -T postgres psql -U trpc_agent -d "$DB_NAME" \
  -Atc 'SELECT value FROM restore_probe WHERE id = 1')"
if [[ "$postgres_marker" != "$MARKER" ]]; then
  echo "PostgreSQL restore marker mismatch" >&2
  exit 1
fi

docker compose exec -T redis redis-cli SET "$REDIS_KEY" "$MARKER" >/dev/null
docker compose exec -T redis rm -f /tmp/backup-drill.rdb
docker compose exec -T redis redis-cli --rdb /tmp/backup-drill.rdb >/dev/null 2>&1
docker compose cp redis:/tmp/backup-drill.rdb "$TEMP_DIR/dump.rdb" >/dev/null 2>&1
chmod 644 "$TEMP_DIR/dump.rdb"
docker compose exec -T redis redis-cli DEL "$REDIS_KEY" >/dev/null

docker run -d --rm --name "$RESTORE_CONTAINER" \
  --user "$(id -u):$(id -g)" \
  -v "$TEMP_DIR:/data" \
  redis:7-alpine redis-server --appendonly no >/dev/null
for _ in $(seq 1 30); do
  docker exec "$RESTORE_CONTAINER" redis-cli ping >/dev/null 2>&1 && break
  sleep 0.2
done
redis_marker="$(docker exec "$RESTORE_CONTAINER" redis-cli GET "$REDIS_KEY")"
if [[ "$redis_marker" != "$MARKER" ]]; then
  echo "Redis restore marker mismatch" >&2
  exit 1
fi

echo "backup restore e2e passed: postgres=ok redis=ok run_id=$RUN_ID"
