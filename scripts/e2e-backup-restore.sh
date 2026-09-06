#!/usr/bin/env bash
# Isolated toolchain drill: no application data, shared containers or ports.
set -euo pipefail
umask 077
DRILL_DIR="$(mktemp -d /tmp/trpc-backup-drill.XXXXXXXX)"
DRILL_ID="$(basename "$DRILL_DIR")"
DRILL_MARKER="restored-$DRILL_ID"

cleanup() {
  local original_status=$?
  local cid_file container_id label cleanup_failed=0
  trap - EXIT INT TERM
  for cid_file in "$DRILL_DIR/postgres.cid" "$DRILL_DIR/redis-source.cid" "$DRILL_DIR/redis-restore.cid"; do
    [[ -s "$cid_file" ]] || continue
    read -r container_id <"$cid_file" || true
    if [[ ! "$container_id" =~ ^[a-f0-9]{64}$ ]]; then cleanup_failed=1; continue; fi
    if ! label="$(docker inspect --format '{{index .Config.Labels "trpc-agent.backup-drill"}}' "$container_id" 2>/dev/null)"; then
      cleanup_failed=1
      continue
    fi
    if [[ "$label" != "$DRILL_ID" ]]; then cleanup_failed=1; continue; fi
    docker rm -f "$container_id" >/dev/null 2>&1 || cleanup_failed=1
  done
  if [[ "$cleanup_failed" == 0 ]]; then
    rm -f -- "$DRILL_DIR/postgres.cid" "$DRILL_DIR/redis-source.cid" \
      "$DRILL_DIR/redis-restore.cid" "$DRILL_DIR/dump.rdb"
    rmdir -- "$DRILL_DIR" || cleanup_failed=1
  fi
  if [[ "$cleanup_failed" != 0 ]]; then
    echo "drill cleanup needs review; only test artifacts retained in $DRILL_DIR" >&2
    [[ "$original_status" != 0 ]] || original_status=1
  else
    echo "removed only this drill's synthetic containers/data; shared services unchanged"
  fi
  exit "$original_status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Use repository baseline images already cached; no automatic pulls or updates.
docker image inspect postgres:16-alpine >/dev/null
docker image inspect redis:7-alpine >/dev/null
docker run -d --pull=never --network none \
  --name "$DRILL_ID-postgres" --label "trpc-agent.backup-drill=$DRILL_ID" \
  --cidfile "$DRILL_DIR/postgres.cid" --tmpfs /var/lib/postgresql/data:rw \
  -e POSTGRES_USER=drill -e POSTGRES_DB=drill_source \
  -e POSTGRES_HOST_AUTH_METHOD=trust postgres:16-alpine >/dev/null
read -r PG_CONTAINER <"$DRILL_DIR/postgres.cid" || true
pg_ready=false
for _ in {1..80}; do
  if docker exec "$PG_CONTAINER" pg_isready -h 127.0.0.1 -U drill -d drill_source >/dev/null 2>&1; then pg_ready=true; break; fi
  sleep 0.25
done
[[ "$pg_ready" == true ]] || { echo "isolated PostgreSQL not ready" >&2; exit 1; }
docker exec "$PG_CONTAINER" psql -U drill -d drill_source -v ON_ERROR_STOP=1 -c \
  "CREATE TABLE restore_probe(id INTEGER PRIMARY KEY, value TEXT NOT NULL);
   INSERT INTO restore_probe(id,value) VALUES(1,'$DRILL_MARKER');" >/dev/null
docker exec "$PG_CONTAINER" pg_dump -U drill -Fc -f /tmp/source.dump drill_source
docker exec "$PG_CONTAINER" createdb -U drill drill_restored
docker exec "$PG_CONTAINER" pg_restore --exit-on-error -U drill -d drill_restored /tmp/source.dump
pg_marker="$(docker exec "$PG_CONTAINER" psql -U drill -d drill_restored -Atc 'SELECT value FROM restore_probe WHERE id=1')"
[[ "$pg_marker" == "$DRILL_MARKER" ]] || { echo "PostgreSQL restore mismatch" >&2; exit 1; }
echo "isolated PostgreSQL dump/restore passed"

docker run -d --pull=never --network none \
  --name "$DRILL_ID-redis-source" --label "trpc-agent.backup-drill=$DRILL_ID" \
  --cidfile "$DRILL_DIR/redis-source.cid" --tmpfs /data:rw \
  redis:7-alpine redis-server --appendonly no --save '' >/dev/null
read -r REDIS_SOURCE <"$DRILL_DIR/redis-source.cid" || true
redis_ready=false
for _ in {1..40}; do
  if [[ "$(docker exec "$REDIS_SOURCE" redis-cli ping 2>/dev/null)" == PONG ]]; then redis_ready=true; break; fi
  sleep 0.25
done
[[ "$redis_ready" == true ]] || { echo "isolated Redis not ready" >&2; exit 1; }
docker exec "$REDIS_SOURCE" redis-cli -e SET restore-probe "$DRILL_MARKER" >/dev/null
docker exec "$REDIS_SOURCE" redis-cli -e SAVE
# Docker archive/cp cannot reliably read a container's tmpfs mounts. Move a
# copy into this isolated container's writable layer before exporting it.
docker exec "$REDIS_SOURCE" test -s /data/dump.rdb
docker exec "$REDIS_SOURCE" cp /data/dump.rdb /tmp/drill.rdb
docker cp "$REDIS_SOURCE:/tmp/drill.rdb" "$DRILL_DIR/dump.rdb" >/dev/null
chmod 600 "$DRILL_DIR/dump.rdb"

# Bind only a synthetic snapshot read-only; no application data volume.
docker run -d --pull=never --network none \
  --name "$DRILL_ID-redis-restore" --label "trpc-agent.backup-drill=$DRILL_ID" \
  --cidfile "$DRILL_DIR/redis-restore.cid" --user "$(id -u):$(id -g)" \
  --tmpfs /data:rw,mode=1777 \
  --mount "type=bind,source=$DRILL_DIR/dump.rdb,target=/data/dump.rdb,readonly" \
  redis:7-alpine redis-server --appendonly no --save '' >/dev/null
read -r REDIS_RESTORE <"$DRILL_DIR/redis-restore.cid" || true
redis_ready=false
for _ in {1..40}; do
  if [[ "$(docker exec "$REDIS_RESTORE" redis-cli ping 2>/dev/null)" == PONG ]]; then redis_ready=true; break; fi
  sleep 0.25
done
[[ "$redis_ready" == true ]] || { echo "restored Redis not ready" >&2; exit 1; }
redis_marker="$(docker exec "$REDIS_RESTORE" redis-cli GET restore-probe)"
[[ "$redis_marker" == "$DRILL_MARKER" ]] || { echo "Redis restore mismatch" >&2; exit 1; }
echo "isolated Redis RDB restore passed"
echo "backup restore drill passed: postgres=ok redis=ok no_shared_data_access=true"
