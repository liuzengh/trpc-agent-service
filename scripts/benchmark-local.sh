#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

REQUESTS="${BENCHMARK_REQUESTS:-1000}"
CONCURRENCY="${BENCHMARK_CONCURRENCY:-50}"
SESSIONS="${BENCHMARK_SESSIONS:-200}"
PORT="${BENCHMARK_PORT:-18083}"
RUN_ID="$(date +%s)-$$"
PREFIX="benchmark-$RUN_ID"
POSTGRES_URL="${TEST_POSTGRES_URL:-postgres://trpc_agent:trpc_agent_dev@127.0.0.1:5432/trpc_agent?sslmode=disable}"
REDIS_URL_VALUE="${TEST_REDIS_URL:-redis://127.0.0.1:6379/0}"
APP_LOG="/tmp/trpc-agent-benchmark-$RUN_ID.log"
APP_PID=""

case "$REQUESTS:$CONCURRENCY:$SESSIONS:$PORT" in
  *[!0-9:]*) echo "benchmark parameters must be positive integers" >&2; exit 2 ;;
esac
if (( REQUESTS <= 0 || CONCURRENCY <= 0 || SESSIONS <= 0 || PORT <= 0 )); then
  echo "benchmark parameters must be positive integers" >&2
  exit 2
fi

cleanup() {
  if [[ -n "$APP_PID" ]]; then
    kill -TERM "$APP_PID" 2>/dev/null || true
    wait "$APP_PID" 2>/dev/null || true
  fi
  docker compose stop redis postgres >/dev/null 2>&1 || true
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

./build.sh >/dev/null
TRPC_AGENT_CONTROL_PLANE_BACKEND=postgres \
TRPC_AGENT_POSTGRES_URL="$POSTGRES_URL" \
TRPC_AGENT_POSTGRES_AUTO_MIGRATE=true \
TRPC_AGENT_POSTGRES_BOOTSTRAP_TUTORIAL=true \
  ./bin/trpc-migrate >/dev/null

env \
  TRPC_AGENT_MODEL_PROVIDER=mock \
  TRPC_AGENT_SESSION_BACKEND=redis \
  TRPC_AGENT_COORDINATOR_BACKEND=redis \
  TRPC_AGENT_IDEMPOTENCY_BACKEND=redis \
  TRPC_AGENT_CONTROL_PLANE_BACKEND=postgres \
  TRPC_AGENT_POSTGRES_URL="$POSTGRES_URL" \
  TRPC_AGENT_POSTGRES_AUTO_MIGRATE=false \
  TRPC_AGENT_POSTGRES_BOOTSTRAP_TUTORIAL=false \
  TRPC_AGENT_QUEUE_BACKEND=redis \
  TRPC_AGENT_QUOTA_BACKEND=redis \
  TRPC_AGENT_ADMIN_ENABLED=false \
  TRPC_AGENT_OTEL_ENABLED=false \
  REDIS_URL="$REDIS_URL_VALUE" \
  REDIS_KEY_PREFIX="$PREFIX" \
  ./bin/trpc-service -role all -addr "127.0.0.1:$PORT" \
  >"$APP_LOG" 2>&1 &
APP_PID=$!

for _ in $(seq 1 100); do
  curl -fsS "http://127.0.0.1:$PORT/readyz" >/dev/null 2>&1 && break
  sleep 0.1
done
if ! curl -fsS "http://127.0.0.1:$PORT/readyz" >/dev/null 2>&1; then
  echo "benchmark service did not become ready; log: $APP_LOG" >&2
  exit 1
fi

echo "environment: cpu=$(nproc) go=$(go env GOVERSION) docker=$(docker version --format '{{.Server.Version}}')"
echo "configuration: requests=$REQUESTS concurrency=$CONCURRENCY sessions=$SESSIONS"
./bin/trpc-loadgen \
  -url "http://127.0.0.1:$PORT/inbound" \
  -binding tutorial-http \
  -requests "$REQUESTS" \
  -concurrency "$CONCURRENCY" \
  -sessions "$SESSIONS" \
  -message-prefix "$PREFIX"

completed=0
sent=0
started_wait="$(date +%s)"
for _ in $(seq 1 120); do
  read -r completed sent < <(docker compose exec -T postgres \
    psql -U trpc_agent -d trpc_agent -At -F ' ' -c "
      SELECT
        count(*) FILTER (WHERE r.status = 'completed'),
        count(*) FILTER (WHERE o.status = 'sent')
      FROM inbound_message i
      JOIN agent_run r ON r.request_id = i.request_id
      LEFT JOIN outbound_message o ON o.request_id = r.request_id
      WHERE i.external_message_id LIKE '$PREFIX-%';")
  if (( completed == REQUESTS && sent == REQUESTS )); then
    break
  fi
  sleep 1
done
drain_seconds=$(( $(date +%s) - started_wait ))
if (( completed != REQUESTS || sent != REQUESTS )); then
  echo "pipeline did not drain: completed=$completed sent=$sent expected=$REQUESTS log=$APP_LOG" >&2
  exit 1
fi

failures="$(docker compose exec -T postgres \
  psql -U trpc_agent -d trpc_agent -Atc "
    SELECT count(*)
    FROM inbound_message i
    JOIN agent_run r ON r.request_id = i.request_id
    WHERE i.external_message_id LIKE '$PREFIX-%' AND r.status = 'failed';")"
echo "pipeline: completed=$completed sent=$sent failed=$failures drain_after_ack=${drain_seconds}s"
echo "local benchmark passed: prefix=$PREFIX"
