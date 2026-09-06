#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

POSTGRES_URL="${TEST_POSTGRES_URL:-postgres://trpc_agent:trpc_agent_dev@127.0.0.1:5432/trpc_agent?sslmode=disable}"
REDIS_URL_VALUE="${TEST_REDIS_URL:-redis://127.0.0.1:6379/0}"
PORT="${TEST_GATEWAY_PORT:-18088}"
RUN_ID="$(date +%s)-$$"
HTTP_API_TOKEN="$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')"
PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    kill -TERM "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  # Dependencies may be shared with a manually running service; leave them up.
}
trap cleanup EXIT

docker compose up -d postgres redis
for _ in $(seq 1 60); do
  pg_health="$(docker inspect --format '{{.State.Health.Status}}' trpc-agent-service-postgres-1 2>/dev/null || true)"
  redis_health="$(docker inspect --format '{{.State.Health.Status}}' trpc-agent-service-redis-1 2>/dev/null || true)"
  if [[ "$pg_health" == "healthy" && "$redis_health" == "healthy" ]]; then
    break
  fi
  sleep 1
done

./build.sh >/dev/null

export TRPC_AGENT_MODEL_PROVIDER=mock
export TRPC_AGENT_HTTP_API_ENABLED=true
export TRPC_AGENT_HTTP_API_TOKEN=
export TRPC_AGENT_HTTP_API_PRINCIPALS_JSON="[{\"name\":\"multiprocess-test\",\"token\":\"$HTTP_API_TOKEN\",\"tenant_id\":\"tutorial-tenant\",\"binding_keys\":[\"tutorial-http\"],\"user_ids\":[\"alice\"]}]"
export TRPC_AGENT_SESSION_BACKEND=redis
export TRPC_AGENT_COORDINATOR_BACKEND=redis
export TRPC_AGENT_IDEMPOTENCY_BACKEND=redis
export TRPC_AGENT_CONTROL_PLANE_BACKEND=postgres
export TRPC_AGENT_POSTGRES_URL="$POSTGRES_URL"
export TRPC_AGENT_POSTGRES_AUTO_MIGRATE=false
export TRPC_AGENT_POSTGRES_BOOTSTRAP_TUTORIAL=false
export TRPC_AGENT_QUEUE_BACKEND=redis
export TRPC_AGENT_QUOTA_BACKEND=redis
export TRPC_AGENT_ADMIN_ENABLED=false
export TRPC_AGENT_OTEL_ENABLED=false
export REDIS_URL="$REDIS_URL_VALUE"
export REDIS_KEY_PREFIX="e2e-$RUN_ID"

TRPC_AGENT_POSTGRES_AUTO_MIGRATE=true \
TRPC_AGENT_POSTGRES_BOOTSTRAP_TUTORIAL=true \
  ./bin/trpc-migrate

start_role() {
  local role="$1"
  shift
  ./bin/trpc-service -role "$role" "$@" >"/tmp/trpc-agent-$RUN_ID-$role-$RANDOM.log" 2>&1 &
  PIDS+=("$!")
}

start_role gateway -addr "127.0.0.1:$PORT"
start_role relay
start_role sender
start_role jobs
start_role worker
worker_a="${PIDS[${#PIDS[@]}-1]}"

for _ in $(seq 1 50); do
  if curl -fsS "http://127.0.0.1:$PORT/readyz" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done

post_message() {
  local message_id="$1"
  local text="$2"
  curl -fsS -X POST "http://127.0.0.1:$PORT/inbound" \
    -H "Authorization: Bearer $HTTP_API_TOKEN" \
    -H 'Content-Type: application/json' \
    -d "{\"binding_key\":\"tutorial-http\",\"message_id\":\"$message_id\",\"user_id\":\"alice\",\"session_id\":\"e2e-$RUN_ID\",\"chat_type\":\"direct\",\"message\":\"$text\"}"
}

first_json="$(post_message "e2e-$RUN_ID-1" '我叫小明。')"
first_request="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["request_id"])' <<<"$first_json")"
for _ in $(seq 1 100); do
  status="$(docker compose exec -T postgres psql -U trpc_agent -d trpc_agent -Atc "SELECT status FROM outbound_message WHERE request_id='$first_request'" 2>/dev/null || true)"
  [[ "$status" == "sent" ]] && break
  sleep 0.1
done
[[ "$status" == "sent" ]]

kill -TERM "$worker_a"
wait "$worker_a" || true
start_role worker

second_json="$(post_message "e2e-$RUN_ID-2" '我叫什么？')"
second_request="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["request_id"])' <<<"$second_json")"
for _ in $(seq 1 100); do
  status="$(docker compose exec -T postgres psql -U trpc_agent -d trpc_agent -Atc "SELECT status FROM outbound_message WHERE request_id='$second_request'" 2>/dev/null || true)"
  [[ "$status" == "sent" ]] && break
  sleep 0.1
done
[[ "$status" == "sent" ]]

reply="$(docker compose exec -T postgres psql -U trpc_agent -d trpc_agent -Atc "SELECT payload->>'text' FROM outbound_message WHERE request_id='$second_request'")"
[[ "$reply" == *"你叫小明"* ]]

job_failures="$(docker compose exec -T postgres psql -U trpc_agent -d trpc_agent -Atc "SELECT count(*) FROM background_job WHERE tenant_id='tutorial-tenant' AND status IN ('dead','failed')")"
[[ "$job_failures" == "0" ]]

echo "multiprocess e2e passed: first=$first_request second=$second_request reply=$reply"
