#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PORT="${TEST_OBSERVABILITY_PORT:-18082}"
SERVICE_NAME="trpc-agent-observability-e2e"
TRACE_ID="$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
SPAN_ID="$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')"
RUN_ID="$(date +%s)-$$"
HTTP_API_TOKEN="$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')"
APP_PID=""
TRACE_OUTPUT="/tmp/trpc-agent-otel-trace-$RUN_ID.json"
METRIC_OUTPUT="/tmp/trpc-agent-otel-metrics-$RUN_ID.txt"
APP_LOG="/tmp/trpc-agent-otel-app-$RUN_ID.log"

cleanup() {
  if [[ -n "$APP_PID" ]]; then
    kill -TERM "$APP_PID" 2>/dev/null || true
    wait "$APP_PID" 2>/dev/null || true
  fi
  # These services may be shared with a manually running Agent. Leave the
  # observability stack up; only the test-owned process is stopped here.
}
trap cleanup EXIT

wait_http() {
  local url="$1"
  for _ in $(seq 1 60); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "timed out waiting for $url" >&2
  return 1
}

docker compose --profile observability up -d \
  tempo otel-collector prometheus grafana
wait_http http://127.0.0.1:3200/ready
wait_http http://127.0.0.1:9090/-/ready
wait_http http://127.0.0.1:3000/api/health

rules="$(curl -fsS http://127.0.0.1:9090/api/v1/rules)"
if ! grep -q 'AgentRunFailureRatioHigh' <<<"$rules" ||
  ! grep -q 'OTelCollectorScrapeDown' <<<"$rules"; then
  echo "Prometheus did not load the Agent platform alert rules" >&2
  exit 1
fi

dashboards=""
for _ in $(seq 1 30); do
  dashboards="$(curl -fsS -u admin:admin \
    'http://127.0.0.1:3000/api/search?query=tRPC%20Agent%20Platform')"
  grep -q 'trpc-agent-platform' <<<"$dashboards" && break
  sleep 1
done
if ! grep -q 'trpc-agent-platform' <<<"$dashboards"; then
  echo "Grafana did not provision the Agent platform dashboard" >&2
  exit 1
fi

./build.sh >/dev/null
env \
  TRPC_AGENT_HTTP_API_ENABLED=true \
  TRPC_AGENT_HTTP_API_TOKEN= \
  TRPC_AGENT_HTTP_API_PRINCIPALS_JSON="[{\"name\":\"trace-test\",\"token\":\"$HTTP_API_TOKEN\",\"tenant_id\":\"tutorial-tenant\",\"binding_keys\":[\"tutorial-http\"],\"user_ids\":[\"alice\"]}]" \
  TRPC_AGENT_MODEL_PROVIDER=mock \
  TRPC_AGENT_SESSION_BACKEND=inmemory \
  TRPC_AGENT_COORDINATOR_BACKEND=local \
  TRPC_AGENT_IDEMPOTENCY_BACKEND=local \
  TRPC_AGENT_CONTROL_PLANE_BACKEND=inmemory \
  TRPC_AGENT_QUEUE_BACKEND=memory \
  TRPC_AGENT_QUOTA_BACKEND=local \
  TRPC_AGENT_ADMIN_ENABLED=false \
  TRPC_AGENT_OTEL_ENABLED=true \
  TRPC_AGENT_OTEL_ENDPOINT=127.0.0.1:4317 \
  TRPC_AGENT_OTEL_INSECURE=true \
  TRPC_AGENT_OTEL_SAMPLE_RATIO=1 \
  TRPC_AGENT_OTEL_SERVICE_NAME="$SERVICE_NAME" \
  ./bin/trpc-service -role all -addr "127.0.0.1:$PORT" \
  >"$APP_LOG" 2>&1 &
APP_PID=$!
wait_http "http://127.0.0.1:$PORT/readyz"

headers="$(curl -fsS -D - -o /dev/null -X POST "http://127.0.0.1:$PORT/chat" \
  -H "Authorization: Bearer $HTTP_API_TOKEN" \
  -H 'Content-Type: application/json' \
  -H "traceparent: 00-$TRACE_ID-$SPAN_ID-01" \
  -d "{\"binding_key\":\"tutorial-http\",\"message_id\":\"otel-chat-$RUN_ID\",\"user_id\":\"alice\",\"session_id\":\"otel-$RUN_ID\",\"message\":\"trace check\"}")"
if ! grep -qi "^X-Trace-Id: $TRACE_ID" <<<"$headers"; then
  echo "HTTP response did not preserve trace ID $TRACE_ID" >&2
  exit 1
fi

curl -fsS -X POST "http://127.0.0.1:$PORT/inbound" \
  -H "Authorization: Bearer $HTTP_API_TOKEN" \
  -H 'Content-Type: application/json' \
  -d "{\"binding_key\":\"tutorial-http\",\"message_id\":\"otel-inbound-$RUN_ID\",\"user_id\":\"alice\",\"session_id\":\"otel-async-$RUN_ID\",\"chat_type\":\"direct\",\"message\":\"metric check\"}" \
  >/dev/null

# The in-memory Worker and Sender finish quickly. Graceful shutdown forces the
# batch trace and periodic metric readers to flush without waiting a minute.
sleep 1
kill -TERM "$APP_PID"
wait "$APP_PID"
APP_PID=""

trace_code=""
for _ in $(seq 1 30); do
  trace_code="$(curl -sS -o "$TRACE_OUTPUT" -w '%{http_code}' \
    "http://127.0.0.1:3200/api/traces/$TRACE_ID" || true)"
  [[ "$trace_code" == "200" ]] && break
  sleep 1
done
if [[ "$trace_code" != "200" ]] ||
  ! grep -q 'POST /chat' "$TRACE_OUTPUT" ||
  ! grep -q 'session.get' "$TRACE_OUTPUT"; then
  echo "Tempo did not return the expected HTTP and Session spans" >&2
  exit 1
fi

for _ in $(seq 1 30); do
  curl -fsS http://127.0.0.1:9464/metrics >"$METRIC_OUTPUT"
  if grep -q 'agent_inbound_messages_total' "$METRIC_OUTPUT" &&
    grep -q 'agent_runs_total' "$METRIC_OUTPUT" &&
    grep -q 'agent_reply_deliveries_total' "$METRIC_OUTPUT"; then
    break
  fi
  sleep 1
done
if ! grep -q 'tenant_id="tutorial-tenant"' "$METRIC_OUTPUT"; then
  echo "tenant-scoped platform metrics were not exported" >&2
  exit 1
fi

echo "observability e2e passed: trace_id=$TRACE_ID service=$SERVICE_NAME dashboard=trpc-agent-platform"
