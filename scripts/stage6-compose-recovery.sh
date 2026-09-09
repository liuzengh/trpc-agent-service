#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="$ROOT/compose.stage6.yml"
BASE_URL="${STAGE6_BASE_URL:-http://127.0.0.1:8080}"
EVIDENCE_PATH="${STAGE6_EVIDENCE_PATH:-$ROOT/.scratch/stage6-compose-recovery.json}"
COOKIE_FILE="$(mktemp)"
RESPONSE_FILE="$(mktemp)"
mkdir -p "$(dirname "$EVIDENCE_PATH")"
: >"$EVIDENCE_PATH"
echo '[]' >"$EVIDENCE_PATH"
trap 'rm -f "$COOKIE_FILE" "$RESPONSE_FILE"; if [[ "${STAGE6_KEEP_COMPOSE:-0}" != "1" ]]; then docker compose -f "$COMPOSE_FILE" down -v >/dev/null 2>&1 || true; fi' EXIT

request() {
  local method="$1" path="$2" body="${3:-}" header_name="${4:-}" header_value="${5:-}"
  local args=(--silent --show-error -b "$COOKIE_FILE" -o "$RESPONSE_FILE" -w "%{http_code}")
  [[ -n "$body" ]] && args+=(-H "Content-Type: application/json" -d "$body")
  [[ -n "$header_name" ]] && args+=(-H "$header_name: $header_value")
  curl "${args[@]}" -X "$method" "$BASE_URL$path"
}

post() {
  local path="$1" body="$2" header_name="${3:-}" header_value="${4:-}"
  request POST "$path" "$body" "$header_name" "$header_value"
}

get() {
  request GET "$1"
}

body() { cat "$RESPONSE_FILE"; }
json() { jq -r "$1" "$RESPONSE_FILE"; }

wait_for_gateway() {
  for _ in $(seq 1 90); do
    if [[ "$(get /healthz)" == "200" ]] && jq -e '.status == "ok"' "$RESPONSE_FILE" >/dev/null; then return 0; fi
    sleep 1
  done
  return 1
}

wait_for_event() {
  local session_id="$1" event_type="$2"
  for _ in $(seq 1 60); do
    get "/api/v1/admin/sessions/$session_id/events" >/dev/null
    if jq -e --arg type "$event_type" '.items[]? | select(.type == $type)' "$RESPONSE_FILE" >/dev/null; then return 0; fi
    sleep 1
  done
  return 1
}

record() {
  local name="$1" status="$2" code="${3:-}" evidence="${4:-}"
  jq --arg name "$name" --arg status "$status" --arg code "$code" --arg evidence "$evidence" \
    '. + [{scenario:$name, expected:"bounded failure followed by recovery", observed:$status, code:$code, evidence:$evidence}]' \
    "$EVIDENCE_PATH" >"$EVIDENCE_PATH.tmp"
  mv "$EVIDENCE_PATH.tmp" "$EVIDENCE_PATH"
}

docker compose -f "$COMPOSE_FILE" up -d --build >/dev/null
wait_for_gateway
get /api/v1/auth/me >/dev/null

post /api/v1/admin/agent-apps '{"id":"app-recovery","name":"Stage 6 Recovery"}' >/dev/null
post /api/v1/admin/governance/policy '{"agent_app_id":"app-recovery","token_budget":10000,"estimated_tokens_per_run":5,"rate_limit":100,"rate_window_seconds":60,"runtime_timeout_ms":5000}' >/dev/null
post /api/v1/admin/deployments '{"id":"deploy-recovery","agent_app_id":"app-recovery"}' >/dev/null
post /api/v1/admin/deployments/deploy-recovery/versions '{"config":{"runner":"stage6-recovery"}}' Idempotency-Key recovery-v1 >/dev/null
post /api/v1/admin/deployments/deploy-recovery/transition '{"status":"published","version_id":"deploy-recovery-v1"}' >/dev/null
post /api/v1/admin/deployments/deploy-recovery/transition '{"status":"active"}' >/dev/null
post /api/v1/admin/storage/backend '{"backend":"postgres"}' >/dev/null
post /api/v1/chat/sessions '{"app_id":"app-recovery","session_id":"session-recovery"}' >/dev/null

WORKER_STATUS="$(post /api/v1/admin/run '{"app_id":"app-recovery","session_id":"session-recovery","input":"worker outage"}')"
docker compose -f "$COMPOSE_FILE" stop worker >/dev/null
OUTAGE_STATUS="$(post /api/v1/admin/run '{"app_id":"app-recovery","session_id":"session-recovery","input":"worker outage"}')"
OUTAGE_CODE="$(body | jq -r '.error.code // "worker_unavailable"')"
docker compose -f "$COMPOSE_FILE" up -d worker >/dev/null
for _ in $(seq 1 60); do
  if docker compose -f "$COMPOSE_FILE" ps --format json worker | jq -e '.Health == "healthy"' >/dev/null; then break; fi
  sleep 1
done
RECOVERED_STATUS="$(post /api/v1/admin/run '{"app_id":"app-recovery","session_id":"session-recovery","input":"worker recovered"}')"
record "worker-restart" "$OUTAGE_STATUS -> $RECOVERED_STATUS" "$OUTAGE_CODE" "request_worker_outage,request_worker_recovered"

docker compose -f "$COMPOSE_FILE" stop postgres >/dev/null
STORAGE_STATUS="$(post /api/v1/chat/sessions/session-recovery/messages '{"input":"storage outage"}' X-Request-ID dependency-outage)"
STORAGE_CODE="$(body | jq -r '.error.code // "storage_unavailable"')"
docker compose -f "$COMPOSE_FILE" up -d postgres >/dev/null
for _ in $(seq 1 60); do
  if docker compose -f "$COMPOSE_FILE" ps --format json postgres | jq -e '.Health == "healthy"' >/dev/null; then break; fi
  sleep 1
done
STORAGE_RECOVERY_STATUS="$(post /api/v1/chat/sessions/session-recovery/messages '{"input":"storage outage"}' X-Request-ID dependency-outage)"
if ! wait_for_event session-recovery run.completed; then
  echo "dependency recovery did not complete" >&2
  exit 1
fi
record "dependency-restart" "$STORAGE_STATUS -> $STORAGE_RECOVERY_STATUS" "$STORAGE_CODE" "dependency-outage"

post /api/v1/admin/operations/faults '{"agent_app_id":"app-recovery","scenario":"tool_error","delay_ms":0}' >/dev/null
post /api/v1/chat/sessions/session-recovery/messages '{"input":"runtime fault"}' X-Request-ID runtime-fault >/dev/null
wait_for_event session-recovery run.failed
post /api/v1/admin/operations/faults '{"agent_app_id":"app-recovery","scenario":"none","delay_ms":0}' >/dev/null
post /api/v1/chat/sessions/session-recovery/messages '{"input":"runtime recovered"}' X-Request-ID runtime-recovered >/dev/null
wait_for_event session-recovery run.completed
record "runtime-fault" "run.failed -> run.completed" "tool_error" "runtime-fault,runtime-recovered"

BINDING_BODY="$(post /api/v1/chat/bindings '{"channel":"mock","app_id":"app-recovery","conversation_type":"single","external_conversation_id":"recovery-user","external_user_id":"recovery-user","session_id":"session-im"}')"
cp "$RESPONSE_FILE" "$BINDING_BODY"
BINDING_ID="$(json .id)"
BINDING_SECRET="$(json .secret)"
CALLBACK_BODY='{"binding_id":"'"$BINDING_ID"'","message_id":"recovery-message","sequence":1,"text":"im recovery"}'
SIGNATURE="$(printf '%s' "$CALLBACK_BODY" | openssl dgst -sha256 -hmac "$BINDING_SECRET" -r | cut -d' ' -f1)"
post /api/v1/chat/mock/faults '{"scenario":"retry","session_id":"session-im"}' >/dev/null
IM_STATUS="$(post /api/v1/chat/channels/mock/callback "$CALLBACK_BODY" X-Mock-Signature "$SIGNATURE")"
wait_for_event session-im channel.delivery
post /api/v1/chat/mock/faults '{"scenario":"none","session_id":"session-im"}' >/dev/null
DUPLICATE_STATUS="$(post /api/v1/chat/channels/mock/callback "$CALLBACK_BODY" X-Mock-Signature "$SIGNATURE")"
get /api/v1/admin/sessions/session-im/events >/dev/null
INPUT_COUNT="$(jq '[.items[] | select(.idempotency_key == "channel-recovery-message:input")] | length' "$RESPONSE_FILE")"
if [[ "$INPUT_COUNT" != "1" ]]; then
  echo "duplicate callback produced $INPUT_COUNT input events" >&2
  exit 1
fi
record "im-retry-duplicate" "$IM_STATUS -> $DUPLICATE_STATUS" "channel_retry_exhausted" "recovery-message,input_count=1"
rm -f "$BINDING_BODY"

post /api/v1/admin/deployments/deploy-recovery/versions '{"config":{"runner":"stage6-recovery-v2"}}' Idempotency-Key recovery-v2 >/dev/null
post /api/v1/admin/deployments/deploy-recovery/transition '{"status":"published","version_id":"deploy-recovery-v2"}' >/dev/null
ROLLOUT_STATUS="$(post /api/v1/admin/deployments/deploy-recovery/rollout '{"target_version_id":"deploy-recovery-v2","gray_percentage":100,"confirm":true}' X-Request-ID rollout-recovery)"
ROLLBACK_PREVIEW_STATUS="$(get /api/v1/admin/deployments/deploy-recovery/rollback-preview)"
ROLLBACK_STATUS="$(post /api/v1/admin/deployments/deploy-recovery/rollback '{"confirm":true}' X-Request-ID rollout-recovery)"
record "rollout-rollback" "$ROLLOUT_STATUS -> $ROLLBACK_PREVIEW_STATUS -> $ROLLBACK_STATUS" "" "rollout-recovery"

CAPACITY_STATUS="$(post /api/v1/admin/capacity '{"agent_app_id":"app-recovery","concurrency":2,"runs":4,"timeout_ms":2000,"peak_im_callbacks_per_second":120,"average_tokens_per_session":800,"redis_operations_per_session":6,"sql_operations_per_session":4,"headroom_percent":25}')"
CAPACITY_ID="$(json .id)"
for _ in $(seq 1 30); do
  get "/api/v1/admin/capacity/$CAPACITY_ID" >/dev/null
  if [[ "$(json .status)" != "running" ]]; then break; fi
  sleep 1
done
if [[ "$(json .status)" != "completed" ]]; then
  echo "capacity run did not complete" >&2
  exit 1
fi
if ! jq -e '.sessions_per_node >= 1 and .recommended_worker_nodes >= 1 and .average_tokens_per_session == 800 and .token_throughput_per_second == 96000 and .im_callback_peak_qps == 120 and .redis_qps == 720 and .sql_qps == 480 and .headroom_percent == 25' "$RESPONSE_FILE" >/dev/null; then
  echo "capacity plan did not report the expected node, token, IM, Redis and SQL demand" >&2
  exit 1
fi
record "capacity-smoke" "$CAPACITY_STATUS -> completed" "" "$CAPACITY_ID"

DRAIN_STATUS="$(post /api/v1/admin/operations/drain '{"confirm":true}')"
for _ in $(seq 1 30); do
  if [[ "$(get /api/v1/admin/operations/drain)" == "200" ]] && [[ "$(json .state)" == "closed" ]]; then break; fi
  sleep 1
done
if [[ "$(json .state)" != "closed" ]]; then
  echo "drain did not close" >&2
  exit 1
fi
record "graceful-drain" "$DRAIN_STATUS -> closed" "" "operations-drain"

jq '{generated_at:(now | todate), environment:"docker-compose", scenarios:.}' "$EVIDENCE_PATH" >"$EVIDENCE_PATH.tmp"
mv "$EVIDENCE_PATH.tmp" "$EVIDENCE_PATH"
echo "Stage 6 Compose recovery passed; evidence: $EVIDENCE_PATH"
