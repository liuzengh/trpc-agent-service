#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
ENV_FILE="${STAGE7_ENV_FILE:-$ROOT/.env.local}"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "error: .env.local is required" >&2
  exit 1
fi
set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a
: "${OPENAI_BASE_URL:?OPENAI_BASE_URL is required}"
: "${OPENAI_API_KEY:?OPENAI_API_KEY is required}"
: "${OPENAI_MODEL:?OPENAI_MODEL is required}"
if [[ "$OPENAI_MODEL" != "gpt-5.6-luna" ]]; then
  echo "error: OPENAI_MODEL must be gpt-5.6-luna for this acceptance smoke" >&2
  exit 1
fi

TEMP_DIR="$(mktemp -d)"
COOKIE_FILE="$TEMP_DIR/cookies"
RESPONSE_FILE="$TEMP_DIR/response"
SERVICE_LOG="$TEMP_DIR/service.log"
SERVICE_PID=""
cleanup() {
  if [[ -n "$SERVICE_PID" ]] && kill -0 "$SERVICE_PID" 2>/dev/null; then
    kill -TERM "$SERVICE_PID" 2>/dev/null || true
    wait "$SERVICE_PID" 2>/dev/null || true
  fi
  if [[ "${STAGE7_KEEP_LIVE_TEMP:-0}" != "1" ]]; then
    rm -rf "$TEMP_DIR"
  else
    echo "diagnostic directory retained: $TEMP_DIR" >&2
  fi
}
trap cleanup EXIT

export TRPC_CONTROL_PLANE_SQLITE_PATH="$TEMP_DIR/control-plane.db"
export TRPC_SQLITE_PATH="$TEMP_DIR/runtime.db"
export TRPC_GOVERNANCE_PATH="$TEMP_DIR/governance.json"
export TRPC_BOT_ROUTES_PATH="$TEMP_DIR/bot-routes.json"
export TRPC_SERVICE_ADDR="127.0.0.1:${STAGE7_LIVE_PORT:-18080}"
go run ./cmd/control-migrate >/dev/null
go run ./cmd/trpc-service >"$SERVICE_LOG" 2>&1 &
SERVICE_PID=$!
BASE_URL="http://$TRPC_SERVICE_ADDR"

for _ in $(seq 1 60); do
  if curl --silent --fail "$BASE_URL/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl --silent --fail -c "$COOKIE_FILE" "$BASE_URL/api/v1/auth/me" >/dev/null
post() {
  local path="$1" body="$2" key="${3:-}"
  local args=(--silent --show-error --fail -b "$COOKIE_FILE" -H "Content-Type: application/json")
  [[ -n "$key" ]] && args+=(-H "Idempotency-Key: $key")
  curl "${args[@]}" -d "$body" "$BASE_URL$path" >"$RESPONSE_FILE"
}
post /api/v1/admin/agent-apps '{"id":"app-live","name":"Live Model Smoke"}'
post /api/v1/admin/governance/policy '{"agent_app_id":"app-live","token_budget":100000,"estimated_tokens_per_run":1000,"rate_limit":10,"rate_window_seconds":60,"runtime_timeout_ms":120000}'
post /api/v1/admin/deployments '{"id":"deploy-live","agent_app_id":"app-live"}'
post /api/v1/admin/deployments/deploy-live/versions '{"config":{"provider_profile":"default-openai","model":"gpt-5.6-luna","prompt":"请只回复：live-smoke-ok"}}' live-model-version
VERSION_ID="$(sed -n 's/.*"id":"\([^"]*\)".*/\1/p' "$RESPONSE_FILE")"
post /api/v1/admin/deployments/deploy-live/transition '{"status":"published","version_id":"'"$VERSION_ID"'"}'
post /api/v1/admin/deployments/deploy-live/transition '{"status":"active"}'
post /api/v1/chat/sessions '{"app_id":"app-live","session_id":"session-live"}'
post /api/v1/chat/sessions/session-live/messages '{"input":"执行在线模型验收"}'
for _ in $(seq 1 150); do
  if curl --silent --fail -b "$COOKIE_FILE" "$BASE_URL/api/v1/admin/sessions/session-live/events" >"$RESPONSE_FILE" && grep -q 'run.completed' "$RESPONSE_FILE"; then
    echo "Stage 7 live model smoke passed"
    exit 0
  fi
  if grep -q 'run.failed\|run.cancelled' "$RESPONSE_FILE"; then
    echo "error: live model run did not complete" >&2
    jq -r '.items[]? | select(.type == "run.failed" or .type == "run.cancelled") | .payload | @base64d' "$RESPONSE_FILE" >&2 || true
    tail -20 "$SERVICE_LOG" >&2
    exit 1
  fi
  sleep 1
done
echo "error: live model smoke timed out" >&2
exit 1
