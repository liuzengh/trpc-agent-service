#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="$ROOT/compose.stage6.yml"
BASE_URL="${STAGE6_BASE_URL:-http://127.0.0.1:8080}"
COOKIE_FILE="$(mktemp)"
RESPONSE_FILE="$(mktemp)"
trap 'rm -f "$COOKIE_FILE" "$RESPONSE_FILE"; if [[ "${STAGE6_KEEP_COMPOSE:-0}" != "1" ]]; then docker compose -f "$COMPOSE_FILE" down -v >/dev/null 2>&1 || true; fi' EXIT

post() {
  local path="$1" body="$2"
  curl --silent --show-error --fail -b "$COOKIE_FILE" -H "Content-Type: application/json" -d "$body" "$BASE_URL$path" >"$RESPONSE_FILE"
  cat "$RESPONSE_FILE"
}

docker compose -f "$COMPOSE_FILE" up -d --build
for _ in $(seq 1 60); do
  if curl --silent --fail "$BASE_URL/healthz" >/dev/null 2>&1; then break; fi
  sleep 1
done
curl --silent --fail "$BASE_URL/healthz" | grep -q '"status":"ok"'
curl --silent --fail -c "$COOKIE_FILE" "$BASE_URL/api/v1/auth/me" >/dev/null

post /api/v1/admin/agent-apps '{"id":"app-smoke","name":"Stage 6 Smoke"}' >/dev/null
post /api/v1/admin/governance/policy '{"agent_app_id":"app-smoke","token_budget":100,"estimated_tokens_per_run":5,"rate_limit":10,"rate_window_seconds":60}' >/dev/null
post /api/v1/admin/deployments '{"id":"deploy-smoke","agent_app_id":"app-smoke"}' >/dev/null
curl --silent --show-error --fail -b "$COOKIE_FILE" \
  -H "Content-Type: application/json" -H "Idempotency-Key: stage6-smoke-version" \
  -d '{"config":{"runner":"stage6-compose"}}' \
  "$BASE_URL/api/v1/admin/deployments/deploy-smoke/versions" >/dev/null
post /api/v1/admin/deployments/deploy-smoke/transition '{"status":"published","version_id":"deploy-smoke-v1"}' >/dev/null
post /api/v1/admin/deployments/deploy-smoke/transition '{"status":"active"}' >/dev/null
post /api/v1/admin/storage/backend '{"backend":"postgres"}' >/dev/null

BINDING_JSON="$(post /api/v1/chat/bindings '{"channel":"mock","app_id":"app-smoke","conversation_type":"single","external_conversation_id":"compose-smoke","external_user_id":"smoke-user","session_id":"session-smoke"}')"
BINDING_ID="$(printf '%s' "$BINDING_JSON" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')"
SECRET="$(printf '%s' "$BINDING_JSON" | sed -n 's/.*"secret":"\([^"]*\)".*/\1/p')"
if [[ -z "$BINDING_ID" || -z "$SECRET" ]]; then
  echo "Mock Channel Binding or secret was not returned" >&2
  exit 1
fi

CALLBACK_BODY='{"binding_id":"'"$BINDING_ID"'","message_id":"smoke-message","sequence":1,"text":"stage6 compose"}'
SIGNATURE="$(printf '%s' "$CALLBACK_BODY" | openssl dgst -sha256 -hmac "$SECRET" -r | cut -d' ' -f1)"
curl --silent --show-error --fail -b "$COOKIE_FILE" \
  -H "Content-Type: application/json" -H "X-Mock-Signature: $SIGNATURE" \
  -d "$CALLBACK_BODY" "$BASE_URL/api/v1/chat/channels/mock/callback" >/dev/null

for _ in $(seq 1 60); do
  if curl --silent --fail -b "$COOKIE_FILE" "$BASE_URL/api/v1/admin/sessions/session-smoke/events" | grep -q 'run.completed'; then
    echo "Stage 6 Compose smoke passed"
    exit 0
  fi
  sleep 1
done

echo "Stage 6 Compose run did not complete" >&2
docker compose -f "$COMPOSE_FILE" logs gateway worker postgres redis >&2 || true
exit 1
