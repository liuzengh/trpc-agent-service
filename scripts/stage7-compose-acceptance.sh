#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="$ROOT/compose.stage7.yml"
BASE_URL="${STAGE7_BASE_URL:-http://127.0.0.1:8080}"
TEMP_DIR="$(mktemp -d)"
COOKIE_A="$TEMP_DIR/cookie-a"
COOKIE_B="$TEMP_DIR/cookie-b"
RESPONSE="$TEMP_DIR/response"
cleanup() {
  rm -rf "$TEMP_DIR"
  if [[ "${STAGE7_KEEP_COMPOSE:-0}" != "1" ]]; then
    docker compose -f "$COMPOSE_FILE" down -v >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT
trap 'echo "error: Stage 7 Compose assertion failed at line $LINENO" >&2' ERR

request() {
  local gateway_id="$1" cookie="$2" method="$3" path="$4" body="${5:-}" request_id="${6:-}"
  local args=(--silent --show-error --max-time 15 -o "$RESPONSE" -w "%{http_code}" -b "$cookie" -c "$cookie" -H "X-Gateway: $gateway_id")
  [[ -n "$body" ]] && args+=(-H "Content-Type: application/json" -d "$body")
  if [[ -n "$request_id" && "$path" == */versions ]]; then
    args+=(-H "Idempotency-Key: $request_id")
  elif [[ -n "$request_id" ]]; then
    args+=(-H "X-Request-ID: $request_id")
  fi
  curl "${args[@]}" -X "$method" "$BASE_URL$path"
}
wait_request_event() {
  local id="$1" cookie="$2" session="$3" request_id="$4" event_type="$5"
  for _ in $(seq 1 90); do
    request "$id" "$cookie" GET "/api/v1/admin/sessions/$session/events" >/dev/null || true
    if jq -e --arg req "$request_id" --arg type "$event_type" '.items[]? | select(.idempotency_key | startswith($req + ":")) | select(.type == $type)' "$RESPONSE" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
	cat "$RESPONSE" >&2
  return 1
}
expect() {
  local want="$1" actual="$2" label="$3"
  if [[ "$actual" != "$want" ]]; then
    echo "error: $label returned $actual, expected $want" >&2
    cat "$RESPONSE" >&2
    exit 1
  fi
}
wait_gateway() {
  local id="$1" cookie="$2"
  for _ in $(seq 1 90); do
    if [[ "$(request "$id" "$cookie" GET /healthz)" == "200" ]]; then
      request "$id" "$cookie" GET /api/v1/auth/me >/dev/null
      return 0
    fi
    sleep 1
  done
	cat "$RESPONSE" >&2
  return 1
}
wait_request_terminal() {
  local id="$1" cookie="$2" session="$3" request_id="$4" terminal="$5"
  for _ in $(seq 1 90); do
    request "$id" "$cookie" GET "/api/v1/admin/sessions/$session/events" >/dev/null || true
    if jq -e --arg req "$request_id" --arg terminal "$terminal" '.items[]? | select((.idempotency_key == ($req + ":run-completed") or .idempotency_key == ($req + ":terminal")) and .type == $terminal)' "$RESPONSE" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
	cat "$RESPONSE" >&2
  return 1
}

docker compose -f "$COMPOSE_FILE" up -d --build >/dev/null
wait_gateway a "$COOKIE_A"
wait_gateway b "$COOKIE_B"

expect 201 "$(request a "$COOKIE_A" POST /api/v1/admin/agent-apps '{"id":"app-stage7","name":"Stage 7"}')" create-app-a
expect 201 "$(request a "$COOKIE_A" POST /api/v1/admin/deployments '{"id":"deploy-stage7","agent_app_id":"app-stage7"}')" create-deployment-a
expect 201 "$(request a "$COOKIE_A" POST /api/v1/admin/deployments/deploy-stage7/versions '{"config":{"runner":"stage7"}}' version-stage7)" create-version-a
expect 200 "$(request a "$COOKIE_A" POST /api/v1/admin/deployments/deploy-stage7/transition '{"status":"published","version_id":"deploy-stage7-v1"}')" publish-a
expect 200 "$(request a "$COOKIE_A" POST /api/v1/admin/deployments/deploy-stage7/transition '{"status":"active"}')" activate-a
expect 200 "$(request a "$COOKIE_A" POST /api/v1/admin/governance/policy '{"agent_app_id":"app-stage7","token_budget":100000,"estimated_tokens_per_run":10,"rate_limit":100,"rate_window_seconds":60}')" policy-a
expect 200 "$(request b "$COOKIE_B" GET '/api/v1/admin/governance/policy?app_id=app-stage7')" policy-read-b
jq -e '.agent_app_id == "app-stage7" and .revision == 1' "$RESPONSE" >/dev/null
expect 200 "$(request a "$COOKIE_A" POST /api/v1/admin/storage/backend '{"backend":"postgres"}')" backend-a
expect 200 "$(request b "$COOKIE_B" GET /api/v1/admin/storage/backend)" backend-read-b
jq -e '.backend == "postgres"' "$RESPONSE" >/dev/null
expect 200 "$(request b "$COOKIE_B" GET /api/v1/admin/agent-apps)" read-app-b
jq -e '.items[] | select(.id == "app-stage7")' "$RESPONSE" >/dev/null

# Provider Route registry is shared by both Gateways. The route is created via
# B, observed via A, then disabled via B and observed from A again.
expect 201 "$(request b "$COOKIE_B" POST /api/v1/admin/providers/routes '{"provider":"telegram","external_subject":"stage7-shared","tenant_id":"tenant-dev","app_id":"app-stage7","conversation_type":"single"}')" create-provider-route-b
expect 200 "$(request a "$COOKIE_A" GET /api/v1/admin/providers/routes)" read-provider-route-a
jq -e '.items[] | select(.provider == "telegram" and .external_subject == "stage7-shared" and .enabled == true)' "$RESPONSE" >/dev/null
expect 200 "$(request b "$COOKIE_B" PATCH '/api/v1/admin/providers/routes?provider=telegram&external_subject=stage7-shared' '{"tenant_id":"tenant-dev","app_id":"app-stage7","conversation_type":"single","enabled":false}')" disable-provider-route-b
expect 200 "$(request a "$COOKIE_A" GET /api/v1/admin/providers/routes)" read-disabled-provider-route-a
jq -e '.items[] | select(.provider == "telegram" and .external_subject == "stage7-shared" and .enabled == false)' "$RESPONSE" >/dev/null

# Channel Binding is immediately shared and each Gateway mutates the latest
# authoritative snapshot instead of replacing it with a stale local copy.
expect 201 "$(request a "$COOKIE_A" POST /api/v1/chat/bindings '{"channel":"mock","app_id":"app-stage7","conversation_type":"single","external_conversation_id":"shared-a","external_user_id":"shared-a","session_id":"session-binding-a"}')" create-binding-a
BINDING_A="$(jq -r '.id' "$RESPONSE")"
expect 200 "$(request b "$COOKIE_B" GET /api/v1/chat/bindings)" read-binding-a-from-b
jq -e --arg id "$BINDING_A" '.items[] | select(.id == $id)' "$RESPONSE" >/dev/null
expect 201 "$(request b "$COOKIE_B" POST /api/v1/chat/bindings '{"channel":"mock","app_id":"app-stage7","conversation_type":"single","external_conversation_id":"shared-b","external_user_id":"shared-b","session_id":"session-binding-b"}')" create-binding-b
BINDING_B="$(jq -r '.id' "$RESPONSE")"
expect 200 "$(request a "$COOKIE_A" GET /api/v1/chat/bindings)" read-both-bindings-from-a
jq -e --arg a "$BINDING_A" --arg b "$BINDING_B" '([.items[].id] | index($a)) != null and ([.items[].id] | index($b)) != null' "$RESPONSE" >/dev/null

expect 201 "$(request a "$COOKIE_A" POST /api/v1/chat/sessions '{"app_id":"app-stage7","session_id":"session-stage7"}')" create-session-a
expect 202 "$(request b "$COOKIE_B" POST /api/v1/chat/sessions/session-stage7/messages '{"input":"run through gateway b"}' request-b)" run-b
wait_request_terminal b "$COOKIE_B" session-stage7 request-b run.completed

# A retry arriving at the other Gateway after completion must return the
# persisted terminal and must not append another input event.
expect 200 "$(request a "$COOKIE_A" POST /api/v1/chat/sessions/session-stage7/messages '{"input":"run through gateway b"}' request-b)" retry-request-on-a
request a "$COOKIE_A" GET /api/v1/admin/sessions/session-stage7/events >/dev/null
jq -e '[.items[] | select(.idempotency_key == "request-b:input")] | length == 1' "$RESPONSE" >/dev/null

# Pause Gateway A until its live lease expires. After it resumes and records an
# exact cancellation, Gateway B must commit with a higher observable token.
expect 201 "$(request a "$COOKIE_A" POST /api/v1/admin/agent-apps '{"id":"app-fence","name":"Fencing"}')" create-fence-app
expect 201 "$(request a "$COOKIE_A" POST /api/v1/admin/deployments '{"id":"deploy-fence","agent_app_id":"app-fence"}')" create-fence-deployment
expect 201 "$(request a "$COOKIE_A" POST /api/v1/admin/deployments/deploy-fence/versions '{"config":{"runner":"fence","deterministic_response_delay_ms":12000}}' fence-version)" create-fence-version
expect 200 "$(request a "$COOKIE_A" POST /api/v1/admin/deployments/deploy-fence/transition '{"status":"published","version_id":"deploy-fence-v1"}')" publish-fence
expect 200 "$(request a "$COOKIE_A" POST /api/v1/admin/deployments/deploy-fence/transition '{"status":"active"}')" activate-fence
expect 200 "$(request a "$COOKIE_A" POST /api/v1/admin/governance/policy '{"agent_app_id":"app-fence","token_budget":100000,"rate_limit":100,"rate_window_seconds":60}')" policy-fence
expect 201 "$(request a "$COOKIE_A" POST /api/v1/chat/sessions '{"app_id":"app-fence","session_id":"session-fence"}')" create-fence-session
expect 202 "$(request a "$COOKIE_A" POST /api/v1/chat/sessions/session-fence/messages '{"input":"first owner"}' fence-a)" fence-a-admitted
wait_request_event a "$COOKIE_A" session-fence fence-a session.lease.acquired

# Cancellation is intentionally sent through the other Gateway while A owns
# the execution. The shared coordinator wakes A within the lease renewal bound.
expect 201 "$(request a "$COOKIE_A" POST /api/v1/chat/sessions '{"app_id":"app-fence","session_id":"session-fence-cancel"}')" create-fence-cancel-session
expect 202 "$(request a "$COOKIE_A" POST /api/v1/chat/sessions/session-fence-cancel/messages '{"input":"cancel me"}' fence-cancel-a)" fence-cancel-admitted
sleep 1
expect 200 "$(request b "$COOKIE_B" POST /api/v1/chat/sessions/session-fence-cancel/cancel '{"request_id":"fence-cancel-a"}')" remote-cancel
jq -e '.status == "cancellation_requested" or .status == "running"' "$RESPONSE" >/dev/null
wait_request_terminal b "$COOKIE_B" session-fence-cancel fence-cancel-a run.cancelled

docker compose -f "$COMPOSE_FILE" pause gateway-a >/dev/null
sleep 4
expect 202 "$(request b "$COOKIE_B" POST /api/v1/chat/sessions/session-fence/messages '{"input":"current owner"}' fence-b)" fence-b-admitted
wait_request_event b "$COOKIE_B" session-fence fence-b session.lease.acquired
wait_request_terminal b "$COOKIE_B" session-fence fence-a run.cancelled
wait_request_terminal b "$COOKIE_B" session-fence fence-b run.completed
docker compose -f "$COMPOSE_FILE" unpause gateway-a >/dev/null
request b "$COOKIE_B" GET /api/v1/admin/sessions/session-fence/events >/dev/null
FENCE_A="$(jq -r '.items[] | select(.idempotency_key | startswith("fence-a:lease-")) | .fencing_token' "$RESPONSE" | head -1)"
FENCE_B="$(jq -r '.items[] | select(.idempotency_key | startswith("fence-b:lease-")) | .fencing_token' "$RESPONSE" | head -1)"
[[ -n "$FENCE_A" && -n "$FENCE_B" && "$FENCE_B" -gt "$FENCE_A" ]] || { echo "error: fencing token did not increase ($FENCE_A -> $FENCE_B)" >&2; exit 1; }
jq -e '[.items[] | select(.idempotency_key == "fence-a:run-completed")] | length == 0' "$RESPONSE" >/dev/null

# Exercise dangerous Tool governance through the independent Worker.
expect 201 "$(request a "$COOKIE_A" POST /api/v1/admin/agent-apps '{"id":"app-tool","name":"Governed Tool"}')" create-tool-app
expect 201 "$(request a "$COOKIE_A" POST /api/v1/admin/deployments '{"id":"deploy-tool","agent_app_id":"app-tool"}')" create-tool-deployment
expect 201 "$(request a "$COOKIE_A" POST /api/v1/admin/deployments/deploy-tool/versions '{"config":{"runner":"tool","tools":["deploy"],"deterministic_tool_call":"deploy"}}' tool-version)" create-tool-version
expect 200 "$(request a "$COOKIE_A" POST /api/v1/admin/deployments/deploy-tool/transition '{"status":"published","version_id":"deploy-tool-v1"}')" publish-tool
expect 200 "$(request a "$COOKIE_A" POST /api/v1/admin/deployments/deploy-tool/transition '{"status":"active"}')" activate-tool
expect 200 "$(request a "$COOKIE_A" POST /api/v1/admin/governance/policy '{"agent_app_id":"app-tool","allowed_tools":["deploy"],"dangerous_tools":["deploy"],"token_budget":100000,"rate_limit":100,"rate_window_seconds":60}')" policy-tool
expect 201 "$(request a "$COOKIE_A" POST /api/v1/chat/sessions '{"app_id":"app-tool","session_id":"session-tool"}')" create-tool-session
expect 202 "$(request a "$COOKIE_A" POST /api/v1/chat/sessions/session-tool/messages '{"input":"approve tool"}' tool-approve)" tool-approve-pending
wait_request_event a "$COOKIE_A" session-tool tool-approve tool.confirmation.pending
expect 200 "$(request a "$COOKIE_A" GET /api/v1/admin/governance/confirmations)" list-tool-confirmations
CONFIRM_APPROVE="$(jq -r '.items[] | select(.request_id == "tool-approve") | .id' "$RESPONSE")"
[[ -n "$CONFIRM_APPROVE" ]] || { echo "error: approval confirmation missing" >&2; exit 1; }
expect 200 "$(request a "$COOKIE_A" POST "/api/v1/admin/governance/confirmations/$CONFIRM_APPROVE/decision" '{"approve":true}')" approve-tool
expect 200 "$(request a "$COOKIE_A" POST "/api/v1/admin/governance/confirmations/$CONFIRM_APPROVE/decision" '{"approve":true}')" duplicate-approve-tool
expect 202 "$(request b "$COOKIE_B" POST /api/v1/chat/sessions/session-tool/messages '{"input":"approve tool"}' tool-approve)" resume-approved-tool
wait_request_terminal b "$COOKIE_B" session-tool tool-approve run.completed
expect 200 "$(request a "$COOKIE_A" GET /api/v1/admin/governance/confirmations)" list-completed-tool
jq -e '.items[] | select(.request_id == "tool-approve" and .status == "completed")' "$RESPONSE" >/dev/null

expect 201 "$(request a "$COOKIE_A" POST /api/v1/chat/sessions '{"app_id":"app-tool","session_id":"session-tool-reject"}')" create-tool-reject-session
expect 202 "$(request a "$COOKIE_A" POST /api/v1/chat/sessions/session-tool-reject/messages '{"input":"reject tool"}' tool-reject)" tool-reject-pending
wait_request_event a "$COOKIE_A" session-tool-reject tool-reject tool.confirmation.pending
request a "$COOKIE_A" GET /api/v1/admin/governance/confirmations >/dev/null
CONFIRM_REJECT="$(jq -r '.items[] | select(.request_id == "tool-reject") | .id' "$RESPONSE")"
expect 200 "$(request a "$COOKIE_A" POST "/api/v1/admin/governance/confirmations/$CONFIRM_REJECT/decision" '{"approve":false}')" reject-tool
expect 200 "$(request a "$COOKIE_A" POST "/api/v1/admin/governance/confirmations/$CONFIRM_REJECT/decision" '{"approve":false}')" duplicate-reject-tool
wait_request_terminal a "$COOKIE_A" session-tool-reject tool-reject run.failed

# Governance outage fails closed before the Tool side effect.
expect 201 "$(request b "$COOKIE_B" POST /api/v1/chat/sessions '{"app_id":"app-tool","session_id":"session-tool-outage"}')" create-tool-outage-session
docker compose -f "$COMPOSE_FILE" stop gateway-a >/dev/null
expect 202 "$(request b "$COOKIE_B" POST /api/v1/chat/sessions/session-tool-outage/messages '{"input":"governance outage"}' tool-governance-outage)" governance-outage-admitted
wait_request_terminal b "$COOKIE_B" session-tool-outage tool-governance-outage run.failed
request b "$COOKIE_B" GET /api/v1/admin/sessions/session-tool-outage/events >/dev/null
jq -e '.items[] | select(.idempotency_key == "tool-governance-outage:terminal" and ((.payload | @base64d | fromjson).error == "run failed"))' "$RESPONSE" >/dev/null
docker compose -f "$COMPOSE_FILE" up -d gateway-a >/dev/null
wait_gateway a "$COOKIE_A"

# Kill the Worker only after the dangerous Tool is executing. The Gateway must
# surface outcome_unknown and must never replay the side effect.
expect 201 "$(request a "$COOKIE_A" POST /api/v1/admin/agent-apps '{"id":"app-tool-loss","name":"Tool Loss"}')" create-tool-loss-app
expect 201 "$(request a "$COOKIE_A" POST /api/v1/admin/deployments '{"id":"deploy-tool-loss","agent_app_id":"app-tool-loss"}')" create-tool-loss-deployment
expect 201 "$(request a "$COOKIE_A" POST /api/v1/admin/deployments/deploy-tool-loss/versions '{"config":{"runner":"tool-loss","tools":["deploy"],"deterministic_tool_call":"deploy","deterministic_tool_delay_ms":20000}}' tool-loss-version)" create-tool-loss-version
expect 200 "$(request a "$COOKIE_A" POST /api/v1/admin/deployments/deploy-tool-loss/transition '{"status":"published","version_id":"deploy-tool-loss-v1"}')" publish-tool-loss
expect 200 "$(request a "$COOKIE_A" POST /api/v1/admin/deployments/deploy-tool-loss/transition '{"status":"active"}')" activate-tool-loss
expect 200 "$(request a "$COOKIE_A" POST /api/v1/admin/governance/policy '{"agent_app_id":"app-tool-loss","allowed_tools":["deploy"],"dangerous_tools":["deploy"],"token_budget":100000,"rate_limit":100,"rate_window_seconds":60}')" policy-tool-loss
expect 201 "$(request a "$COOKIE_A" POST /api/v1/chat/sessions '{"app_id":"app-tool-loss","session_id":"session-tool-loss"}')" create-tool-loss-session
expect 202 "$(request a "$COOKIE_A" POST /api/v1/chat/sessions/session-tool-loss/messages '{"input":"worker loss"}' tool-worker-loss)" tool-loss-pending
wait_request_event a "$COOKIE_A" session-tool-loss tool-worker-loss tool.confirmation.pending
request a "$COOKIE_A" GET /api/v1/admin/governance/confirmations >/dev/null
CONFIRM_LOSS="$(jq -r '.items[] | select(.request_id == "tool-worker-loss") | .id' "$RESPONSE")"
expect 200 "$(request a "$COOKIE_A" POST "/api/v1/admin/governance/confirmations/$CONFIRM_LOSS/decision" '{"approve":true}')" approve-tool-loss
expect 202 "$(request a "$COOKIE_A" POST /api/v1/chat/sessions/session-tool-loss/messages '{"input":"worker loss"}' tool-worker-loss)" resume-tool-loss
for _ in $(seq 1 30); do
  request a "$COOKIE_A" GET /api/v1/admin/governance/confirmations >/dev/null
  jq -e '.items[] | select(.request_id == "tool-worker-loss" and .status == "executing")' "$RESPONSE" >/dev/null 2>&1 && break
  sleep 1
done
jq -e '.items[] | select(.request_id == "tool-worker-loss" and .status == "executing")' "$RESPONSE" >/dev/null
docker compose -f "$COMPOSE_FILE" kill -s SIGKILL worker >/dev/null
wait_request_terminal a "$COOKIE_A" session-tool-loss tool-worker-loss run.failed
for _ in $(seq 1 30); do
  request a "$COOKIE_A" GET /api/v1/admin/governance/confirmations >/dev/null
  jq -e '.items[] | select(.request_id == "tool-worker-loss" and .status == "outcome_unknown")' "$RESPONSE" >/dev/null 2>&1 && break
  sleep 1
done
jq -e '.items[] | select(.request_id == "tool-worker-loss" and .status == "outcome_unknown")' "$RESPONSE" >/dev/null
docker compose -f "$COMPOSE_FILE" up -d worker >/dev/null
for _ in $(seq 1 60); do
  if docker compose -f "$COMPOSE_FILE" ps --format json worker | jq -e '.Health == "healthy"' >/dev/null; then break; fi
  sleep 1
done
expect 200 "$(request a "$COOKIE_A" POST /api/v1/chat/sessions/session-tool-loss/messages '{"input":"worker loss"}' tool-worker-loss)" no-replay-tool-loss
request a "$COOKIE_A" GET /api/v1/admin/governance/confirmations >/dev/null
jq -e '.items[] | select(.request_id == "tool-worker-loss" and .status == "outcome_unknown")' "$RESPONSE" >/dev/null

# Request-correlated runtime timeout and Tool failure.
expect 200 "$(request a "$COOKIE_A" POST /api/v1/admin/governance/policy '{"agent_app_id":"app-stage7","token_budget":100000,"rate_limit":100,"rate_window_seconds":60,"runtime_timeout_ms":100}')" timeout-policy
expect 200 "$(request a "$COOKIE_A" POST /api/v1/admin/operations/faults '{"agent_app_id":"app-stage7","scenario":"runner_delay","delay_ms":1000}')" configure-timeout
expect 202 "$(request a "$COOKIE_A" POST /api/v1/chat/sessions/session-stage7/messages '{"input":"model timeout"}' request-timeout)" timeout-admitted
wait_request_terminal a "$COOKIE_A" session-stage7 request-timeout run.cancelled
expect 200 "$(request a "$COOKIE_A" POST /api/v1/admin/operations/faults '{"agent_app_id":"app-stage7","scenario":"tool_error","delay_ms":0}')" configure-tool-failure
expect 202 "$(request a "$COOKIE_A" POST /api/v1/chat/sessions/session-stage7/messages '{"input":"tool failure"}' request-tool-failure)" tool-failure-admitted
wait_request_terminal a "$COOKIE_A" session-stage7 request-tool-failure run.failed
expect 200 "$(request a "$COOKIE_A" POST /api/v1/admin/operations/faults '{"agent_app_id":"app-stage7","scenario":"none","delay_ms":0}')" clear-runtime-fault
expect 200 "$(request a "$COOKIE_A" POST /api/v1/admin/governance/policy '{"agent_app_id":"app-stage7","token_budget":100000,"rate_limit":100,"rate_window_seconds":60,"runtime_timeout_ms":5000}')" restore-policy

# PostgreSQL outage returns the stable public code, then the same topology recovers.
docker compose -f "$COMPOSE_FILE" stop postgres >/dev/null
expect 503 "$(request b "$COOKIE_B" POST /api/v1/admin/run '{"app_id":"app-stage7","session_id":"session-stage7","input":"database outage"}' postgres-outage)" postgres-outage
jq -e '.error.code == "control_plane_unavailable"' "$RESPONSE" >/dev/null
docker compose -f "$COMPOSE_FILE" up -d postgres >/dev/null
for _ in $(seq 1 60); do
  if docker compose -f "$COMPOSE_FILE" ps --format json postgres | jq -e '.Health == "healthy"' >/dev/null; then break; fi
  sleep 1
done
expect 202 "$(request b "$COOKIE_B" POST /api/v1/chat/sessions/session-stage7/messages '{"input":"database recovered"}' postgres-recovered)" postgres-recovered
wait_request_terminal b "$COOKIE_B" session-stage7 postgres-recovered run.completed

# IM retry plus duplicate delivery must persist one input only.
expect 201 "$(request a "$COOKIE_A" POST /api/v1/chat/bindings '{"channel":"mock","app_id":"app-stage7","conversation_type":"single","external_conversation_id":"stage7-user","external_user_id":"stage7-user","session_id":"session-im"}')" create-im-binding
BINDING_ID="$(jq -r '.id' "$RESPONSE")"
BINDING_SECRET="$(jq -r '.secret' "$RESPONSE")"
CALLBACK_BODY='{"binding_id":"'"$BINDING_ID"'","message_id":"stage7-message","sequence":1,"text":"im retry"}'
SIGNATURE="$(printf '%s' "$CALLBACK_BODY" | openssl dgst -sha256 -hmac "$BINDING_SECRET" -r | cut -d' ' -f1)"
expect 200 "$(request a "$COOKIE_A" POST /api/v1/chat/mock/faults '{"scenario":"retry","session_id":"session-im"}')" configure-im-retry
# The mock signature is sent separately because request() reserves its optional
# header for request identity.
curl --silent --show-error --fail --max-time 15 -b "$COOKIE_A" -H 'X-Gateway: a' -H 'Content-Type: application/json' -H "X-Mock-Signature: $SIGNATURE" -d "$CALLBACK_BODY" "$BASE_URL/api/v1/chat/channels/mock/callback" >"$RESPONSE"
wait_request_event a "$COOKIE_A" session-im channel-stage7-message channel.delivery
expect 200 "$(request b "$COOKIE_B" GET /api/v1/admin/memory/session-im)" read-im-memory-from-b
jq -e '.items[] | select(.key == "latest_agent_reply" and .value == "framework:im retry" and .fencing_token > 0)' "$RESPONSE" >/dev/null
expect 200 "$(request a "$COOKIE_A" POST /api/v1/chat/mock/faults '{"scenario":"none","session_id":"session-im"}')" clear-im-retry
curl --silent --show-error --fail --max-time 15 -b "$COOKIE_A" -H 'X-Gateway: a' -H 'Content-Type: application/json' -H "X-Mock-Signature: $SIGNATURE" -d "$CALLBACK_BODY" "$BASE_URL/api/v1/chat/channels/mock/callback" >"$RESPONSE"
request a "$COOKIE_A" GET /api/v1/admin/sessions/session-im/events >/dev/null
jq -e '[.items[] | select(.idempotency_key == "channel-stage7-message:input")] | length == 1' "$RESPONSE" >/dev/null

docker compose -f "$COMPOSE_FILE" restart gateway-a >/dev/null
wait_gateway a "$COOKIE_A"
expect 200 "$(request a "$COOKIE_A" GET /api/v1/admin/agent-apps)" read-after-restart
jq -e '.items[] | select(.id == "app-stage7")' "$RESPONSE" >/dev/null

docker compose -f "$COMPOSE_FILE" stop worker >/dev/null
expect 202 "$(request a "$COOKIE_A" POST /api/v1/chat/sessions/session-stage7/messages '{"input":"worker outage"}' request-worker-outage)" worker-outage-admitted
wait_request_terminal a "$COOKIE_A" session-stage7 request-worker-outage run.failed
jq -e '.items[] | select(.idempotency_key == "request-worker-outage:terminal" and ((.payload | @base64d | fromjson).error == "worker_unavailable"))' "$RESPONSE" >/dev/null
docker compose -f "$COMPOSE_FILE" up -d worker >/dev/null
for _ in $(seq 1 60); do
  if docker compose -f "$COMPOSE_FILE" ps --format json worker | jq -e '.Health == "healthy"' >/dev/null; then break; fi
  sleep 1
done
expect 202 "$(request a "$COOKIE_A" POST /api/v1/chat/sessions/session-stage7/messages '{"input":"worker recovered"}' request-worker-recovered)" worker-recovered
wait_request_terminal a "$COOKIE_A" session-stage7 request-worker-recovered run.completed

echo "Stage 7 two-Gateway Compose acceptance passed"
