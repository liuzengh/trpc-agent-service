#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
ADMIN_USER="${ADMIN_USER:-admin}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-admin-dev-password}"
DRY_RUN=false

if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=true
fi

steps=(
  "wait for /readyz"
  "upsert tenant-a and tenant-b"
  "create app-a(redis+pgvector) and app-b(mysql+mem0)"
  "bind two isolated WebUI routes"
  "run one streamed conversation per tenant"
  "migrate app-a Session redis->mysql through six phases"
  "verify post-migration conversation and tenant metrics"
)

if $DRY_RUN; then
  printf 'T17 demo plan (%s)\n' "$BASE_URL"
  printf ' - %s\n' "${steps[@]}"
  exit 0
fi

for dependency in curl jq; do
  if ! command -v "$dependency" >/dev/null 2>&1; then
    echo "missing dependency: $dependency" >&2
    exit 1
  fi
done

api() {
  local method="$1"
  local path="$2"
  local body="${3:-}"
  if [[ -n "$body" ]]; then
    curl --fail --silent --show-error \
      --user "$ADMIN_USER:$ADMIN_PASSWORD" \
      --request "$method" \
      --header "Content-Type: application/json" \
      --data "$body" \
      "$BASE_URL$path"
  else
    curl --fail --silent --show-error \
      --user "$ADMIN_USER:$ADMIN_PASSWORD" \
      --request "$method" \
      "$BASE_URL$path"
  fi
}

echo "[1/7] waiting for platform readiness"
for _ in $(seq 1 60); do
  if curl --fail --silent "$BASE_URL/readyz" >/dev/null; then
    break
  fi
  sleep 2
done
curl --fail --silent "$BASE_URL/readyz" >/dev/null

echo "[2/7] creating two tenants"
api POST /api/v1/tenants '{
  "tenant_id":"tenant-a","name":"Tenant A","is_active":true,
  "quota":{"daily_token_limit":100000,"rate_per_minute":60},
  "policy":{"redact_patterns":[],"audit_level":"full","budget_cny":100}
}' >/dev/null
api POST /api/v1/tenants '{
  "tenant_id":"tenant-b","name":"Tenant B","is_active":true,
  "quota":{"daily_token_limit":100000,"rate_per_minute":60},
  "policy":{"redact_patterns":[],"audit_level":"full","budget_cny":100}
}' >/dev/null

echo "[3/7] creating apps with different backends"
api POST /api/v1/tenants/tenant-a/apps '{
  "app_id":"app-a","tenant_id":"tenant-a","app_name":"tenant-a-support",
  "model":{"provider":"openai-compatible","model":"deepseek-v4-flash","api_key_ref":"env:MODEL_API_KEY_TENANT_A","base_url":"https://api.deepseek.com","timeout":30000000000},
  "tools":[],"backends":{"session":"redis","memory":"pgvector"}
}' >/dev/null
api POST /api/v1/tenants/tenant-b/apps '{
  "app_id":"app-b","tenant_id":"tenant-b","app_name":"tenant-b-support",
  "model":{"provider":"openai-compatible","model":"deepseek-v4-flash","api_key_ref":"env:MODEL_API_KEY_TENANT_B","base_url":"https://api.deepseek.com","timeout":30000000000},
  "tools":[],"backends":{"session":"mysql","memory":"mem0"}
}' >/dev/null

echo "[4/7] binding isolated WebUI routes"
api POST /api/v1/apps/app-a/bindings '{
  "binding_id":"binding-a","tenant_id":"tenant-a","app_id":"app-a",
  "channel":"webui","route_key":"binding-a","config":{},"is_active":true
}' >/dev/null
api POST /api/v1/apps/app-b/bindings '{
  "binding_id":"binding-b","tenant_id":"tenant-b","app_id":"app-b",
  "channel":"webui","route_key":"binding-b","config":{},"is_active":true
}' >/dev/null

chat() {
  local binding="$1"
  local text="$2"
  local cookie_file
  cookie_file="$(mktemp)"
  local message_id response session_id
  message_id="demo-$(date +%s%N)-$binding"
  response="$(curl --fail --silent --show-error \
    --cookie-jar "$cookie_file" \
    --header "Content-Type: application/json" \
    --data "$(jq -nc --arg id "$message_id" --arg text "$text" '{id:$id,text:$text}')" \
    "$BASE_URL/channels/webui/$binding/messages")"
  session_id="$(jq -er '.session_id' <<<"$response")"
  curl --fail --silent --show-error --no-buffer \
    --cookie "$cookie_file" \
    "$BASE_URL/channels/webui/$binding/stream?session=$(jq -rn --arg value "$session_id" '$value|@uri')" \
    | tee "/tmp/${binding}-events.log"
  rm -f "$cookie_file"
  grep -q "event: done" "/tmp/${binding}-events.log"
}

echo "[5/7] running isolated conversations"
chat binding-a "Reply with TENANT_A only."
chat binding-b "Reply with TENANT_B only."
grep -q "TENANT_A" /tmp/binding-a-events.log
grep -q "TENANT_B" /tmp/binding-b-events.log
if grep -q "TENANT_B" /tmp/binding-a-events.log || grep -q "TENANT_A" /tmp/binding-b-events.log; then
  echo "tenant isolation assertion failed" >&2
  exit 1
fi

echo "[6/7] migrating tenant-a Session from Redis to MySQL"
migration_response="$(
  curl --silent --show-error --write-out '\n%{http_code}' \
    --user "$ADMIN_USER:$ADMIN_PASSWORD" \
    --request POST \
    --header "Content-Type: application/json" \
    --data '{"from":"redis","to":"mysql"}' \
    "$BASE_URL/api/v1/apps/app-a/migrations"
)"
migration_code="${migration_response##*$'\n'}"
migration_body="${migration_response%$'\n'*}"
if [[ "$migration_code" == "409" ]] || \
   grep -Eq 'effective session backend|migration conflict|already has migration' <<<"$migration_body"; then
  echo "  skipped: app-a Session is already on MySQL from a previous demo"
else
  if [[ "$migration_code" != "200" ]]; then
    echo "start migration failed ($migration_code): $migration_body" >&2
    exit 1
  fi
  migration_id="$(jq -er '.migration_id' <<<"$migration_body")"
  for expected in dual_write backfill verify cut_read stop_old_write done; do
    phase="$(
      api POST "/api/v1/migrations/$migration_id/advance" \
        | jq -er '.phase'
    )"
    if [[ "$phase" != "$expected" ]]; then
      echo "migration phase $phase, expected $expected" >&2
      exit 1
    fi
  done
fi

echo "[7/7] verifying post-migration path and metrics"
chat binding-a "Reply with MIGRATION_OK only."
grep -q "MIGRATION_OK" /tmp/binding-a-events.log
metrics="$(curl --fail --silent "$BASE_URL/metrics")"
grep -q 'tenant="tenant-a"' <<<"$metrics"
grep -q 'tenant="tenant-b"' <<<"$metrics"

echo "T17 demo completed successfully"
