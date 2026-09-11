#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
solution_root="$(cd "${script_dir}/.." && pwd)"
smoke_port="${SMOKE_PORT:-}"
smoke_admin_token="smoke-placeholder-admin-token"
tmp_dir="$(mktemp -d -t trpc-service-smoke-XXXXXX)"
service_pid=""

cleanup() {
  if [[ -n "${service_pid}" ]] && kill -0 "${service_pid}" 2>/dev/null; then
    kill "${service_pid}" 2>/dev/null || true
    wait "${service_pid}" 2>/dev/null || true
  fi
  rm -rf "${tmp_dir}"
}
trap cleanup EXIT INT TERM

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "required command not found: $1" >&2
    exit 1
  fi
}

port_in_use() {
  local port="$1"
  (exec 3<>"/dev/tcp/127.0.0.1/${port}") >/dev/null 2>&1
}

choose_smoke_port() {
  local attempt candidate
  if [[ -n "${smoke_port}" ]]; then
    if [[ ! "${smoke_port}" =~ ^[0-9]+$ ]] || ((smoke_port < 1 || smoke_port > 65535)); then
      echo "SMOKE_PORT must be an integer between 1 and 65535" >&2
      exit 1
    fi
    if port_in_use "${smoke_port}"; then
      echo "SMOKE_PORT ${smoke_port} is already in use" >&2
      exit 1
    fi
    return
  fi
  for ((attempt = 1; attempt <= 40; attempt++)); do
    candidate=$((20000 + (RANDOM << 1 ^ RANDOM) % 30000))
    if ! port_in_use "${candidate}"; then
      smoke_port="${candidate}"
      return
    fi
  done
  echo "could not find an unused local port for the smoke service" >&2
  exit 1
}

wait_for_http() {
  local url="$1"
  local attempts="${2:-80}"
  local attempt
  for ((attempt = 1; attempt <= attempts; attempt++)); do
    if curl --silent --show-error --fail --max-time 2 "${url}" >/dev/null 2>&1; then
      return 0
    fi
    if [[ -n "${service_pid}" ]] && ! kill -0 "${service_pid}" 2>/dev/null; then
      echo "service exited before becoming ready" >&2
      sed -n '1,240p' "${tmp_dir}/service.log" >&2
      return 1
    fi
    sleep 0.25
  done
  echo "timed out waiting for ${url}" >&2
  sed -n '1,240p' "${tmp_dir}/service.log" >&2
  return 1
}

require_command go
require_command curl
require_command npm
choose_smoke_port
echo "==> using local smoke port ${smoke_port}"

echo "==> frontend build"
(
  cd "${solution_root}/im-console"
  if [[ ! -d node_modules ]]; then
    npm ci --no-audit --no-fund
  fi
  npm run build:embed
)

echo "==> unit tests"
(
  cd "${solution_root}"
  go test ./...
)

echo "==> static analysis"
(
  cd "${solution_root}"
  go vet ./...
  CGO_ENABLED=0 go build -trimpath -o "${tmp_dir}/trpc-service" ./cmd/trpc-service
  CGO_ENABLED=0 go build -trimpath -o "${tmp_dir}/trpc-migrate" ./cmd/trpc-migrate
  CGO_ENABLED=0 go build -trimpath -o "${tmp_dir}/trpc-data-migrate" ./cmd/trpc-data-migrate
  "${tmp_dir}/trpc-service" -h >/dev/null 2>&1
  "${tmp_dir}/trpc-migrate" -h >/dev/null 2>&1
  "${tmp_dir}/trpc-data-migrate" -h >/dev/null 2>&1
)

echo "==> deployment syntax"
if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  docker compose \
    -f "${solution_root}/deploy/compose.yaml" \
    config --quiet
else
  echo "docker compose not installed; compose rendering skipped"
fi

if command -v kubectl >/dev/null 2>&1; then
  kubectl kustomize "${solution_root}/deploy/k8s" >"${tmp_dir}/kubernetes.yaml"
  test -s "${tmp_dir}/kubernetes.yaml"
else
  echo "kubectl not installed; kustomize rendering skipped"
fi

if command -v promtool >/dev/null 2>&1; then
  promtool check config "${solution_root}/observability/prometheus.yaml"
else
  echo "promtool not installed; Prometheus semantic validation skipped"
fi

echo "==> local HTTP smoke test"
sed \
  "s/address: \":8080\"/address: \"127.0.0.1:${smoke_port}\"/" \
  "${solution_root}/config/example.yaml" \
  >"${tmp_dir}/config.with-primary.yaml"
# Keep smoke tests deterministic and offline: replace only the first tenant's
# model block and disable external long connections. Placeholder credentials
# must never cause the offline smoke test to dial a real IM platform.
awk '
  !done && $0 == "    model:" {
    in_model = 1
    print
    print "      provider: mock"
    print "      name: deterministic-mock"
    next
  }
  in_model && $0 == "    tools:" {
    in_model = 0
    done = 1
    print
    next
  }
  /^      - type:/ { in_aibot = ($0 == "      - type: wecom-aibot") }
  in_aibot && /^        enabled:/ { print "        enabled: false"; next }
  !in_model { print }
' "${tmp_dir}/config.with-primary.yaml" >"${tmp_dir}/config.yaml"

ADMIN_TOKEN="${smoke_admin_token}" \
  "${tmp_dir}/trpc-service" -config "${tmp_dir}/config.yaml" \
  >"${tmp_dir}/service.log" 2>&1 &
service_pid="$!"

base_url="http://127.0.0.1:${smoke_port}"
wait_for_http "${base_url}/readyz"

curl --silent --show-error --fail "${base_url}/healthz" \
  | grep -q '"status":"ok"'
curl --silent --show-error --fail "${base_url}/readyz" \
  | grep -q '"status":"ready"'
curl --silent --show-error --fail \
  -H "Authorization: Bearer ${smoke_admin_token}" \
  "${base_url}/admin/v1/tenants" \
  | grep -q '"tenant_id":"acme"'

request_body='{"message_id":"smoke-message-1","user_id":"smoke-user","conversation_id":"smoke-user","scope":"direct","text":"hello from smoke test"}'
first_response="$(curl --silent --show-error --fail \
  -H "Authorization: Bearer ${smoke_admin_token}" \
  -H 'Content-Type: application/json' \
  --data "${request_body}" \
  "${base_url}/v1/chat/acme")"
grep -q '"duplicate":false' <<<"${first_response}"
grep -q '"session_id":"' <<<"${first_response}"

duplicate_response="$(curl --silent --show-error --fail \
  -H "Authorization: Bearer ${smoke_admin_token}" \
  -H 'Content-Type: application/json' \
  --data "${request_body}" \
  "${base_url}/v1/chat/acme")"
grep -q '"duplicate":true' <<<"${duplicate_response}"

curl --silent --show-error --fail "${base_url}/metrics" \
  | grep -q '^agent_requests_total'

if grep -Fq "${smoke_admin_token}" "${tmp_dir}/service.log"; then
  echo "smoke placeholder secret leaked into application logs" >&2
  exit 1
fi

echo "smoke test passed"
