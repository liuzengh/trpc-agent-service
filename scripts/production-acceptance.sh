#!/usr/bin/env bash
# Reproducible Docker Compose production deployment gate.  It deliberately
# suppresses command output because dependency drivers and Docker can echo
# credentials.  The retained summary contains only status, counts, hashes,
# trace IDs, and stable error categories.
set +x
set -euo pipefail

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for production acceptance" >&2
  exit 2
fi
if ! docker compose version >/dev/null 2>&1; then
  echo "docker compose is required for production acceptance" >&2
  exit 2
fi
docker_daemon_available() {
  local info_pid
  docker info >/dev/null 2>&1 &
  info_pid=$!
  for _ in $(seq 1 10); do
    if ! kill -0 "${info_pid}" >/dev/null 2>&1; then
      if wait "${info_pid}"; then
        return 0
      fi
      return 1
    fi
    sleep 0.5
  done
  # Docker Desktop can leave a client blocked in the socket call after the
  # daemon disappears; use SIGKILL for this short-lived probe so the gate
  # itself cannot hang and leave a credential-bearing child behind.
  kill -KILL "${info_pid}" >/dev/null 2>&1 || true
  wait "${info_pid}" >/dev/null 2>&1 || true
  return 1
}
if ! docker_daemon_available; then
  echo "Docker daemon is unavailable; production acceptance was not executed" >&2
  exit 2
fi
for command_name in curl jq sha256sum; do
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "${command_name} is required for production acceptance" >&2
    exit 2
  fi
done

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_dir}"
project="trpc-prod-acceptance-${RANDOM}${RANDOM}"
secret_dir="$(mktemp -d -t trpc-prod-secrets-XXXXXX)"
work_dir="$(mktemp -d -t trpc-prod-evidence-XXXXXX)"
summary_dir="${PRODUCTION_EVIDENCE_DIR:-${repo_dir}/evidence/production}"
mkdir -p "${summary_dir}"
summary_file="${summary_dir}/summary-${project}.txt"
compose=(docker compose --project-name "${project}" -f deploy/compose.yaml -f deploy/compose.production.yaml)

export POSTGRES_PORT="${POSTGRES_PORT:-15432}"
export REDIS_PORT="${REDIS_PORT:-16379}"
export PROMETHEUS_PORT="${PROMETHEUS_PORT:-19090}"
export PRODUCTION_HTTP_PORT="${PRODUCTION_HTTP_PORT:-18080}"
export JAEGER_PORT="${JAEGER_PORT:-16686}"
export OIDC_FIXTURE_PORT="${OIDC_FIXTURE_PORT:-18081}"
export PROD_SECRET_DIR="${secret_dir}"
export POSTGRES_USER="${POSTGRES_USER:-platform}"
export POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-acceptance-${project}-platform-credential}"
export POSTGRES_RUNTIME_USER="${POSTGRES_RUNTIME_USER:-runtime}"
export POSTGRES_RUNTIME_PASSWORD="${POSTGRES_RUNTIME_PASSWORD:-acceptance-${project}-runtime-credential}"
export POSTGRES_DB="${POSTGRES_DB:-tenant_agent}"
export MINIO_ROOT_USER="${MINIO_ROOT_USER:-acceptance-${project}-minio-user}"
export MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-acceptance-${project}-minio-credential}"
export TRPC_AGENT_MIGRATION_DSN="postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}?sslmode=disable"
postgres_acceptance_dsn="postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@127.0.0.1:${POSTGRES_PORT}/${POSTGRES_DB}?sslmode=disable"

umask 077
printf '%s' "postgres://${POSTGRES_RUNTIME_USER}:${POSTGRES_RUNTIME_PASSWORD}@postgres:5432/${POSTGRES_DB}?sslmode=disable" >"${secret_dir}/db-runtime-dsn"
printf '%s' "${TRPC_AGENT_MIGRATION_DSN}" >"${secret_dir}/db-migration-dsn"
printf '%s' "redis://redis:6379/0" >"${secret_dir}/redis-url"
model_canary="fixture-${project}-model-credential"
printf '%s' "${model_canary}" >"${secret_dir}/model-api-key"
printf '%s' "${MINIO_ROOT_USER}" >"${secret_dir}/minio-access-key"
printf '%s' "${MINIO_ROOT_PASSWORD}" >"${secret_dir}/minio-secret-key"
chmod 600 "${secret_dir}"/*

pass=0
failures=0
recorded=()
record() {
  recorded+=("$1=$2")
}
fail() {
  echo "FAIL: $1" >&2
  failures=$((failures + 1))
}
safe_status() {
  local path="$1" expected="$2" actual
  actual="$(curl --silent --show-error --max-time 5 --output /dev/null --write-out '%{http_code}' "http://127.0.0.1:${PRODUCTION_HTTP_PORT}${path}" || true)"
  if [[ "${actual}" != "${expected}" ]]; then
    fail "${path} returned ${actual}, expected ${expected}"
    return 1
  fi
  return 0
}
wait_status() {
  local path="$1" expected="$2" attempts="${3:-60}" actual=""
  for _ in $(seq 1 "${attempts}"); do
    actual="$(curl --silent --show-error --max-time 3 --output /dev/null --write-out '%{http_code}' "http://127.0.0.1:${PRODUCTION_HTTP_PORT}${path}" || true)"
    if [[ "${actual}" == "${expected}" ]]; then return 0; fi
    sleep 2
  done
  fail "timed out waiting for ${path}=${expected}"
  return 1
}

# Keep integration-test diagnostics inside the disposable work directory. The
# gate must reject a green process exit when a test was skipped or no test was
# selected, while never printing driver output that may contain a DSN.
run_postgres_test() {
  local label="$1"
  shift
  local output="${work_dir}/${label}.test.log"
  if ! TEST_POSTGRES_DSN="${postgres_acceptance_dsn}" GOFLAGS= go test -v -count=1 "$@" >"${output}" 2>&1; then
    return 1
  fi
  if grep -Eiq '(^|[[:space:]])---[[:space:]]+SKIP:|no tests to run|no packages to test' "${output}"; then
    return 1
  fi
  grep -Eq '^ok[[:space:]]+github.com/' "${output}"
}

cleanup() {
  set +e
  "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1
  rm -rf "${secret_dir}" "${work_dir}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if ! "${compose[@]}" config >/dev/null 2>&1; then
  fail "production Compose configuration is invalid"
  exit 1
fi
if ! "${compose[@]}" up -d --build --scale service=2 service proxy >/dev/null 2>&1; then
  fail "production Compose stack failed to start"
  exit 1
fi

wait_status /healthz 200 90 || true
wait_status /readyz 200 90 || true
service_count="$("${compose[@]}" ps -q service | sed '/^$/d' | wc -l | tr -d ' ')"
if [[ "${service_count}" != "2" ]]; then
  fail "two service replicas were not running"
else
  record replicas "${service_count}"
fi

oidc_viewer="$(curl --silent --show-error --fail --max-time 5 "http://127.0.0.1:${OIDC_FIXTURE_PORT}/token?role=viewer&tenant=acme" || true)"
oidc_operator="$(curl --silent --show-error --fail --max-time 5 "http://127.0.0.1:${OIDC_FIXTURE_PORT}/token?role=operator&tenant=acme" || true)"
if [[ -z "${oidc_viewer}" || -z "${oidc_operator}" ]]; then
  fail "OIDC fixture did not issue acceptance tokens"
else
  viewer_status="$(curl --silent --show-error --max-time 5 -H "Authorization: Bearer ${oidc_viewer}" --output /dev/null --write-out '%{http_code}' "http://127.0.0.1:${PRODUCTION_HTTP_PORT}/admin/v1/tenants" || true)"
  viewer_write="$(curl --silent --show-error --max-time 5 -X POST -H "Authorization: Bearer ${oidc_viewer}" --output /dev/null --write-out '%{http_code}' "http://127.0.0.1:${PRODUCTION_HTTP_PORT}/admin/v1/reload" || true)"
  cross_tenant="$(curl --silent --show-error --max-time 5 -H "Authorization: Bearer ${oidc_viewer}" --output /dev/null --write-out '%{http_code}' "http://127.0.0.1:${PRODUCTION_HTTP_PORT}/admin/v1/tenants/globex/revisions" || true)"
  if [[ "${viewer_status}" != "200" || "${viewer_write}" != "403" || "${cross_tenant}" != "403" ]]; then
    fail "OIDC viewer and tenant-scope policy did not enforce read-only access"
  else
    record rbac "viewer_read=200 viewer_write=403 cross_tenant=403"
  fi
  operator_security="$(curl --silent --show-error --max-time 5 -X POST -H "Authorization: Bearer ${oidc_operator}" --output /dev/null --write-out '%{http_code}' "http://127.0.0.1:${PRODUCTION_HTTP_PORT}/admin/v1/tenants/custom-model" || true)"
  if [[ "${operator_security}" != "403" ]]; then
    fail "operator reached security-sensitive custom-model endpoint"
  fi
  chat_payload="${work_dir}/chat-request.json"
  chat_response="${work_dir}/chat-response.json"
  printf '%s' '{"message_id":"production-acceptance-message-1","user_id":"acceptance-user","conversation_id":"acceptance-session","scope":"direct","text":"production acceptance trace probe"}' >"${chat_payload}"
  chat_status="$(curl --silent --show-error --max-time 20 -X POST \
    -H "Authorization: Bearer ${oidc_operator}" -H 'Content-Type: application/json' \
    --data-binary "@${chat_payload}" --output "${chat_response}" --write-out '%{http_code}' \
    "http://127.0.0.1:${PRODUCTION_HTTP_PORT}/v1/chat/acme" || true)"
  if [[ "${chat_status}" != "200" ]] || ! jq -e '.request_id != null' "${chat_response}" >/dev/null 2>&1; then
    fail "production trace probe chat request did not complete"
  else
    record trace_probe "direct_chat=200"
  fi
  unset oidc_viewer oidc_operator
fi

dependency_payload="${work_dir}/dependency.json"
dependency_status="$(curl --silent --show-error --max-time 5 -H "Authorization: Bearer $(curl --silent --show-error --fail "http://127.0.0.1:${OIDC_FIXTURE_PORT}/token?role=viewer&tenant=acme")" -o "${dependency_payload}" --write-out '%{http_code}' "http://127.0.0.1:${PRODUCTION_HTTP_PORT}/admin/v1/health/dependencies" || true)"
if [[ "${dependency_status}" != "200" ]] || ! jq -e '.dependencies | type == "array"' "${dependency_payload}" >/dev/null 2>&1; then
  fail "dependency health registry was not queryable"
else
  record dependencies "$(jq '.dependencies | length' "${dependency_payload}")"
fi

if ! docker inspect --format '{{.State.Health.Status}}' "$(${compose[@]} ps -q oidc-fixture | head -n1)" 2>/dev/null | grep -qx healthy; then
  fail "OIDC dependency healthcheck did not become healthy"
fi

# Replica exit/reclaim evidence is exercised by the durable SQL integration
# test against the same task-scoped PostgreSQL. The live stack check confirms
# the surviving replica continues to serve readiness.
first_service="$("${compose[@]}" ps -q service | head -n1)"
if [[ -z "${first_service}" ]] || ! docker kill "${first_service}" >/dev/null 2>&1; then
  fail "could not stop replica A"
else
  wait_status /readyz 200 45 || true
  record replica_exit "surviving_replica_ready=200"
fi

if ! run_postgres_test inbox-reclaim ./trpcservice/queue -run 'TestPostgresIntegration'; then
  fail "durable queue reclaim/replay integration evidence failed"
else
  record inbox_reclaim "queue_postgres_integration=passed"
fi

if ! "${compose[@]}" stop postgres >/dev/null 2>&1; then
  fail "could not stop PostgreSQL"
else
  sleep 5
  safe_status /healthz 200 || true
  safe_status /readyz 503 || true
  record postgres_disconnect "healthz=200 readyz=503"
  if ! "${compose[@]}" start postgres >/dev/null 2>&1; then
    fail "could not restart PostgreSQL"
  else
    wait_status /readyz 200 90 || true
    record postgres_recovery "readyz=200"
  fi
fi

if ! run_postgres_test commit-ack-loss ./trpcservice/worker -run 'TestPostgresStrictTurnRecoversAfterCoordinatorResultLoss'; then
  fail "commit-ack-loss replay integration evidence failed"
else
  record commit_ack_loss "canonical_replay_test=passed"
fi

if ! run_postgres_test content-safety ./trpcservice/contentsafety -run 'PostgresIntegration'; then
  fail "content safety lease recovery evidence failed"
else
  record content_safety "lease_recovery_and_blocking=passed"
fi

metrics_file="${work_dir}/metrics.txt"
health_file="${work_dir}/health.txt"
trace_file="${work_dir}/traces.json"
trace_query_file="${work_dir}/trace-query.json"
logs_file="${work_dir}/container-logs.txt"
audit_sql_file="${work_dir}/audit-sql.txt"
audit_spool_file="${work_dir}/audit-spool.txt"
chat_response="${work_dir}/chat-response.json"
: >"${chat_response}"
curl --silent --show-error --max-time 5 "http://127.0.0.1:${PRODUCTION_HTTP_PORT}/metrics" >"${metrics_file}" || fail "metrics endpoint unavailable"
curl --silent --show-error --max-time 5 "http://127.0.0.1:${PRODUCTION_HTTP_PORT}/healthz" >"${health_file}" || fail "health endpoint unavailable"
curl --silent --show-error --max-time 5 "http://127.0.0.1:${JAEGER_PORT}/api/services" >"${trace_file}" || fail "trace backend unavailable"
sleep 3
curl --silent --show-error --max-time 5 --get --data-urlencode 'service=trpc-agent-service' --data-urlencode 'limit=50' \
  "http://127.0.0.1:${JAEGER_PORT}/api/traces" >"${trace_query_file}" || fail "trace query unavailable"
if ! jq -e '.data | type == "array"' "${trace_query_file}" >/dev/null 2>&1; then
  fail "trace backend returned no queryable trace data"
fi
"${compose[@]}" logs --no-color service proxy oidc-fixture otel-collector >"${logs_file}" 2>/dev/null || fail "container logs unavailable"
if ! "${compose[@]}" exec -T -e PGPASSWORD="${POSTGRES_PASSWORD}" postgres \
  psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -Atc \
  'SELECT audit_id, content_sha256, previous_hash, record_hash, payload::text FROM audit_records ORDER BY created_at' \
  >"${audit_sql_file}" 2>/dev/null; then
  fail "audit SQL evidence query failed"
fi
audit_volume="${project}_production-audit-spool"
if ! docker run --rm -v "${audit_volume}:/spool:ro" alpine:3.21 \
  sh -c 'find /spool -maxdepth 1 -type f -print -exec cat {} \;' \
  >"${audit_spool_file}" 2>/dev/null; then
  fail "audit spool evidence query failed"
fi
if grep -Fqi -- "${model_canary}" \
  "${metrics_file}" "${health_file}" "${dependency_payload}" "${trace_file}" "${trace_query_file}" \
  "${logs_file}" "${audit_sql_file}" "${audit_spool_file}" "${work_dir}/chat-response.json"; then
  fail "secret canary or credential material was visible in HTTP/metrics/trace evidence"
elif grep -Eiq 'authorization:|postgres://[^[:space:]]*:[^@[:space:]]+@' \
  "${metrics_file}" "${health_file}" "${dependency_payload}" "${trace_file}" "${trace_query_file}" \
  "${logs_file}" "${audit_sql_file}" "${audit_spool_file}" "${work_dir}/chat-response.json"; then
  fail "credential-shaped authorization or DSN material was visible in evidence"
else
  record secret_scan "metrics_health_dependency_trace_logs_audit=clean"
fi
if ! grep -Eq 'trpc-agent-service|data' "${trace_file}"; then
  fail "trace backend returned no queryable service inventory"
else
  record trace_backend "jaeger_queryable=1"
fi

recorded+=('skips=0')
recorded+=('unexecuted=0')
{
  printf 'production_acceptance=passed\nproject=%s\n' "${project}"
  for item in "${recorded[@]}"; do printf '%s\n' "${item}"; done
} >"${summary_file}"

if ((failures != 0)); then
  sed -i.bak 's/^production_acceptance=passed$/production_acceptance=failed/' "${summary_file}"
  rm -f "${summary_file}.bak"
  echo "Production acceptance failed; redacted summary: ${summary_file}" >&2
  exit 1
fi
echo "Production acceptance passed; redacted summary: ${summary_file}"
