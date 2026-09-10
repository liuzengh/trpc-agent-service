#!/usr/bin/env bash
# Verifies that composition nodes fail readiness during a brief PostgreSQL or
# Redis outage, then recover without restarting either application node.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
compose_file="${repo_root}/deploy/compose/docker-compose.local.yml"
secret_file="${repo_root}/deploy/compose/secrets/deepseek-api-key"
project="trpc-local-recovery-${RANDOM}${RANDOM}"
port_base="$((20000 + RANDOM))"
port_postgres="${port_base}"
port_redis="$((port_base + 1))"
port_jaeger="$((port_base + 2))"
port_otel="$((port_base + 3))"
port_metrics="$((port_base + 4))"
port_a="$((port_base + 5))"
port_b="$((port_base + 6))"
diagnostics="$(mktemp -d "${TMPDIR:-/tmp}/trpc-local-recovery.XXXXXX")"

command -v docker >/dev/null 2>&1 || { echo "Docker Desktop is required" >&2; exit 2; }
command -v curl >/dev/null 2>&1 || { echo "curl is required" >&2; exit 2; }
docker compose version >/dev/null 2>&1 || { echo "Docker Compose v2 is required" >&2; exit 2; }
test -s "${secret_file}" || { echo "DeepSeek key file is required at ${secret_file}" >&2; exit 2; }

compose() {
  TRPC_LOCAL_POSTGRES_PORT="${port_postgres}" TRPC_LOCAL_REDIS_PORT="${port_redis}" \
    TRPC_LOCAL_JAEGER_PORT="${port_jaeger}" TRPC_LOCAL_OTEL_HTTP_PORT="${port_otel}" TRPC_LOCAL_OTEL_PROMETHEUS_PORT="${port_metrics}" \
    TRPC_LOCAL_MULTINODE_NODE_A_PORT="${port_a}" TRPC_LOCAL_MULTINODE_NODE_B_PORT="${port_b}" \
    docker compose --project-name "${project}" -f "${compose_file}" --profile webui-multinode "$@"
}

cleanup() {
  local status=$?
  if [[ ${status} -ne 0 ]]; then
    compose logs --no-color >"${diagnostics}/compose.log" || true
    echo "local dependency recovery smoke failed; diagnostics retained at ${diagnostics}/compose.log" >&2
  fi
  compose down --volumes --remove-orphans >/dev/null 2>&1 || true
  [[ ${status} -ne 0 ]] || rmdir "${diagnostics}" || true
  exit "${status}"
}
trap cleanup EXIT

wait_ready() {
  local port="$1"
  for _ in $(seq 1 90); do
    if curl --fail --silent --max-time 2 "http://127.0.0.1:${port}/readyz" >/dev/null; then
      return 0
    fi
    sleep 1
  done
  return 1
}

wait_unready() {
  local port="$1"
  for _ in $(seq 1 45); do
    if ! curl --fail --silent --max-time 2 "http://127.0.0.1:${port}/readyz" >/dev/null; then
      return 0
    fi
    sleep 1
  done
  return 1
}

assert_outage_and_recovery() {
  local dependency="$1"
  compose stop "${dependency}"
  wait_unready "${port_a}"
  wait_unready "${port_b}"
  compose start "${dependency}"
  wait_ready "${port_a}"
  wait_ready "${port_b}"
}

compose up --detach --build
wait_ready "${port_a}"
wait_ready "${port_b}"
assert_outage_and_recovery postgres
assert_outage_and_recovery redis
echo "local dependency recovery smoke passed: PostgreSQL/Redis outage made both nodes unready and both recovered"
