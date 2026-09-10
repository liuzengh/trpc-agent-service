#!/usr/bin/env bash
set -euo pipefail
suite="${1:-all}"
case "${suite}" in
  all|migration|migration-coverage|runtime|storage|artifact) ;;
  *) echo "usage: $0 [all|migration|migration-coverage|runtime|storage|artifact]" >&2; exit 2 ;;
esac
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
compose_file="${repo_root}/deploy/compose/docker-compose.backend-smoke.yml"
project="trpc-${suite}-e2e-${RANDOM}${RANDOM}"
diagnostics="$(mktemp -d "${TMPDIR:-/tmp}/trpc-backend-smoke.XXXXXX")"
command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1 || { echo "Docker Compose v2 is required" >&2; exit 2; }
cleanup() {
  local status=$?
  if [[ ${status} -ne 0 ]]; then docker compose --project-name "${project}" -f "${compose_file}" logs --no-color >"${diagnostics}/compose.log" || true; echo "backend adapter smoke failed; diagnostics retained at ${diagnostics}/compose.log" >&2; fi
  docker compose --project-name "${project}" -f "${compose_file}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  [[ ${status} -ne 0 ]] || rmdir "${diagnostics}" || true
  exit "${status}"
}
trap cleanup EXIT

compose() {
  TRPC_E2E_SUITE="${suite}" docker compose --project-name "${project}" -f "${compose_file}" "$@"
}

# The smoke image runs as root and writes coverage profiles to the host bind
# mount. Make successful profiles readable by the GitHub Actions runner before
# upload-artifact packages them; /out contains only coverage metadata, never
# source payloads or credentials.
publish_coverage() {
  compose run --rm --no-deps --entrypoint bash smoke -c 'chmod -R a+rX /out'
}

wait_healthy() {
  local service="$1" container state
  container="$(compose ps -q "${service}")"
  [[ -n "${container}" ]] || { echo "${service} container was not created" >&2; return 1; }
  for _ in $(seq 1 60); do
    state="$(docker inspect -f '{{.State.Health.Status}}' "${container}")"
    [[ "${state}" == "healthy" ]] && return 0
    sleep 1
  done
  echo "${service} did not become healthy" >&2
  return 1
}

case "${suite}" in
  migration)
    compose up --detach postgres
    wait_healthy postgres
    compose run --rm --no-deps smoke
    ;;
  migration-coverage)
    # The smoke container persists the raw PostgreSQL adapter coverage
    # profile under /out (bind-mounted from TRPC_COVERAGE_DIR).  CI reads the
    # directory back via GITHUB_ENV to upload the profile as a workflow
    # artifact; locally it stays in TMPDIR for inspection.
    coverage_dir="$(mktemp -d "${TMPDIR:-/tmp}/trpc-adapter-coverage.XXXXXX")"
    export TRPC_COVERAGE_DIR="${coverage_dir}"
    # Baseline measured on the disposable contract matrix (2026-09-10).  The
    # floor ratchets upward as adapter contracts grow; override via env.
    export TRPC_MIN_POSTGRES_ADAPTER_COVERAGE="${TRPC_MIN_POSTGRES_ADAPTER_COVERAGE:-53.0}"
    if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
      echo "TRPC_ADAPTER_COVERAGE_DIR=${coverage_dir}" >>"${GITHUB_ENV}"
    fi
    compose up --detach postgres
    wait_healthy postgres
    compose run --rm --no-deps smoke
    publish_coverage
    ;;
  runtime|storage|artifact)
    if [[ "${TRPC_E2E_COVERAGE:-}" == "1" ]]; then
      # Opt-in coverage attribution: the smoke container persists raw go
      # cover profiles under /out (bind-mounted from TRPC_COVERAGE_DIR) and
      # asserts the per-suite floors below.  CI reads the directory back via
      # GITHUB_ENV to upload the profiles as workflow artifacts; locally they
      # stay in TMPDIR for inspection.
      coverage_dir="$(mktemp -d "${TMPDIR:-/tmp}/trpc-e2e-coverage.XXXXXX")"
      export TRPC_COVERAGE_DIR="${coverage_dir}"
      # Baselines measured on the disposable smoke matrix (2026-09-10).  The
      # floors ratchet upward as the e2e matrices grow; override via env.
      export TRPC_MIN_RUNTIME_SLICE_COVERAGE="${TRPC_MIN_RUNTIME_SLICE_COVERAGE:-39.5}"
      export TRPC_MIN_STORAGE_E2E_COVERAGE="${TRPC_MIN_STORAGE_E2E_COVERAGE:-67.0}"
      export TRPC_MIN_ARTIFACT_E2E_COVERAGE="${TRPC_MIN_ARTIFACT_E2E_COVERAGE:-27.0}"
      if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
        echo "TRPC_E2E_COVERAGE_DIR_${suite^^}=${coverage_dir}" >>"${GITHUB_ENV}"
      fi
    fi
    case "${suite}" in
      runtime)
        compose up --detach postgres redis
        wait_healthy postgres
        wait_healthy redis
        ;;
      storage)
        compose up --detach qdrant minio vault
        wait_healthy vault
        ;;
      artifact)
        compose up --detach postgres minio
        wait_healthy postgres
        ;;
    esac
    compose run --rm --no-deps smoke
    if [[ "${TRPC_E2E_COVERAGE:-}" == "1" ]]; then
      publish_coverage
    fi
    ;;
  all)
    # Do not pass --abort-on-container-exit: vault-init is a one-shot container
    # and would abort the whole stack the moment it exits, before smoke starts.
    compose up --exit-code-from smoke
    ;;
esac
