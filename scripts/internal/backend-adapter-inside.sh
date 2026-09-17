#!/usr/bin/env bash
set -euo pipefail
suite="${TRPC_E2E_SUITE:-all}"
case "${suite}" in
  all|migration|migration-coverage|runtime|storage|artifact) ;;
  *) echo "TRPC_E2E_SUITE must be all, migration, migration-coverage, runtime, storage, or artifact" >&2; exit 2 ;;
esac

for name in TRPC_MIGRATION_TEST TRPC_POSTGRES_ADMIN_DSN; do
  [[ -n "${!name:-}" ]] || { echo "${name} is required" >&2; exit 2; }
done

# The disposable smoke image starts with an empty module cache. A transient
# proxy EOF must not be reported as a backend-adapter regression; populate the
# complete module graph before the test matrix and retry only this network step.
download_modules() {
  local attempt
  for attempt in 1 2 3; do
    if go mod download; then
      return 0
    fi
    if [[ ${attempt} -lt 3 ]]; then
      echo "go mod download failed (attempt ${attempt}/3); retrying" >&2
      sleep "${attempt}"
    fi
  done
  return 1
}

download_modules

valid_coverage_percent() {
  local value="${1%\%}"
  [[ "${value}" =~ ^[0-9]+([.][0-9]+)?$ ]] || return 1
  awk -v value="${value}" 'BEGIN { exit !(value >= 0 && value <= 100) }'
}

# covered_go_test runs a go test matrix and, when TRPC_E2E_COVERAGE=1,
# attributes execution to an explicit adapter package set, asserts the
# configured coverage floor and persists the raw profile for the admission
# gate.  Without the opt-in it degrades to the exact invocations the suites
# used before coverage attribution existed, so developer runs stay fast.
#
# Usage: covered_go_test <label> <floor_env> <out_file> <require_no_skip>
#                        <coverpkg_csv> [go test args...]
covered_go_test() {
  local label="$1" floor_env="$2" out_file="$3" require_no_skip="$4" coverpkg="$5"
  shift 5
  if [[ "${TRPC_E2E_COVERAGE:-}" != "1" ]]; then
    if [[ "${require_no_skip}" == "require-no-skip" ]]; then
      bash scripts/lib/test-no-skip.sh "$@"
    else
      go test -count=1 "$@"
    fi
    return
  fi
  local profile result_file status=0
  profile="$(mktemp "${TMPDIR:-/tmp}/trpc-e2e-coverage.XXXXXX")"
  result_file="$(mktemp "${TMPDIR:-/tmp}/trpc-e2e-test-json.XXXXXX")"
  set +e
  go test -count=1 -json -covermode=atomic \
    -coverpkg="${coverpkg}" \
    -coverprofile="${profile}" "$@" | tee "${result_file}"
  status=${PIPESTATUS[0]}
  set -e
  if [[ ${status} -ne 0 ]]; then
    echo "${label} e2e test matrix failed" >&2
    exit "${status}"
  fi
  if [[ "${require_no_skip}" == "require-no-skip" ]] && grep -q '"Action":"skip"' "${result_file}"; then
    echo "${label} e2e suite skipped one or more tests despite provisioned backends" >&2
    exit 1
  fi
  rm -f "${result_file}"
  local total floor
  total="$(go tool cover -func="${profile}" | awk '/^total:/ { gsub("%", "", $3); print $3 }')"
  if ! valid_coverage_percent "${total}"; then
    echo "${label} e2e coverage is invalid: ${total:-missing}" >&2
    exit 1
  fi
  echo "${label} e2e coverage: ${total}%"
  floor="${!floor_env:-}"
  if [[ -n "${floor}" ]]; then
    if ! valid_coverage_percent "${floor}"; then
      echo "${floor_env} must be a decimal percentage from 0 to 100, got ${floor}" >&2
      exit 2
    fi
    floor="${floor%\%}"
    awk -v actual="${total}" -v required="${floor}" 'BEGIN { exit !(actual >= required) }' || {
      echo "${label} e2e coverage ${total}% is below required ${floor}%" >&2
      exit 1
    }
  fi
  if [[ -n "${out_file}" ]]; then
    mkdir -p "$(dirname "${out_file}")"
    cp "${profile}" "${out_file}"
  fi
  rm -f "${profile}"
}

run_migration() {
  # The Compose image also serves runtime-e2e. Explicitly suppress its opt-in
  # Redis slice here so migration-e2e remains a PostgreSQL-only dependency.
  TRPC_RUNTIME_TEST=0 go run ./cmd/postgres-migration-test
  # The operator role stays bounded and needs no production credentials in this
  # disposable environment. Its transition/journal suite is nevertheless a
  # required real-backend gate, never an optional developer-only check.
  go test -count=1 ./cmd/trpc-service ./trpcservice/migration/...
}

run_migration_coverage() {
  # This is intentionally opt-in: the normal migration job remains a fast
  # contract gate. The migration runner creates the random database, keeps it
  # alive for the contract matrix, and attributes those executions to the
  # adapter packages before it tears the database down.
  TRPC_RUNTIME_TEST=0 TRPC_POSTGRES_ADAPTER_COVERAGE=1 go run ./cmd/postgres-migration-test
}

run_runtime() {
  [[ -n "${TRPC_REDIS_TEST_ADDR:-}" ]] || { echo "TRPC_REDIS_TEST_ADDR is required" >&2; exit 2; }
  # Do not permit opt-in contracts to turn into green builds because a Redis
  # address or the runtime slice was accidentally omitted.
  bash scripts/lib/test-no-skip.sh ./trpcservice/broker/redis ./trpcservice/coordination/redis ./trpcservice/relay/redis
  # The runtime slice itself is covered by the Go-side attribution inside
  # cmd/postgres-migration-test (worker/relay/coordination/session core).
  if [[ "${TRPC_E2E_COVERAGE:-}" == "1" ]]; then
    export TRPC_RUNTIME_SLICE_COVERAGE=1
  fi
  TRPC_RUNTIME_TEST=1 go run ./cmd/postgres-migration-test
  go test -count=1 ./trpcservice/admin ./trpcservice/agent ./trpcservice/agent/condition
}

run_storage() {
  for name in TRPC_QDRANT_TEST_ENDPOINT TRPC_MINIO_TEST_ENDPOINT TRPC_VAULT_TEST_ENDPOINT TRPC_VAULT_TEST_TOKEN; do
    [[ -n "${!name:-}" ]] || { echo "${name} is required" >&2; exit 2; }
  done
  # Qdrant has no shell HTTP client, so readiness is asserted by the smoke
  # runner. MinIO and Vault receive the same real protocol checks.
  for _ in $(seq 1 60); do
    curl -fsS "${TRPC_QDRANT_TEST_ENDPOINT}/healthz" >/dev/null 2>&1 && break
    sleep 1
  done
  curl -fsS "${TRPC_QDRANT_TEST_ENDPOINT}/healthz" >/dev/null
  for _ in $(seq 1 60); do
    curl -fsS "${TRPC_MINIO_TEST_ENDPOINT}/minio/health/live" >/dev/null 2>&1 && break
    sleep 1
  done
  curl -fsS "${TRPC_MINIO_TEST_ENDPOINT}/minio/health/live" >/dev/null
  curl -fsS -X POST -H "X-Vault-Token: ${TRPC_VAULT_TEST_TOKEN}" -H "Content-Type: application/json" \
    -d '{"data":{"value":"integration-secret"}}' "${TRPC_VAULT_TEST_ENDPOINT}/v1/secret/data/model" >/dev/null
  covered_go_test "Storage adapters (Vault/Qdrant/S3)" TRPC_MIN_STORAGE_E2E_COVERAGE "${TRPC_STORAGE_COVERAGE_OUT:-}" require-no-skip \
    "./trpcservice/secrets/vault,./trpcservice/storage/knowledge/qdrant,./trpcservice/storage/objectstore/s3" \
    ./trpcservice/secrets/vault ./trpcservice/storage/knowledge/qdrant ./trpcservice/storage/objectstore/s3
  go test -count=1 ./trpcservice/skill ./trpcservice/storage/knowledge ./trpcservice/tool/codeexec
}

run_artifact() {
  for name in TRPC_S3_ENDPOINT TRPC_S3_BUCKET TRPC_S3_ACCESS_KEY TRPC_S3_SECRET_KEY; do
    [[ -n "${!name:-}" ]] || { echo "${name} is required" >&2; exit 2; }
  done
  # Artifact metadata is schema-owned by this repository, so migrate the
  # disposable PostgreSQL database before composing it with real MinIO.  The
  # compose test itself runs inside cmd/postgres-migration-test while the
  # migrated database is still alive — only that command can hand the test
  # its TRPC_POSTGRES_TEST_DSN; from the shell it silently skipped.  The
  # Go-side attribution is enabled with TRPC_ARTIFACT_E2E_COVERAGE.
  if [[ "${TRPC_E2E_COVERAGE:-}" == "1" ]]; then
    export TRPC_ARTIFACT_E2E_COVERAGE=1
  fi
  TRPC_RUNTIME_TEST=0 TRPC_ARTIFACT_E2E=1 go run ./cmd/postgres-migration-test
}

case "${suite}" in
  migration) run_migration ;;
  migration-coverage) run_migration_coverage ;;
  runtime) run_runtime ;;
  storage) run_storage ;;
  artifact) run_artifact ;;
  all) run_migration; run_runtime; run_storage; run_artifact ;;
esac
