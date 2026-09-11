#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

ACTION="${1:-all}"
PROJECT="${TEST_COMPOSE_PROJECT:-trpc-agent-service-acceptance}"
POSTGRES_PORT="${TEST_POSTGRES_PORT:-15432}"
REDIS_PORT="${TEST_REDIS_PORT:-16379}"
KAFKA_PORT="${TEST_KAFKA_PORT:-29092}"
S3_PORT="${TEST_S3_PORT:-19000}"
S3_CONSOLE_PORT="${TEST_S3_CONSOLE_PORT:-19001}"
ARTIFACTS="${TEST_INFRA_ARTIFACTS_DIR:-$ROOT/tests/infrastructure/artifacts}"
COMPOSE=(docker compose -p "$PROJECT" -f "$ROOT/deploy/compose.test.yaml")
mkdir -p "$ARTIFACTS"

export TEST_POSTGRES_PORT="$POSTGRES_PORT"
export TEST_REDIS_PORT="$REDIS_PORT"
export TEST_KAFKA_PORT="$KAFKA_PORT"
export TEST_S3_PORT="$S3_PORT"
export TEST_S3_CONSOLE_PORT="$S3_CONSOLE_PORT"
export TEST_POSTGRES_DSN="postgres://trpc_test:test-postgres-password@127.0.0.1:${POSTGRES_PORT}/trpc_agent_test?sslmode=disable"
export TEST_KAFKA_BROKERS="127.0.0.1:${KAFKA_PORT}"
export TEST_S3_CONFIG_JSON="{\"endpoint\":\"http://127.0.0.1:${S3_PORT}\",\"region\":\"us-east-1\",\"bucket\":\"trpc-artifacts\",\"access_key\":\"trpc_test\",\"secret_key\":\"test-minio-password\"}"

up() {
  echo "[infra] starting isolated project $PROJECT"
  "${COMPOSE[@]}" up -d --wait postgres redis redpanda minio
  "${COMPOSE[@]}" run --rm minio-init
  "${COMPOSE[@]}" ps --all >"$ARTIFACTS/compose-ps.txt"
}

run_tests() {
  echo "[infra] applying current database baseline"
  go test -p=1 -count=1 ./migrations
  echo "[infra] full Go suite with PostgreSQL, Redis, Kafka, S3 and RLS enabled"
  go test -p=1 -count=1 ./...
}

run_coverage() {
  local coverage_dir="${GO_COVERAGE_DIR:-$ROOT/tests/coverage/infrastructure}"
  local profile="$coverage_dir/go-cover.out"
  local summary="$coverage_dir/go-cover-summary.txt"
  mkdir -p "$coverage_dir"
  echo "[infra] applying current database baseline before coverage"
  go test -p=1 -count=1 ./migrations
  echo "[infra] collecting full Go coverage with PostgreSQL, Redis, Kafka, S3 and RLS enabled"
  go test -p=1 -count=1 -covermode=atomic -coverprofile="$profile" ./...
  go tool cover -func="$profile" | tee "$summary"
  echo "[infra] coverage report generated at $coverage_dir"
}

down() {
  echo "[infra] stopping isolated project $PROJECT"
  "${COMPOSE[@]}" down --volumes --remove-orphans
}

collect_failure() {
  "${COMPOSE[@]}" ps --all >"$ARTIFACTS/compose-ps-failed.txt" 2>&1 || true
  "${COMPOSE[@]}" logs --no-color >"$ARTIFACTS/compose-failure.log" 2>&1 || true
}

case "$ACTION" in
  up)
    up
    ;;
  test)
    run_tests 2>&1 | tee "$ARTIFACTS/test.log"
    ;;
  coverage)
    run_coverage 2>&1 | tee "$ARTIFACTS/coverage.log"
    ;;
  down)
    down
    ;;
  all)
    cleanup() {
      local status=$?
      trap - EXIT
      if (( status != 0 )); then collect_failure; fi
      if [[ "${KEEP_TEST_INFRA:-false}" != "true" ]]; then down || true; fi
      exit "$status"
    }
    trap cleanup EXIT
    up
    run_tests 2>&1 | tee "$ARTIFACTS/test.log"
    ;;
  *)
    echo "usage: $0 [up|test|coverage|down|all]" >&2
    exit 2
    ;;
esac
