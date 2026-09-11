#!/usr/bin/env bash
# Run the release gate against an explicitly supplied isolated PostgreSQL DB.
# Disable tracing before reading secrets, including when invoked with bash -x.
set +x
set -euo pipefail

if [[ ! "${TEST_POSTGRES_DSN:-}" =~ [^[:space:]] ]]; then
  echo "TEST_POSTGRES_DSN must name an isolated PostgreSQL test database; acceptance was not run." >&2
  exit 2
fi
if ! command -v go >/dev/null 2>&1; then
  echo "go is required for PostgreSQL acceptance." >&2
  exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${script_dir}/.."

# Include every package with a TEST_POSTGRES_DSN-gated integration test, plus
# migration checksum/schema tests. Run all tests, including names that do not
# contain PostgresIntegration (for example SQL Runtime and object Artifact).
packages=(
  ./migrations
  ./trpcservice/configcontrol
  ./trpcservice/contentsafety
  ./trpcservice/agent
  ./trpcservice/budget
  ./trpcservice/backend
  ./trpcservice/datamigration
  ./trpcservice/log
  ./trpcservice/memoryvisibility
  ./trpcservice/queue
  ./trpcservice/sessionturn
  ./trpcservice/store
  ./trpcservice/tooloperation
  ./trpcservice/worker
)

umask 077
test_log="$(mktemp -t trpc-postgres-acceptance-XXXXXX)"
cleanup() { rm -f "${test_log}"; }
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

echo "Running PostgreSQL acceptance with race detection and no cached results (${#packages[@]} packages)."
test_status=0
# Some package-level SQL integration tests intentionally assume the selected
# database has already passed the normal migrator gate (for example the Agent
# runtime wiring test). Prepare the task-scoped database once, then run every
# package against that same schema. The migrator never prints its DSN, and its
# output is discarded here so a driver error cannot leak credentials.
echo "Applying and verifying migrations through the current version."
if ! GOFLAGS= go run ./cmd/trpc-migrate -dsn-env TEST_POSTGRES_DSN >/dev/null 2>&1; then
  echo "FAIL PostgreSQL migration preflight" >&2
  echo "PostgreSQL acceptance failed. Raw diagnostics were suppressed and deleted to protect database credentials." >&2
  exit 1
fi
# Connection errors from drivers may contain credentials. Never relay raw test
# output or keep it after the command; expose only allowlisted package names
# and identifier-only top-level failing/skipped test names below.
# Clear inherited test filters so this gate cannot silently run a subset.
GOFLAGS= go test -v -race -count=1 "${packages[@]}" >"${test_log}" 2>&1 || test_status=$?

for package in "${packages[@]}"; do
  module_package="github.com/cyl6/trpc-agent-service/${package#./}"
  if awk -v package="${module_package}" '$1 == "ok" && $2 == package { found = 1 } END { exit !found }' "${test_log}"; then
    echo "PASS ${package}"
  else
    echo "FAIL ${package}" >&2
    test_status=1
  fi
done

awk '$1 == "---" && ($2 == "FAIL:" || $2 == "SKIP:") && $3 ~ /^Test[A-Za-z0-9_]+$/ { print $2 " " $3 }' "${test_log}"
if grep -Eq '^[[:space:]]*--- SKIP:' "${test_log}"; then
  echo "Skipped tests do not satisfy PostgreSQL acceptance." >&2
  test_status=1
fi
if ((test_status != 0)); then
  echo "PostgreSQL acceptance failed. Raw diagnostics were suppressed and deleted to protect database credentials." >&2
  exit "${test_status}"
fi
echo "PostgreSQL acceptance passed."
