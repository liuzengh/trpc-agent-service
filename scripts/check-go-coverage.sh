#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

ARTIFACTS="${GO_COVERAGE_DIR:-$ROOT/tests/coverage/artifacts}"
PROFILE="$ARTIFACTS/go-cover.out"
SUMMARY="$ARTIFACTS/go-cover-summary.txt"
mkdir -p "$ARTIFACTS"

reported_packages=(
  cmd/trpc-service
  trpcservice/agent
  trpcservice/assembly
  trpcservice/identity
  trpcservice/messaging
  trpcservice/storage
  trpcservice/tenant
  trpcservice/web
)

echo "[coverage] running all Go tests with atomic coverage"
go test -count=1 -covermode=atomic -coverprofile="$PROFILE" ./...

coverage_for() {
  local package_prefix="$1"
  awk -v prefix="github.com/liuzengh/trpc-agent-service/${package_prefix}/" '
    NR == 1 { next }
    {
      file = $1
      sub(/:.*/, "", file)
      if (prefix != "" && index(file, prefix) != 1) next
      statements += $2
      if ($3 > 0) covered += $2
    }
    END {
      if (statements == 0) print "0.00"
      else printf "%.2f", covered * 100 / statements
    }
  ' "$PROFILE"
}

{
  total="$(go tool cover -func="$PROFILE" | awk '/^total:/ { gsub(/%/, "", $3); print $3 }')"
  printf 'total %.2f%%
' "$total"
  for package_name in "${reported_packages[@]}"; do
    actual="$(coverage_for "$package_name")"
    printf '%s %.2f%%
' "$package_name" "$actual"
  done
} >"$SUMMARY"
cat "$SUMMARY"
echo "[coverage] report generated at $ARTIFACTS"
