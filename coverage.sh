#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

go test ./... -coverprofile=coverage.out
go tool cover -func=coverage.out

# Pure platform-domain packages are expected to remain at or above 80% each.
core_packages=(
  ./trpcservice/channels
  ./trpcservice/config
  ./trpcservice/secrets
  ./trpcservice/tenant
  ./trpcservice/metrics
)
for package in "${core_packages[@]}"; do
  coverage="$(go test "$package" -cover | sed -n 's/.*coverage: \([0-9.]*\)%.*/\1/p')"
  awk -v package="$package" -v coverage="$coverage" 'BEGIN {
    if (coverage + 0 < 80) {
      printf "core coverage failed: %s=%s%% (minimum 80%%)\n", package, coverage > "/dev/stderr"
      exit 1
    }
  }'
done
