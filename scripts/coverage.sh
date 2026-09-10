#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# The PostgreSQL control-plane integration suite exercises repositories that
# live in sibling packages. One native Go profile instruments every package,
# so Codecov receives the same cross-package execution data as `go tool cover`
# without depending on a custom profile merger or running the integration
# schema setup twice.
coverage_packages="$(go list ./... | paste -sd, -)"
go test -coverpkg="$coverage_packages" -coverprofile=coverage.out ./...

# A cross-package Go profile contains one entry per test binary. Merge duplicate
# blocks so every coverage consumer observes the aggregate execution count.
coverage_merged="$(mktemp)"
awk '
  NR == 1 { print; next }
  {
    key = $1 SUBSEP $2
    if (!(key in seen)) {
      seen[key] = 1
      order[++count] = key
      block[key] = $1
      statements[key] = $2
      hits[key] = $3
    } else {
      hits[key] += $3
    }
  }
  END {
    for (i = 1; i <= count; i++) {
      key = order[i]
      print block[key], statements[key], hits[key]
    }
  }
' coverage.out >"$coverage_merged"
mv "$coverage_merged" coverage.out

go tool cover -func=coverage.out
