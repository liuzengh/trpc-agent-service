#!/usr/bin/env bash
set -euo pipefail

# Guards high-risk package coverage without starting external services or
# reading runtime configuration. Integration coverage is verified separately.
cache_dir="${GOCACHE:-/tmp/trpc-agent-service-coverage-cache}"

check_package() {
  local package="$1"
  local minimum="$2"
  local output coverage
  output="$(GOCACHE="$cache_dir" go test -cover "$package")"
  coverage="$(printf '%s\n' "$output" | sed -n 's/.*coverage: \([0-9.]*\)% of statements.*/\1/p')"
  if [[ -z "$coverage" ]]; then
    printf 'coverage unavailable: %s\n' "$package" >&2
    return 1
  fi
  if ! awk -v actual="$coverage" -v required="$minimum" 'BEGIN { exit !(actual + 0 >= required + 0) }'; then
    printf 'coverage below threshold: %s actual=%s%% required=%s%%\n' "$package" "$coverage" "$minimum" >&2
    return 1
  fi
  printf 'coverage: %s %s%% (minimum %s%%)\n' "$package" "$coverage" "$minimum"
}

check_package ./trpcservice/recovery 15
check_package ./trpcservice/backend 20
check_package ./trpcservice/storage 18
check_package ./trpcservice/worker 50
printf '%s\n' 'critical package coverage acceptance: PASS'
