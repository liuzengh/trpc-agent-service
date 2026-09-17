#!/usr/bin/env bash
# Runs a deliberately selected integration suite and fails if Go reports a
# skipped test.  Optional developer-only live-provider tests are intentionally
# not passed here; CI must never silently skip a test it claims to exercise.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

if [[ $# -eq 0 ]]; then
  echo "usage: $0 <go-test-package> [...]" >&2
  exit 2
fi

result_file="$(mktemp "${TMPDIR:-/tmp}/trpc-test-json.XXXXXX")"
cleanup() {
  local status=$?
  rm -f -- "$result_file"
  exit "$status"
}
trap cleanup EXIT

set +e
go test -count=1 -json "$@" | tee "$result_file"
test_status=${PIPESTATUS[0]}
set -e

if grep -q '"Action":"skip"' "$result_file"; then
  echo "CI integration suite skipped one or more tests" >&2
  grep '"Action":"skip"' "$result_file" >&2
  exit 1
fi
exit "$test_status"
