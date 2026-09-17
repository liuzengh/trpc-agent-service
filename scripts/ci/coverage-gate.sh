#!/usr/bin/env bash
# Coverage is a ratchet: repository coverage may not fall beneath the current
# baseline, and pull requests must cover at least 80% of changed executable Go
# lines.  The latter is intentionally scoped to PRs because push/workflow runs
# do not have a trustworthy base revision.
#
# The profile is built with -coverpkg=./trpcservice/... so a single profile
# attributes cross-package execution: subpackage tests (for example the
# storage/session contract harnesses) count toward the parent packages they
# exercise instead of only their own package.  cmd/ mains are excluded from
# the denominator on purpose — they carry no unit-attributable logic and are
# exercised by the backend-adapter e2e jobs; the diff ratchet below is
# therefore explicitly scoped to trpcservice/ paths as well.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

# Baseline measured locally on 2026-09-10 (54.2% actual).  The floor is a
# ratchet: raise it whenever the measured total exceeds it by ~0.5%.
minimum_total="${TRPC_MIN_TOTAL_COVERAGE:-54.0}"
base_sha="${TRPC_DIFF_BASE_SHA:-}"
coverage_file="$(mktemp "${TMPDIR:-/tmp}/trpc-coverage.XXXXXX")"
changed_file="$(mktemp "${TMPDIR:-/tmp}/trpc-coverage-diff.XXXXXX")"
lines_file="$(mktemp "${TMPDIR:-/tmp}/trpc-coverage-lines.XXXXXX")"
test_log="$(mktemp "${TMPDIR:-/tmp}/trpc-coverage-test.XXXXXX")"
cleanup() {
  local status=$?
  rm -f -- "$coverage_file" "$changed_file" "$lines_file" "$test_log"
  exit "$status"
}
trap cleanup EXIT

valid_coverage_percent() {
  local value="${1%\%}"
  [[ "${value}" =~ ^[0-9]+([.][0-9]+)?$ ]] || return 1
  awk -v value="${value}" 'BEGIN { exit !(value >= 0 && value <= 100) }'
}

if ! valid_coverage_percent "$minimum_total"; then
  echo "TRPC_MIN_TOTAL_COVERAGE must be a decimal percentage from 0 to 100, got ${minimum_total}" >&2
  exit 2
fi
minimum_total="${minimum_total%\%}"

if ! go test -count=1 -covermode=atomic -coverpkg=./trpcservice/... -coverprofile="$coverage_file" ./... >"$test_log" 2>&1; then
  cat "$test_log" >&2
  exit 1
fi
total="$(go tool cover -func="$coverage_file" | awk '/^total:/ { gsub("%", "", $3); print $3 }')"
if ! valid_coverage_percent "$total"; then
  echo "repository coverage is invalid: ${total:-missing}" >&2
  exit 1
fi
awk -v actual="$total" -v minimum="$minimum_total" 'BEGIN { exit !(actual >= minimum) }' || {
  echo "repository coverage ${total}% is below required ${minimum_total}%" >&2
  exit 1
}
echo "repository coverage: ${total}% (minimum ${minimum_total}%)"

[[ -n "$base_sha" ]] || exit 0
git cat-file -e "${base_sha}^{commit}" 2>/dev/null || {
  echo "diff coverage base is unavailable: ${base_sha}" >&2
  exit 2
}
git diff --unified=0 "$base_sha"...HEAD -- '*.go' >"$changed_file"
# The ratchet only gates trpcservice/ changes: the coverage profile is scoped
# to ./trpcservice/..., and cmd/ glue is validated by the backend-adapter e2e
# jobs instead of unit diff coverage.
awk '
  /^\+\+\+ b\// { file=""; if (substr($0, 7, 12) == "trpcservice/") file=substr($0, 7); next }
  /^@@ / {
    if (file == "") next
    split($0, pieces, "+"); split(pieces[2], range, " "); split(range[1], coords, ",")
    line=coords[1]; next
  }
  /^\+/ && !/^\+\+\+/ { if (file != "") print file ":" line; line++; next }
  /^ / { if (file != "") line++ }
' "$changed_file" >"$lines_file"

[[ -s "$lines_file" ]] || exit 0
awk '
  NR == FNR { wanted[$0]=1; next }
  NR == 1 { next }
  {
    split($1, pathAndRange, ":"); path=pathAndRange[1]
    sub("^github.com/liuzengh/trpc-agent-service/", "", path)
    split(pathAndRange[2], span, ","); split(span[1], start, "."); split(span[2], end, ".")
    for (key in wanted) {
      split(key, keyParts, ":")
      if (keyParts[1] == path && keyParts[2] + 0 >= start[1] && keyParts[2] + 0 <= end[1]) {
        executable[key]=1
        if ($3 + 0 > 0) covered[key]=1
      }
    }
  }
  END {
    for (key in executable) { total++; if (covered[key]) hit++ }
    if (total == 0) exit 0
    pct=(100 * hit / total)
    printf("changed executable-line coverage: %.1f%% (%d/%d)\n", pct, hit, total)
    exit !(pct >= 80)
  }
' "$lines_file" "$coverage_file" || {
  echo "changed executable-line coverage is below 80%" >&2
  exit 1
}
