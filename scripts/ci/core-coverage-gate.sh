#!/usr/bin/env bash
# Keep the three stateful platform seams that carry tenant identity, session
# semantics and provider delivery above a useful unit-test floor. This is a
# package gate, not a substitute for the repository/diff coverage ratchet.
set -euo pipefail

minimum="${TRPC_CORE_PACKAGE_MIN_COVERAGE:-80.0}"
packages=(
  ./trpcservice/tenant
  ./trpcservice/storage/session
  ./trpcservice/channels/delivery
)

valid_coverage_percent() {
  local value="${1%\%}"
  [[ "${value}" =~ ^[0-9]+([.][0-9]+)?$ ]] || return 1
  awk -v value="${value}" 'BEGIN { exit !(value >= 0 && value <= 100) }'
}

if ! valid_coverage_percent "${minimum}"; then
  echo "TRPC_CORE_PACKAGE_MIN_COVERAGE must be a decimal percentage from 0 to 100, got ${minimum}" >&2
  exit 2
fi
minimum="${minimum%\%}"

for package in "${packages[@]}"; do
  output="$(go test -count=1 -cover "${package}")"
  printf '%s\n' "${output}"
  coverage="$(awk '/coverage: [0-9.]+% of statements/ { for (i = 1; i <= NF; i++) if ($i == "coverage:") { value = $(i + 1); sub("%", "", value); print value } }' <<<"${output}" | tail -n 1)"
  [[ -n "${coverage}" ]] || { echo "coverage output missing for ${package}" >&2; exit 1; }
  if ! valid_coverage_percent "${coverage}"; then
    echo "coverage output is invalid for ${package}: ${coverage}" >&2
    exit 1
  fi
  awk -v actual="${coverage}" -v required="${minimum}" 'BEGIN { exit !(actual >= required) }' || {
    echo "core package ${package} coverage ${coverage}% is below required ${minimum}%" >&2
    exit 1
  }
  echo "core package coverage: ${package} ${coverage}% (minimum ${minimum}%)"
done
