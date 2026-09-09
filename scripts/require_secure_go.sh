#!/usr/bin/env bash
set -euo pipefail

# Production validation deliberately has a stricter toolchain contract than
# the module's source-compatibility floor. Keep this value aligned with the
# pinned CI and Docker builder images.
minimum_go_version="go1.26.7"
actual_go_version="$(go env GOVERSION)"

parse_stable_go_version() {
  local value="$1"
  if [[ ! "$value" =~ ^go([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
    echo "production validation requires a stable Go release; found ${value}" >&2
    return 1
  fi
  printf '%s %s %s\n' "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}" "${BASH_REMATCH[3]}"
}

read -r actual_major actual_minor actual_patch < <(parse_stable_go_version "$actual_go_version")
read -r minimum_major minimum_minor minimum_patch < <(parse_stable_go_version "$minimum_go_version")

if (( actual_major < minimum_major ||
      (actual_major == minimum_major && actual_minor < minimum_minor) ||
      (actual_major == minimum_major && actual_minor == minimum_minor && actual_patch < minimum_patch) )); then
  echo "production validation requires ${minimum_go_version} or newer; found ${actual_go_version}" >&2
  echo "Go 1.25.14 remains a source-compatibility test only and is not the production security baseline." >&2
  exit 1
fi

echo "secure Go toolchain gate passed: ${actual_go_version} (minimum ${minimum_go_version})"
