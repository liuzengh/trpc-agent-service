#!/usr/bin/env bash
set -euo pipefail

fixture_dir=$(mktemp -d)
cleanup() {
  rm -f -- "$fixture_dir/fixture.go" "$fixture_dir/SHA256SUMS.txt"
  rmdir -- "$fixture_dir"
}
trap cleanup EXIT

(
  cd "$fixture_dir"
  printf 'package fixture\n' > fixture.go
  sha256sum fixture.go > SHA256SUMS.txt

  if ! output=$(sha256sum --check --quiet SHA256SUMS.txt 2>&1); then
    printf 'Valid checksum verification failed: %s\n' "$output" >&2
    exit 1
  fi
  # setup-go treats successful "file.go: OK" lines as compiler diagnostics.
  if [[ -n "$output" ]]; then
    printf 'Successful checksum verification must be silent.\n' >&2
    exit 1
  fi

  printf '// modified\n' >> fixture.go
  if output=$(sha256sum --check --quiet SHA256SUMS.txt 2>&1); then
    printf 'Checksum verification accepted a modified file.\n' >&2
    exit 1
  fi
  if [[ "$output" != *'fixture.go: FAILED'* ]]; then
    printf 'Checksum verification suppressed the mismatch diagnostic.\n' >&2
    exit 1
  fi
)

printf 'Checksum output contracts passed.\n'
