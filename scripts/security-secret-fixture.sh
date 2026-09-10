#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
GITLEAKS_BIN="${GITLEAKS_BIN:-gitleaks}"

if [[ ! -x "$GITLEAKS_BIN" ]] && ! command -v "$GITLEAKS_BIN" >/dev/null 2>&1; then
  echo "gitleaks is required (set GITLEAKS_BIN to a pinned binary)" >&2
  exit 1
fi

fixture_root="$(mktemp -d)"
trap 'rm -rf "$fixture_root"' EXIT

git -C "$fixture_root" init --quiet
git -C "$fixture_root" config user.name "security-fixture"
git -C "$fixture_root" config user.email "security-fixture@example.invalid"

printf '%s\n' 'safe fixture content' > "$fixture_root/README.txt"
git -C "$fixture_root" add README.txt
git -C "$fixture_root" commit --quiet -m "safe fixture"

"$GITLEAKS_BIN" git --redact --no-banner --config "$ROOT/gitleaks.toml" \
  --exit-code 1 "$fixture_root"

# Construct a deterministic, non-live GitHub token at runtime so this test
# proves blocking behavior without committing a credential-like value.
token_prefix="$(printf '%s' 'Z2hwXw==' | base64 --decode)"
printf '%s%s\n' "$token_prefix" 'Ab3dE6fG9hJ2kL5mN8pQ1rS4tU7vW0xYzA1b' > "$fixture_root/secret.txt"
git -C "$fixture_root" add secret.txt
git -C "$fixture_root" commit --quiet -m "secret fixture"

if "$GITLEAKS_BIN" git --redact --no-banner --config "$ROOT/gitleaks.toml" \
  --exit-code 1 "$fixture_root"; then
  echo "::error::gitleaks fixture did not block the synthetic secret" >&2
  exit 1
fi

echo "gitleaks pass/fail fixtures behaved as expected"
