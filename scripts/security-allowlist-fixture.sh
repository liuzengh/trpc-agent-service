#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
fixture_root="$(mktemp -d)"
trap 'rm -rf "$fixture_root"' EXIT

cp "$ROOT/gitleaks.toml" "$fixture_root/gitleaks.toml"

cat > "$fixture_root/gitleaks.toml" <<'EOF'
[allowlist]
# allowlist-expiry: 2099-12-31
regexes = ["deadbeef"]
files = ["secret.txt"]
stopwords = ["fixture"]
EOF
if SECURITY_ALLOWLIST_ROOT="$fixture_root" "$ROOT/scripts/validate-security-allowlist.sh" >/dev/null 2>&1; then
  echo "::error::allowlist fixture accepted a Gitleaks entry without metadata" >&2
  exit 1
fi
cp "$ROOT/gitleaks.toml" "$fixture_root/gitleaks.toml"

cat > "$fixture_root/gitleaks.toml" <<'EOF'
[allowlist]
# Owner: @security-team | Issue: #100 | Reason: test-only | allowlist-expiry: 2099-12-31
commits = ["deadbeef"]

[[rules.allowlists]]
regexes = ["deadbeef"]
EOF
if SECURITY_ALLOWLIST_ROOT="$fixture_root" "$ROOT/scripts/validate-security-allowlist.sh" >/dev/null 2>&1; then
  echo "::error::allowlist fixture accepted a rule-level Gitleaks entry without metadata" >&2
  exit 1
fi
cp "$ROOT/gitleaks.toml" "$fixture_root/gitleaks.toml"

cat > "$fixture_root/gitleaks.toml" <<'EOF'
[[rules.allowlists]]
regexes = ["deadbeef"]
[[rules]]
# Owner: @security-team | Issue: #100 | Reason: neighboring rule | allowlist-expiry: 2099-12-31
description = "not an allowlist"
EOF
if SECURITY_ALLOWLIST_ROOT="$fixture_root" "$ROOT/scripts/validate-security-allowlist.sh" >/dev/null 2>&1; then
  echo "::error::allowlist fixture misattributed metadata from a neighboring rule" >&2
  exit 1
fi
cp "$ROOT/gitleaks.toml" "$fixture_root/gitleaks.toml"

cat > "$fixture_root/gitleaks.toml" <<'EOF'
[allowlist]
commits =
[
  "deadbeef",
]
EOF
if SECURITY_ALLOWLIST_ROOT="$fixture_root" "$ROOT/scripts/validate-security-allowlist.sh" >/dev/null 2>&1; then
  echo "::error::allowlist fixture accepted a split-line Gitleaks array without metadata" >&2
  exit 1
fi
cp "$ROOT/gitleaks.toml" "$fixture_root/gitleaks.toml"

printf '%s' '[allowlist]
commits = ["deadbeef"]' > "$fixture_root/gitleaks.toml"
if SECURITY_ALLOWLIST_ROOT="$fixture_root" "$ROOT/scripts/validate-security-allowlist.sh" >/dev/null 2>&1; then
  echo "::error::allowlist fixture accepted a Gitleaks config without a final newline or metadata" >&2
  exit 1
fi
cp "$ROOT/gitleaks.toml" "$fixture_root/gitleaks.toml"

sed 's/2026-12-31/2099-99-99/' "$ROOT/gitleaks.toml" > "$fixture_root/gitleaks.toml"
if SECURITY_ALLOWLIST_ROOT="$fixture_root" "$ROOT/scripts/validate-security-allowlist.sh" >/dev/null 2>&1; then
  echo "::error::allowlist fixture accepted an invalid calendar date" >&2
  exit 1
fi
cp "$ROOT/gitleaks.toml" "$fixture_root/gitleaks.toml"

echo "security allowlist pass/fail fixtures behaved as expected"
