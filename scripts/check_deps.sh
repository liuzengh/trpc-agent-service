#!/usr/bin/env bash
# Dependency freeze gate.
#
#   scripts/check_deps.sh            offline gate: verify + baseline diff
#   scripts/check_deps.sh --tidy     also require `go mod tidy` to be a no-op
#   scripts/check_deps.sh --update   regenerate docs/依赖基线.txt
#
# Every mode runs offline: `go mod tidy` only resolves against the warm module
# cache, `go mod verify` only hashes it, and the baseline diff reads go.mod as
# text. That keeps the gate usable in CI and on a machine with no proxy access.
# Written for bash 3.2 (the macOS default), so no `;&` case fallthrough and no
# GNU-only grep BRE. Drift is detected by comparing file snapshots rather than
# `git diff`, so the gate also works outside a git checkout and does not
# misreport when go.mod already carries unrelated uncommitted edits.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

BASELINE="docs/依赖基线.txt"

# direct_deps prints the module's own go/toolchain directives plus every
# direct require of go.mod as "path version", sorted. Indirect requires are
# skipped: go.sum already pins them, and listing them would make the baseline
# churn on every transitive bump.
direct_deps() {
  awk '
    /^[ \t]*\/\// { next }                       # comments
    /^go /        { print "go", $2; next }
    /^toolchain / { print "toolchain", $2; next }
    /^require \(/ { inreq = 1; next }
    /^\)/         { inreq = 0; next }
    /^require /   { if ($0 !~ /\/\/ indirect/) print $2, $3; next }
    inreq         { if ($0 !~ /\/\/ indirect/ && NF >= 2) print $1, $2 }
  ' go.mod | sort
}

baseline_deps() {
  grep -v '^#' "$BASELINE" | grep -v '^[[:space:]]*$' | sort
}

dep_count() {
  baseline_deps | grep -v '^go ' | grep -v '^toolchain ' | wc -l | tr -d ' '
}

want_tidy=0
case "${1:-}" in
  --update)
    {
      sed -n '/^#/p' "$BASELINE"
      direct_deps
    } > "$BASELINE.tmp"
    mv "$BASELINE.tmp" "$BASELINE"
    echo "updated: $BASELINE ($(dep_count) direct dependencies)"
    exit 0
    ;;
  --tidy)
    want_tidy=1
    ;;
  "")
    ;;
  *)
    echo "usage: $0 [--tidy|--update]" >&2
    exit 2
    ;;
esac

if [ "$want_tidy" = 1 ]; then
  before_mod="$(mktemp)"
  before_sum="$(mktemp)"
  trap 'rm -f "$before_mod" "$before_sum"' EXIT
  cp go.mod "$before_mod"
  cp go.sum "$before_sum" 2>/dev/null || true

  go mod tidy

  drift=0
  diff -u "$before_mod" go.mod || drift=1
  if [ -s "$before_sum" ]; then
    diff -u "$before_sum" go.sum || drift=1
  fi
  if [ "$drift" = 1 ]; then
    cat >&2 <<MSG
FAIL: go mod tidy is not a no-op (diff above).
Freeze policy: the committed go.mod/go.sum must already be tidy. Either commit
the tidied result, or revert the edit that made tidy change something.
MSG
    exit 1
  fi
  echo "ok: go mod tidy is idempotent"
fi

go mod verify

if ! diff -u <(baseline_deps) <(direct_deps); then
  cat >&2 <<MSG
FAIL: direct dependencies drifted from $BASELINE.
Freeze policy: adding or bumping a third-party module is a deliberate act —
rerun '$0 --update' and let the change show up in review.
MSG
  exit 1
fi

echo "ok: $(dep_count) direct dependencies match the frozen baseline"
echo "ok: go mod verify"
