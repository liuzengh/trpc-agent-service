#!/bin/bash
# P2-04 release gate (fast local checks).
#
# Scope: local/pre-production gate only. Passing this script does NOT mean
# production deployment, production capacity or security clean. It runs the
# cheap, deterministic checks a release candidate must pass before the
# heavier test suites are invoked:
#   1. worktree hygiene (targeted diff-check)
#   2. dependency baseline (go.mod/go.sum reference hashes, overridable)
#   3. migration inventory checksum
#   4. gofmt + go vet
#   5. compose config validity (default profile)
#   6. secret scan for high-signal credential patterns
# Exit code 0 = gate passed; non-zero = the first failed step.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

echo "=== P2-04 release gate ==="

echo "[1/6] targeted diff-check"
git diff --check -- cmd trpcservice scripts docs/implementation-plan.md docs/project-status.md docs/ARCHITECTURE.md
echo "ok"

echo "[2/6] dependency baseline"
EXPECTED_MOD="${P204_EXPECTED_GO_MOD:-50ace4330977e4cc0f57f55965bcaf8a61390d01f8b6c47e1c5d3c49c4d7d5ed}"
EXPECTED_SUM="${P204_EXPECTED_GO_SUM:-23cba731c8cc436c8cab0f6e4df247e793e573b2fb3a32206c5e7918157b43f1}"
ACTUAL_MOD="$(sha256sum go.mod | cut -d' ' -f1)"
ACTUAL_SUM="$(sha256sum go.sum | cut -d' ' -f1)"
if [ "$ACTUAL_MOD" != "$EXPECTED_MOD" ] || [ "$ACTUAL_SUM" != "$EXPECTED_SUM" ]; then
  echo "FAIL: dependency baseline drifted (go.mod=$ACTUAL_MOD go.sum=$ACTUAL_SUM)" >&2
  exit 1
fi
echo "ok"

echo "[3/6] migration inventory"
EXPECTED_MIGRATION_COUNT="${P204_EXPECTED_MIGRATION_FILES:-22}"
ACTUAL_MIGRATION_COUNT="$(ls migrations/*.sql | wc -l)"
if [ "$ACTUAL_MIGRATION_COUNT" != "$EXPECTED_MIGRATION_COUNT" ]; then
  echo "FAIL: migration file count drifted ($ACTUAL_MIGRATION_COUNT != $EXPECTED_MIGRATION_COUNT)" >&2
  exit 1
fi
sha256sum migrations/*.sql > /dev/null
echo "ok ($ACTUAL_MIGRATION_COUNT files)"

echo "[4/6] gofmt + go vet"
UNFORMATTED="$(gofmt -l cmd trpcservice internal 2>/dev/null)"
if [ -n "$UNFORMATTED" ]; then
  echo "FAIL: unformatted files: $UNFORMATTED" >&2
  exit 1
fi
GOFLAGS=-mod=readonly go vet ./... > /dev/null
echo "ok"

echo "[5/6] compose config (default profile)"
ENV_DIR="$(mktemp -d)"
chmod 700 "$ENV_DIR"
printf 'gate' > "$ENV_DIR/pg_password"
printf 'gate' > "$ENV_DIR/pg_runtime_password"
chmod 600 "$ENV_DIR"/*
GATE_ENV="$ENV_DIR/compose.env"
printf 'P109_RUN_ID=p204-gate\nP109_SECRET_DIR=%s\nP202_RUN_ID=p204-gate\nP202_SECRET_DIR=%s\nP109_APP_IMAGE=p204-gate:local\n' "$ENV_DIR" "$ENV_DIR" > "$GATE_ENV"
docker compose --project-directory "$REPO_ROOT" --env-file "$GATE_ENV" config -q
rm -rf "$ENV_DIR"
echo "ok"

echo "[6/6] secret scan (high-signal patterns)"
SCAN_HITS="$(grep -rnI --include='*.go' --include='*.yml' --include='*.yaml' --include='*.sh' \
  -E 'AKIA[0-9A-Z]{16}|-----BEGIN [A-Z ]*PRIVATE KEY-----|[0-9]{8,10}:AA[Ew][A-Za-z0-9_-]{30,}|xox[baprs]-' \
  cmd trpcservice scripts docker-compose.yml Dockerfile Dockerfile.recovery deploy 2>/dev/null | grep -v '_test.go' || true)"
if [ -n "$SCAN_HITS" ]; then
  echo "FAIL: credential-pattern hits" >&2
  echo "$SCAN_HITS" | awk '{print FILENAME}' | head -5 >&2
  exit 1
fi
echo "ok"

echo "=== P2-04 release gate: PASS (local/pre-production only) ==="
echo "This gate does not represent production deployment, capacity, HA or security clean."
