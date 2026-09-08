#!/usr/bin/env bash
# Safe default: no .env loading, live DSNs, service restart or external IM calls.
set -euo pipefail
REGRESSION_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REGRESSION_ROOT"
REGRESSION_ISOLATED="${TRPC_AGENT_VERIFY_ISOLATED:-0}"
case "$REGRESSION_ISOLATED" in
  0|1) ;;
  *) echo 'TRPC_AGENT_VERIFY_ISOLATED must be 0 or 1' >&2; exit 2 ;;
esac

# Do not accidentally enable an integration suite against inherited live data.
unset TEST_POSTGRES_URL TEST_REDIS_URL TEST_S3_ENDPOINT TEST_QDRANT_HOST TEST_QDRANT_PORT
unset TEST_TRACE_OTLP_ENDPOINT TEST_PERMISSIONS_DOCKER TEST_RECOVERY_DOCKER
unset TEST_SANDBOX_DOCKER TEST_ADMIN_UI_BROWSER

go test -race ./...
./lint.sh
go build ./...
git diff --check

if [[ "$REGRESSION_ISOLATED" == 1 ]]; then
  TEST_PERMISSIONS_DOCKER=1 go test -race -count=1 ./deploy/permissions -run TestRedisACLIntegration
  TEST_RECOVERY_DOCKER=1 go test -race -count=1 ./trpcservice/recovery
  TEST_SANDBOX_DOCKER=1 go test -race -count=1 ./trpcservice/workspace ./trpcservice/agent -run 'Test(DockerSandboxIntegration|SkillRunnerDockerIntegration)$'
  ./scripts/e2e-backup-restore.sh
  docker run --rm --pull=never --network none \
    -v "$REGRESSION_ROOT/deploy/compose:/rules:ro" --workdir /rules \
    --entrypoint /bin/promtool prom/prometheus:v3.5.0 test rules prometheus-rules.test.yaml
fi
echo "regression passed (isolated Docker checks=$REGRESSION_ISOLATED); running application unchanged"
