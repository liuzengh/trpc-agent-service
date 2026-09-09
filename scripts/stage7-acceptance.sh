#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
COMMIT_SHA="$(git rev-parse HEAD)"
SCHEMA_VERSION="$(sed -n 's/^const ControlPlaneSchemaVersion = \([0-9][0-9]*\)$/\1/p' trpcservice/platform/control_plane.go)"
EVIDENCE_DIR="${STAGE7_EVIDENCE_DIR:-$ROOT/.scratch/evidence}"
mkdir -p "$EVIDENCE_DIR"
EVIDENCE_PATH="$EVIDENCE_DIR/stage7-${COMMIT_SHA}-schema-${SCHEMA_VERSION}.json"

./format.sh
go test ./...
go test -race ./...
./lint.sh
./build.sh
npm --prefix frontend run typecheck
npm --prefix frontend test
npm --prefix frontend run build
npm --prefix frontend run test:e2e
go test ./trpcservice/platform -run 'TestSQLiteControlPlaneSurvivesGatewayRestart|TestOpenAICompatibleModelStreamsThroughPublicChatSSE|TestResponsesModelStreamsThroughPublicChatSSE|TestRemoteWorker|TestRemoteRunnerUnexpectedEOFEmitsFailedTerminal|TestExecutionManifest|TestPostgresSessionLease|TestRuntimeStreamEmitsCancelledWhenLeaseIsLostWithoutWorkerEvent|TestGovernanceMarksExecutingToolOutcomeUnknownAfterWorkerLoss|TestMemoryKnowledgeArtifactAndTraceCompletePublicWorkflow' -count=1
./scripts/verify-docs.sh
if [[ "${STAGE7_SKIP_COMPOSE:-0}" != "1" ]]; then
  ./scripts/stage7-compose-acceptance.sh
fi

jq -n --arg commit "$COMMIT_SHA" --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --argjson schema "$SCHEMA_VERSION" \
  '{stage:7,commit_sha:$commit,control_plane_schema_version:$schema,generated_at:$generated_at,result:"passed"}' >"$EVIDENCE_PATH"
echo "Stage 7 acceptance passed; evidence: $EVIDENCE_PATH"
