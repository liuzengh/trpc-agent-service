#!/bin/sh
# Recompile the owner inputs, compare the frozen wire fixture, then execute the
# actual SDK assembly chain. This does not use live providers/backends/services.
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
TEMP=$(mktemp -d "${TMPDIR:-/tmp}/worker-manifest-sdk.XXXXXX")
trap 'rm -rf "$TEMP"' EXIT HUP INT TERM
cd "$ROOT"
CONTROL_WORKER_SDK_FIXTURE_DIR="$TEMP" go test \
  ./services/control-api/internal/deployment/domain \
  -run '^TestExportWorkerSDKMemoryFixture$' -count=1 -v
cmp "$TEMP/runtime-manifest.json" \
  services/agent-worker/internal/execution/adapter/outbound/trpcagent/testdata/compiler-memory-manifest.json
printf 'COMPILER_FIXTURE_MATCH=PASS\n'
go test -race ./services/agent-worker/internal/execution/adapter/outbound/trpcagent \
  -run 'TestManifestCapability|TestCompiledManifestRunsSDKMemoryCycle|TestCompiledMemoryModelRequiresRecallResult' -count=3 -v
