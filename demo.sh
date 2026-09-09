#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$repo_root"

command -v go >/dev/null 2>&1 || {
  echo "FAIL Go toolchain is required" >&2
  exit 1
}

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/trpc-agent-demo.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT
export GOCACHE="${GOCACHE:-${TMPDIR:-/tmp}/trpc-agent-service-demo-cache}"

run_check() {
  local name="$1"
  shift
  if "$@" >"$work_dir/check.log" 2>&1; then
    echo "PASS $name"
    return
  fi
  echo "FAIL $name (run the documented command directly for diagnostics)" >&2
  exit 1
}

echo "tRPC-Agent-Go multi-tenant platform: offline evaluator demo"
echo "No Docker, external model, IM account, database, or credential is used."

run_check "Go modules are verified" go mod verify
run_check "tRPC-Agent-Go Runner executes with the deterministic model" \
  go run ./examples/quickstart ./configs/demo.yaml
run_check "two tenants and two Workers preserve routing, state, outbox, and trace isolation" \
  go test -count=1 ./trpcservice/gateway/openclaw \
  -run '^(TestTwoTenantTwoWorkerEndToEndToolMemoryOutboxAndTrace|TestDeniedIMIdentityNeverBuildsOrCallsRunner|TestGatewayModeHasNoWorkerRuntimeOrConsumers|TestWorkerModeHasNoHTTPServer)$'
run_check "WeCom encrypted callback, idempotency, and controlled media path work" \
  go test -count=1 ./trpcservice/channels/wecom \
  -run '^(TestEncryptedCallbackEntersDurableGatewayOnce|TestControlledImageDownloadEntersRunnerWithoutProviderKey|TestDynamicBindingDisambiguatesTenantAndFailsClosed)$'
run_check "Feishu encrypted callback, duplicate delivery, and tenant isolation work" \
  go test -count=1 ./trpcservice/channels/feishu \
  -run '^(TestEncryptedEventDecryptsAndEntersGateway|TestDuplicateEventIDClaimsInboxOnce|TestTenantsSharingAppIDAndBindingIDStayIsolated|TestControlledFileDownloadExtractsTextAndDropsProviderKey)$'
run_check "tenant IM ACL and safe Session/Memory metrics work" \
  go test -count=1 ./trpcservice/policy ./trpcservice/storage ./trpcservice/metrics \
  -run '^(TestIMIdentityAccessIsTenantScopedAndRedactedFromToolContext|TestObservedSessionAndMemoryExposeOnlyBoundedScope|TestObservedSessionClassifiesFailureWithoutExportingError|TestStorageOperationEmitsDurationAndFailureCounter)$'

echo "PASS offline evaluator demo completed"
echo "Run ./check.sh for the full repository gate; use docs/production-acceptance.md for Compose and Kubernetes acceptance."
