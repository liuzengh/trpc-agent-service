#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"

test -f go.mod
if grep -Eq '^[[:space:]]*replace[[:space:]]' go.mod; then
  echo "go.mod must not contain replace directives" >&2
  exit 1
fi
for command in gateway worker summary-worker consumer delivery admin migrate data-migrate replay queue-bench; do
  test -f "cmd/$command/main.go"
done
if grep -RIEq 'trpc\.group/trpc-go/trpc-agent-go/enterprise/cmd/' cmd pkg --include='*.go'; then
  echo "cmd packages must not import another command package" >&2
  exit 1
fi
for migration in migrations/*.up.sql; do
  test -f "${migration%.up.sql}.down.sql"
done
test -f deploy/kubernetes/network-policies.yaml
test -f deploy/kubernetes/runtime-data-plane-config.yaml
for policy in default-deny gateway-boundaries consumer-boundaries worker-boundaries worker-runtime-data-plane-egress summary-worker-boundaries delivery-boundaries admin-boundaries; do
  grep -q "name: $policy" deploy/kubernetes/network-policies.yaml
done
grep -q 'ADMIN_PRINCIPALS_JSON' cmd/admin/admin_bootstrap.go
grep -q 'WORKER_CACHE_SIZE' cmd/worker/bootstrap.go
grep -q 'STORAGE_BACKEND_PROFILES' cmd/worker/bootstrap.go
grep -q 'DATA_PLANE_PROFILES' cmd/worker/bootstrap.go
grep -q 'EXECUTION_LEASE_TTL' cmd/worker/bootstrap.go
grep -q 'EXECUTION_HEARTBEAT_INTERVAL' cmd/worker/bootstrap.go
while IFS= read -r -d '' source_file; do
  set +e
  grep -HnIE 'TODO|FIXME|not implemented' "$source_file"
  grep_status=$?
  set -e
  if ((grep_status == 0)); then
    echo "forbidden unfinished marker in production Go source" >&2
    exit 1
  fi
  if ((grep_status > 1)); then
    echo "failed to scan production Go source: $source_file" >&2
    exit "$grep_status"
  fi
done < <(find cmd pkg -type f -name '*.go' ! -name '*_test.go' -print0)
for shell_script in scripts/*.sh; do
  bash -n "$shell_script"
done
echo "static verification passed (this is not a compile/test result)"
