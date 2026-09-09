#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"

command -v kubectl >/dev/null || { echo "required tool missing: kubectl" >&2; exit 127; }

platform_namespace="${PLATFORM_NAMESPACE:-}"
if [[ -z "$platform_namespace" ]]; then
  echo "PLATFORM_NAMESPACE must explicitly name the dedicated application namespace" >&2
  exit 1
fi
release_manifest_dir="${RELEASE_MANIFEST_DIR:-}"
if [[ -z "$release_manifest_dir" || ! -d "$release_manifest_dir" ]]; then
  echo "RELEASE_MANIFEST_DIR must name a rendered, digest-pinned release bundle directory" >&2
  exit 1
fi
network_policy_file="${NETWORK_POLICY_FILE:-}"
rollout_timeout="${ROLLOUT_TIMEOUT:-10m}"
if [[ -z "$network_policy_file" || ! -f "$network_policy_file" ]]; then
  echo "NETWORK_POLICY_FILE must name the reviewed in-cluster or managed-backend policy overlay" >&2
  exit 1
fi
release_schema_class="${RELEASE_SCHEMA_CLASS:-}"
case "$release_schema_class" in
  bootstrap|compatible|breaking) ;;
  *)
    echo "RELEASE_SCHEMA_CLASS must be bootstrap, compatible, or breaking" >&2
    exit 1
    ;;
esac

release_files=(
  "$release_manifest_dir/migration.yaml"
  "$release_manifest_dir/profiles.yaml"
  "$release_manifest_dir/availability-policies.yaml"
  "$release_manifest_dir/worker.yaml"
  "$release_manifest_dir/summary.yaml"
  "$release_manifest_dir/pipeline.yaml"
  "$release_manifest_dir/admin.yaml"
  "$release_manifest_dir/gateway.yaml"
)
application_workloads=(
  agent-gateway
  agent-consumer
  agent-worker
  agent-summary-worker
  agent-delivery
  agent-admin
)
for release_file in "${release_files[@]}"; do
  if [[ ! -f "$release_file" ]]; then
    echo "release bundle is missing $release_file" >&2
    exit 1
  fi
done

# Validate the rendered artifacts before any cluster mutation. The base files
# in deploy/kubernetes are intentionally not a release: their image tags and
# mesh assertion are placeholders until CI renders real image digests and a
# cluster-specific confidential-transport overlay.
release_verify_args=()
for release_file in "${release_files[@]}"; do
  release_verify_args+=(--manifest "$release_file")
done
if [[ -n "${RELEASE_VERIFY_BIN:-}" ]]; then
  if [[ ! -x "$RELEASE_VERIFY_BIN" ]]; then
    echo "RELEASE_VERIFY_BIN must name an executable release verifier" >&2
    exit 1
  fi
  "$RELEASE_VERIFY_BIN" "${release_verify_args[@]}" \
    --network-policy "$network_policy_file" \
    --schema-class "$release_schema_class" \
    --breaking-change-id "${BREAKING_MIGRATION_CHANGE_ID:-}"
elif command -v go >/dev/null; then
  GOTOOLCHAIN=local go run ./cmd/releaseverify "${release_verify_args[@]}" \
    --network-policy "$network_policy_file" \
    --schema-class "$release_schema_class" \
    --breaking-change-id "${BREAKING_MIGRATION_CHANGE_ID:-}"
else
  echo "release verification requires RELEASE_VERIFY_BIN or a local Go toolchain" >&2
  exit 127
fi

for access in public-ingress observability admin-ingress egress-gateway data-plane; do
  if [[ -z "$(kubectl get namespace -l "agent-platform-access=$access" -o name)" ]]; then
    echo "no namespace is labelled agent-platform-access=$access" >&2
    exit 1
  fi
done

require_workload_absent() {
  local workload="$1"
  if kubectl -n "$platform_namespace" get deployment "$workload" >/dev/null 2>&1; then
    echo "$workload already exists; bootstrap releases must not cross an active runtime boundary" >&2
    exit 1
  fi
  local pods
  pods="$(kubectl -n "$platform_namespace" get pods -l "app=$workload" -o name)"
  if [[ -n "$pods" ]]; then
    echo "$workload Pods already exist; bootstrap releases must not cross an active runtime boundary" >&2
    exit 1
  fi
}

require_workload_stopped() {
  local workload="$1"
  local pods
  pods="$(kubectl -n "$platform_namespace" get pods -l "app=$workload" -o name)"
  if ! kubectl -n "$platform_namespace" get deployment "$workload" >/dev/null 2>&1; then
    if [[ -n "$pods" ]]; then
      echo "$workload has Pods without a Deployment; remove them before a breaking migration" >&2
      exit 1
    fi
    return
  fi
  local desired replicas
  desired="$(kubectl -n "$platform_namespace" get deployment "$workload" -o jsonpath='{.spec.replicas}')"
  replicas="$(kubectl -n "$platform_namespace" get deployment "$workload" -o jsonpath='{.status.replicas}')"
  if [[ "${desired:-1}" != "0" || "${replicas:-0}" != "0" || -n "$pods" ]]; then
    echo "$workload must be scaled to zero with no remaining Pods before a breaking migration" >&2
    exit 1
  fi
}

case "$release_schema_class" in
  bootstrap)
    for workload in "${application_workloads[@]}"; do
      require_workload_absent "$workload"
    done
    ;;
  breaking)
    if [[ "${BREAKING_MIGRATION_APPROVED:-}" != "true" ]]; then
      echo "BREAKING_MIGRATION_APPROVED=true is required after the documented drain review" >&2
      exit 1
    fi
    if [[ ! "${BREAKING_MIGRATION_CHANGE_ID:-}" =~ ^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$ ]]; then
      echo "BREAKING_MIGRATION_CHANGE_ID must be a bounded change record identifier" >&2
      exit 1
    fi
    if [[ ! -s "${BREAKING_MIGRATION_DRAIN_EVIDENCE:-}" ]]; then
      echo "BREAKING_MIGRATION_DRAIN_EVIDENCE must name a non-empty drain evidence artifact" >&2
      exit 1
    fi
    for workload in "${application_workloads[@]}"; do
      require_workload_stopped "$workload"
    done
    ;;
esac

# Secrets must be provisioned out-of-band (External Secrets/KMS/Vault or
# kubectl create secret). The checked-in secrets-example.yaml is never applied.
kubectl -n "$platform_namespace" get secret \
  db-credentials redis-credentials tenant-storage-credentials otel-collector-tls \
  runtime-data-plane-credentials master-key service-auth audit-identity metrics-auth admin-api-token >/dev/null

# Establish deny-by-default before starting application workloads. The verified
# policy must contain exact managed DB/Redis routing and force Worker/Delivery
# external traffic through the separately operated egress gateway.
kubectl -n "$platform_namespace" apply -f "$network_policy_file"

# The public profile catalog is part of the signed/rendered release and is
# validated above. It contains endpoint metadata and Secret env names only;
# credential values stay in the separately provisioned Worker-only Secret.
kubectl -n "$platform_namespace" apply -f "$release_manifest_dir/profiles.yaml"

# Kubernetes does not order a Job and Deployment merely because they share a
# directory. Never delete a running or failed migration: killing it can leave
# an operator with an unknown schema state. A completed Job is removed only so
# this release can run the migration binary and verify checksums again.
if kubectl -n "$platform_namespace" get job agent-migrate >/dev/null 2>&1; then
  active="$(kubectl -n "$platform_namespace" get job agent-migrate -o jsonpath='{.status.active}')"
  failed="$(kubectl -n "$platform_namespace" get job agent-migrate -o jsonpath='{.status.failed}')"
  if [[ "${active:-0}" != "0" ]]; then
    echo "agent-migrate is active; wait for it to finish before another rollout" >&2
    exit 1
  fi
  if [[ "${failed:-0}" != "0" ]]; then
    echo "agent-migrate failed; inspect and resolve it before another rollout" >&2
    exit 1
  fi
  complete="$(kubectl -n "$platform_namespace" get job agent-migrate -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}')"
  if [[ "$complete" != "True" ]]; then
    echo "agent-migrate is not complete; wait for completion before another rollout" >&2
    exit 1
  fi
  kubectl -n "$platform_namespace" delete job agent-migrate --wait=true
fi
kubectl -n "$platform_namespace" apply -f "$release_manifest_dir/migration.yaml"
kubectl -n "$platform_namespace" wait --for=condition=complete --timeout=32m job/agent-migrate

# Install disruption budgets before their selected workloads. This does not
# create Pods itself, but ensures a subsequent voluntary disruption cannot race
# ahead of the workload rollout.
kubectl -n "$platform_namespace" apply -f "$release_manifest_dir/availability-policies.yaml"

# Bring up control-plane and asynchronous execution before opening public
# intake. Gateway can safely accept a durable Inbox only after the downstream
# paths are ready to make progress. Wait explicitly: `kubectl apply` only
# submits desired state and is not evidence that a Deployment is available.
kubectl -n "$platform_namespace" apply -f "$release_manifest_dir/worker.yaml"
kubectl -n "$platform_namespace" apply -f "$release_manifest_dir/summary.yaml"
kubectl -n "$platform_namespace" apply -f "$release_manifest_dir/admin.yaml"
kubectl -n "$platform_namespace" rollout status --timeout="$rollout_timeout" deployment/agent-worker
kubectl -n "$platform_namespace" rollout status --timeout="$rollout_timeout" deployment/agent-summary-worker
kubectl -n "$platform_namespace" rollout status --timeout="$rollout_timeout" deployment/agent-admin

# Consumer performs a fail-closed Worker startup health check. Submit the
# asynchronous pipeline only after Worker is available so a normal rollout
# cannot turn this dependency check into avoidable crash loops.
kubectl -n "$platform_namespace" apply -f "$release_manifest_dir/pipeline.yaml"
for deployment in agent-consumer agent-delivery; do
  kubectl -n "$platform_namespace" rollout status --timeout="$rollout_timeout" "deployment/$deployment"
done

kubectl -n "$platform_namespace" apply -f "$release_manifest_dir/gateway.yaml"
kubectl -n "$platform_namespace" rollout status --timeout="$rollout_timeout" deployment/agent-gateway
