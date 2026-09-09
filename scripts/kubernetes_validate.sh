#!/usr/bin/env bash
set -euo pipefail

command -v kubectl >/dev/null 2>&1 || {
  echo "kubectl is required to render Kubernetes manifests" >&2
  exit 1
}

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
render_dir="$(mktemp -d "${TMPDIR:-/tmp}/trpc-agent-k8s.XXXXXX")"
trap 'rm -rf "$render_dir"' EXIT

kubectl kustomize "$repo_root/deploy/kubernetes/base" >"$render_dir/base.yaml"
kubectl kustomize "$repo_root/deploy/kubernetes/migration" >"$render_dir/migration.yaml"
kubectl kustomize "$repo_root/deploy/kubernetes/demo/infra" >"$render_dir/demo-infra.yaml"
kubectl kustomize "$repo_root/deploy/kubernetes/demo/app" >"$render_dir/demo-app.yaml"
kubectl kustomize "$repo_root/deploy/kubernetes/demo/migration" >"$render_dir/demo-migration.yaml"
cp "$repo_root/deploy/kubernetes/demo/metrics-server.yaml" "$render_dir/demo-metrics-server.yaml"

require_text() {
  local pattern="$1" file="$2"
  grep -Eq -- "$pattern" "$file" || {
    echo "missing required manifest pattern: $pattern" >&2
    exit 1
  }
}

for kind in Namespace ServiceAccount ConfigMap Service Deployment PodDisruptionBudget HorizontalPodAutoscaler NetworkPolicy; do
  require_text "^kind: ${kind}$" "$render_dir/base.yaml"
done
require_text '^kind: Job$' "$render_dir/migration.yaml"
require_text 'path: /readyz' "$render_dir/base.yaml"
require_text 'path: /healthz' "$render_dir/base.yaml"
require_text 'secretKeyRef:' "$render_dir/base.yaml"
require_text 'automountServiceAccountToken: false' "$render_dir/base.yaml"
require_text 'readOnlyRootFilesystem: true' "$render_dir/base.yaml"
require_text 'maxUnavailable: 0' "$render_dir/base.yaml"
require_text 'name: trpc-agent-gateway' "$render_dir/base.yaml"
require_text 'name: trpc-agent-worker' "$render_dir/base.yaml"
require_text 'app.kubernetes.io/component: gateway' "$render_dir/base.yaml"
require_text 'app.kubernetes.io/component: worker' "$render_dir/base.yaml"
require_text 'name: trpc-agent-worker-deny-ingress' "$render_dir/base.yaml"

deployment_count="$(grep -c '^kind: Deployment$' "$render_dir/base.yaml")"
hpa_count="$(grep -c '^kind: HorizontalPodAutoscaler$' "$render_dir/base.yaml")"
if [[ "$deployment_count" != "2" || "$hpa_count" != "2" ]]; then
  echo "base must render exactly two role Deployments and two HPAs" >&2
  exit 1
fi
if ! grep -A190 '^  name: trpc-agent-gateway$' "$render_dir/base.yaml" | grep -q -- '- gateway'; then
  echo "Gateway Deployment must select the gateway process role" >&2
  exit 1
fi
if ! grep -A190 '^  name: trpc-agent-worker$' "$render_dir/base.yaml" | grep -q -- '- worker'; then
  echo "Worker Deployment must select the worker process role" >&2
  exit 1
fi

if grep -Eq '^kind: Secret$|^[[:space:]]*stringData:' "$render_dir/base.yaml" "$render_dir/migration.yaml"; then
  echo "rendered manifests must not contain Kubernetes Secret values" >&2
  exit 1
fi
if grep -Eq '^kind: Secret$|^[[:space:]]*stringData:' "$render_dir"/demo-*.yaml; then
  echo "demo manifests must not contain Kubernetes Secret values" >&2
  exit 1
fi
require_text '^kind: StatefulSet$' "$render_dir/demo-infra.yaml"
require_text 'name: demo-postgres' "$render_dir/demo-infra.yaml"
require_text 'name: demo-redis' "$render_dir/demo-infra.yaml"
require_text 'name: demo-model' "$render_dir/demo-infra.yaml"
require_text 'name: demo-otel-collector' "$render_dir/demo-infra.yaml"
require_text 'trpc-agent-service:kubernetes-demo' "$render_dir/demo-app.yaml"
require_text 'GATEWAY_TOKEN' "$render_dir/demo-app.yaml"
require_text '^kind: Job$' "$render_dir/demo-migration.yaml"
require_text 'registry.k8s.io/metrics-server/metrics-server:v0.8.0' "$render_dir/demo-metrics-server.yaml"
require_text '--kubelet-insecure-tls' "$render_dir/demo-metrics-server.yaml"
if grep -Eq '^kind: Job$' "$render_dir/base.yaml"; then
  echo "migration Job must remain separate from the service rollout" >&2
  exit 1
fi

echo "Kubernetes manifests rendered and passed structural safety checks"
