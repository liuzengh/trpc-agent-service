#!/usr/bin/env bash
# Uses only an explicitly supplied cluster context.
set -euo pipefail

: "${KUBE_CONTEXT:?KUBE_CONTEXT is required}"
namespace="${KUBE_NAMESPACE:-trpc-agent}"
args=(--context "$KUBE_CONTEXT" --namespace "$namespace")

kubectl "${args[@]}" get namespace "$namespace" >/dev/null
kubectl "${args[@]}" auth can-i get deployments | grep -qx yes
for deployment in trpc-agent-gateway trpc-agent-worker; do
  kubectl "${args[@]}" rollout status "deployment/$deployment" --timeout="${KUBE_ROLLOUT_TIMEOUT:-5m}"
done
for component in gateway worker; do
  kubectl "${args[@]}" get pods \
    -l "app.kubernetes.io/name=trpc-agent-service,app.kubernetes.io/component=$component" \
    -o jsonpath='{range .items[?(@.status.containerStatuses[0].ready)]}{.metadata.name}{"\n"}{end}' | grep -q .
done
echo "Kubernetes smoke passed for gateway + worker in $namespace on $KUBE_CONTEXT"
