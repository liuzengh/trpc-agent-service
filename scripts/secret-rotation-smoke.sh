#!/usr/bin/env bash
# Proves a provider-managed secret rotation causes a healthy rollout.
set -euo pipefail

: "${KUBE_CONTEXT:?KUBE_CONTEXT is required}"
: "${SECRET_ROTATION_COMMAND:?SECRET_ROTATION_COMMAND is required}"
namespace="${KUBE_NAMESPACE:-trpc-agent}"

case "$SECRET_ROTATION_COMMAND" in
  /*) ;;
  *)
    echo "SECRET_ROTATION_COMMAND must be an absolute executable path, not shell text" >&2
    exit 2
    ;;
esac
if [[ ! -x "$SECRET_ROTATION_COMMAND" ]]; then
  echo "SECRET_ROTATION_COMMAND is not executable: $SECRET_ROTATION_COMMAND" >&2
  exit 2
fi

"$SECRET_ROTATION_COMMAND"
for deployment in trpc-agent-gateway trpc-agent-worker; do
  kubectl --context "$KUBE_CONTEXT" --namespace "$namespace" rollout restart "deployment/$deployment"
  kubectl --context "$KUBE_CONTEXT" --namespace "$namespace" rollout status "deployment/$deployment" --timeout="${KUBE_ROLLOUT_TIMEOUT:-5m}"
done
KUBE_CONTEXT="$KUBE_CONTEXT" KUBE_NAMESPACE="$namespace" scripts/kubernetes-smoke.sh
