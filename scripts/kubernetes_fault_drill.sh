#!/usr/bin/env bash
set -euo pipefail

namespace="${TRPC_AGENT_DRILL_NAMESPACE:-trpc-agent}"
component="${TRPC_AGENT_DRILL_COMPONENT:-worker}"
case "$component" in gateway|worker) ;; *) echo "component must be gateway or worker" >&2; exit 1 ;; esac
deployment="trpc-agent-${component}"
selector="app.kubernetes.io/name=trpc-agent-service,app.kubernetes.io/component=${component}"
mode="${1:---preview}"

command -v kubectl >/dev/null 2>&1 || { echo "kubectl is required" >&2; exit 1; }
case "$namespace" in
  default|kube-system|kube-public|kube-node-lease|"")
    echo "refusing unsafe drill namespace: $namespace" >&2
    exit 1
    ;;
esac
if [[ "$mode" != "--preview" && "$mode" != "--execute" ]]; then
  echo "usage: $0 [--preview|--execute]" >&2
  exit 1
fi

pods=()
while IFS= read -r pod; do
  [[ -n "$pod" ]] && pods+=("$pod")
done < <(kubectl -n "$namespace" get pods -l "$selector" \
  --field-selector=status.phase=Running -o jsonpath='{range .items[?(@.status.containerStatuses[0].ready==true)]}{.metadata.name}{"\n"}{end}')
if (( ${#pods[@]} < 3 )); then
  echo "pod-restart drill requires at least 3 ready replicas; found ${#pods[@]}" >&2
  exit 1
fi
target="${pods[0]}"

echo "drill=pod-restart namespace=$namespace deployment=$deployment target=$target ready_before=${#pods[@]}"
if [[ "$mode" == "--preview" ]]; then
  echo "preview only: one Pod would be deleted, then rollout and readiness would be verified"
  echo "execute only after active traffic/alerts are observed: TRPC_AGENT_DRILL_CONFIRM=pod-restart $0 --execute"
  exit 0
fi
if [[ "${TRPC_AGENT_DRILL_CONFIRM:-}" != "pod-restart" ]]; then
  echo "execution requires TRPC_AGENT_DRILL_CONFIRM=pod-restart" >&2
  exit 1
fi

kubectl -n "$namespace" delete pod "$target" --wait=false
kubectl -n "$namespace" rollout status "deployment/$deployment" --timeout=10m
kubectl -n "$namespace" wait --for=condition=Ready pod -l "$selector" --timeout=10m
ready_after="$(kubectl -n "$namespace" get pods -l "$selector" \
  --field-selector=status.phase=Running -o jsonpath='{range .items[?(@.status.containerStatuses[0].ready==true)]}{.metadata.name}{"\n"}{end}' | wc -l | tr -d ' ')"
if (( ready_after < 3 )); then
  echo "drill failed: only $ready_after Pods are ready after rollout" >&2
  exit 1
fi
echo "pod-restart drill passed; ready_after=$ready_after"
