#!/usr/bin/env bash
# Builds, loads, deploys, and verifies the complete application in ephemeral Kind.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

cluster="${KIND_CLUSTER_NAME:-trpc-agent}"
context="${KUBE_CONTEXT:-kind-${cluster}}"
namespace="${KUBE_NAMESPACE:-trpc-agent}"
evidence="$root/deploy/kubernetes/kind/evidence"
kind_bin="${KIND_BIN:-kind}"
if ! command -v "$kind_bin" >/dev/null 2>&1 && [[ -x "$root/.toolchain/bin/kind" ]]; then
  kind_bin="$root/.toolchain/bin/kind"
fi
command -v docker >/dev/null
command -v kubectl >/dev/null
command -v curl >/dev/null
if [[ "$kind_bin" == */* ]]; then
  [[ -x "$kind_bin" ]]
else
  command -v "$kind_bin" >/dev/null
fi
mkdir -p "$evidence"
rm -f "$evidence/pods.txt" "$evidence/jobs.txt" "$evidence/readyz.txt" \
  "$evidence/readyz-body.txt" "$evidence/smoke.txt" "$evidence/events.txt" \
  "$evidence/port-forward.log"


if ! "$kind_bin" get clusters | grep -Fxq "$cluster"; then
  "$kind_bin" create cluster --name "$cluster" --wait 120s
fi

if [[ "${KIND_SKIP_BUILD:-0}" != "1" ]]; then
docker build -t trpc-agent-service:kind .
docker build -t trpc-mock-openai:kind -f deploy/kubernetes/kind/mock-openai.Dockerfile .
"$kind_bin" load docker-image --name "$cluster" trpc-agent-service:kind trpc-mock-openai:kind
fi

kubectl --context "$context" apply -f deploy/kubernetes/namespace.yaml
kubectl --context "$context" -n "$namespace" delete events --all --ignore-not-found >/dev/null
kubectl --context "$context" apply -f deploy/kubernetes/kind/secret.yaml
kubectl --context "$context" -n "$namespace" delete job minio-bucket-init --ignore-not-found
kubectl --context "$context" apply -f deploy/kubernetes/kind/dependencies.yaml

for dependency in postgres redis redpanda minio mock-openai; do
  kubectl --context "$context" -n "$namespace" rollout status "deployment/$dependency" --timeout=5m
done
kubectl --context "$context" -n "$namespace" wait --for=condition=complete job/minio-bucket-init --timeout=3m

kubectl --context "$context" -n "$namespace" delete job trpc-agent-service-migrate --ignore-not-found
sed 's#ghcr.io/example/trpc-agent-service:replace-me#trpc-agent-service:kind#' \
  deploy/kubernetes/migration-job.yaml |
  kubectl --context "$context" apply -f -
kubectl --context "$context" -n "$namespace" wait --for=condition=complete job/trpc-agent-service-migrate --timeout=5m

mapfile -t old_service_pods < <(kubectl --context "$context" -n "$namespace" get pods \
  -l 'app.kubernetes.io/name=trpc-agent-service,pod-template-hash' -o name)
kubectl kustomize --load-restrictor=LoadRestrictionsNone deploy/kubernetes/kind |
  kubectl --context "$context" apply -f -
for deployment in trpc-agent-gateway trpc-agent-worker; do
  kubectl --context "$context" -n "$namespace" rollout restart "deployment/$deployment"
  kubectl --context "$context" -n "$namespace" rollout status "deployment/$deployment" --timeout=5m
done
for old_pod in "${old_service_pods[@]}"; do
  kubectl --context "$context" -n "$namespace" wait --for=delete "$old_pod" --timeout=2m
done

forward_log="$evidence/port-forward.log"
kubectl --context "$context" -n "$namespace" port-forward service/trpc-agent-service 18080:80 >"$forward_log" 2>&1 &
forward_pid=$!
cleanup() {
  kill "$forward_pid" >/dev/null 2>&1 || true
  wait "$forward_pid" >/dev/null 2>&1 || true
}
trap cleanup EXIT

ready=0
for _ in $(seq 1 60); do
  if curl --fail --silent http://127.0.0.1:18080/readyz >"$evidence/readyz.txt" 2>/dev/null; then
    ready=1
    break
  fi
  sleep 2
done
if [[ "$ready" -ne 1 ]]; then
  echo "application /readyz did not become available" >&2
  exit 1
fi
curl --fail --silent --show-error --dump-header "$evidence/readyz.txt" \
  --output /dev/null http://127.0.0.1:18080/readyz

KUBE_CONTEXT="$context" KUBE_NAMESPACE="$namespace" scripts/kubernetes-smoke.sh |
  tee "$evidence/smoke.txt"
kubectl --context "$context" -n "$namespace" get pods -o wide >"$evidence/pods.txt"
kubectl --context "$context" -n "$namespace" get jobs -o wide >"$evidence/jobs.txt"
echo "Kind acceptance passed; evidence: $evidence"
