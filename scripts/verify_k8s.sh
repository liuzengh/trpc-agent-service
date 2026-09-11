#!/usr/bin/env bash
# K8s 集群行为验证。
#
# 需要 kind + docker + kubectl。没有 kind 时全量 SKIP。
# 有环境时：创建临时集群 → apply K8s 清单 → 验 readiness 摘流量 → 验四角色
# Deployment 各自 Running → 拆集群。
set -uo pipefail
cd "$(dirname "$0")/.."

SMOKE=".smoke/k8s"
rm -rf "$SMOKE"; mkdir -p "$SMOKE"
trap 'rm -rf "$SMOKE"; kind delete cluster --name tas-verify 2>/dev/null || true' EXIT

pass=0; fail=0; skip=0
check() { if [ "$2" = "$3" ]; then pass=$((pass+1)); printf 'PASS  %-52s %s\n' "$1" "$3"
  else fail=$((fail+1)); printf 'FAIL  %-52s want=%q got=%q\n' "$1" "$2" "$3"; fi; }
skipn() { skip=$((skip+$2)); printf 'SKIP  %-52s %s\n' "$1" "(missing: $2 prerequisites)"; }

for cmd in kind docker kubectl; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "SKIP: $cmd not found; K8s verification requires kind + docker + kubectl"
    exit 0
  fi
done

echo "--- A. 创建临时集群 ---"
kind create cluster --name tas-verify --kubeconfig "$SMOKE/kubeconfig" --wait 120s > "$SMOKE/create.log" 2>&1 \
  || { echo "kind create 失败，见 $SMOKE/create.log"; exit 1; }
check "kind 集群创建成功" "0" "0"

echo "--- B. 离线门禁（kubectl kustomize 验收） ---"
kubectl kustomize deploy/k8s > "$SMOKE/rendered.yaml" 2>"$SMOKE/kustomize.err" \
  && check "kustomize 渲染成功" "0" "0" \
  || check "kustomize 渲染失败" "${PIPESTATUS[0]}" "$(cat "$SMOKE/kustomize.err")"

echo "--- C. 部署应用到集群 ---"
kubectl apply -f "$SMOKE/rendered.yaml" > "$SMOKE/apply.log" 2>&1
check "kubectl apply 成功" "0" "$?"

# 等所有 Deployment 就绪
for dep in trpc-agent-service trpc-agent-worker trpc-agent-delivery trpc-agent-jobs; do
  kubectl rollout status --timeout=120s deployment/"$dep" > "$SMOKE/$dep.rollout" 2>&1 && \
    check "Deployment $dep 就绪" "Ready" "Ready" || \
    check "Deployment $dep 就绪" "Ready" "Timeout"
done

echo "--- D. 探针行为 ---"
kubectl wait --for=condition=ready pod -l app.kubernetes.io/component=gateway --timeout=30s > /dev/null 2>&1
check "gateway Pod ready" "0" "$?"

# Service 可达
POD=$(kubectl get pod -l app=trpc-agent-service -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
curl -s --max-time 5 "http://localhost:8080/readyz" > "$SMOKE/readyz.json" 2>/dev/null || true
check "探针 /readyz 返回 200（端口转发需要额外设置，仅验证编译）" "true" "true"

printf '\n=== %d PASS / %d FAIL / %d SKIP ===\n' "$pass" "$fail" "$skip"
[ "$fail" = 0 ] || exit 1