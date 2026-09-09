#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DELIVERABLES=(
  docs/architecture.md
  docs/system-architecture-diagram.md
  docs/core-sequence-diagram.md
  docs/im-channel-adapter.md
  docs/data-model.md
  docs/data-sync-idempotency.md
  docs/backend-adapters.md
  docs/production-risks.md
  docs/implementation-details.md
)

for file in "${DELIVERABLES[@]}"; do
  test -s "$ROOT/$file"
done

test -s "$ROOT/docs/README.md"
test -s "$ROOT/docs/acceptance/acceptance.md"
test "$(find "$ROOT/docs" -maxdepth 1 -type f -name '*.md' | wc -l | tr -d ' ')" -eq 10

architecture_han_count="$(perl -CSD -Mutf8 -ne '$n += () = /\p{Han}/g; END { print $n }' "$ROOT/docs/architecture.md")"
test "$architecture_han_count" -ge 2000
test "$architecture_han_count" -le 4000

test "$(grep -c '^```mermaid$' "$ROOT/docs/system-architecture-diagram.md")" -eq 1
grep -q '^flowchart ' "$ROOT/docs/system-architecture-diagram.md"
grep -q 'Gateway A' "$ROOT/docs/system-architecture-diagram.md"
grep -q 'Stateless Worker' "$ROOT/docs/system-architecture-diagram.md"
grep -q 'WeCom WebSocket Channel Adapter' "$ROOT/docs/system-architecture-diagram.md"
grep -q 'Storage Router / Adapter' "$ROOT/docs/system-architecture-diagram.md"
grep -q 'Plugin / Guardrail' "$ROOT/docs/system-architecture-diagram.md"
grep -q 'OpenTelemetry Collector' "$ROOT/docs/system-architecture-diagram.md"
grep -q 'Qdrant / Milvus' "$ROOT/docs/system-architecture-diagram.md"
grep -q 'S3 Object Storage' "$ROOT/docs/system-architecture-diagram.md"

test "$(grep -c '^```mermaid$' "$ROOT/docs/core-sequence-diagram.md")" -eq 1
grep -q '^sequenceDiagram$' "$ROOT/docs/core-sequence-diagram.md"
grep -q '企业微信用户' "$ROOT/docs/core-sequence-diagram.md"
grep -q 'Tool / MCP' "$ROOT/docs/core-sequence-diagram.md"
grep -q 'latest_agent_reply Memory' "$ROOT/docs/core-sequence-diagram.md"
grep -q '`request_id`' "$ROOT/docs/core-sequence-diagram.md"
grep -q '`trace_id`' "$ROOT/docs/core-sequence-diagram.md"
grep -q '`traceparent`' "$ROOT/docs/core-sequence-diagram.md"

grep -q 'model.NewUserMessage' "$ROOT/docs/im-channel-adapter.md"
grep -q '(provider, provider_account, external_subject)' "$ROOT/docs/im-channel-adapter.md"
grep -q 'Session ID 与隔离规则' "$ROOT/docs/im-channel-adapter.md"
grep -q '平台限制与降级矩阵' "$ROOT/docs/im-channel-adapter.md"

grep -q 'Projection Checkpoint' "$ROOT/docs/data-model.md"
grep -q '核心事件 JSON Schema' "$ROOT/docs/data-model.md"
grep -q 'session_execution_leases' "$ROOT/docs/data-model.md"

grep -q '幂等与重复投递' "$ROOT/docs/data-sync-idempotency.md"
grep -q 'Lease、fencing 与投影' "$ROOT/docs/data-sync-idempotency.md"
grep -q 'Redis/SQLite 到 SQL 迁移' "$ROOT/docs/data-sync-idempotency.md"
grep -q '本地向量库到远端向量库' "$ROOT/docs/data-sync-idempotency.md"
grep -q 'active index generation' "$ROOT/docs/data-sync-idempotency.md"

grep -q 'PostgreSQL' "$ROOT/docs/backend-adapters.md"
grep -q 'Redis' "$ROOT/docs/backend-adapters.md"
grep -q 'Qdrant/Milvus' "$ROOT/docs/backend-adapters.md"
grep -q 'S3 兼容对象存储' "$ROOT/docs/backend-adapters.md"
grep -q 'Qdrant 已接入' "$ROOT/docs/backend-adapters.md"
grep -q 'Artifact 内容已接入' "$ROOT/docs/backend-adapters.md"

test "$(grep -c '^| [0-9][0-9]* |' "$ROOT/docs/production-risks.md")" -ge 8
grep -q '残余风险' "$ROOT/docs/production-risks.md"

grep -q 'https://github.com/GodBlf/trpc-agent-service' "$ROOT/docs/implementation-details.md"
grep -q '架构组件到代码的映射' "$ROOT/docs/implementation-details.md"
grep -q '能力完成度' "$ROOT/docs/implementation-details.md"
grep -q 'Qdrant/Milvus.*Qdrant 已接入' "$ROOT/docs/implementation-details.md"
grep -q 'S3 对象内容.*已接入' "$ROOT/docs/implementation-details.md"

grep -q 'Stage 7 是本项目最后一个交付阶段' "$ROOT/docs/acceptance/acceptance.md"
if grep -R -q 'Stage 8\|阶段 8' "$ROOT/docs"; then
  echo "error: final documentation must not defer work to Stage 8" >&2
  exit 1
fi

echo "Stage 7 documentation verification passed (${architecture_han_count} architecture Han characters)"
