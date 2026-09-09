# README 客观规则审查：已通过项清单

审查日期：2026-09-07

审查分支：`review/readme-objective-rules`

审查基线：`main` / `0629c3eb01a0f55d11e99ad4d859bea8129a57b5`

本清单以 README 的明确要求为规格，并结合可机械检查、可在干净环境复验、难以仅靠文字声明满足这三个特征，核对可能用于入围筛选的客观规则。这里只记录有文档、代码、测试或运行结果支撑的通过项。

## README 七项验收标准

- [x] 架构方案覆盖多租户、节点化部署、数据同步、多后端支持、IM 接入、治理监控和故障恢复。证据：[`architecture.md`](../architecture.md)；Stage 7 Compose 验收通过。
- [x] 数据模型表达 tenant、agent、channel binding、session、event、memory、summary、audit log 的关系。证据：[`data-model.md`](../data-model.md) 的“所有权与主键”和“关系”。
- [x] 说明并实现至少两种 IM 通道，其中包含企业微信。证据：企业微信智能机器人 WebSocket 与 Telegram long polling，见 [`stage-4-im.md`](../stages/stage-4-im.md)；对应 provider runtime 测试通过，且 [`2026-09-09 真实消息 smoke`](live-im-smoke-2026-09-09.md) 已完成两种客户端回环。
- [x] 说明至少三类后端的数据存储和同步策略。证据：[`backend-adapters.md`](../backend-adapters.md) 与 [`data-sync-idempotency.md`](../data-sync-idempotency.md)。
- [x] 给出包含 `request_id`、`trace_id` 和 W3C `traceparent` 的完整消息时序。证据：[`core-sequence-diagram.md`](../core-sequence-diagram.md)；远程 Worker identity/trace/version 测试通过。
- [x] 列出不少于 8 个生产风险及缓解措施。证据：[`production-risks.md`](../production-risks.md) 共列出 12 项。
- [x] 明确区分 tRPC-Agent-Go 可复用能力和新增平台模块。证据：[`architecture.md`](../architecture.md) 与 [`implementation-details.md`](../implementation-details.md)；`go.mod` 和 `trpcservice/platform/framework_runtime.go` 存在真实框架依赖与调用。

## README 八项交付物

- [x] 架构设计文档：[`architecture.md`](../architecture.md)。
- [x] 系统架构图：[`system-architecture-diagram.md`](../system-architecture-diagram.md)，明确展示 Gateway、Worker、Channel/Storage Adapter、Admin API、Plugin/Guardrail、Telemetry Collector、数据库与 IM 平台的关系。
- [x] 企业微信核心时序图：[`core-sequence-diagram.md`](../core-sequence-diagram.md)。
- [x] 数据模型设计：[`data-model.md`](../data-model.md)。
- [x] 数据同步和幂等策略：[`data-sync-idempotency.md`](../data-sync-idempotency.md)，覆盖关系型后端和本地到远端向量索引迁移。
- [x] 多后端适配方案：[`backend-adapters.md`](../backend-adapters.md) 的后端职责表与取舍说明。
- [x] 至少 8 项风险及缓解措施：[`production-risks.md`](../production-risks.md) 共列出 12 项。
- [x] GitHub 实现代码：[`implementation-details.md`](../implementation-details.md) 提供源码映射，`origin` 可访问，受审实现提交与远端 `HEAD` 一致。

## 预测的客观入围门槛

- [x] 仓库可获取：`git ls-remote --exit-code origin HEAD` 成功。
- [x] 项目可构建：`./build.sh` 成功生成前端生产资源、`bin/trpc-service` 和 `bin/control-migrate`。
- [x] 项目可从 Compose 拓扑运行：双 Gateway、独立 Worker、Redis、PostgreSQL 和入口服务均完成健康检查及场景验收。
- [x] 不是仅有架构文字：代码真实依赖 `trpc.group/trpc-go/trpc-agent-go v1.11.2`，并接入 Agent、Runner、Plugin、Model 和 Tool。
- [x] 多租户隔离有自动化证据：身份、Agent App、Session、Channel Binding、Memory/Knowledge/Artifact、Audit/Trace 均有跨租户拒绝或空集合测试。
- [x] IM 幂等和乱序处理有自动化证据：重复 provider message 不触发第二次执行，乱序 sequence 被拒绝，失败投递可有界重试。
- [x] 多节点并发 Session 有自动化证据：PostgreSQL Session Lease 和单调 fencing token 可阻止旧 owner 写入终态。
- [x] 故障恢复可复现：Compose 验证 Gateway/Worker/PostgreSQL 重启、运行时超时、Tool 失败、治理不可用和 IM 重试场景；Responses 模型流取消另有单元测试覆盖。
- [x] 治理与审计不是展示占位：Tool/MCP allowlist、Guardrail、危险 Tool 确认、预算、限流、Audit 和 Trace 均有 API 或运行时测试。
- [x] 敏感信息边界有验证：生产身份校验、服务端凭据 Profile、日志/输出脱敏与 write-only redaction pattern 均有测试。

## 本次实测门禁

- [x] `./scripts/stage7-acceptance.sh` 完整通过。
- [x] Go 全量测试通过：`go test ./...`。
- [x] Go race 检查通过：`go test -race ./...`。
- [x] Go 格式化、静态检查和 lint 通过：`format.sh`、`git diff --check`、`lint.sh`。
- [x] Go 与前端生产构建通过：`./build.sh`。
- [x] 前端类型检查通过。
- [x] 前端单元测试通过：9 个测试文件、22 个测试。
- [x] 前端桌面与移动端 E2E 通过：8 个测试。
- [x] README 关键纵向测试通过，包括 SQLite 重启、Chat Completions/Responses、远程 Worker、Execution Manifest、Session Lease、治理恢复和 Memory/Knowledge/Artifact/Trace。
- [x] 文档自动检查通过：交付文件、两张 Mermaid 图、企业微信、风险数量、多后端说明及最终阶段声明均满足断言。
- [x] 双 Gateway Compose 验收通过。
- [x] Telegram 与企业微信真实客户端消息、真实模型和 Provider 回复回环通过；凭据和外部身份未写入证据。
- [x] 受审实现基线证据已生成：`.scratch/evidence/stage7-0629c3eb01a0f55d11e99ad4d859bea8129a57b5-schema-1.json`。

## 审查边界

本次“通过”只表示对应单项有 README、代码、自动化或显式人工 smoke 证据支撑；未列入的要求不视为通过，也不代表主办方尚未公布的规则已被确认。真实模型与真实外部 IM 凭据 smoke 不属于自动化通过项；2026-09-09 两种真实 IM 已人工执行成功，后续提交仍需单独复验。S3 Artifact 内容和 Qdrant Knowledge 索引已有真实后端集成测试，Milvus、增量向量 outbox、对象孤儿自动回收和 Kubernetes 仍是设计或部署指导。
