# 文档目录

在此放置架构设计、时序图、数据模型和运维方案。建议至少包含：

- 系统架构图：Gateway、Worker、Channel Adapter、Storage Adapter、Plugin / Guardrail、Telemetry
- 核心时序图：IM 消息 → Runner 执行 → Tool 调用 → Session / Memory 写入 → IM 回复
- 数据模型与多后端适配说明
- 风险清单

阶段验收记录：

- `stage1-spike.md`：最小真实 Agent 闭环
- `stage1.5-storage-spike.md`：官方 Redis/SQL 子模块验证
- `stage2-multi-tenant.md`：多租户、可信绑定和 RunnerRegistry
- `stage3-reliable-messaging.md`：Gateway/Worker 可靠消息、Inbox、租约和恢复
