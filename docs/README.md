# 设计文档索引

本目录对应根目录 `README.md` 中的架构设计交付物。文档以 tRPC-Agent-Go `v1.11.x` 为基线，版本升级时需要重新验证 Runner、Session、Memory、OpenClaw 和各存储子模块的兼容性。

| 文档 | 内容 |
| --- | --- |
| [getting-started.md](getting-started.md) | 从 `POST /chat` 开始认识 Message、Runner、Event 和 Session |
| [architecture.md](architecture.md) | 总体架构、组件职责、节点拓扑、租户隔离和框架复用边界 |
| [sequence.md](sequence.md) | 企业微信消息进入平台后的完整执行时序，以及取消和故障处理 |
| [data-model.md](data-model.md) | 控制面、运行面核心表结构和索引设计 |
| [data-consistency.md](data-consistency.md) | session 并发、event/state/summary 顺序、Memory 可见性、消息幂等和迁移策略 |
| [backend-adapters.md](backend-adapters.md) | Session、Memory、Knowledge、Artifact 等多后端适配和选型 |
| [im-channels.md](im-channels.md) | 企业微信、微信公众号和 Telegram 通道接入设计 |
| [governance-operations.md](governance-operations.md) | Plugin/Guardrail、审计、监控、密钥、故障恢复、容量和部署方案 |
| [risks.md](risks.md) | 生产风险、触发条件、监控信号和缓解措施 |
| [implementation-roadmap.md](implementation-roadmap.md) | 代码模块、迭代顺序、测试策略和验收映射 |
| [deployment.md](deployment.md) | Docker、Kubernetes、可观测栈、灰度、备份与告警 |
| [capacity.md](capacity.md) | Worker/Token/Redis/SQL 容量公式和压测工具 |

## 推荐阅读顺序

第一次接触 Agent 框架时，从上手指南开始，先运行两轮对话。理解 Message、Runner、Event 和 Session 后，再读总体架构和核心时序。准备编码时，从实施路线开始，按其中的里程碑逐步完成。准备上线时，重点复核治理运维和风险清单。

## 文档约定

- `tenant_id` 是平台租户标识，不接受外部 IM 用户直接传入。
- `app_id` 表示一个租户下的 Agent 应用，`revision_id` 表示不可变发布版本。
- `request_id` 是一次 Agent 执行的全局幂等键。
- `storage_scope` 是平台写入 tRPC-Agent-Go `AppName` 的内部命名空间，格式为 `t/{tenant_id}/a/{app_id}`。
- `runtime_user_id` 是传给 `runner.Run` 的用户键；它可能是真实用户，也可能是群聊共享模式下的合成主体。
- Mermaid 图可以在 GitHub、支持 Mermaid 的 Markdown 工具或文档站点中直接渲染。
