## 背景

本 PR 补齐 tRPC Agent Platform 的架构设计文档，用于评审多租户、节点化部署、IM 接入、数据同步、治理监控和故障恢复方案。

本次只修改文档，不改变运行时代码。文档描述的是目标生产架构，同时明确 Stage 0--6 的交付边界，避免把后续阶段的能力误写成当前已经实现的能力。

## 文档结构

| 文档 | 内容 |
| --- | --- |
| `docs/README.md` | 文档入口、阶段边界、真实 IM provider 范围和部署前提 |
| `docs/architecture.md` | 组件边界、租户隔离、路由、并发、前端契约、框架复用边界 |
| `docs/diagrams.md` | 系统架构图、企业微信消息全链路时序图、SSE 事件契约 |
| `docs/data-model.md` | 实体关系、核心不变量、最小 SQL 结构和租户隔离键 |
| `docs/storage-and-sync.md` | Redis / SQL / 向量库 / 对象存储分工、事件顺序、幂等和迁移 |
| `docs/operations-and-security.md` | 授权、策略、审计、指标、Tracing、故障恢复、灰度和容量 |
| `docs/risks.md` | 14 项生产风险、监控信号、缓解措施和残余风险 |
| `docs/server-owned-routing-and-stable-event-contract.md` | 服务端持有路由决策与稳定事件契约的架构决策 |

## 核心设计

### 1. 平台边界

本项目是围绕 `trpc-agent-go` 的多租户控制面和执行面：

- 复用上游 Runner、Agent 编排、Tool/MCP、Session、Memory、Knowledge、Artifact、Plugin/Guardrail 和 Telemetry 能力。
- 本项目负责租户管理、Agent 应用注册、部署路由、Channel Binding、策略、审计和平台运维。
- 上游框架不反向依赖本项目，通用运行时能力不复制到平台层。

### 2. 运行链路

外部 IM 请求先进入 Channel Adapter，完成签名校验、重复投递去重、身份映射和 Channel Binding 解析，再转换为平台内部消息。

Gateway 是可信入口，负责建立 Tenant Context、鉴权、选择 Active Deployment、执行准入控制，并把请求路由到 Worker。

Worker 保持无状态：

- 从共享 Storage Adapter 加载 Session、Memory 和 Summary。
- 通过 Runner Adapter 调用 `trpc-agent-go`。
- 将运行事件写入 append-only Session Event Log。
- 将状态和摘要作为物化视图维护。
- 通过 outbound adapter 回复 IM。

### 3. 多租户隔离

Tenant Context 只能由服务端可信中间件建立，请求体、任意 header 或浏览器存储都不能授予租户权限。

隔离覆盖：

- SQL 查询条件
- Redis key
- 向量库和对象存储 namespace
- 审计日志
- 指标和 trace 标签
- 工具白名单和密钥解析

密钥只保存 secret reference，不在日志、trace、错误信息或 DOM 中暴露明文。

### 4. Session 一致性

Session Event 是不可变、按 Session 单调递增的事件流：

- 使用 `(tenant_id, session_id, event_id)` 和请求操作键做幂等。
- 通过 session lease 或 conditional sequence 保证同一 Session 的写入顺序。
- Summary 和当前状态是事件流的物化视图，可以按 sequence 重建。
- Worker 可以任意调度，正确性不依赖 sticky session。

### 5. IM 接入

两个真实 provider 固定为 Enterprise WeChat 和 Telegram：

- 企业微信：回调 URL、token / encoding secret、签名校验、XML/JSON 回调、同步确认窗口、用户和部门映射、异步回复。
- Telegram：bot token、HTTPS webhook、update ID、chat/user 映射、编辑和媒体消息、长度与 flood limit。

两者共享 Channel Adapter 契约，但保留各自的凭证轮换、签名材料、确认机制和限流错误处理。

Stage 3 的 Chat Workspace 和 Mock IM 只用于本地验证，不计入真实 provider 数量。

### 6. 治理与可观测性

Stage 3 固化 `request_id`，Stage 5 增加 `trace_id`，贯穿：

- IM callback
- Gateway
- Worker
- Runner
- Tool / MCP
- Storage
- policy
- audit
- provider delivery

审计事件包含 `tenant_id`、`channel`、`user_id`、`session_id`、`agent_name`、`tool_name`、`decision`、`latency`、`error_type`、`cost` 和 `trace_id`。

## 验收映射

| Spec 要求 | 对应文档 |
| --- | --- |
| 覆盖多租户、节点化部署、数据同步、多后端、IM、治理和故障恢复 | `architecture.md`、`storage-and-sync.md`、`operations-and-security.md` |
| 表达 tenant、agent、channel binding、session、event、memory、summary、audit log 关系 | `data-model.md` |
| 至少两种 IM 通道差异，包含企业微信 | `architecture.md`、`diagrams.md` |
| 至少三类后端存储和同步策略 | `storage-and-sync.md` |
| 完整消息链路和 `request_id` / `trace_id` 贯穿 | `diagrams.md`、`operations-and-security.md` |
| 至少 8 项生产风险和缓解措施 | `risks.md` |
| 明确上游复用能力和平台新增模块 | `architecture.md` |

## 已知边界

- 本 PR 是架构文档交付，不包含实现代码。
- 文档描述目标架构；Stage 1--6 的具体实现仍按阶段交付。
- 默认开发环境是本地 Go/frontend 测试和 Docker Compose。
- Kubernetes 是生产 profile，不是开发前置条件。
- Stage 4 CI 使用确定性协议 fixture；未提供凭证时不宣称 live credential smoke 成功。
- 生产认证、完整授权、灰度、回滚和恢复演练在后续阶段落地。

## 验证

- `git diff --check upstream/main...HEAD`
- 检查 PR 内文档链接均指向已提交文件
- 未运行 Go / frontend 测试：本次无代码变更

## 建议评审重点

1. Gateway、Worker、Channel Adapter、Storage Adapter 和 Runner Adapter 的职责边界是否清晰。
2. 多租户隔离是否覆盖所有数据面和控制面路径。
3. Session Event 的顺序、幂等、物化视图和迁移策略是否可靠。
4. 企业微信与 Telegram 的协议差异是否足够具体。
5. 治理、审计、Tracing、故障恢复和风险清单是否满足生产评审要求。
