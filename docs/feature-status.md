# 功能实现与验证状态

本文用于区分“代码已经实现”和“已经在真实外部环境完成联调”。README、设计文档和进度汇报均以这里的状态定义为准。

## 状态定义

| 状态 | 含义 |
| --- | --- |
| 已编码 | 仓库中存在实现，但不代表已完成外部联调 |
| 自动测试 | 通过单元测试、模拟 HTTP 服务或内存替身验证 |
| 本地集成 | 使用本地真实 PostgreSQL、Redis、MinIO 或 Qdrant 验证 |
| 真实联调 | 使用真实模型、IM 账号、云服务或 Kubernetes 集群完成端到端验证 |

## 当前状态

| 能力 | 当前状态 | 已验证范围 | 尚未完成 |
| --- | --- | --- | --- |
| tRPC-Agent-Go LLMAgent / Runner / Event | 本地集成 | Mock Model、多轮 Session、Event 消费与关闭 | Graph/Chain/Parallel/Cycle 的平台化注册 |
| OpenAI-compatible Model | 真实联调（开发环境） | `glm-5.3-flash`、Runner/LLMAgent、Redis 多轮 Session 与进程重启恢复 | 多租户不同模型、供应商限流/超时、流式响应和生产凭据 |
| 多租户控制面 | 本地集成 | Tenant/App/Revision/Binding、PostgreSQL migration、路由隔离 | 企业 SSO/OIDC 和生产权限审计 |
| HTTP 与角色入口安全 | 自动测试 + 独立进程测试 | 调试入口默认关闭、Bearer + 租户/Binding/用户授权、HTTP 不可冒充 IM、Gateway/Admin 路由分离 | SSO/OIDC、真实集群入口策略 |
| Redis Session / Coordinator / Idempotency | 本地集成 | 跨 Runtime Session、租约、续租、幂等和 Worker 接管 | 生产 Redis Cluster/Sentinel 验证 |
| PostgreSQL Inbox / Outbox / Background Job | 本地集成 | 事务写入、claim、重试、dead 状态和多进程 E2E | 高并发与长时间故障压测 |
| Memory | 本地集成 | InMemory、Redis、PostgreSQL 路由和迁移测试 | 真实业务数据和大规模迁移演练 |
| Knowledge | 本地集成 | InMemory、Qdrant、切块、过滤和本地 Qdrant 集成 | 外部 Embedding 服务和远端 Qdrant 联调 |
| Artifact | 本地集成 | InMemory、MinIO/S3-compatible、版本锁 | AWS S3 或其他云对象存储联调 |
| 企业微信 | 自动测试 | URL 验证、签名、解密、文本入站、Token 获取、文本发送模拟 | 真实企业账号、公网回调、真实收发、媒体与卡片发送 |
| Telegram | 真实联调 + 自动测试 | 固定域名、私聊/群聊/Topic、重启/去重/Webhook 恢复；current_time 完整收发；审批基础收发、新版格式拦截和确定性拒绝回执；模拟 429 重试测试 | 新版批准结果正文复验、重复操作提示、真实 429、编辑和媒体发送 |
| 微信公众号 / 微信客服 | 设计 | 数据模型与接入差异说明 | Adapter 代码和真实联调 |
| Tool 治理与审批 | 自动测试 / 本地集成；只读 Tool 与审批基础真实联调 | 参数绑定、会话隔离、批准/拒绝/重复/过期；权限允许后预留执行；直接回执、Journal 驱动结果与事务回滚测试；新版格式拦截/拒绝回执真实复验 | 新版批准结果正文复验、真实业务 Tool |
| MCP | 设计 | 权限、密钥、超时和审计边界 | MCP Client/Server 的实际接入 |
| OpenTelemetry | 真实联调（开发环境）+ 自动测试 | 真实 Telegram→模型→current_time→Session→回复的完整 trace，Tempo 实际读回；共享框架 tracer、审批 span link 与 Memory 组件测试、元数据过滤；现有 metrics/Dashboard/Alert Rule | 真实审批跨请求 link、Alertmanager 与实际通知渠道 |
| Secret 管理 | 自动测试 | `env://` 精确租户/用途授权，Admin 保存前检查与运行时校验、角色用途收窄、S3/Embedding 隐式凭据拒绝 | Vault/KMS、在线轮换撤销、真实分角色账号权限 |
| Docker Compose | 本地集成 | 依赖启动、镜像构建、非 root 运行 | 长时间稳定性验证 |
| Kubernetes | 配置 | Deployment、HPA、PDB、NetworkPolicy YAML 校验 | 测试或生产集群部署 |
| 容量与故障恢复 | 本地集成 | Worker 切换、readiness 故障演练、1000 请求 Mock 基线、PostgreSQL/Redis 隔离备份恢复 | 真实模型/多 Worker 压测、主库及 MinIO/Qdrant 恢复和完整故障矩阵 |

## IM 能力边界

2026-09-06 的 HTTP 工具预检、Telegram 工具真实收发、Sender 重试自动测试和群策略用户反馈见[验证记录](validation/current-time-2026-09-06.md)。HTTP 工具预检不能替代 Telegram 全链路验收，模拟 429 也不能标为真实 Telegram 限流验证。

工具审批的测试、真实模型预检、基础收发、反馈缺陷和新版拒绝回执复验见[审批验证记录](validation/approval-2026-09-06.md)。重复操作缺少新提示仍为待优化项。

框架追踪接线、元数据过滤、Collector/Tempo 组件链路、真实模型 HTTP trace 和已通过的真实 Telegram 完整 trace 证据见[追踪验证记录](validation/tracing-2026-09-06.md)。

`Capabilities` 描述的是 Reply Sender 当前真正可以调用的出站能力，不代表 Adapter 能识别同类型的入站消息。

- 企业微信当前可以验证和解密回调、规范化文本及媒体 ID，并发送应用文本消息；不声明卡片或文件发送能力。
- Telegram 当前可以解析文本、图片/文件 ID 和 edited update，并通过 `sendMessage` 发送文本；不声明消息编辑或文件发送能力。
- 媒体下载、病毒扫描、Artifact 转存和媒体回复是后续工作，不能仅凭 media/file ID 解析视为已经支持。

## 更新规则

能力状态提升时必须附带可重复的验收证据：测试命令、真实环境记录或部署记录。只有真实账号或真实外部服务完成完整收发，状态才可以标记为“真实联调”。
