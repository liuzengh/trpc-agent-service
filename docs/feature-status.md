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
| OpenAI-compatible Model | 自动测试 | Model Factory、请求格式、错误传播 | 可用外部模型的成功调用和多租户真实凭据验证 |
| 多租户控制面 | 本地集成 | Tenant/App/Revision/Binding、PostgreSQL migration、路由隔离 | 企业 SSO/OIDC 和生产权限审计 |
| Redis Session / Coordinator / Idempotency | 本地集成 | 跨 Runtime Session、租约、续租、幂等和 Worker 接管 | 生产 Redis Cluster/Sentinel 验证 |
| PostgreSQL Inbox / Outbox / Background Job | 本地集成 | 事务写入、claim、重试、dead 状态和多进程 E2E | 高并发与长时间故障压测 |
| Memory | 本地集成 | InMemory、Redis、PostgreSQL 路由和迁移测试 | 真实业务数据和大规模迁移演练 |
| Knowledge | 本地集成 | InMemory、Qdrant、切块、过滤和本地 Qdrant 集成 | 外部 Embedding 服务和远端 Qdrant 联调 |
| Artifact | 本地集成 | InMemory、MinIO/S3-compatible、版本锁 | AWS S3 或其他云对象存储联调 |
| 企业微信 | 自动测试 | URL 验证、签名、解密、文本入站、Token 获取、文本发送模拟 | 真实企业账号、公网回调、真实收发、媒体与卡片发送 |
| Telegram | 自动测试 | Webhook Secret、Update 解析、Topic、文本发送模拟 | 真实 Bot、公网 Webhook、真实收发、编辑和媒体发送 |
| 微信公众号 / 微信客服 | 设计 | 数据模型与接入差异说明 | Adapter 代码和真实联调 |
| Tool 治理与审批 | 自动测试 / 本地集成 | Tool 白名单、参数哈希审批、Journal、审计 | 真实业务 Tool 和真实 IM 审批 |
| MCP | 设计 | 权限、密钥、超时和审计边界 | MCP Client/Server 的实际接入 |
| OpenTelemetry | 本地集成 | traceparent 传播、Tempo HTTP/Session spans、租户级 metrics、Grafana Dashboard、Prometheus Alert Rule | Alertmanager 和实际通知渠道 |
| Secret 管理 | 已编码 | `env://` 和测试用 Static Store | Vault、KMS 或云 Secret Manager Adapter |
| Docker Compose | 本地集成 | 依赖启动、镜像构建、非 root 运行 | 长时间稳定性验证 |
| Kubernetes | 配置 | Deployment、HPA、PDB、NetworkPolicy YAML 校验 | 测试或生产集群部署 |
| 容量与故障恢复 | 本地集成 | Worker 切换、readiness 故障演练、1000 请求 Mock 基线、PostgreSQL/Redis 隔离备份恢复 | 真实模型/多 Worker 压测、主库及 MinIO/Qdrant 恢复和完整故障矩阵 |

## IM 能力边界

`Capabilities` 描述的是 Reply Sender 当前真正可以调用的出站能力，不代表 Adapter 能识别同类型的入站消息。

- 企业微信当前可以验证和解密回调、规范化文本及媒体 ID，并发送应用文本消息；不声明卡片或文件发送能力。
- Telegram 当前可以解析文本、图片/文件 ID 和 edited update，并通过 `sendMessage` 发送文本；不声明消息编辑或文件发送能力。
- 媒体下载、病毒扫描、Artifact 转存和媒体回复是后续工作，不能仅凭 media/file ID 解析视为已经支持。

## 更新规则

能力状态提升时必须附带可重复的验收证据：测试命令、真实环境记录或部署记录。只有真实账号或真实外部服务完成完整收发，状态才可以标记为“真实联调”。
