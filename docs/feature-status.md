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

六类代码补齐及配置说明见[代码缺口补齐记录](code-gap-closure.md)：调用级指标、模型预算与后台用量、Memory 单调水位、租户审计策略、Knowledge 自动迁移、Redis 队列所有权/背压。代码开发阶段没有重启日常实例；后续部署与真实验证单独记录，不自动继承历史 IM 结论。

当前本地程序为 rc.9、控制面 schema 23。文本附件/MinIO、PostgreSQL 长期记忆、只读文档 MCP、真实 Embedding + Qdrant，以及本地工作项的申请/批准/重复确认均有对应的真实开发环境记录，分别见[附件](validation/artifact-minio-2026-09-07.md)、[记忆](validation/memory-postgres-2026-09-07.md)、[MCP](validation/project-docs-mcp-2026-09-08.md)、[Knowledge](validation/knowledge-qdrant-2026-09-08.md)和[工作项](validation/workitem-approval-2026-09-08.md)。后端迁移与分段回复等修复见[补齐记录](reliability-followup.md)，发送故障分类见[诊断说明](telegram-delivery-diagnostics.md)。下表保留每项具体的验证层级，不将单身份/单节点的成功扩大为生产多租户验收。

| 能力 | 当前状态 | 已验证范围 | 尚未完成 |
| --- | --- | --- | --- |
| tRPC-Agent-Go LLMAgent / Runner / Event | 基础真实联调 + 自动测试 | 真实 IM/模型/工具执行、多轮 Session；Event 消费与关闭自动测试 | Graph/Chain/Parallel/Cycle 的平台化注册 |
| OpenAI-compatible Model | 真实联调（开发环境） | `glm-5.3-flash`、Runner/LLMAgent、Redis 多轮 Session 与进程重启恢复 | 多租户不同模型、供应商限流/超时、流式响应和生产凭据 |
| 多租户控制面 | 本地集成 | Tenant/App/Revision/Binding、PostgreSQL migration、路由隔离 | 企业 SSO/OIDC 和生产权限审计 |
| HTTP 与角色入口安全 | 自动测试 + 独立进程测试 | 调试入口默认关闭、Bearer + 租户/Binding/用户授权、HTTP 不可冒充 IM、Gateway/Admin 路由分离 | SSO/OIDC、真实集群入口策略 |
| Redis Session / Coordinator / Idempotency | 本地集成 | 跨 Runtime Session、租约、续租、幂等和 Worker 接管 | 生产 Redis Cluster/Sentinel 验证 |
| PostgreSQL Inbox / Outbox / Background Job | 本地集成 | 事务写入、claim、重试、dead 状态和多进程 E2E | 高并发与长时间故障压测 |
| Memory | 基础真实联调（开发环境）+ 自动/隔离测试 | Telegram memory_add → PostgreSQL → Agent 重启 → memory_load → 回复；受限数据库账号；私聊限制、空白 Session 及迁移自动测试 | 真实多用户/多节点、自动提取、大规模迁移与恢复演练 |
| Knowledge | 基础真实联调（本地 Qdrant + 外部 Embedding） | 持久 Job 入库一次成功，1024 维检索与重启读取；真实 Telegram knowledge_search、Embedding/检索 trace、来源引用及单次送达回执确认；作用域过滤与隔离测试，见[记录](validation/knowledge-qdrant-2026-09-08.md) | 多租户真实账号、语义质量评估、远端 Qdrant、恢复演练和生产容量 |
| Artifact | 文本附件真实联调（开发环境）+ 本地集成 | Telegram 原附件恢复到 MinIO、容器重建/Agent 重启后按原编号读取、版本锁和桶权限 | AWS S3、通用在线迁移、大规模与异地恢复 |
| 企业微信 | 自动测试 | URL 验证、签名、解密、文本入站、Token 获取、文本发送模拟 | 真实企业账号、公网回调、真实收发、媒体与卡片发送 |
| 企业微信消息 MCP | 基础群文本真实联调；自动测试 + PostgreSQL 集成 | 真实群消息→队列→glm-5.3-flash/Runner/Session→机器人自动回复；用户确认送达，Run/Outbound 状态及 Tempo 完整 trace 已核对，发送 1 次；绑定、指纹、共享发送状态测试 | 真实多群/多租户与异常联调、私聊/机器人消息回读为空原因、真实分页及延迟、媒体与容量验证 |
| Telegram | 基础真实联调 + 自动测试 | 固定域名、私聊/群聊/Topic；工具、MCP、记忆、知识库、文本附件、审批及重复确认真实回执；失败/媒体/编辑拒绝与模拟 429 测试 | 真实 429、多账号/高负载、编辑与媒体发送 |
| 微信公众号 / 微信客服 | 设计 | 数据模型与接入差异说明 | Adapter 代码和真实联调 |
| Tool 治理与审批 | 基础真实联调 + 自动/隔离测试 | 本地工作项批准前无写入、批准后单次写入、重复确认不重做；单用户/私聊限制、参数绑定及会话隔离测试；拒绝回执真实复验 | 外部有副作用的业务 Tool、多身份真实审批边界 |
| Agent MCP 工具 | 文档检索真实联调 + 自动/进程集成测试 | 同请求内真实模型调用文档工具、Tool Journal succeeded、Tempo 父子关系和 Telegram 单次送达/回执均确认；部署者授权、schema、审批、脱敏有自动测试，见[说明](project-docs-mcp.md) | 外部企业业务 MCP；多租户真实 MCP 联调；不支持 stdio 或任意服务自助接入 |
| Telegram 附件导入 | 文本真实联调 + 自动测试 | 本地已启用；真实文本导入及 read_attachment 调用、MinIO 持久化、回执/审计；PNG/JPEG、2 MiB、图片尺寸与拒绝路径由自动测试覆盖 | 图片真实导入、完整杀毒、PDF/Office、图片理解、文件发送；企业微信 MCP 仍隔离媒体 |
| 迁移/分段回复后续修复 | 已部署 + 自动/独立 PostgreSQL/Redis 测试 | Session staging 切换、Event 身份/内容/顺序和摘要校验、已登记清单证明、写后失效、分段回执/恢复、并发租约及旧 owner 拒绝 | 真实故障和容量验证；历史未登记主体需完整迁移清单 |
| 工具业务操作与对账 | 基础真实联调 + 自动/隔离 PostgreSQL 测试 | Telegram 发起→pending 无写入→用户批准→create_work_item 写入一条→重复批准不重做；回执和跨审批 trace link 核对，单用户/私聊限制与业务恢复测试，见[记录](validation/workitem-approval-2026-09-08.md) | 外部业务 Provider、跨新请求真实幂等/恢复与历史无事实记录核对 |
| OpenTelemetry | 真实联调（开发环境）+ 自动测试 | Telegram/Runner/工具/Session/回复 trace、MCP HTTP 与 Knowledge Embedding 链路、真实审批跨请求 span link 均从 Tempo 读回；元数据过滤、metrics/Dashboard/Alert Rule | Alertmanager 与实际通知渠道、生产采样和容量 |
| 调用预算与租户审计策略 | 自动测试 + 隔离 PostgreSQL/Redis 集成 | 模型调用预留/结算，Summary/Memory/Embedding 用量；审计分级、私有缓冲、受限保留期和带版本策略更新 | 生产价格配置、账单对账、节点持久卷、真实故障与容量验证 |
| Knowledge 自动迁移 | 自动测试 + 隔离 Qdrant/PostgreSQL 集成 | 现存 chunk/vector 回填、修复意图、游标、逐项及检索校验、服务器切换门禁 | 远端大数据量演练；当前单 app 上限 100000 chunks，更换 embedding 需重建 |
| Secret 管理 | 自动测试 | `env://` 精确租户/用途授权，Admin 保存前检查与运行时校验、角色用途收窄、S3/Embedding 隐式凭据拒绝 | Vault/KMS、在线轮换撤销、真实分角色账号权限 |
| 分角色数据/网络权限 | 自动测试 + 本地隔离集成 | SQL GRANT/Redis ACL 生成器、允许与拒绝测试、按角色初始化后端和网络模板 | 真实 LOGIN/Redis 账号、CNI 与依赖标签的部署验证；不是租户 RLS |
| MCP 异常隔离/恢复 | 自动测试 + 本地 PostgreSQL 集成；图片隔离与去重真实联调 | 检查点 CAS、禁用/版本检查、恢复审计原子性；图片不阻塞文字；rc.2 新图片持久隔离经完整重叠读取仍只产生一条隔离/审计，不调用 Agent 或发送回复 | 其他媒体类型/真实分页；不支持媒体分析或下载；同人同秒同类型附件可能合并 |
| 积压与异常告警 | 自动测试 + 本地 SQL/规则验证与启用 | 聚合积压、unknown/attempting、检查点停滞、快照失败/过期、多节点去重聚合；候选实例快照 up=1，13 条规则加载健康 | 生产部署、阈值/SLO、实际通知渠道 |
| Docker Compose | 本地集成 | 依赖启动、镜像构建、非 root 运行 | 长时间稳定性验证 |
| Kubernetes | 配置 | Deployment、HPA、PDB、NetworkPolicy YAML 校验 | 测试或生产集群部署 |
| 容量与故障恢复 | 自动测试 + 本地隔离集成 | Worker 取消接管、完成确认丢失、故障退避；PostgreSQL 暂停/业务 schema 恢复与发送事实保护，Redis RDB 工具链；历史 1000 请求 Mock 基线 | 真实模型/多 Worker 压测、PITR/主从切换、MinIO/Qdrant 恢复和完整故障矩阵 |

## IM 能力边界

2026-09-06 的 HTTP 工具预检、Telegram 工具真实收发、Sender 重试自动测试和群策略用户反馈见[验证记录](validation/current-time-2026-09-06.md)。HTTP 工具预检不能替代 Telegram 全链路验收，模拟 429 也不能标为真实 Telegram 限流验证。

工具审批的测试、真实模型预检、基础收发、反馈缺陷和新版拒绝回执复验见[审批验证记录](validation/approval-2026-09-06.md)。重复操作缺少新提示已在后续代码修复并通过自动测试，业务幂等与回执证据见[后续开发记录](validation/operations-feedback-2026-09-06.md)，不要把历史联调范围扩大到新版本。

框架追踪接线、元数据过滤、Collector/Tempo 组件链路、真实模型 HTTP trace 和已通过的真实 Telegram 完整 trace 证据见[追踪验证记录](validation/tracing-2026-09-06.md)。

`Capabilities` 描述的是 Reply Sender 当前真正可以调用的出站能力，不代表 Adapter 能识别同类型的入站消息。

企业微信 MCP 的真实单次读发证据见[取样记录](validation/wecom-sample-2026-09-06.md)，平台接入与默认关闭方式见[运行链路](wecom-mcp-runtime.md)。未知发送不自动重发，指纹不是源消息唯一 ID。现有媒体固定反馈不覆盖 MCP 尚未取得样例的媒体格式。

MCP 图片采用隔离、不回复策略，与自建应用/Telegram 的媒体能力提示不同。rc.2 新图片在真实接收器、PostgreSQL 和重叠轮询中的去重结果见[媒体联调记录](validation/wecom-media-2026-09-06.md)，不代表图片内容已交给模型理解。

- 企业微信当前可以验证和解密回调、规范化文本及媒体 ID，并发送应用文本消息；不声明卡片或文件发送能力。
- Telegram 当前可以解析文本、图片/文件 ID 和 edited update，并通过 `sendMessage` 发送文本；不声明消息编辑或文件发送能力。
- 默认仍对媒体返回固定说明、对编辑消息拒绝执行。rc.4 可显式开启 Telegram 限类型附件导入 Artifact，但导入本身不进入模型/审批；随后读取文本需启用 read_attachment。完整杀毒、多模态分析和媒体回复尚未实现，不能把保存图片当作图片理解。

## 更新规则

能力状态提升时必须附带可重复的验收证据：测试命令、真实环境记录或部署记录。只有真实账号或真实外部服务完成完整收发，状态才可以标记为“真实联调”。
