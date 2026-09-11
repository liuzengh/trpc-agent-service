# 功能范围与使用条件

平台提供 Agent 管理、运行、IM 接入、存储路由和治理能力。部署前应根据所需功能准备对应的模型账号、后端资源和权限。

## 1. 功能范围

| 模块 | 支持能力 | 使用边界 |
| --- | --- | --- |
| 多租户 | 应用、模型、工具、通道、后端、配额和审计策略隔离 | 管理端按租户与角色授权 |
| Agent | tRPC-Agent-Go LLMAgent、Runner、多轮会话、工具和知识检索 | Agent 类型为 LLMAgent；不提供 Graph/Chain/Parallel/Cycle 注册配置 |
| 控制台 | 工作空间、模型连接、草稿、在线调试、审批、发布、灰度、回滚和运行记录 | 不提供企业 SSO 或账号自助注册 |
| 模型连接 | OpenAI Chat Completions 兼容模型，API Key 加密保存与更新 | 模型协议、访问地址和额度由部署者确认 |
| 工具 | 白名单、用户限制、调用预算、危险操作审批、MCP 与执行记录 | MCP 地址和凭据由部署者授权；未知副作用不能自动重放 |
| 存储连接 | 按后端类型填写连接参数、加密保存凭据、按名称选择并绑定 | 外部连接由平台管理员创建；已有数据后端通过迁移切换，不直接覆盖 |
| 知识库 | 网页配置 Embedding、上传和删除文本资料、查看处理状态、租户隔离检索和后端迁移 | 支持 UTF-8 文本、Markdown、JSON、CSV；PDF/Word 需先转为文本，变更模型或维度需重建或迁移索引 |
| Skill | 网页上传、版本审核、租户授权、固定校验值；支持说明型和脚本型 | SKILL.md 必需，run.sh 可选，也支持受限 ZIP；说明型只加载内容，脚本执行需审批和沙箱 |
| 多节点 | 分角色部署、共享状态、会话租约、队列重领和持久完成态恢复 | 需要共享控制面、队列、协调器及数据后端；InMemory 仅用于单进程 |
| 治理 | 租户预算、脱敏、审计、OpenTelemetry 和告警规则 | 通知接收方、保留策略和供应商硬额度需单独配置 |

## 2. 存储后端

| 数据 | 支持后端 |
| --- | --- |
| Session、State、Summary | InMemory、Redis、PostgreSQL |
| Memory | InMemory、Redis、PostgreSQL |
| Knowledge | InMemory、Qdrant，独立 Embedding 配置 |
| Artifact | InMemory、S3-compatible，包括 MinIO |
| 控制面、消息记录、审计、后台任务 | PostgreSQL；单进程可使用 InMemory |

平台提供 Session、Memory 和 Knowledge 的迁移、回填、校验与切换流程。MySQL、MongoDB、Milvus、外部 Memory 服务属于扩展选项，未提供对应平台配置。详细存储策略见[多后端适配](backend-adapters.md)。

网页存储连接和 Skill 上传使用 PostgreSQL 的 schema 32，知识资料目录使用 schema 33。Skill 每个文件最多 64 KiB、ZIP 最多 256 KiB，每租户最多保留 128 个上传版本；已保存版本不原地改写。普通工具代码上传以及 PDF/Word 解析不属于这两个资源管理入口。

## 3. IM 通道

| 通道 | 支持能力 | 使用边界 |
| --- | --- | --- |
| Telegram | 私聊、群聊、Topic、文本回复、受控附件导入 | 不提供卡片、文件发送和回复编辑 |
| 企业微信消息 MCP | 群文本接收与回复、近期接收、历史补读、逐群成员授权 | 不提供私聊或媒体发送；无可靠源消息 ID 时采用指纹去重，不保证端到端 exactly-once |
| 企业微信自建应用 | 回调 URL 验证、签名校验、AES 解密和应用文本发送 | 使用独立的 CorpID、AgentID 和应用凭据，接入时须确认回调和发送配置 |

微信客服和微信公众号未提供接入实现。Telegram 附件仅支持受限的文本、PNG/JPEG 导入，不包含图片理解、PDF/Office 解析或完整杀毒服务。接入步骤见[IM 通道说明](im-channels.md)。

## 4. 部署条件

- 单机安装需要 Docker 与 Compose；源码构建需要 Go、Node.js 和 npm。
- 模型服务和 IM 账号由部署者准备，不随系统提供。
- 管理接口置于受限网络或身份代理后，公网仅开放必要回调和健康检查；远程管理使用 HTTPS。
- 加密主密钥与数据库必须分别备份，不能直接替换主密钥或删除安装数据卷。
- 启用知识库、对象存储或沙箱时，需要额外配置资源、租户授权和网络权限。
- 上线前确认模型额度、消息峰值、告警通知、数据保留期和恢复目标。

完整部署步骤见[安装运行手册](operations-runbook.md)，风险与缓解措施见[生产风险](risks.md)。
