# 框架能力与平台职责

本项目固定使用 tRPC-Agent-Go `v1.11.2`，Go 工具链下限为 1.24.1。框架负责 Agent 运行能力，平台负责租户、配置发布、路由、状态所有权、IM 和治理。代码能力见[实现与验证](acceptance.md)，项目概览见仓库 [README](../README.md)。

## 6. 上游能力基线与平台新增职责

以下包路径以 tRPC-Agent-Go `v1.11.2` 为准。

| 领域 | 可直接复用的上游能力 | 本项目新增职责 |
| --- | --- | --- |
| Agent 编排 | `agent/llmagent`、`agent/graphagent`、`agent/chainagent`、`agent/parallelagent`、`agent/cycleagent` | 租户级注册、版本、发布、灰度和路由 |
| 执行 | `runner`、流式 Event、`context.Context` 取消 | Worker 调度、并发配额、会话串行化和运行生命周期 |
| Session | `session/inmemory`、`session/redis`、`session/mysql`、`session/postgres`、`session/sqlite`、`session/mongodb` 等 | 租户级后端选择、键空间隔离、并发写和迁移 |
| Memory | `memory/inmemory`、`memory/redis`、`memory/mysql`、`memory/postgres`、`memory/pgvector` 等 | 跨节点可见性、租户路由、保留策略和迁移 |
| Knowledge | `knowledge` 及 `knowledge/vectorstore/*` | 知识库权限、索引任务、版本和租户隔离 |
| Artifact | `artifact/inmemory`、`artifact/s3`、`artifact/cos` | 配额、对象命名、生命周期、病毒检查和访问控制 |
| Tool/Skill | `tool`、MCP、`skill` | 白名单、租户密钥注入、审批和沙箱策略 |
| 治理 | `plugin`、`plugin/guardrail`、Callbacks | 租户策略编排、预算、身份检查和审计决策 |
| 服务协议 | `server/openai`、`server/agui`、`server/a2a`、`server/trpcagent` | 统一 Gateway、Admin API、租户鉴权和路由 |
| IM 参考 | OpenClaw Gateway、Channel 和 delivery 的职责模型；`v1.11.2` 无可直接复用的 Go Channel 包 | 平台自有 Channel 契约，以及企业微信/飞书的账号绑定、验签、去重和回复适配 |
| 可观测性 | `telemetry` 和 OpenTelemetry | 统一资源属性、租户指标、成本统计、告警和审计关联 |

## 7. 技术选择

| 领域 | 当前参考实现 | 生产扩展设计 |
| --- | --- | --- |
| 服务形态 | 控制面与数据面逻辑分离，单 Go 进程 | Gateway/Worker/Channel 独立部署 |
| 配置与 Pin | InMemory 或统一 PostgreSQL profile | 共享持久配置，节点无 sticky session |
| Session | InMemory/PostgreSQL/Redis，租户 BackendProfile 路由 | 按后端能力声明、版本与迁移策略扩展 |
| 并发协调 | 内存/Redis 合作型 Run 租约 | 存储条件写实现严格 fencing |
| 知识与对象 | 平台路由尚未接线 | 向量库用于 Knowledge/Memory，S3-compatible 用于 Artifact |
| IM | 企微、飞书长连接，公共文本消费者 | 微信客服、公众号、Telegram 等适配器 |
| 观测 | 独立 OTel provider，三阶段 Span 与指标 | 全链路关联、采样、成本和审计 |

Session 子模块及其语义差异见 [Session 后端](session-backend.md)。上游版本更新需要编译、契约测试和持久化集成回归；不根据版本号推断跨模块兼容性。

## 8. 核心术语

| 术语 | 定义 |
| --- | --- |
| Tenant | 平台中配置、身份、数据、工具、密钥、预算和审计的最高隔离单元 |
| Agent App | 租户拥有的逻辑 Agent 应用，具有稳定标识和多个发布版本 |
| Agent Revision | Agent App 的不可变配置快照，包含模型、Prompt、Tool、Skill、Knowledge 和策略引用 |
| Channel Binding | 外部 IM 账号/应用与 Tenant、Agent App 之间的绑定关系 |
| External Principal | IM 或 API 来源的外部用户/群身份，经映射后形成租户内用户身份 |
| Session | 由应用、租户用户和会话标识共同限定的多轮交互上下文 |
| Event | 用户消息、Agent 输出、Tool 调用结果等按顺序写入 Session 的事实记录 |
| Memory | 可跨 Session 检索的长期用户或业务记忆 |
| Summary | 从 Session Event 派生的上下文压缩结果，必须记录覆盖范围和版本 |
| Knowledge Base | 由租户管理、供 Agent 检索的文档与索引集合 |
| Artifact | 图片、音频、文件等独立存储的大对象及其元数据 |
| Run | 一次从输入消息到最终结果或终止状态的 Agent 执行实例 |
| Node | 可独立部署和扩缩容的 Gateway、Worker、Channel 或后台任务进程 |

## 9. 设计原则

1. Tenant、App、Principal、Session 来自可信配置与认证，缺失作用域或越权请求直接拒绝。
2. 数据、缓存、工具、凭据、对象路径和审计均携带或可唯一映射到租户作用域。
3. Revision 不可变，Session 首轮固定 Revision；正常发布与回滚只影响新会话。
4. 持久 Session 配合持久配置与 Pin；Worker 的本地缓存不能成为会话真相源。
5. 同 Session 通过入口租约互斥，严格防止过期写入需要存储层条件写，不能由非原子 hook 冒充。
6. IM 入站持久去重；当前最终回复最多一次发送尝试，未知结果不重发、不重跑 Agent。生产重试须有平台依据和业务幂等。
7. 只保存 Secret 引用或密文，禁止在日志、Trace、错误和审计中记录凭据与原始敏感内容。
8. request_id 关联已实现的 IM 三阶段；完整 traceparent 传播与细分 Span 按目标设计扩展。
9. 每个 goroutine、Runner、连接和存储对象都有所有者、取消关系和关闭顺序，退出时排空事件与受理回调。
