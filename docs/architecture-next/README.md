# 下一代架构文档

本目录记录从远端基线重新建设的生产级 Agent SaaS 平台架构，并作为新代码的
规范性依据。根目录 `README.md` 只提供项目入口和实现状态概览。

## 文档状态

| 状态 | 含义 |
| --- | --- |
| 草案 | 尚在讨论，不约束实现 |
| 已接受 | 设计已经确认，新增实现必须遵守 |
| 已实现 | 已有代码和测试证据证明约束落地 |
| 已替代 | 已由后续文档或 ADR 取代 |

设计状态和实现状态必须分开记录，不能因为文档“已接受”就声称功能已经完成。

## 当前文档

- [架构级约束](constraints.md)：当前已经确认的系统边界、源码组织、依赖规则、
  运行发布和多租户约束。
- [Admin 子领域](control-api/admin.md)：Platform Operator、平台级管理命令、
  Tenant 开通与共享 Control Web 边界。
- [Identity 子领域](control-api/identity.md)：本地用户账号、密码凭证、Session 与
  认证上下文。
- [Tenant 子领域](control-api/tenant.md)：Tenant、Membership、初始 Owner 与 V1
  租户授权；Invitation 留待后续纵向切片。
- [Agent 子领域](control-api/agent.md)：Agent、当前 Draft、AgentSpec 校验与不可变
  AgentVersion 发布；运行配置绑定与执行不属于该切片。
- [AgentSpec V1](control-api/agent-spec.md)：节点协议、三层校验、稳定诊断、
  Canonicalization、Digest 与 Schema 演进规则。
- [Runtime Profile 子领域](control-api/runtime-profile.md)：已实现的可复用运行资源、单一
  Draft、11 个管理 API、强延迟幂等发布、私有凭据与 Deployment 边界。
- [RuntimeProfileSpec V1](control-api/runtime-profile-spec.md)：当前四种 Resource Kind、
  Write/Canonical/Read 分离、内部 CredentialID、校验、Digest、Schema 与 Fixture。
- [Runtime Profile 凭据](control-api/runtime-profile-credentials.md)：直接录入、加密存储、
  Draft COW、live 更新与已实现的消费边界；已加入真实 Run/Attempt mTLS 反查接线，联合验收另记。
- [Deployment V1](control-api/deployment.md)：已接受无 Environment、无用户绑定表的
  简化边界；确定 AgentVersion + ProfileRevision，经同名匹配、校验和编译，生成不可变
  DeploymentRevision + 最小 RuntimeManifest。Schema / Event、Compiler、Application、
  PostgreSQL 原子发布、八个 HTTP 路由、Bootstrap 与多副本固定 Digest 启动门禁已实现；发布事务停在
  `PENDING` Outbox；新增 Manifest Relay/Owner Export 见 [Worker Runtime](../../services/control-api/WORKER_RUNTIME.md)。
- [ChannelAccount V1 设计与实现](control-api/channel-account.md)：租户自助接入机器人、账户内部
  凭据、完整账户快照、Gateway 认证解析与状态回传；管理/内部 HTTP 与真实 PostgreSQL 已实现。
- [ChannelBinding V1 设计与实现](control-api/channelbinding.md)：精确部署目标、Binding CAS、
  账户 RouteGeneration、账户启停联动、事务 Outbox 及路由发布；管理 API/受限 Producer 已实现。
- [Telegram接入预检V1](control-api/telegram-preflight-v1.md)：冻结的2公开/3私有协议、短时任务、
  精确BotToken解析、固定配置指纹与8项去秘密诊断；Control代码及真实PG/Session/mTLS回归已实现，
  Gateway/Web/真实Telegram联合验收由协调任务记录，不等同于启用、注册或消息投递。
- [WeCom 接入预检](control-api/wecom-preflight-v1.md)：显式连接探测确认、独立 Provider policy/consumer、
  精确 Bot Secret 解析与三项闭合诊断；沿用现有端点/事务，真实连接和消息验收独立记录。
- [Gateway Control 接入与验收](channel-gateway/control-integration-v1.md)：可信快照/凭据 Adapter、
  动态 Telegram、启动装配与运行观测已实现；真实 Telegram 入站到 RunRequested 已验收。
  此历史入口仅证明入站；Worker/Reply 新增实现及联合验收见 [Worker 状态](agent-worker/implementation-status.md)。
- [产品易用性 TODO](control-api/product-usability-todo.md)：同名表单骨架、托管 Session Storage、
  模板/独立连接测试、分层状态展示和客户端技术字段处理；目标已确认，能力待设计与实现。
- [Channel Gateway 技术栈、代码结构与部署设计](channel-gateway/README.md)：Go Gateway、
  进程内公开 Go 企微库、四 Module 与持久接纳/执行所有权；当前 Control 默认来源已接
  账户/凭据和 Delivery Runner，由 Runner 独占 Maintenance，共 11 个迁移（0001–0011）。
  ReplyIntent Consumer/Worker 证明已有实现，完整 Final 联合验收另记。
- [Channel Gateway 实施状态](channel-gateway/implementation-status.md)：保留各切片历史验收，当前 Control 接入与真实 Telegram 入站集中于 §13–14。
- [Delivery Runtime V1](channel-gateway/delivery-runtime-v1.md)：独立维护、有界 Runner/PG RuntimePorts、LocalOwner，以及 Control Runner 与 fixture 独立维护的互斥装配。
- [Database V1](operations/database-v1.md)：同一 PostgreSQL/database、四 Schema 与独立角色；
  Control/Gateway/Worker 双身份启动，Session 显式准备；真实权限验收与执行 gate 分开。
- [Worker V1](agent-worker/README.md)：首版为 Telegram 文本 → 单 LLM → 正式 Session → Final；
  W1 贯通 Manifest/装配/执行/Session，W2 接入真实回复与恢复验收。Session 采用候选内容与接受引用的
  实现；组合按 SDK 原生能力，Memory/累计 Token 账本列入后续计划。代码与真实 PG/NATS/SDK
  fixture 已有验证，真实模型/Telegram 尚待联合验收；[实现状态](agent-worker/implementation-status.md) 与术语见
  [Execution 术语表](agent-worker/CONTEXT.md)。
- [运行管理与审计 V1](run-management-audit-v1.md)：新增租户授权的运行记录、Run 时间线与
  Control/Worker 业务审计聚合；明确区分 Worker Reply 交接和 Gateway 实际送达。
- [租户使用治理 V1](tenant-usage-governance-v1.md)：Control 管理单一当前策略，Gateway
  落实 IM allowlist 和共享限流，Worker 落实跨副本并发、Token 预留/结算及未知用量投影。
- [危险工具二次确认 V1](tool-approval-v1.md)：只对测试工单状态修改提供管理 Web 确认；
  固定租户/调用/目标/参数摘要，CAS 防重复执行，并对 UNKNOWN 与 Worker 重启禁止整轮重跑。
- [Channel Gateway 四 Module 入门说明](channel-gateway/module-introduction.md)：先理解四类事实和调用关系，再读详细规范。
- [Channel Gateway 四个业务 Module](channel-gateway/module-boundaries.md)：解释单一 Gateway
  Workload 内 routing、admission、connection、delivery 的职责追踪、目录分工、深接口、事务 seam 和故障时序。
- [Channel Gateway 设计复审](channel-gateway/design-review.md)：记录本轮已修正问题、仍需冻结
  的 D0 决策、历史分支同步及分阶段实施门禁。
- [公开 Go Connector 设计](channel-gateway/public-go-connector.md)：`platform/im/wecom` 的
  包职责、导入方式、生命周期、ACK 相关性和故障测试；不独立部署。
- [Channel Gateway：首批 IM 行为与 SDK 调研](channel-gateway/im-channel-sdk-semantics.md)：
  Telegram 与企业微信智能机器人接入语义、固定 SDK 源码和历史实验；SDK 行为验证
  与当前 Gateway 真实入站验收分列，不表示完整执行/回复闭环已实现。
- [首个 Platform Operator 引导决策](decisions/0001-initial-platform-operator-bootstrap.md)：
  首次启动的数据库判定、Secret 输入、并发原子性与管理员直接创建用户。
- [部署目录结构](operations/deployment.md)：Compose、NATS、Observability 与
  Helm 部署资产的统一目录和所有权。
- [可观测性与 Telemetry](operations/observability.md)：OpenTelemetry、Metrics、
  Trace、日志、Dashboard、告警和基础设施观测的生产基线。
- [IM 运行链路 Tracing V1 计划](operations/im-runtime-tracing-v1-plan.md)：当前 Worker task 独立实施；
  Gateway → 持久 Carrier/NATS → Runner/正式 Session → IM 回复；M1 本地验证通过，M2 持久入站、M3 Session 与 M4 Reply/Delivery 已实现并进行专项验证，M5 实施中：观测部署及真实进程 fixture gate 已通过，真实 IM/故障窗口待完成。

## 当前实现与后续路线

Deployment 的 Control Publication 已经落地并覆盖以下阶段：

1. 已实现关闭的 Deployment Input、RuntimeManifest、公开 Manifest View、
   `RuntimeManifestPublished.v1` Event、稳定诊断和 PlatformExecutionContract。
2. 已实现纯同名匹配 Compiler、Storage 角色选择、最小闭包和节点级工具分配。
3. 已实现授权、真实 `ProfileCredentialChecker`、CAS、Receipt-first 幂等
   Application 及四个 Command / 四个 Query。
4. 已实现 PostgreSQL 原子发布、不可变触发器、`PENDING` Outbox、八个
   HTTP 路由、OpenAPI、Bootstrap 和真实 PostgreSQL 集成测试。
5. Control Publication 闭环已验证；它的终点是持久化 `PENDING` Outbox，而不是消息已分发
   或 Runtime 已执行。
6. 已实现多副本 PlatformExecutionContract 固定 expected Digest：发布配置提供同一个
   `CONTROL_DEPLOYMENT_EXPECTED_CONTRACT_DIGEST`，Bootstrap 在 DB / HTTP 前比对，
   不匹配即启动失败；只读 CLI 支持按最终 Host 配置预计算，Compose 强制要求预期值。

渠道切片已按以下依赖落地；Worker 联合验收继续推进，不把“Binding 表完成”当作完整执行闭环：

1. 已实现 ChannelAccount：租户账户、模块私有凭据、版本/CAS与静态管理纵切。
2. 已实现 ChannelBinding：精确目标、账户有效路由与幂等事务Outbox；与账户启停一起验证原子性。
3. 已实现 Control→Gateway 扩展：完整账户快照、认证凭据读取、Gateway动态接入与运行观测。
4. Distribution：Control路由Relay已通过真实PG/JetStream回归；Gateway固定可信路由三元组。
   Manifest正文读取/执行属于Worker，路由日志恢复由Gateway验证。
5. Worker：W1 贯通固定 Manifest、凭据、单 LLM 与正式 Session；W2 完成 Telegram Final 回复。
   组合与 Memory 为后续计划；租户级 Token 预留/结算已由使用治理切片实现，代码/fixture 与真实渠道验收分别记录。

上面1～4的Control代码与真实PG/mTLS/NATS回归已接通，公开12个操作列入OpenAPI；
Gateway 的账户门禁、动态 Telegram 和运行观测已接线。联合真实 Telegram 收信已通过，
见[Control 联合验收](control-api/channel-acceptance.md)与[Gateway 入站证据](channel-gateway/telegram-real-inbound-20260906.md)；
模型/Storage/Worker 执行与完整回复仍不属于本次收信验收，当前服务在线状态另行检查。
Channel Web 与产品易用性 TODO 另行交付；Helm 留到全部 Workload 完成后的 FINAL-INTEGRATION。
已完成的多副本Digest门禁仍是平台发布配置，不引入Environment、Secret产品或用户映射表。

发布、持久 Outbox、事件已分发、运行面实际执行分别记录状态；Control 发布成功不
代表已进入运行链路。详细阶段与验收案例以 Deployment 文档为准。

## 后续文档结构

下列目录按需创建，不预先生成空文档：

```text
docs/architecture-next/
├── README.md
├── constraints.md
├── decisions/          # 满足 ADR 条件的架构决策
├── control-api/        # Control API 领域、用例、HTTP 与持久化设计
├── channel-gateway/    # IM 接入、Run Admission 与回复投递
├── agent-worker/       # Run 消费、Agent 执行与完成语义
├── protocols/          # OpenAPI、事件和兼容性规则
└── operations/         # 构建、部署、迁移和可观测性
```

只有在已有实质讨论内容时才创建对应文件，禁止预先创建空文档。尚未确认的结论
必须明确标记为“草案”；架构约束变化时，应先更新 `constraints.md`，再修改实现或
新增 ADR。
