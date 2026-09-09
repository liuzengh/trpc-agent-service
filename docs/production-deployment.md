# 生产部署设计

本文集中说明目标生产拓扑、部署前提、扩缩容、发布回滚、故障恢复和观测。它是架构设计，不是当前版本可直接执行的 Kubernetes 部署包；参考实现的启动步骤见[本地部署](local-deployment.md)。

## 1. 部署边界

| 项目 | 当前参考实现 | 生产目标 |
| --- | --- | --- |
| 进程 | 一个 Go 进程承载网页、Admin、Agent 和可选 IM | 按 Gateway、Worker、Channel、后台任务拆分部署 |
| 网络入口 | 仅监听回环地址，明文 HTTP | TLS 入口、受控管理网络及经过认证的组件通信 |
| 配置与任务 | PostgreSQL 配置、Pin、Inbox/Run/Outbox；公共消费者按绑定扫描 | 共享持久状态、Redis 唤醒、独立调度与恢复 |
| 扩容与隔离 | 单绑定串行执行，消费者终止会关闭整个进程 | Worker 独立扩容，Channel 按绑定分片，按角色配置资源和探针 |
| 运维能力 | 本地启动脚本、可选三阶段遥测 | 角色就绪检查、生产 Secret 管理、完整 Trace 与部署监督 |

采用下述拓扑前，需落实角色启动入口、跨进程执行与回复契约、网络认证及角色探针。当前没有 `--role` 参数，也没有生产 Deployment/Helm 配置；只把现有进程增加副本不能获得本方案的隔离与恢复能力。实现状态和已有证据见[实现与验证](acceptance.md)。

## 2. 推荐拓扑

目标以 Kubernetes Deployment 或等价的独立进程部署角色，应用进程不保存唯一业务状态：

```text
HTTPS / SSE -> TLS Load Balancer -> Gateway replicas
管理网络    -> Admin API
IM 平台     <-> Channel instances（按 Binding 分片）

Gateway / Channel -> PostgreSQL Inbox / Run
                  -> Redis Streams（仅唤醒）
Worker replicas   -> Runner -> Session / Model / Tool
                  -> PostgreSQL Run / Reply Outbox
Channel instances -> 所属 Binding 的 IM 回复
Background Jobs   -> 派生任务 / 状态扫描 / 索引

共享依赖：PostgreSQL HA、Redis HA、向量库、对象存储
遥测出口：OpenTelemetry Collector -> 查询与告警后端
```

| 角色 | 扩缩容与依赖 |
| --- | --- |
| Gateway | 按请求并发扩容；依据可信凭据解析租户，访问共享配置和会话目录。单条 HTTP/SSE 连接仍归接入节点持有 |
| Admin API | 控制面独立入口，限制访问主体和网络；发布租户配置及不可变 Revision，不向公共聊天入口暴露管理权限 |
| Worker | 按可执行队列年龄、活跃 Run 和依赖容量扩容；共享 Session/Memory，无需 sticky session |
| Channel | 每个 Bot 一个活动连接，按不同 Binding 扩容；受理与出站由相应适配器处理，不假设任意实例可使用企微旧连接回复目标 |
| Background Jobs | 对持久任务做有限批次扫描，负责派生与恢复；任务幂等和租户作用域不能由调度副本数量替代 |

IM 入站先提交 PostgreSQL Inbox/Run，再发队列通知；Webhook 持久受理后确认，长连接按平台协议确认，企微推送没有 HTTP ACK。Redis 是可丢失的唤醒层。Consumer Group 只恢复尚未成功 claim 的通知，claim 后由持久任务状态决定恢复，不能将所有超时 Run 重新执行。具体顺序与配额、租约、XACK 的组合见[调度规则](storage-and-consistency.md#61-入站)。

## 3. 共享后端与配置

| 依赖 | 数据和部署约束 |
| --- | --- |
| PostgreSQL | 保存租户配置、Revision、Pin、Inbox/Run/Outbox，生产扩展保存审计与预算；配置连接池预算、备份和恢复演练 |
| Redis | 热 Session、合作型租约和唤醒；HA 不等于 failover 下租约互斥或计数单调，协调结果不确定时拒绝新有状态 Run |
| 向量库 | 按租户保存 Knowledge/Memory 索引，查询固定索引版本；索引延迟与故障降级由应用策略决定 |
| 对象存储 | 保存源文档和附件，按租户路径、版本、访问权限及保留策略管理，索引可由源数据重建 |
| Collector | 经过认证的遥测写入、租户授权查询、有界导出队列；不能作为费用或任务的权威账本 |

配置携带租户、应用、Revision、Binding 和后端引用；只保存 SecretRef，不在镜像、配置样例或日志中写入真实凭据。生产 Resolver 使用工作负载身份并先验证租户授权，出站端点受策略约束；管理接口与存储不直接暴露到公共网络。当前二进制的回环监听限制仍有效，生产网络入口需要配套实现，不能直接套用本地配置对外发布。具体边界见[Secret 与遥测出口](security-and-governance.md#113-secret-与遥测出口)。

高可用与备份分别验证。部署者按业务确定 RPO/RTO，恢复时核对配置、Pin、Session 和任务记录的关联；缺失或结果未知时保持显式失败，不能通过恢复旧快照重放副作用。跨后端迁移与索引回退遵循[冻结、校验和切换规则](storage-and-consistency.md#7-后端迁移)，不因切换部署就改变 Session Pin。

## 4. 容量与扩缩容

容量以实测 P95 模型延迟、平均 Tool 次数和消息大小校准。初始估算示例：

- 单 Worker 允许 100 个并发 Run，平均一轮 15 秒，则理论吞吐约 `100 / 15 = 6.7 RPS`；按 60% 安全水位规划约 4 RPS。
- 峰值 20 RPS 时，Worker 数量至少为 `ceil(20 / 4) = 5`，再增加 1 个故障冗余，共 6 个。
- 每轮平均写入 6 个 Event，则 Session Backend 峰值写 QPS 约 `20 × 6 = 120`，按两倍突发准备 240 QPS。
- 每轮输入输出合计 4,000 token，20 RPS 时模型消耗约 `80,000 token/s`，必须按租户和模型供应商设置预算与限速。
- IM 回调按日均峰值系数 10 估算，Inbox 和 Gateway 保留至少两倍突发余量。以上数值是容量规划假设，不是实测吞吐；部署前需通过压测校准。

扩容前同时核对数据库连接总数、模型供应商配额和 IM 账号额度；增加 Worker 不能突破这些上限。同租户执行预算与有界队列用于限制相互影响，具体策略见[治理设计](security-and-governance.md#111-策略与预算)。缩容先停止领取任务，等待或取消并排空活动 Run，再退出；Channel 迁移需先结束旧所有者，不能通过副本滚动重叠制造同 Bot 双连接。

## 5. 发布与回滚

平台程序发布与业务 Agent Revision 发布分别管理：

1. 程序发布前固定构建版本，核对配置、数据库结构与消息契约兼容性；生产拆分实现后先小范围验证健康和错误率，再逐步替换实例。回退旧程序前确认它仍能读取现有数据，不把数据库回滚等同于应用回滚。
2. 更新 Worker 时停止领取、排空 Event 并关闭引用；更新 Channel 时保持每个 Bot 单活动所有者。中断导致的未知执行不自动重跑，旧企微目标不跨连接补发。
3. 业务发布创建新的不可变 Revision，生产灰度调整新 Session 的路由权重。Worker 使用通知与路由版本检查更新缓存，通知丢失不允许永久使用旧路由。
4. 常规回滚重新选择历史发布版本，仅影响新会话；已有 Session 继续使用原 Pin。退役旧 Revision 前确认没有有效 Pin 和活动引用。紧急禁用先阻断受影响执行，换代必须显式操作并记录审计，不静默改写旧会话。

当前已实现发布默认 Revision 和常规回滚；动态权重、Pin 换代、独立管理审计与角色滚动部署仍为设计。详细模型见[发布和配置传播](architecture.md#4-控制面)，操作接口见[Admin API](admin-api.md)。

## 6. 故障隔离与恢复

**当前实现。** 网页、Admin API、企微和飞书共用一个进程。任一 IM 消费者发生终止性错误时，[启动入口](../cmd/trpc-service/main.go)会关闭 HTTP，再停止其他通道、Runtime 和存储，因此故障会影响同进程全部入口及其租户。单次模型失败、发送拒绝或发送结果未知会记录为对应 Run/Outbox 的结果，不等同于消费者终止。当前 `/healthz` 只表示 HTTP 服务可响应，不表示 Bot 已连接或可以持久受理；没有按通道隔离的监督与重启机制。

**生产目标。** 以下隔离、探针、资源配额和自动恢复属于部署设计，尚未实现。Gateway、Worker 和 Channel 分别部署；Channel 按 Binding 或明确的 Binding 组划分进程，分组内共享故障范围。每个 Bot 保持一个活动连接，按不同 Binding 分片扩容，不直接增加同一 Bot 的连接副本。企微受理与回复由同一连接所有者处理，旧连接的回复目标不能交给任意新实例补发。

| 故障范围 | 隔离和降级 | 检测与恢复 |
| --- | --- | --- |
| 单个 Channel 连接或消费者终止 | 只停止该进程内 Binding 的受理与投递；其他 Channel、Gateway、Worker 保持运行。已受理消息保留在 Inbox，未持久受理不报告成功 | 按 Binding 监测连接状态、受理失败、队列年龄和进程重启次数。连接类故障有界退避重连或重启；凭据、权限和配置错误告警并等待修正，避免重启循环 |
| 单个 Worker 崩溃或耗尽资源 | 独立 CPU/内存、并发和数据库连接预算限制影响范围；停止该 Worker 领取任务，由其他健康 Worker 继续领取可执行任务 | 监测进程存活、活跃 Run、处理延迟和超期任务；恢复先核对持久执行标记与终态，不能因进程重启就重跑 Agent/Tool |
| 共享 PostgreSQL、Redis 或模型供应商不可用 | 进程隔离不能消除共享依赖故障；按依赖能力暂停相关受理或执行，有界排队和超时，不把持久 Session 降级为 InMemory | 监测依赖错误率、连接池等待与队列积压；依赖恢复后逐步放量。共享库容量和供应商级故障仍可能同时影响多个入口 |

存活检查只判断本进程能否继续工作；就绪检查判断该角色能否履行职责，Channel 还需检查连接和持久受理能力。单个 Bot 不可用不得触发无关 Gateway 的存活检查失败。进程监督器负责局部重启，业务错误与外部依赖短暂故障通过就绪状态、超时和告警处理，避免整个部署反复重启。上述检查不以当前 `/healthz` 代替。

恢复以 PostgreSQL Inbox/Run/Outbox 为依据。停止组件时先停止受理或领取，再取消并排空正在处理的 Event、保存可确认的终态。当前参考实现只恢复尚未开始执行的任务；已启动但结果未知的 Run 明确失败，发送结果未知不自动重发，发送失败不重跑 Agent。生产恢复同样不得盲目重放；增加重试前必须具备结果查询、业务幂等与平台支持，企微旧连接目标按失效处理。未落库消息、已经发生的 Tool 副作用和发送未知结果仍有残余风险，详见[恢复边界](im-channels.md#恢复与平台限制)与[风险清单](risks.md)。

## 7. 生产观测设计

目标设计中 OpenTelemetry Span 覆盖 Channel、Gateway、Run、Model、Tool、Session、Memory 和 Outbox。核心指标包括每租户请求量、并发 Run、模型/Tool/后端延迟、错误率、IM 投递成功率、token、费用和队列等待时间。审计记录包含 `tenant_id`、`channel`、`user_id`、`session_id`、`agent_name`、`tool_name`、`decision`、`latency`、`error_type`、`cost` 和 `trace_id`。

| 指标 | 采集点和统计口径 |
| --- | --- |
| 新请求与重复入站 | Inbox 首次提交计一个新请求，重复命中单独计数；持久化内部重试不能重复增加受理次数 |
| 阶段、模型、Tool、后端与排队耗时 | 各阶段起止点记录带明确单位的直方图，区分等待与实际调用；失败样本保留 outcome，不能只统计成功耗时。当前阶段指标使用毫秒 `ms` |
| IM 投递结果 | 平台明确成功 ACK / 实际发送尝试数为成功率；拒绝、未知、目标过期分开。Outbox 状态写入重试不是新发送，ACK 不表示用户已读 |
| 错误率与并发 | 错误阶段数 / 同阶段处理总数；活跃 Run 为已开始且未结束数量，不能将排队数计作运行并发 |
| token 与租户成本 | 模型返回的 usage 按输入/输出及供应商语义记录，以模型与价格版本计算费用；缺失 usage 或价格时标记未知，不填零。跨租户查询由受控成本明细按租户聚合 |

完整 Trace 为目标设计：受理端创建上下文，将 W3C `traceparent` 随 Inbox/Run/Outbox 持久化，Worker/发送器恢复上下文，Runner、Model、Tool 与存储适配器传递同一 Context；异步派生 Memory 使用父上下文或 Span Link 关联。当前表结构没有持久 Trace 上下文字段。

当前[可选观测](local-deployment.md#观测边界)使用独立 provider，在受理、执行、发送三个阶段生成 Span、次数与耗时，以持久 `request_id` 关联；阶段可能属于不同 Trace，尚未实现跨队列连续父子 Trace、Model/Tool/Session/Memory 细分采集或成本统计。指标标签只用有限阶段、通道、结果类别及静态绑定的内部租户/App；request/user/session 等高基数 ID 不进入指标。观测只采集白名单，上游自动 tracing 保持关闭，具体结果见[实现与验证](acceptance.md#验证结果)。

当前 `trpc.channel.stage.count` 统计阶段处理次数，`trpc.channel.stage.duration` 记录对应毫秒耗时。`execute` 的结果是本次执行决策，不能替代持久 Run 终态；`deliver` 还含发送前跳过和旧目标记录，计算实际投递率时须区分 outcome，不直接用全部阶段次数作分母。启用时 OTel 进程级错误处理器只输出固定诊断，避免 SDK 将 Collector 原文写入日志；它不安装全局 provider，但会影响进程中其他 OTel 错误的诊断详细度，关闭遥测时不安装。

生产 Collector 使用内网认证写入与租户授权查询，按策略配置保留期、采样和有界导出队列；Secret 解析、第三方诊断及统一脱敏出口的约束见[Secret 与遥测出口](security-and-governance.md#113-secret-与遥测出口)。这些生产管控尚未实现，本地 debug Collector 不提供相同保证。

## 8. 采用前的验证

生产实现完成后，按上述承诺验证角色就绪与退出、跨节点 Session 隔离、同会话互斥、重复入站、未知执行不重跑、Channel 单活动所有者、共享依赖故障、发布回滚与备份恢复。容量目标以实际压测和恢复演练为依据，不把当前本地协议测试或真实正常收发记录当作生产容灾证明。

本方案仍保留合作型租约、IM 补投与未知发送等残余限制。部署前结合[十三项风险](risks.md)确定可接受边界；当前可复现的检查和真实平台记录以[验证结果](acceptance.md#验证结果)为准。
