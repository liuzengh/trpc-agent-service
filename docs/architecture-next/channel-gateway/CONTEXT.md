# Channel Gateway 术语

本文是设计草案的术语表，统一讨论含义，不冻结数据库所有权或接口字段。Routing、
Admission、Connection、Delivery 是同一个 Gateway Workload 内的四个业务 Module，
不是四个进程、镜像、服务或部署单元；完整边界见[四个业务 Module](module-boundaries.md)。

| 术语 | 含义 |
| --- | --- |
| ChannelAccount | 一个稳定的外部机器人/渠道身份，不随绑定切换而重建 |
| ChannelBinding | 将渠道入口明确关联到一个 DeploymentRevision 的管理关系 |
| Routing Projection | 供消息接纳使用的已发布绑定与运行版本的只读视图 |
| Inbound Event | 渠道交付的一次外部事件；它可能是文本、按钮或其他事件，不必产生 Run |
| AuthenticatedInbound | Provider Adapter 完成来源鉴权、大小限制与 DTO 解码后形成的可信账户输入；不携带 Tenant 或 Binding |
| ExternalEventKey | `provider + stable account_id + external_event_id`；V1 中永久对应一个首次 Decision/Receipt |
| ExternalConversationKey | Provider、稳定账户、chat identity 与 topic/thread 的稳定组合；Gateway 传递，Worker 拥有 Session generation 与执行顺序 |
| RouteSnapshot | Routing 为首次新输入解析并由 Admission 固定的 Tenant、账户路由代次 RouteGeneration、DeploymentRevision 与 Manifest Ref/Digest |
| RouteGeneration | 同一稳定渠道账户的路由变更序列；切换 Binding、停用和重新启用均继续递增，不等于 Binding 实体编辑版本 |
| Decision | Admission 对输入作出的 `ignore / interaction / admit-run` 分类；只有 `admit-run` 分配 Run 身份并产生运行请求，Execution 另行物化 Run |
| Admission | 平台已持久接纳某输入及其固定运行目标的事实；不是执行完成事实 |
| Run | 一次逻辑 Agent 执行；接纳时分配身份与执行方物化是两个阶段 |
| ExecutionAttempt | Worker / Execution 拥有的 Run 执行尝试；执行重试不改变 Run 的逻辑身份 |
| DeliveryAttempt | Gateway Delivery 拥有的回复投递单元调用尝试；发送重试不重新执行 Agent 或创建新 Run |
| Attempt | 在 Execution 上下文中简称 ExecutionAttempt；投递规范优先显式写 DeliveryAttempt |
| ReplyContext | 回复到原渠道会话所需的目标与上下文，不等同于平台 Session |
| ReplyOrigin | 原回复所属连接的稳定关联；首次受理固定原实例、owner 代次、连接配置代次与 socket 代次，和原回调标识共同区分原连接与后来接管或重连的连接 |
| ReplyIntent | 执行侧产生的回复意图，不等同于外部渠道已收到消息 |
| Delivery | 将回复意图投递到外部渠道的受跟踪过程与结果 |
| Connector / Protocol Client | 将渠道协议交互封装为可调用能力的库角色，不等同于独立服务或部署单元 |
| Connection Owner | 当前有权维持某个有状态渠道账户连接并执行其协议操作的 Gateway 实例；企微维持原连接租约；Telegram 双模式由物理 Bot receiver owner 协调 polling 与远端注册，分布式 Webhook HTTP 收件不要求独占 owner |
| ReceiveMode | ChannelAccount 的接收配置 `long_polling / webhook`，不改变 Binding 目标或外部事件键 |
| Telegram Cursor | 已持久接纳的实际 Update 前缀对应的下次 offset；不是仅已读入内存的最大 ID |
| Owner Epoch / Fence | 用于拒绝过期 owner 的本地 claim/commit 与后续新调用，并触发旧 Client 取消；无法撤销已经写到 Provider 的请求，也不代表外部平台提供幂等保证 |
| Connection Generation | 单个企微 Client 内隔离 socket、pending `req_id` 与迟到 ACK 的协议代次；不等于 Owner Epoch |
| Delivery Certainty | Provider 调用事实：`NOT_SENT / ACCEPTED / REJECTED / UNKNOWN`；是否重试由独立 RetryPolicy 决定 |
| Progress / Final | 临时进度展示与最终回复；两者具有不同生命周期和交付含义 |
| SourceDigest | 同一外部事件在确定规范化版本下的语义内容摘要，用于区分等价重投和身份冲突 |
| Source Observation Age | 距最后一次可信路由源观察的时间，表示来源可观察性，不表示事件已经应用 |
| Projection Apply Lag | 源已观察水位与本地连续应用水位的差距及其持续时间，不等同于初始化完成 |
| Projection Watermark | 表示一份路由视图已经覆盖某个确定发布范围的同步进度；空集合也可具有完整水位 |
| Delivery Sequence | 一个运行回复展示流中的逻辑先后身份，不等同于网络到达顺序 |
| Final Barrier | 最终回复确定后，阻止旧进度重新覆盖该展示流的状态界线 |
