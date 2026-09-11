# Channel Gateway 四个 Module：从一条消息理解分工

> 本文是入门说明，回答“为什么分成四块、每块应该做什么、开发时改哪里”。
> 精确事务与接口仍以[详细边界规范](module-boundaries.md)为准；术语见[CONTEXT](CONTEXT.md)。
> 下文的完整收发流程和未来目录是目标设计，不代表所有能力已经实现。
> 2026-09-06 当前状态：默认 Control 来源已接账户/凭据和 Delivery Runner，共 10 个迁移（0001–0010）；真实 Telegram 入站已验收，ReplyIntent Consumer 与真实 Worker 完成证明仍未接入。
> 2026-09-05 的介绍复审与 Runtime 历史验收保留于[复审 §19](design-review.md#19-四-module-讲解补充与文档一致性复审)及实施状态 §11；当前接线与真实入站见实施状态 §13–14。

## 1. 先记住四句话

- **Routing 管版本：这条新消息应该交给哪个已发布的 Agent 运行目标？**
- **Admission 管受理：这条外部消息是否已经处理，第一次应该作出什么决定？**
- **Connection 管线路资格：哪个实例现在可以持有某个企微 Bot 的连接？**
- **Delivery 管投递结果：这次回复是否真正调用了渠道，结果是否确定？**

四者都在同一个 `channel-gateway` Workload 内，使用一个 Go 二进制和一个镜像。
它们不是四个微服务，也不是 Handler → Service → Repository → Database 四层。
划分依据是**业务事实、修改原因和失败后的恢复责任**，不是调用发生的先后顺序。

每个 Module 内部仍可有 Domain、Application、Adapter：
Domain 表达规则，Application 编排用例，Adapter 对接数据库、消息传输或外部 SDK。
所以“修改路由规则”通常只改 Routing，而不是横跨四个 Module 各加一层代码。

### 1.1 用“谁能修改哪类事实”判断归属

| Module | 核心问题 | 自己写入的事实 | 通过别人取得的能力 | 明确不做 |
| --- | --- | --- | --- | --- |
| Routing | 新输入该固定哪个运行目标？ | 投影、RouteGeneration、停用标记、同步水位 | Control 的已发布事件 | 不编辑 Draft，不保存首次受理决定 |
| Admission | 这个事件第一次如何处理？ | Receipt、Decision、Admission、RunRequested Outbox | Routing 的快照与事务 guard；企微还需 Connection owner guard | 不物化 Worker Run，不执行模型 |
| Connection | 本实例能否继续使用这个企微 Bot？ | 非秘密连接配置/revision、owner lease/epoch、账户级恢复状态；本地 Client registry | 可信账户配置、专用凭据解析、公开协议库 | 不选择 Agent，不决定 Final 重试 |
| Delivery | 哪次发送发生了，结果是什么？ | ReplyIntent Receipt、Delivery、发送 Attempt、结果证据 | 原 Admission 的回复快照；渠道 Sender；企微原连接匹配与当前 owner 资格 | 不重跑 Agent，不改写 Worker 终态 |

表中是目标写入责任，不是已建表清单。业务事实的拥有方只能有一个；共享 PostgreSQL
不等于互相写表。Admission 的 `RunID` 是交接身份，真正的 Run/执行 Attempt 由 Worker
物化并维护；Delivery 的发送 Attempt 则属于另一个状态机。

也不必把所有逻辑依次穿过四块：Telegram 入站主要是 Admission 调用 Routing；
Telegram 出站主要是 Delivery 调用 Sender。Connection 是企微入站、出站共同需要的
连接资格能力，不是所有消息必经的第三层。

### 1.2 一个需求应该怎样拆给四块

以“把 Bot 切换到新的 Agent 版本，稍后旧请求仍要回复”为例：

1. **Control** 发布新绑定；**Routing** 更新新接纳使用的运行投影。
2. **Admission** 让新事件固定新目标，让旧事件重投仍返回原 Receipt。
3. **Worker** 继续执行旧 Run 固定的 Manifest，而非改读 latest。
4. **Delivery** 使用旧 Admission 的原回复目标完成回复，不随 Binding 改收件人。
5. **Connection** 仅在企微账户连接配置/资格发生变化时处理 Client；单纯换 Agent
   不应无故重建 Bot 连接。路由代次与连接配置代次不是同一个版本概念。

这样划分是为了让“改运行目标”“可靠接纳”“维持连接”“可靠投递”分别有明确负责人。

### 1.3 两种 Attempt，不是同一次重试

| 名称 | 谁拥有 | 失败后重新做什么 | 不会重新做什么 |
| --- | --- | --- | --- |
| ExecutionAttempt | Worker / Execution | 按执行恢复规则尝试同一个 Run | 不因消息重投另建逻辑 Run |
| DeliveryAttempt | Gateway Delivery | 仅在确定性和策略允许时再次投递某个回复单元 | 不再次调用 Agent、模型或工具 |

例如 Agent 已生成回答，但一次发送超时：Worker 的完成事实保持不变，Delivery 记录
UNKNOWN 并进入专用恢复。让 Worker 再跑一遍，不但不能证明上一条是否已发送，还可能
重复调用工具。反过来，SDK 收到发送 ACK，也不能替 Worker 提交执行完成。

**四种成功必须分开：** Admission 提交表示已受理；NATS PubAck 表示传输接收；Worker
完成表示执行事实已提交；Delivery ACCEPTED 表示 Provider 明确接受调用，不是用户已读。

### 1.4 “子领域”、Module 和部署单元不是同一个维度

这里把“四个子领域”更准确地称为四个业务 Module：它们先划分事实与恢复责任，
不承诺每块都是独立 Bounded Context，更不要求拆进程。可以同时从三个维度看代码：

| 维度 | 本设计的划分 | 解决的问题 |
| --- | --- | --- |
| 业务 Module | Routing / Admission / Connection / Delivery | 一条规则、一个事实及其恢复由谁负责 |
| Module 内部结构 | Domain / Application / Adapter | 业务规则怎样与协议、数据库和框架隔离 |
| 部署单元 | 一个 Channel Gateway Workload | 哪个二进制、镜像和进程一起启动、发布、扩容 |

例如 `admission/adapter/outbound/postgres` 属于 Admission，不因为使用数据库就划到
一个全局 Repository Module。`delivery/adapter/outbound/telegram` 属于 Delivery，
不因为它调用网络就变成独立微服务。公开企微库是可复用协议实现，和业务 Module 是两个概念。

### 1.5 四块之间怎样协作，而不是怎样排队

可以把 Gateway 想成一个团队，而不是四道必须依次经过的工序：

```text
Routing ──固定运行目标──> Admission ──RunRequested──> Worker
                             ▲                        │
                             │                        │ ReplyIntent
企微协议事件 ──入站 Adapter───┘                        ▼
                                                   Delivery
Connection ──当前 owner / 原来源 Sender 能力───────────┘
     └──────新企微入站的提交资格校验──> Admission
```

图中的线表示运行时提供的能力，不表示业务包可以互相 import Adapter 或直接写对方的表。
Telegram 用入站 HTTP Adapter 替换左侧企微事件，发送直接走 Telegram SDK；它跳过
Connection，而 Routing、Admission、Delivery 的核心责任不变。

用三个变化检查这种划分是否有价值：

- **切换 Agent 版本**：Routing 改新消息的目标；Admission 和 Delivery 保留旧请求快照。
  这个变化不要求重连企微，也不要求重写发送结果账本。
- **更换企微 WebSocket 库**：公开协议库及其 Adapter 处理协议差异；Routing 的版本规则、
  Admission 的去重键、Delivery 的 UNKNOWN 规则不应一起跟着 SDK DTO 改动。
- **调整发送重试**：Delivery 调整确定性/预算与恢复；Worker 不重新跑 Agent，Connection
  不代替 Delivery 把“重连成功”解释成“上一条没发出去”。

所以四块既不是四个部署单元，也不是要求每个功能都改四块。划分的作用是：变化发生时，
谁负责修改规则、谁保存事实、谁交付故障测试都清楚。

### 1.6 同一条消息为什么需要四份独立事实

以一个合成例子看它们各自保存什么。下面的标识只用于讲解，不是实际账户或执行记录：

| 事实 | 首次消息到达时固定或观察到什么 | 后面发生变化时，谁处理 |
| --- | --- | --- |
| 路由投影 | 账户 `account-A` 的 RouteGeneration=12 指向 DeploymentRevision=7 | Routing 可继续更新到 generation=13，供未来新消息选择 |
| 首次受理 | EventKey 对应 Receipt；Admission 固定 revision=7 的运行目标并分配 RunID | Admission 对重复输入返回同一 Receipt，不随新路由改写 |
| 连接资格 | 企微当前 owner epoch=3、socket generation=2；Admission 另保存此次回调的 ReplyOrigin | Connection 可以失租、重连或获得新 epoch，但不替 Admission 改旧来源 |
| 发送结果 | Delivery 对这个 Run 的 Final 建立稳定投递记录，逐次记录是否调用及确定性 | Delivery 根据原 Attempt 与证据收束，不因重新连上就宣称旧回复可重发 |

**路由会更新，旧受理快照保持不变；连接会换代，旧回复关联也保持不变。**
因此这四块没有一个共同的“最新版”字段，更没有一个共同的成功状态。
Telegram 同样需要路由、受理、投递三类事实，只是不使用此处的企微 owner/socket 资格。

再看两个容易混淆的结果：

- 原 Run 固定了 revision=7，即使路由变成 revision=8，Worker 仍执行原快照。
  “运行版本稳定”是 Admission 与 Execution 的交接保证，不依赖一直维持原企微连接。
- 原企微连接已消失时，这个 Run 仍可能合法完成，但原 Final 未必还能发送。
  “执行完成”和“原渠道可投递”是不同条件；Connection 报告来源匹配结果，Delivery
  记录未发送或保留 UNKNOWN，不重新路由、不重跑 Agent、不把旧 req_id 搬到新 socket。

## 2. 一条消息怎样走完整条链路

假设用户在 Telegram 私聊中发送：“帮我总结今天的消息”。下面展示完整目标流程：

```text
Control 发布目标 ──事件──> Routing 的本地运行投影
                               ▲
                               │ 首次新消息查询
Telegram Webhook → 入站 Adapter → Admission → PG 事实 + Outbox
                                                 │
                                                 ▼
                                           NATS RunRequested
                                                 │
                                                 ▼
                                           Worker 执行 Agent
                                                 │ ReplyIntent
                                                 ▼
                                              Delivery
                                                 │
                                                 ▼
                                        Telegram SDK → 外部 IM
```

1. 入站 Adapter 校验请求来源、大小与字段，把可信账户和外部消息映射成内部输入。
   外部请求不能自行指定 Tenant、Binding 或 Deployment。
2. Admission 先查询 `provider + account_id + external_event_id` 对应的旧 Receipt。
   若已经受理且内容一致，直接返回第一次结果，不因 Binding 已切换而再执行一次。
3. 对新消息，Admission 决定 `ignore`、`interaction` 或 `admit-run`。
   普通文字、按钮和服务消息不是同一种输入，不把每个 Update 都转成 Prompt。
4. 只有 `admit-run` 向 Routing 取得固定的运行快照，并在接纳事务中核验版本。
   Tenant 与运行目标来自可信投影，不在消息热路径联查 Control Draft/latest。
5. Admission 在同一个 PG 事务保存 Inbox、Decision、Admission 和 RunRequested Outbox。
   提交成功后才确认 Telegram Webhook；接纳成功不等于 Worker 已经在线或执行完成。
6. Relay 从持久 Outbox 发布事件到 NATS，失败后仍按原 EventID/Payload 重试。
7. Worker 在自己的事务中记录消费事实并物化 Run，随后 ACK NATS，再执行固定快照。
   Worker 拥有 Run、Attempt、执行顺序和终态；Gateway 不调用模型完成 Agent 执行。
8. Worker 产生 ReplyIntent，Delivery 校验其授权和原 Admission 的回复目标并持久接纳。
   Worker 完成 Run 与 IM 送达是两个事实，发送失败不应导致 Agent 重新执行。
9. Delivery 调用对应渠道 Adapter，记录明确接受、明确拒绝、确定未发送或 UNKNOWN。
   Telegram HTTP 发送不经过 Connection；企微发送还必须匹配原回调连接并具备当前 owner 资格。

**PG 保存业务事实，NATS 传递事实对应的事件，Worker 执行 Agent。**
NATS PubAck、消费 ACK、Webhook HTTP 成功、Provider 发送回执的含义各不相同。
企微入站 handler 完成只表示本地交接，不新增服务端持久消费 ACK 或离线补发保证。

## 3. Routing：维护运行地址簿

**它拥有：** 本地账户/路由运行投影、账户级 RouteGeneration、停用标记、消费 Receipt 与同步水位。
它回答的是“新消息现在应固定哪个已发布目标”，不是“如何管理这个 Agent”。

**入口 → 出口：** Control 的版本化投影事件 → 本地投影；Admission 的查询 → RouteSnapshot。
快照固定 Tenant、Binding、DeploymentRevision 和 Manifest 引用/摘要，不携带凭据。

**它不负责：** 编辑 Control Draft、编译 Manifest、去重外部消息、执行 Run 或发送回复。
Control 仍拥有账户/Binding 的管理事实；Routing 只保存运行侧需要的可信投影。

**例子：** 管理员把机器人从 Deployment A 切换到 B。
Routing 应用较新 RouteGeneration 后，新消息选择 B；已经接纳的消息仍保留原来的 A。
这个代次按账户单调递增，换一个 Binding 不从 1 重新计数；它不是 Binding 实体自己的编辑版本。
若切换与首次接纳竞争，Admission 通过 Routing 的同事务只读 guard 保证提交一致。
“查过一次路由”不能替代提交时的版本核验。

**恢复重点：** 有一条路由不代表投影已经同步完成，空集合也可能是合法的完整状态。
初始化要追到可信完整水位；历史缺口或内容冲突不能靠覆盖旧行掩盖。
运行期也限制增量应用滞后：源可查询不等于新路由已应用。当前连续已知积压满 60 秒会
拒绝新 Run；起点由 0004 迁移持久保存，部分进展、重启和新 heartbeat 不续期，完全追平
才清除。这个门禁独立于 5 分钟来源观察超时，详见[当前实施状态](implementation-status.md)。

## 4. Admission：保存第一次受理决定

**它拥有：** 外部事件去重身份、内容摘要、首次 Decision/Receipt、Admission 和运行请求 Outbox。
它确定“第一次发生了什么”，而不是代替 Worker 创建执行状态机。

**入口 → 出口：** 经过鉴权和归一化的输入 → 稳定 Receipt，必要时附 AdmissionID/RunID。
`ignore` 保存忽略决定，`interaction` 保存交互决定，只有 `admit-run` 产生 RunRequested。
交互的即时反馈、跨 Worker 取消命令和取消最终结果需要各自的授权与契约。

**它不负责：** 解码原生 WebSocket 帧、控制 Bot 连接、执行模型或计算发送重试。
Provider/SDK DTO 留在 Adapter，Tenant 与运行目标由 Routing 的可信投影派生。

**例子：** Telegram 因网络原因重投同一条消息，此时 Binding 已停用。
Adapter 仍先鉴权；Admission 对同键同内容返回旧 Receipt，不重新路由、不创建第二个 Run。
同键不同内容应作为冲突处理，不能把另一条内容伪装成已成功受理。

**恢复重点：** 接纳事务把相关事实与 Outbox 一起提交，容量预算也在事务内检查。
readiness 只是接流量信号，不是并发准入屏障；已持久重复事件不因新工作饱和改写结果。

## 5. Connection：管理企微连接的资格与生命周期

**它拥有：** 有状态账户的 owner lease/epoch、本地 Client 生命周期、Client registry 与连接状态。
V1 仅企微需要这一层；Telegram Webhook/HTTP 没有此处的单账户独占连接要求。

**入口 → 出口：** 可信账户运行投影和 Supervisor 启动 → 当前 owner 的受控本地 Client 能力。
Supervisor 内部完成取得资格、续租、失租、停用、轮换和关闭，不让 Bootstrap 拼租约步骤。
这里的 registry 是本实例的协议 Client 索引，不是 Worker 拥有的会话历史或 Session generation。

**它不负责：** 选择 Tenant/Agent、接纳消息、记录 Delivery 或实现 WebSocket 协议细节。
协议由公开企微库实现；Connection 决定“是否允许这个实例启动和继续使用该 Client”。

**例子：** Gateway A 持有某企微 Bot，B 没有资格，因此 B 不为它建立第二条连接。
A 失租后停止新调用与重连并取消 Client；取得新资格的 B 才启动自己的 Client。
同一物理 Bot 也不能同时注册为两个有效账户，否则两个合法账户 lease 仍会冲突。
这里描述的是本地授权规则；暂停进程或在途网络请求仍可能造成外部连接/发送的短暂重叠。

**恢复重点：** Owner Epoch 约束本地资格，不会撤销已经发给 Provider 的请求。
公开库的 Connection Generation 只关联本连接的 pending/ACK，不能代替跨实例 lease。
连接 READY 也不等于还能持续发送：库的累计 Final 身份容量耗尽需要 Gateway 的账户级
状态与恢复策略，不能只短暂重试，详见[公开库组合门禁](public-go-connector.md#41-累计身份容量不等于瞬时背压gateway-组合门禁)。
正常关闭期间有界 drain 并续租；失租或超时转为快速关闭，不无限拖延退出。

企微与 Telegram 复用的是 Admission 业务用例，不是完全相同的资格校验。**新企微事件**
还必须在接纳事务内通过当前 owner 校验；已持久的同内容旧 Receipt 不重新检查 owner，
Telegram 不增加这道 lease 校验。取消旧 Client 是停止信号，不能替代数据库中的提交屏障。

临时 PG/投影故障、协议认证失败、被其他连接替换也不是同一种恢复原因。临时接纳错误
应有有界等待与恢复；确认被替换则需要跨副本隔离，不能由另一个副本立即抢回。
目标规则见[详细规范 §6.4–6.5](module-boundaries.md#64-入站失败与连接恢复的组合契约)，本轮实现与联合验证见[实施状态 §9.5](implementation-status.md#95-实际工作树联合验收)。

## 6. Delivery：区分“想发送”和“发送结果”

**它拥有：** ReplyIntent 接纳、Delivery/Attempt、发送额度、deadline、分段结果与重试决定。
它把 Worker 的回复意图变成可恢复的外部投递，而不是修改 Worker 的 Run 终态。

**入口 → 出口：** 可信 ReplyIntent → DeliveryReceipt；调度器 → 渠道调用和持久结果。
回复目标以原 Admission 的不可变 ReplyContext 为依据，不因为后来换 Binding 而换收件人。

**它不负责：** 执行 Agent、编译 Manifest、自行争抢企微连接或管理原生协议 pending 队列。
Telegram Adapter 直接使用 SDK；企微 Adapter 从 Connection 预留与原回调连接匹配的本地 Sender，
并另外检查当前 owner 资格。它不是“随便取一个当前 Client”。

**例子：** Provider 已接受 Final，但 Gateway 在保存结果前崩溃。
重启时“没有成功记录”不等于“没有发送”；该调用应进入 UNKNOWN，而非无条件重发整条回复。
Progress 和 Final 的展示顺序、分段完成与恢复也由 Delivery 的业务契约约束。

**恢复重点：** 目标状态明确区分 PENDING → CLAIMED → CALLING。
过期 CLAIMED 可按 token CAS 重新调度；MarkCalling 持久成功后才允许外部调用。
过期 CALLING 保守进入 UNKNOWN；企微发送门和结果提交还要核验 owner epoch。
这些状态本切片已在独立 Delivery 账本中实现；它们不同于 Admission Outbox 的发布 claim。
当前精确选择见[Final V1](delivery-final-v1.md)。[Runtime V1](delivery-runtime-v1.md) 已有独立维护与有界 Runner；
当前 Control App 已启动 Runner 并由它独占 Maintenance，fixture App 只独立维护。
ReplyIntent Consumer 与真实 Worker 完成证明仍未接入，不把有发送调度器等同完整回复闭环。

还要区分“旧 owner 无权推进新状态”与“旧调用已收到明确 ACK”。前者由 fence/CAS
拒绝，后者仍需绑定原发送 Attempt 保存为受限结果证据；不覆盖新 Attempt、不触发自动重发。
UNKNOWN 可以与已保存的可信晚到证据并存；只有显式恢复确认原 Attempt 存在唯一一致的
ACCEPTED/REJECTED 证据时才收束。矛盾证据或尚未执行恢复时仍保留 UNKNOWN。

### 6.1 为什么企微“连接恢复了”不等于“旧回复能继续发”

假设 A 在 socket generation 7 收到回调，Worker 尚未完成时 A 重连到 generation 8。
即使 Bot、account 和 owner epoch 都没变，原 `req_id` 也不因此变成新连接的请求。
因此目标设计要求 Admission 首次受理时保存独立的 **ReplyOrigin**，记录原实例、owner、
配置代次和 socket generation；Delivery 只能申请匹配这个来源的 Sender。

历史 Admission 缺少来源时，当前读 Port 在接纳前返回不支持，新 Delivery 尚未创建；
已接纳的 Delivery 后来失去原连接时，才由发送准备路径记录失败。两者都不猜当前
generation，也不凭“数据库里还没写成功”推断旧调用没发过。已进入 CALLING 的未知调用
仍按 UNKNOWN 处理。ReplyOrigin 留在 Gateway，不塞给 Worker，不改变外部消息去重身份；当前已由 0006 保存，精确实现见[Final V1](delivery-final-v1.md)；完整设计见[详细规范 §7.5](module-boundaries.md#75-企微原回复关联不等于当前发送资格)。

Final 的另一道门是**执行授权**：Delivery 校验已提交的不可变完成事实，不把 Worker 当前
是否仍持有执行 lease 当作完成证明。一个正常完成后释放 lease 的 Run，其合法 Final
仍应可以接纳；未获接受的旧执行 Attempt 则应拒绝。Gateway 已有 CommittedFinalVerifier
Port 和逐字段匹配校验；待交付的是可信 Execution 端实现、认证与生产接线。
业务 deadline 也不等于渠道承诺的可回复时限；有效发送截止及可信计时来源仍需冻结，见
[Final V1 §3.1](delivery-final-v1.md#31-业务-deadline-与有效发送截止剩余设计门禁)。

## 7. 公开企微库与 Gateway Adapter 有什么不同

| 位置 | 解决的问题 | 不应该知道的东西 |
| --- | --- | --- |
| `platform/im/wecom` | connect/subscribe/ping、帧读写、原生 DTO、req_id/ACK、取消与协议错误 | Tenant、Binding、Run、平台租约、PG、NATS |
| Gateway 入站 Adapter | 原生消息鉴权/归一化，转换成 Admission 输入 | Agent 怎么执行、最终回复怎么重试 |
| Gateway 出站 Adapter | 将 Delivery 操作转换成协议请求，并翻译调用结果 | Control Draft 或 Worker 执行状态机 |
| Connection | 控制哪个账户 Client 应该在本实例存活 | ReplyIntent 的业务重试决定 |

“公开”表示其他 Go 包可以 import，不意味着独立进程、镜像、仓库或 Go module。
因此企微库不放在 Gateway 的 internal 中；平台业务 Adapter 仍保留在服务内部。
Telegram 已有可直接引用的 Go SDK，不为目录对称性增加只转发方法的公共封装。
公开库也不是第五个 Gateway 业务 Module，更不是 `wecom-connector` 部署单元。
详细协议和错误语义见[公开 Go Connector](public-go-connector.md)。

## 8. 代码应该怎样分工

以下是**完整目标目录草图**。当前已有四 Module 源码、Sender lookup、维护与有界 Runner；
Control 账户/凭据与生产 Runner 已接线，ReplyIntent 消费、真实 Worker 和渠道扩展仍未交付。

```text
services/channel-gateway/internal/
├── bootstrap/             # 组装依赖、bridge 与进程生命周期
├── routing/               # domain / application / adapter
├── admission/             # domain / application / adapter
├── connection/            # 资格、Client 生命周期与原 ReplyOrigin Sender reservation
├── delivery/              # Final 账本/维护/有界 Runner；Control 已启动 Runner，Reply Consumer 待接线
└── infra/                 # PG/NATS/HTTP 等技术连接
platform/im/wecom/         # 已有 P0；本轮实现 Gateway 已直接装配
api/events/...            # 跨 Workload Schema 来源
gen/events/...            # 生成 DTO；不充当共享业务 Domain
```

四个人协作时，可以各自负责一个 Module 的完整用例、Adapter 与测试。
不要一人写所有 Handler、一人写所有 Repository，最终让事务与恢复没有明确负责人。
公开库的协议实现可单独分工，但需要与 Connection/Delivery 共用故障测试契约。
跨模块通过使用方定义的 Port 协作；Bootstrap 只装配，不成为业务流程总控制器。
共享 PG 允许必要的同事务只读 guard，不允许随意写另一个 Module 的表。

### 8.1 运行时调用与编译时依赖是两张图

以 Admission 向 Routing 查询为例：

```text
运行时：Admission 用例 → 它需要的 RouteReader → bridge → Routing 用例
编译时：bridge → Admission 定义的 Port
             → Routing 暴露的查询能力
        Admission 用例不 import Routing 的 PG Adapter
```

Admission 告诉外部“我需要什么”，Bootstrap 显式把实现注入进来。PG 接纳事务再调用
只读 guard，是 Adapter 层的技术 seam：`pgx.Tx` 不进入业务 Application/Domain。
Bootstrap 可以在纯装配代码中构造 SDK/Adapter，但归一化、重试、租约编排不能藏在那里。
公开库的存在不改变依赖方向，也不要求创建一个包罗万象的共享 Connector Interface。

跨 Workload 则通过版本化协议交接，不用跨服务 SQL 或分布式大事务拼完整链路。

### 8.2 如何给一个 Module 分配完整工作

| 负责人 | 交付给调用方的能力 | 必须一起交付的验证 |
| --- | --- | --- |
| Routing | 应用可信投影、查询固定运行目标和健康状态 | 乱序/重复事件、代次切换、空初始化、持续积压与接纳竞争 |
| Admission | 返回稳定受理回执并可靠交接 RunRequested | 同键同文重放、异文冲突、事务回滚、容量门禁、企微失租 |
| Connection | 维护本地有效 Client，并预留匹配原来源的 Sender | 双副本竞争、失租、轮换、被替换隔离、晚到 ACK 与 drain |
| Delivery | 接纳授权 Final、跟踪分段调用及恢复结果 | Final 唯一性、A1/A2 崩溃窗口、UNKNOWN、证据冲突、无人 owner 的过期处理 |

跨 Module 工作先约定使用方所需的 Port、输入含义、错误分类、幂等与事务确认点，再各自
实现。不是先把四套目录填满，也不是所有 Module 都需要相同数量的方法与 Adapter。

以“企微回复超时”为例，责任拆法是：公开库报告这次协议调用的确定性；Connection
维护原 Sender 的资格和生命周期；Delivery 保存 Attempt/Observation 并决定是否恢复。
Admission 保留第一次回复目标；Routing 不因为超时重新选 Agent。故障会涉及多人协作，
但发送结果的写入拥有方仍然只有 Delivery。

### 8.3 骨架完成、Module 完成和系统完成怎样区分

| 交付层级 | 完成意味着什么 | 仍未证明什么 |
| --- | --- | --- |
| 目录与编译骨架 | 入口、装配、依赖方向和构建成立 | 尚未证明事务、重试或机器人行为正确 |
| 一个 Module 的纵向切片 | 公开 Interface、业务规则、真实 Adapter 和对应故障测试一起交付 | 尚未证明上下游生产接线完成 |
| Gateway 组合验收 | 入站接纳、持久传输、连接资格、投递恢复能按约定协作 | 测试 Worker / 本地 HTTP、WS 仍不是实际外部 IM E2E |
| 平台端到端验收 | 真实 Control 发布、Worker 执行、Gateway 与渠道完整收发得到验证 | 仍须检查所有 Workload 的部署契约是否稳定 |
| FINAL-INTEGRATION | 全部 Workload 已完成后，把稳定的镜像与运行契约组装成 Helm | 不是提前用 Chart 模板代替业务交付 |

人员可以按 Module 分工，但合并验收需要共同覆盖消息交接、原回复关联、失租、重复消费
和崩溃窗口。不要只证明“四个目录分别 go test 通过”，就宣布 Gateway 闭环完成。

### 8.4 跨 Module 交接时要先说清什么

不要只约定一个函数名称。每个交接至少要说明输入由谁确认、何时成为持久事实、失败后
谁恢复。下面是四块可以据此拆分开发任务的检查表：

| 交接 | 调用方依赖的保证 | 成功确认点 | 失败后的拥有方 |
| --- | --- | --- | --- |
| Routing → Admission | 新消息得到一致的固定版本；提交时仍由同事务 guard 校验 | Admission 事务完成，而非只完成 Resolve | Routing 修复投影；Admission 重试接纳或返回原 Receipt |
| Admission → Execution | 稳定 RunID、原输入和固定运行快照经 Outbox 交接 | Gateway 先提交受理；Worker 再独立提交消费事实与 Run | Admission Relay 重发同一事件；Execution 幂等物化与恢复 Run |
| Connection → Admission | 新企微输入来自当前有效 owner；重复旧 Receipt 不重受理 | Admission 事务内的 owner guard 与事实一起提交 | Connection 处理资格；Admission 保持首次决定 |
| Connection → Delivery | 预留匹配原 ReplyOrigin 的 Sender；预留本身不发送 | A1 → Reserve → A2 确认提交 → Send；证据窗口结束后 Release | Connection 维护线路；Delivery 拥有准备失败、Attempt 与投递结果 |
| Execution → Delivery | Final 对应可信、已提交的不可变完成事实及原回复目标 | Delivery 持久接纳；未来 Consumer 还须接管 transport receipt 后 ACK | 暂不可验证与明确无权分开；Delivery 不用发布权限替代 Run 授权 |

最后一行是生产接线契约，当前并没有已经启用的 ReplyIntent Consumer 或真实 Execution
验证器。同样，Runner 的账户单在途由**单个 Runner 实例**的 active map 保证；生产装配
必须另行保证调度职责不重叠，不能把它说成已经存在的进程级共享账户锁。

用例验收应直接跨各 Module 对外提供的 Interface，并一起覆盖相邻提交窗口，而不是
只检查目录、方法名或正常路径。精确发送时序见[Runtime §6](delivery-runtime-v1.md#6-localowner-不是原-sender也不是授权证明)，
待完成的生产交接见[最新复审](design-review.md#19-四-module-讲解补充与文档一致性复审)。

## 9. 部署阶段与当前完成边界

Compose 随真实 Workload 逐步交付，承担本地集成和单机验收，不等待整个系统完成。
Gateway 是一个独立 Go Workload；NATS 是传输基础设施；Worker 是另一独立执行 Workload。
启用企微是 Gateway 的账户配置与连接生命周期，不增加 Node 镜像、sidecar 或 Connector RPC。
Helm 只在**全部生产 Workload 完成、镜像/端口/Probe/权限/Secret 契约稳定后**进入
**FINAL-INTEGRATION**；它不属于 Gateway P1/P2，也不为四个 Module 各建一个 Deployment。

目前已有入站第一切片及 CGR-27 连续积压门禁；公开企微 P0 已可导入并有本地真实 WS 测试。
上一切片已核验 SDK 全矩阵、实际工作树与镜像；本轮实现进一步实现 Connection 持久租约、
Supervisor 与企微入站/同事务 owner guard，并直接装配 SDK。该切片的工作树与镜像验收记录保留于实施状态 §9；当前 Runtime 另行验收。
既有 Delivery 模块、Sender lookup 与 ReplyOrigin 保留；2026-09-05 Runtime 切片补充
RuntimePorts、0007、LocalOwner 与有界 Runner，历史验收见[实施状态 §11](implementation-status.md#11-delivery-runtime-v1实现与本轮验收)。
2026-09-06 当前默认 Control 来源已接账户目录、托管凭据、A1/A2 资格与生产 Runner，
由 Runner 独占 Maintenance；fixture 来源保留独立维护，空账户仍处理旧账本。
真实 Control publisher 与 Telegram 用户入站已形成固定目标的持久 RunRequested，
详见[真实验收](telegram-real-inbound-20260906.md)。ReplyIntent Consumer、真实 Worker、
CGR-37 与有效截止策略仍未完成；历史镜像数字不代替当前运行接线验证。
测试发布者、测试订阅者和健康探针不代表真实机器人回复闭环；准确证据见[实施状态](implementation-status.md)。
本说明不替代[设计复审与 D0 门禁](design-review.md)，也不新增实现完成声明。

**阅读顺序：本文 → [四 Module 详细规范](module-boundaries.md) → [总体设计](README.md) →
[部署设计](../operations/deployment.md)。需要改代码时，再按对应 Module 查精确契约与测试。**
