# Channel Gateway 四个业务 Module：职责、接口与运行流程

- **设计状态**：四个 Module 的职责方向与部分事件/表集合已落实；已有选择与剩余生产协议分别见 §13.1，不把整个设计再次列为待冻结
- **实现状态**：四 Module、Final/ReplyOrigin、Control 账户/凭据接入、动态 Telegram 注册与 0001–0010 已实现。Control 模式启动有界 Runner，由 Runner 独占 Maintenance 生命周期；ReplyIntent Consumer、真实 Execution 完成证明及完整 IM 回复 E2E 仍待交付。真实 Telegram 入站见[实施状态 §13–14](implementation-status.md)。
- **部署关系**：四个 Module 编译进同一个 Channel Gateway 二进制与镜像，不是四个微服务
- **复核日期**：2026-09-06
- **独立入门**：[从一条消息理解四个 Module](module-introduction.md)；本文保留详细 Interface、事务与故障规则。
- **关联文档**：[Gateway 总览](README.md)、[术语](CONTEXT.md)、[公开企微 Go 库](public-go-connector.md)、[IM 行为调研](im-channel-sdk-semantics.md)、[本轮设计复审](design-review.md)

## 0. 先用一条消息理解分工

**Routing 决定“交给谁”；Admission 决定“是否已经受理”；Connection 管“当前谁持有线路”；
Delivery 管“回复究竟发到哪一步”。** 它们不是四层 Handler/Service/Repository，也不是
四个独立部署的“子领域”。每个 Module 都围绕自己拥有的事实封装用例、规则和 Adapter。

假设用户对一个机器人发送“帮我总结今天的消息”：

1. 入站 Adapter 先验证来源，把外部消息交给 Admission。
2. Admission 先问自己的账本“这条消息见过没有”。新消息才向 Routing 查询已发布目标。
3. Routing 返回固定版本；Admission 连同输入和运行请求一起落库，然后确认接纳。
4. Worker 独立执行 Agent，把回复意图交给 Delivery；它不直接拿机器人 Token 发消息。
5. Delivery 负责把回复发出去并记账。Telegram 直接调用 SDK；企微需要找到 Connection
   正在维护的本地 Client。Connection 不代替 Delivery 决定重试，也不代替 Routing 选 Agent。

**读法**：先读本节和第 8 节的两条流程；需要分配代码工作时读第 9 节；评估故障时读
第 14 节；精确字段与尚未接受的决策见[设计复审](design-review.md)。以下是目标设计，
这里保留完整目标接口，不将这些片段算作当前可编译 API；已落代码的精确边界与选择见
[实施状态](implementation-status.md)，也不以第一切片替代后续 Module 的设计。

### 0.1 以后一个需求应该交给谁

| 要改变的行为 | 主要修改哪个 Module | 其他 Module 为什么不接管 |
| --- | --- | --- |
| 把机器人从 Deployment A 切到 B | Control 发布，Routing 更新投影 | Admission 已受理的旧消息继续使用原快照；Delivery 不重新选 Agent |
| Telegram 重投同一条消息 | Admission | Routing 不为重复输入重新选版本；Connection 不参与 Telegram 路径 |
| 企微断线、换凭据、实例接管 | Connection + 公开企微协议库 | Connection 管资格与 Client 生命周期；协议库管连接内行为；二者不改 Run |
| Final 超时，判断该不该再次发送 | Delivery | 库报告调用确定性；Delivery 按操作和 deadline 决定恢复，Worker 不重新执行 Agent |
| 更换模型、工具或 Agent 执行策略 | Control / Worker | Gateway 只固定并传递发布结果，不编译 Manifest，也不执行 Agent |

可以把分工记成四类事实：**Routing 的版本、Admission 的受理回执、Connection 的连接资格、
Delivery 的投递结果。** 它们有运行时调用关系，但不是上下四层；每个 Module 内部再按
Domain / Application / Adapter 组织。下面的接口、状态名和目录均是设计草案。

## 1. 本文回答什么

Channel Gateway 内部先划分为四个业务 Module：

| Module | 它回答的问题 |
| --- | --- |
| `routing` | 一条首次接纳的新消息现在应该固定到哪个已发布 Deployment？ |
| `admission` | 这条外部输入是否已处理、是否受理，以及是否创建一次 Run？ |
| `connection` | 哪个 Gateway 副本当前有资格持有某个有状态渠道账户的连接？ |
| `delivery` | Worker 的回复意图怎样持久、调用 Provider，并怎样记录确定或不确定的结果？ |

这四者是同一个 `channel-gateway` Workload 内的 Module。划分目的是隔离四种状态、事务和
失败语义，不是增加进程、镜像、网络 RPC 或 Helm Deployment：

```text
services/channel-gateway/cmd/channel-gateway
  → bootstrap
      → routing
      → admission
      → connection
      → delivery
```

公开 `platform/im/wecom` 是企微协议库，不是第五个业务 Module 或独立服务。Telegram
Adapter 直接 import 第三方 Go SDK，也不增加 `platform/im/telegram` 转发包。

## 2. 统一语言

| 名称 | 本文含义 |
| --- | --- |
| `AuthenticatedInbound` | Provider Adapter 已完成外部协议鉴权、请求限制和 DTO 解码后交给 Admission 的可信账户上下文；不预带 Tenant 或 Binding |
| `ExternalEventKey` | `provider + stable account_id + external_event_id`；一次外部事件的永久去重身份 |
| `RouteSnapshot` | Routing 为首次新输入解析出的固定 Tenant、账户路由代次 RouteGeneration、DeploymentRevision 与 Manifest Ref/Digest |
| `Decision` | Admission 对输入作出的 `ignore / interaction / admit-run` 分类 |
| `Admission` | 平台已持久接受一次 `admit-run` 输入并固定运行目标的事实；不是 Worker 已执行或 IM 已回复 |
| `RunRequested` | Gateway 持久 Outbox 发布给 Worker 的运行请求；发布成功不等于 Worker 已物化 Run |
| `ReplyIntent` | Worker 产生的回复意图；不是 Provider 已接受或用户已看到的事实 |
| `Delivery` | Gateway 跟踪一次外部投递的持久事实与生命周期 |
| `Owner Epoch` | Gateway 数据库中用于 fence 有状态连接 owner 的本地代次；只约束本地 claim/commit 和新调用资格 |
| `Connection Generation` | `platform/im/wecom` 单个 Client 内隔离 socket、pending `req_id` 与迟到 ACK 的连接代次 |
| `Final` | 一种投递操作，不代表外部用户一定已收到；实际结果仍由 Delivery 记录 |

Owner Epoch 与 Connection Generation 不互换，也都不进入 `ExternalEventKey`。Owner Epoch
无法撤销已经写到 Provider 的请求；旧 owner 的在途调用仍可能成功，因此需要 `UNKNOWN`。

## 3. 总体协作图

```text
Control API ──Account/Route Projection Event──> routing
                                                    ▲
                                                    │ 仅首次新事件 Resolve
Telegram Webhook ──telegram inbound adapter──> admission
WeCom Event ───────wecom inbound adapter──────> admission
                                                    │
                                                    │ PG: Inbox + Decision
                                                    │     + Admission + Outbox
                                                    ▼
                                             RunRequested / NATS
                                                    │
                                                    ▼
                                                Agent Worker
                                                    │ ReplyIntent
                                                    ▼
                                                delivery
                                                  │     │
                      Telegram SDK / HTTPS <──────┘     └─────> WeCom sender
                                                                  │
可信账户配置源 ───────────────> connection Supervisor ──local Client Registry
```

账户连接配置源与 Agent 路由投影是不同能力：生产 Connection 从可信 Control 完整快照获取目录，
配置文件仅用于显式 fixture；不从 RouteGeneration 推导凭据 revision，也不让换 Agent 强制重连。

模块间通过使用方定义的 Application Port 协作，bridge 由 bootstrap 显式装配。
当前第一切片的 `routeBridge` 在 `bootstrap/app.go`，并由 `New` 注入；
`bootstrap/application_ports.go` 只是未来拆文件的候选名称，不是已有源码入口。
同事务只读 guard 是 §9.2 的 Adapter 技术 seam。一个 Module 不 import 另一个 Module 的
PostgreSQL Adapter。

## 4. Routing：运行地址簿

### 4.1 输入与输出

Routing 消费 Control 发布的版本化账户/路由投影事件，幂等维护 Gateway 本地只读投影。
对 Admission 提供的核心结果是：

```go
type RouteSnapshot struct {
    Provider             string
    AccountID            string
    TenantID             string
    BindingID            string
    Generation           int64 // RouteGeneration; wire: route.generation
    DeploymentRevisionID string
    ManifestRef          string
    ManifestDigest       string
}
```

上述字段与第一切片 RouteSnapshot 对齐；语义上的 RouteGeneration（wire 字段 `generation`）
是 Control 按 `(provider, account_id)` 发布的**账户路由变更序列**。切换 Binding、停用或重新启用都不重置
该序列。Binding A 的路由代次为 20，改绑到 B 应发布更高代次，例如 21，而不是复用 B 的实体
编辑版本 1。Binding 自己的管理 CAS 如需传递应另列字段，不替代 RouteGeneration；
它也不同于 Owner Epoch、Connection Generation 和 Worker 执行代次。Control publisher
仍待实现，此处对齐现有消费契约，不代表发布侧序列分配已经落地。

建议未来外部 Interface 保持很小；下方 ConversationSelector 是候选扩展，不是当前方法签名：

```go
type RouteResolver interface {
    Resolve(
        ctx context.Context,
        accountID string,
        conversation ConversationSelector,
    ) (RouteSnapshot, error)
}
```

当前第一切片只支持账户级单个有效路由：实际方法为 `Resolve(ctx, provider, accountID)`，
投影按 `(provider, account_id)` 整体替换。Conversation/Thread 此时用于入站、回复和会话
标识，不参与路由选择。未来按群/topic 匹配不同 Binding，需要另行冻结 selector 的优先级、
冲突拒绝、投影键和代次，不把候选接口或“单事件单决策”当成该功能已经交付。

Control 应发布能独立形成 `RouteSnapshot` 的自包含事件，避免 Gateway 在接纳热路径联查
Control Draft/latest，或依赖多个乱序事件临时拼接出半新半旧的路由。

### 4.2 拥有与不拥有

Routing 拥有：

- 按 ChannelAccount 标识组织的非秘密路由投影，不拥有全部连接配置；
- 账户级 RouteGeneration 与明确 tombstone/disabled；
- 精确 DeploymentRevision 与 Manifest Ref/Digest；
- Control 事件 Consumer Receipt 与投影应用结果；
- projection initialized/staleness 状态。

Routing 不拥有：

- Control 里的 Account/Binding 管理事实或 Draft；
- RuntimeManifest 编译；
- Bot Token、企微 Secret 或 Profile 凭据；
- 外部消息去重、Run、Attempt 或 Delivery。

### 4.3 事务边界

一次 Control 事件的：

```text
Consumer Receipt + generation/CAS + Projection 更新
```

在同一个 Gateway PostgreSQL 事务提交，随后才 ACK NATS。旧 generation 幂等忽略；同
generation 不同内容属于协议冲突，不以最后写入覆盖。

### 4.4 初始化不是“收到任意一个事件”

Routing 要区分“路由集合确实为空”与“尚未完成同步”。完整设计建议 D0-05 冻结一次有版本的
全量快照及完成水位，再接增量事件：快照覆盖范围、完整性标记与水位一起持久提交，
只有追到声明水位才标记 initialized。空快照可以初始化成功，某条路由存在不代表全局已同步。

如果只使用可回放事件流，必须证明保留范围足以重建全部当前路由和 disabled tombstone；
否则需要 Control 的快照/重建接口。重建入口属于同步流程，不是 Admission 每条消息查询
Control。多 Gateway 共享 PG 投影时，消费者 checkpoint 与完整性状态也应持久共享。

同步健康必须拆成三件事：启动是否覆盖完整性水位、源观察是否新鲜（source-observation age）、
增量是否持续应用（apply lag）。不使用“最近一次业务配置变更时间”判断过期；长期没有修改
的路由不应自行失效。源 heartbeat/StreamInfo 成功只证明源仍可观察，不证明消费者已经追上。

CGR-27 历史复审指出来源观察成功不能替代持续增量追平；本轮已实现独立的连续积压门禁。
`highest_sequence > contiguous_sequence` 首次形成已知积压时，PG 时钟记录
`apply_lag_since`；持续满 60 秒后，Resolve 与 Admission 同事务 guard 拒绝新 Run，
readiness 返回 503，旧 Receipt 仍可重放。部分应用、更新的源水位、重复/迟到观察与重启
均不重置这个起点；只有完整追到已知最高水位才清空，下次新积压开启新 episode。

来源观察年龄超过 5 分钟仍是另一条门禁；初始化 target 不随运行期 ObserveSource 推进。
本轮不是改大启动 target 来冒充 apply-lag 检查，也不把 60 秒起点解释成 Control 发布时刻
或外部停用传播 SLA。0004 迁移为旧库的已有积压从数据库升级时刻起计时，已追平行保留
NULL；上一切片真实 61 秒 PG 红绿与旧库升级测试已通过，本轮联合回归见[实施状态 §9.5](implementation-status.md#95-实际工作树联合验收)。
历史问题保留在[复审 CGR-27](design-review.md#10-本次说明文档与设计代码对照复审)，后续交付另记第 11 节。

第一切片选择前述“完整保留事件流”路径，而非已实现 Control 全量快照接口；具体
Stream lineage、连续水位和同步 Initialize 行为见[实施状态](implementation-status.md)。

## 5. Admission：受理台

### 5.1 Adapter 与 Application 的边界

Telegram/企微入站 Adapter 负责：

- 外部协议鉴权与可信 `account_id`；
- 请求体、帧与字段上限；
- Provider/SDK DTO 解码；
- 映射为 `AuthenticatedInbound`。

Admission Application 不依赖 Gin Header、Telegram SDK 类型或企微原生帧。它复核输入
完整性与账户运行资格，先查重、再分类，Tenant 只能由 Routing 的可信投影派生。

```go
type InboundAcceptor interface {
    AcceptInbound(
        ctx context.Context,
        input AuthenticatedInbound,
    ) (AdmissionReceipt, error)
}
```

### 5.2 固定顺序

```text
验证来源并固定 account identity
→ 按 ExternalEventKey 查询旧 Receipt
→ 已存在：返回第一次结果，不重新解析 Binding
→ 新事件：分类为 ignore / interaction / admit-run
→ 仅 admit-run 解析当前 RouteSnapshot
→ 原子提交事实与 Outbox
```

V1 中一个 `ExternalEventKey` 永久对应一个首次 Decision/Receipt。Binding 后来切换、停用
或投影暂时不可用，都不为旧事件创建第二次 Admission。V1 不支持把同一个 Provider 事件
隐式扇出到多个 Binding；未来若需要 fan-out，必须新增显式 target identity、聚合 Receipt
和事务协议。

### 5.3 三种 Decision

- `ignore`：记录稳定忽略原因，不创建 Run。
- `interaction`：持久记录交互 Receipt；Provider 即时反馈与 Stop/取消等业务命令分开建模，
  不把按钮文字重新当作普通 Prompt。
- `admit-run`：固定 RouteSnapshot，生成稳定 AdmissionID/RunID/ReplyContext，并写
  `RunRequested` Outbox。

Telegram `answerCallbackQuery` 是 Provider 交互反馈，不是 Webhook transport ACK，也不是
业务操作已完成；跨 Worker 的 Stop/取消命令事件仍需在 D0 冻结。

### 5.4 事务边界

```text
Inbox Event
+ Decision
+ Admission（仅 admit-run）
+ RunRequested Outbox（仅 admit-run）
```

在一个 Gateway PostgreSQL 事务提交。Telegram 仅在该事务成功后返回 HTTP 2xx；企微
handler 完成只表示本地交接，不制造服务端持久消费或离线补发保证。

Application 不应操作多个浅 Repository 来拼事务；内部 Port 应表达整个原子改变：

```go
type AcceptanceLedger interface {
    FindReceipt(
        ctx context.Context,
        key ExternalEventKey,
    ) (AdmissionReceipt, bool, error)

    Commit(
        ctx context.Context,
        change AcceptanceChange,
    ) (AdmissionReceipt, error)
}
```

`Commit` 隐藏唯一约束、并发 loser 读取 winner Receipt、路由 generation 一致性检查和
Outbox 写入。对首次企微事件还必须调用 Connection 的同事务只读 owner guard：校验
稳定账户、实例、owner epoch、账户连接配置代次以及数据库时钟下的租约有效期，校验锁
保持至提交；`ignore / interaction` 也不能绕过 owner。旧 Receipt 重放在该检查之前完成，
Telegram 则完全绕过 Connection guard。本地 fence 不进入 EventKey、SourceDigest 或事件 wire。
具体技术 seam 与锁顺序见 §9.2；这条规则已在本轮实现并联合验收，证据见实施状态 §9.5。

### 5.5 去重还需要内容一致性与保留规则

同一 ExternalEventKey 不能只比较键、忽略内容变化。建议保存版本化 `SourceDigest`：对
明确的语义字段做无损规范化，不把收到时间、连接 epoch、JSON 空白/字段顺序纳入摘要。
未知字段、数字等价写法和编辑事件如何处理要由 Provider fixture 冻结；拒绝重复 JSON key。
同键同摘要返回第一次 Receipt；同键不同摘要记录协议冲突，不覆盖原记录、不再创建 Run。

“永久幂等”约束的是身份与首次决定，不要求永久保留 Prompt 或完整 Provider Payload。
D0-10 要分别定义消息正文、最小去重墓碑、审计记录、Outbox 的生命周期：允许先清理正文，
保留有明确访问控制的最小键/摘要/Receipt；摘要也按受控数据管理。若整个去重身份必须删除，
就要显式缩短支持的重放窗口或引入新的账户身份，不能仍声称永久防重。关闭/删除账户后，
稳定 account_id 不可重新分配给另一个物理机器人。

反向关系也要约束：**同一物理 Bot 不得同时注册为多个有效 ChannelAccount**。建议 Control
在注册/激活事务中，对 `provider + provider scope（如需）+ external bot identity` 保证唯一
有效映射；scope 的具体字段按 Provider 契约冻结，不以用户填写的显示名或 Token 摘要作键。
身份由 Provider 认证/核验结果确认，凭据轮换不改变 account identity。Gateway 对检测到的冲突
投影阻止相关新接纳与建连并记录冲突；它是纵深校验，不能替代 Control 注册处的唯一性约束。
否则两个 account_id 各自取得合法 lease，仍可能连接同一个企微 Bot，并把同一物理消息按
两个 ExternalEventKey 接纳。账户迁移、停用再启用与重新注册必须保留稳定身份/去重关系，
或定义显式的迁移与重放边界，不以删建账户绕开历史 Receipt。该约束纳入 D0-01/05/10。
当前本地账户配置不等于 Control 物理 Bot 注册管理。本轮实现的 Connection 表通过唯一 bot_id 防止两个企微账户获取同一 Bot 的租约，且不删除账户身份；这项本地纵深约束不代替 Provider 身份核验、Control 注册管理或完整冲突投影流程。

### 5.6 本地预算与确认点

readiness 只影响接流量倾向，不是拒绝新接纳的事务屏障。Admission 对**新事件**在
Commit 中检查账户资格和持久容量预算；只有 admit-run 再核验路由 generation，interaction
使用自己的授权依据，不为 ignore/interaction 强行解析运行路由。并发预留/计数必须原子，
不能让所有副本先数后插绕过上限。已持久接纳的重复事件仍返回原 Receipt，不因为 Outbox
饱和就改写首次决定。PG 故障则不凭内存声称查重成功。

interaction 的 Receipt、待执行业务命令及必要 Outbox 也应原子记录。Webhook 2xx、按钮
即时反馈、Worker 接受取消命令和最终取消结果是四个不同事实；D0-03 未冻结前不宣称 Stop
已经端到端可用。Provider 即时反馈的临时调度和失败结果仍应有明确拥有方，不交给 Bootstrap。

### 5.7 Receipt-first 的并发失败窗口

第一次 `FindReceipt` 查空，并不保证稍后的路由解析仍可用。例如 A、B 同时查空，B 先提交，
随后 Binding 停用，A 在进入 `Commit` 前就解析失败。仅靠 Commit 内查重，A 仍可能返回
暂时失败；它不会产生第二个 Run，也不会改写 B 的事实。

目标补充规则：来源鉴权、身份与内容校验有效时，首次处理的准备阶段因动态依赖失败，
在剩余请求期限内使用新数据库快照做**一次最终 Receipt 查询**。同键同摘要则重放；同键
异摘要则冲突；仍无 Receipt 则保留原错误。PG 不可用或期限已尽不能以缓存冒充成功，
也不通过无限重试等待并发请求。该补充不承诺尚未提交的并发结果一定可见。
本轮实现已补此查询路径；实际应用与完整验收见实施状态 §9，不改写 CGR-33 的历史发现。

## 6. Connection：有状态线路管理员

### 6.1 适用范围

Connection 只服务需要长连接和单账户连接所有权的 **stateful provider**；首版就是企业微信。

Telegram 当前采用 Webhook 入站和 Bot API HTTP 出站：Webhook 可进入任意健康副本，HTTP
发送也不依赖独占 owner。Telegram SDK Client 的进程内缓存属于 Adapter 技术优化，不把
Telegram 纳入 owner lease 或 connection generation。

### 6.2 拥有与不拥有

Connection 拥有：

- 本地非秘密连接配置投影：bot identity、credential reference、revision、enabled；
  生产来源为 Control 账户目录。RouteGeneration/Route.enabled 与连接 revision/enabled 分别生效，
  不互相替代；目录/凭据协议及运行接线见 §15；
- 有状态账户的 owner lease、epoch 和 CAS；
- 账户凭据 generation 到本实例 Client 生命周期的切换；
- 企微 Client 启停、续租、失租取消和有界关闭；
- 本实例 Client Registry 与账户级连接状态；这不是 Execution 的 Session 历史。

Connection 不拥有：

- Tenant/Binding/Deployment 选择；
- Admission、Run 或 Delivery 账本；
- WebSocket 帧、subscribe、ping、`req_id` 的协议实现；这些属于 `platform/im/wecom`；
- Provider 的外部 exactly-once 保证。

### 6.3 深 Interface

Bootstrap 不编排 Acquire/Renew/Release、Client start/stop 或重连。Connection 对进程级
调用方暴露一个 Supervisor：

```go
type ConnectionSupervisor interface {
    Run(ctx context.Context) error
}
```

下方为完整目标的用途示意；本轮实现的实际 LeaseStore 使用 ApplyAndAcquire，将可信配置
revision 更新与 claim 合并，并增加 Check、MarkReplaced；精确签名见源码与实施状态 §9。
其内部用途包括：

```go
type LeaseStore interface {
    Acquire(context.Context, AccountID, InstanceID, time.Duration) (OwnerGrant, error)
    Renew(context.Context, OwnerGrant, time.Duration) (OwnerGrant, error)
    Release(context.Context, OwnerGrant) error
}

type WeComConnectionCredentialResolver interface {
    ResolveForConnection(context.Context, CredentialRequest) (CredentialMaterial, error)
}
```

以及 AccountProjectionReader、ClientFactory 和 ClientRegistry。WebSocket 建连、Reply 和
Close 位于数据库租约事务外。失租、停用或凭据轮换触发旧 Client context 取消并停止重连；
正常 shutdown 按第 10 节先关闭新工作入口、有界 drain 并续租，完成后关闭 Client；失租或
超时再进入立即取消分支。Supervisor 不把同一个已取消 context 同时用于 drain 和 renew。

渠道凭据的 Owner/解析协议仍是 D0。Connection 只消费企微建连用途的窄解析 Port；Bot
Token/企微 Secret 不复用 Worker Profile 的
`RuntimeCredentialResolver`，也不使用 `CONTROL_PROFILE_CREDENTIAL_KEY`；值不进入路由事件、
NATS Payload、普通投影、日志或指标。

### 6.4 入站失败与连接恢复的组合契约

实际公开库的活动 handler 返回错误会结束当前 `Run`，并归类为 `ErrHandler`；
其内置传输重连不重试该错误。参见[库生命周期](../../../platform/im/wecom/README.md#公开-api)
及[当前实现](../../../platform/im/wecom/client.go)。因此把 Admission 返回值原样交给 SDK，
再把所有终态封禁到配置升级，会把临时 PG/投影故障升级成账户长期停止。

目标在 Gateway Provider Adapter 与 Connection 的 seam 上区分原因，不让公开 SDK
依赖 Admission 错误，也不让 Connection 猜测 SDK 包装错误中的业务原因：

| 原因 | 目标恢复责任 |
| --- | --- |
| 同键异内容、确定无效输入 | 入站 Adapter 记录明确拒绝；不创建 Run，不因单个坏事件终止整账户；反馈完成不是远端持久 ACK |
| 临时接纳不可用 | Adapter 在当前有效 owner/context 内有界重试同一事件，保持原 EventKey/摘要；reader/心跳独立继续工作 |
| 重试期限或队列预算耗尽 | 停止接入并报告账户级不可用；用明确的暂时失败类别进入有上限/退避的恢复路径，而非永久封禁相同配置代次 |
| 认证拒绝、协议不兼容、SDK 重连预算耗尽 | 记录各自终态与恢复条件；不得无限新建 Client 清零重连预算 |
| 已确认 replaced | 按 §6.5 隔离相应账户配置代次；不是普通传输断线 |

具体重试时限、退避/最大次数、是否及怎样跨重启保留预算、恢复触发方式在 D0-02/08
冻结并测试。暂时恢复不依赖更换凭据或递增配置版本；系统也不能因此制造无限内存队列。
超出本地保留窗口或断线丢失的未接纳 callback，不能据此宣称远端会补发。接纳重试更不
授权重发 UNKNOWN Final；发送副作用继续由 Delivery 决策。

### 6.5 Replacement 隔离不能依赖 Client.Close 成功

公开库停止被替换的 Client 只解决本实例；若 Supervisor 在 lease 到期后重新创建 Client，
或者另一副本重新取得同配置资格，仍可能发生来回抢连接。因此目标 Connection 需要
跨副本持久的账户恢复/隔离事实，而不是只有进程内 `Replaced` 标志。

已确认 replacement 后，先关闭新操作入口并取消旧 Client，用独立、有界 context 持久
记录对应账户、配置代次与 owner grant 的隔离原因；写入按原 owner/epoch/配置代次 CAS，
旧通知不能隔离更新配置或新 owner。**Client.Close 失败不应跳过这次记录尝试**，清理
失败与隔离失败分别可观测；失败路径不能通过 Release 加速未经隔离的接管。

持久记录成功后，相同配置的其他副本也不自动重拨；恢复须来自可信更新配置或显式、
有版本的解封动作，精确接口仍待 D0 冻结。数据库不可写时只能保留本地停止/故障状态，
不能宣称跨副本隔离已提交；残余 lease 到期风险须纳入故障测试与运维处置。
这与普通临时 Admission 错误的恢复路径不同，不以一个 `Terminal` 布尔值代替两种策略。

### 6.6 当前本轮实现采用的首版切片

Connection 已有账户 Domain、持久 Store、Supervisor 与企微入站；本切片增加供 Delivery
使用的 ReserveFinal 一次性 Sender。生产 ReplyIntent Consumer/调度仍未装配；“可组合并通过
本地纵切”不等于完整外部 Final 已启用，精确契约见[Final V1](delivery-final-v1.md)。
实际 LeaseStore 为 ApplyAndAcquire/Renew/Release/Check/MarkReplaced；账户配置 revision
独立于 RouteGeneration，永久账户行、递增 epoch 与 blocked_revision 由 0005 保存。

Adapter 对同一归一化事件最多重试 6 次/2 秒、100ms 起步封顶 400ms；仅明确临时耗尽才
由组合 Adapter 标为 Retryable。Supervisor 默认 1/2/4 秒最多 3 次重建，之后每 60 秒半开，
短暂 Ready 不清预算；范围为本进程/账户/revision。replaced 用 PG 隔离，不走该普通重建路径。
共享 Ready 检查 Supervisor 控制循环/完整配置源，不要求每个 Bot ready；账户错误独立记录。
实际配置、锁序、轮换、隔离结果字段、验收层级及剩余目标统一见[实施状态 §9](implementation-status.md#9-connection-与企微持久入站已应用并验收)。

## 7. Delivery：外部投递账本

### 7.1 输入与输出

Delivery 幂等接收 Worker 的 `ReplyIntent`，拥有独立 Delivery/DeliveryAttempt 状态和外部结果。
Worker Run 完成与外部 IM 送达是两个不同事实。

```go
type ReplyIntentAcceptor interface {
    AcceptReplyIntent(
        ctx context.Context,
        intent ReplyIntent,
    ) (DeliveryReceipt, error)
}

type DeliveryDispatcher interface {
    Run(ctx context.Context) error
}
```

上面保留目标 Interface。当前 Dispatcher 仍提供 DispatchAccount/Recover，新增 Runner.Run
负责有界账户发现并组合它，另有可独立运行的 Maintainer。Control 模式由 Runner 独占 Maintenance 生命周期，
fixture 模式由 App 独立维护；Bootstrap 只启动/关闭，不编排 claim/reclaim、账户选择或重试状态机。
精确生命周期、默认预算与本轮验收状态见[Runtime V1](delivery-runtime-v1.md)。

### 7.2 先保存确定性，再计算重试策略

Provider 调用事实统一为：

```go
type Certainty string

const (
    CertaintyNotSent  Certainty = "NOT_SENT"
    CertaintyAccepted Certainty = "ACCEPTED"
    CertaintyRejected Certainty = "REJECTED"
    CertaintyUnknown  Certainty = "UNKNOWN"
)
```

`RETRYABLE/PERMANENT` 是 RetryPolicy 根据：

```text
certainty + operation kind + deadline + provider error class
```

计算出的后续动作，不是 Provider 调用本身的事实。`UNKNOWN Final` 默认停止普通自动重发，
进入专用恢复或人工审计。

### 7.3 外部副作用事务边界

为了与下方 `ClaimDue` / `MarkCalling` 两个持久调用保持一致，建议明确区分**领取资格**与
**开始外部调用**，而不是把它们写成一个实际上并不存在的事务：

```text
事务 A1：PENDING → CLAIMED（claim token + claim deadline）
→ 本地准备、会话额度与发送资格检查，不调用 Provider
→ 事务 A2：校验有效 claim token/deadline，原子 CLAIMED → CALLING + attempt CAS
→ 仅 MarkCalling 确认提交成功的调用方，在数据库事务外调用 Provider
→ 事务 B：按 attempt CAS 写 ACCEPTED/REJECTED/NOT_SENT/UNKNOWN
```

`MarkCalling` 是唯一发送门。失败、超时或提交结果不确定时不发起 Provider 调用，也不以
内存中的领取结果绕过它。旧 claimant 在 claim 过期或被接管后，必须在此被 CAS 拒绝。
批量 `ClaimDue` 只预留工作，不把整批未发送项提前标成 `CALLING`。

Telegram 只使用 Delivery claim/attempt CAS；企微等 stateful-provider Delivery 还必须在
调用前与推进活动投递状态时额外核验 owner epoch。该附加 fence 不把 Telegram 耦合进 Connection。

**推进状态的权限与保存调用证据分开。** 旧 owner 收到可与原请求唯一匹配的明确 ACK，
即使当前 fence 不允许 `Finish`，也不能直接丢弃证据并只留下 UNKNOWN。账本需要独立的、
幂等的结果观察记录路径，严格关联已存在的原发送 Attempt、请求相关 ID 与可信调用身份；
不接受任意 Worker 自报的发送成功。若原 Attempt 仍满足完成条件，可按状态 CAS 收束；
若已恢复或接管，则仅追加晚到观察，由恢复/审计用例处理，不覆盖新 Attempt、不自动重发。
恢复与晚到观察并发时也保持这条规则。缺少可信明确结果时必须保留 UNKNOWN；已有可信
Observation 也不自动推进状态，未执行显式恢复或存在矛盾证据时仍保持 UNKNOWN。
当前 Final 首版已实现 Observation capability/Schema/Port 与显式 ResolveObserved；
精确结果规则见[Final V1 §6](delivery-final-v1.md#6-unknown-与晚到证据cgr-32)。完整保留/GC、
真实 Execution 授权拥有方和生产调度仍属 D0-09/10。

恢复时分别处理两个窗口：

| 持久状态 | 可能的崩溃窗口 | 恢复语义 |
| --- | --- | --- |
| `CLAIMED` 已过期 | 领取完成，但 `MarkCalling` 尚未提交 | 按旧 claim token CAS 释放/重新调度；因为发送门尚未通过，确定未调用 Provider |
| `CALLING` 已过期 | `MarkCalling` 已提交，实际发送前或发送后均可能 | 保守进入 `UNKNOWN`；即使本地没有成功结果，也不猜测“确定没发” |

Claim deadline 与 CALLING 恢复期限是不同概念；后者需覆盖有界 Provider 调用与结果提交窗口。
数据库恢复要与迟到的 `MarkCalling` / `Finish` 使用同一状态 CAS；旧 token/attempt 的完成
不得覆盖接管后的记录。具体状态字段与期限纳入 D0-04/08/09 的 schema 和竞争测试。

```go
type DeliveryLedger interface {
    Accept(context.Context, ReplyIntent) (DeliveryReceipt, error)
    ClaimDue(context.Context, InstanceID, int) ([]ClaimedDelivery, error)
    MarkCalling(context.Context, ClaimedDelivery) (DeliveryAttempt, error)
    Finish(context.Context, DeliveryAttempt, DeliveryResult) error
    RecoverExpiredClaims(context.Context, time.Time) error
    RecoverStaleCalling(context.Context, time.Time) error
}
```

Telegram Sender 直接调用 `github.com/go-telegram/bot`，并通过 Delivery Adapter 自己定义的
purpose-scoped `TelegramSenderProvider` 按 account/credential generation 取得已认证 Sender；
Token 的解析、内存缓存与轮换协议属于 D0-01，但不经过 Connection lease。WeCom Sender 通过
Delivery 自己定义的窄 `WeComSessionLookup` Port 预留与原 ReplyOrigin 匹配、且当前仍有效的
本地 Client 能力（§7.5）；Delivery
不 import Connection 的 PostgreSQL Adapter，也不自行抢连接。

候选 Port 只表达用途，不把凭据值放进 Domain：

```go
type TelegramSenderProvider interface {
    SenderForDelivery(
        context.Context,
        AccountID,
        CredentialGeneration,
    ) (TelegramSender, error)
}

// 候选用途示意，类型/字段仍待冻结；不是当前可编译 API。
type WeComSessionLookup interface {
    ReserveOriginal(
        context.Context,
        WeComReplyOrigin, // account + instance + epoch + config revision + socket generation + req_id
    ) (WeComSenderReservation, error)
}
```

### 7.4 回复目标、执行代次和展示顺序

Delivery 不能因为事件来自有权限发布的 Worker 就相信其中任意 chat_id。建议在其 Application
定义 `AdmissionReplyReader`：按 admission_id/run_id 读取 Admission 拥有的不可变回复快照；
返回 Delivery 自己的只读类型，由 Bootstrap bridge 转换，不暴露 Admission 的数据库 Row。
校验 tenant、account、run 与原 Admission 一致，固定原 ReplyContext，Binding 后续切换也不
把旧 Run 的回复送给新目标。账户停用是否阻止已有回复由独立发送资格策略决定，不改写历史事实。

Worker 负责在自己的执行事务中 fence 失效 Attempt，只从被接受的执行事实生成 ReplyIntent。
D0-09 仍须冻结 Execution 侧授权依据与 Gateway 校验方式（可信终态证明或窄验证 Port），
不能把“具有 NATS Publish 权限”当作“这次 Attempt 有权向这个目标发送”。

**Final 授权细化（D0-09，Gateway Final V1 已实现，Execution 接线待交付）**：
验证对象是 Execution 已接受并持久提交的不可变完成
事实，而不是“此刻仍活跃的执行 lease”。正常完成释放 lease 后，合法 Final 不因投递延迟
而失去依据。Gateway 当前按 Final V1 精确绑定 tenant、admission、run、被接受的 execution
attempt / generation、completion identity、intent identity/content digest、sequence 与固定
manifest digest；Port/字段已有实现，可信 Execution 实现、认证传递与保留期仍须联合交付，
不在此创建第二份 wire Schema。
首次接纳必须取得并核验该事实。暂时未查到完成事实、验证依赖不可用与明确授权否决
分别建模：只有可信验证协议确认的永久否决，或已取得证明与期望身份/摘要不匹配，才进入
未来 Consumer 的持久拒绝路径；暂时不可验证保持有界失败/重试，不先写永久拒绝 Receipt。
精确错误分类仍须由 Execution 验证协议与 §7.7 的接线验收冻结，不默认许可。
已持久接纳的同摘要意图先返回原 Receipt，不因当前 lease、Binding 或验证服务状态
变化产生第二次投递。Final 屏障须覆盖 Run/展示流，不能让新 attempt generation 自动绕过
同一逻辑 Final 的唯一性；Progress 的活跃权限另行设计。

还要分别冻结：稳定 intent_id 与内容摘要、每个 Run/展示流的 generation + sequence、Final
屏障、分段序号和 deadline。旧 Progress 在 Final 之后到达时不再覆盖 Final；Final 的多个
分段各自保存结果，某一段 UNKNOWN 不重发已 ACCEPTED 的整组。Progress 可以合并或丢弃，
Final 不使用 Progress 的可丢弃策略。单个网络调用的成功不能冒充整条逻辑回复完成。

上文保留完整目标。当前 Final/text 的确切 Schema、字段、授权 Port 和 Run 唯一屏障
已按[Final V1](delivery-final-v1.md)实现；Progress/多展示流及真实 Execution 接线仍待交付。

### 7.5 企微原回复关联不等于当前发送资格

当前公开 Go P0 的 `Event` 与 `ReplyRequest` 都要求保留原 `RequestID + Generation`；
见[原生类型](../../../platform/im/wecom/types.go)。CGR-34 复审时 Admission 入站只把 `req_id` 放入
ReplyContext，未持久保存 socket generation；ConnectionFence 仍是 `json:"-"` 的临时授权
上下文。该缺口现由 0006 的独立 ReplyOrigin 与 Connection ReserveFinal 处理，见
[Final V1 §7](delivery-final-v1.md#7-企微-replyorigin-与受限-sender)；不以新实现改写历史复审结果。

目标契约：

1. **Admission 保存来源**：首次成功接纳时，在自有事实中另外保存 ReplyOrigin，包含可信
   account、原实例、owner epoch、连接配置 revision、SDK socket generation，并与原 callback
   req_id / ReplyContext 绑定。当前 Final 首版字段与 0006 见 Final V1；旧迁移不重写。
2. **去重与授权分离**：来源不进入 ExternalEventKey / SourceDigest，也不放入 RunRequested
   或 ReplyIntent。重复 callback 在新连接上到达仍返回原 Receipt，不能更新首次来源。
   企微新接纳的 Connection guard 仍用于授权当前提交；保存原来源不取代该 guard。
3. **Connection 只交付匹配能力**：Delivery 用自己的窄 Port 申请原来源；Connection 必须
   匹配原实例、epoch、revision、Client/socket generation 与请求关联，并验证当前 lease。
   同一 owner 内重连也必须拒绝旧 generation，不能只比较 account/epoch。
4. **准备和发送分离**：预留 Sender 不产生 Provider 副作用。只有 A2 确认提交后才调用
   一次性、有界的 Sender；能力有效性还需在调用前校验。Release 在回执/有界证据持久化后
   结束本地 reservation，使正常 drain 覆盖这段工作；不持有 PG 事务等待网络。
5. **失效与恢复**：历史 Admission 缺少 ReplyOrigin 时，当前 AdmissionReplyReader 返回
   不支持，新 Delivery 尚未建立；不是已经持久记录了 Preparation 失败。已经接纳的 Delivery
   后来发生失配或原进程退出，才由 FinishPreparation/deadline 收束。两条路径都不猜当前
   连接、不重新绑定 req_id，也不把 Sender lookup 失败当作已调用 Provider。
   已有 CALLING/UNKNOWN 保持原投递事实，晚到 ACK 仍走 §7.3 的 Observation。
   当前 P0 不自动接续旧流；未来若要跨连接恢复，必须有单独验证的协议与授权契约。

验收至少覆盖：同 owner 重连、owner 接管、配置轮换、重复事件不改来源、历史 NULL、
A2 前后取消、一次性发送、ACK 后落库前失租、正常 drain 等待及 deadline 到期。
这是 Gateway 组合设计的保守规则，不新增企微服务端的跨连接/离线补发保证。

<a id="76-后台调度与无连接时的恢复目标补充尚未实现"></a>

### 7.6 后台调度与无连接时的恢复（当前实现与剩余接线）

Delivery 的恢复不依赖“本实例恰好能发送”。当前 Maintainer 独立、有界地运行过期 CLAIMED
回收、stale CALLING→UNKNOWN、未发送 pending 的 deadline 清理及候选 Observation 恢复；
默认 App 已接此维护循环。Control 模式的 Runner 已提供有界账户扫描/调度，ReplyIntent 输入与真实 Final verifier 仍待接入。
ClaimDue 仍不是完整清理器；独立 ExpirePending/发现 Port 与 0007 已补齐 CGR-35 的实现，
本轮最终验收已通过。精确语义见[Runtime V1](delivery-runtime-v1.md)。

- 独立过期清理使用 PG 时钟、有限批次和 part 行锁/CAS；即使账户停用、无 owner，或前一
  part 为 UNKNOWN，也能收束到期 PENDING/可重试 NOT_SENT。它不发起发送，也不需要 owner。
- 清理不把 CALLING/UNKNOWN 改成 EXPIRED 来假装“确定没发出”，不改已完成历史。
  expiry 只推进状态，不删除 Intent/part、Receipt 或 Final 屏障；当前总行数容量计数不会
  因 EXPIRED 自动释放，容量回收另需保留/GC 设计。
- 到期账户按稳定 keyset 轮转；跳过非本地 owner/不可用账户也推进游标。候选证据扫描同样
  轮转，避免第一页的冲突证据阻塞其他 Attempt。分页、查询时间、工作池和每轮任务量均有上限。
- 单个 Runner 实例对同一 provider/account 最多一个在途 dispatch，每次交给 Dispatcher
  的 ClaimRequest 限定一个 part；不要大量预领再排队到 claim 到期。生产装配还须保证
  同一进程的账户调度范围不重叠；当前没有进程级共享 active map，也没有重复装配检查。
  同 part 的 PG claim/CAS 不等于同账户跨 Runner 串行；限流公平性仍须负载验证。
- Connection 的本地当前资格通过窄读 Port 提供，不能从 Status 或原 ReplyOrigin 拼接 grant；
  它只是 eligibility 预检查，A1/A2 的权威 owner guard 仍须执行。Telegram 不增加 lease。
- 正常停止先关 scan/claim 入口，有界等已开始调用及证据落账，再关闭 Connection/PG。

验收覆盖无人 owner 到期、前段 UNKNOWN 后段过期、claim/expiry 竞争、多页公平、全部账户
不可用时仍恢复，以及 Quiesce 后不接纳新的 dispatch。已接纳的在途工作按有界上下文
收束，不宣称取消外部副作用。以上源码与对应测试已有，本轮实际应用/运行验收见实施状态 §11；
生产 Runner 装配、全局配额和负载公平性仍待交付。同一 Maintainer 不可同时由 App 和 Runner 启动。

### 7.7 ReplyIntent 传输确认点（D0-06/09/10 的补充）

Consumer 只把事件交给 Delivery 的持久接纳/拒绝用例，随后确认消费；不等待 Provider
发送结果。当前 eventadapter 仅做严格解码/映射，并不是已经运行的 NATS Consumer。

目标接入须区分：业务幂等键 intent_id + 内容摘要，以及可信 Broker 源身份/sequence + 原文摘要。
确认成功/永久拒绝之前，PG 必须已接管相应事实；ACK 丢失后重放不依赖当前验证服务在线。
接纳事实与 transport receipt 的提交协议需明确冻结：同一事务则复用账本内部写入；若选择
分事务，必须测试 Final 已提交而 receipt 未提交的恢复窗口，且在 terminal receipt 持久前
不 ACK。不得把两次提交描述成原子事务，也不持有 PG 事务等待远程授权。

永久坏 schema、确定冲突或明确授权否决，先持久拒绝再 ACK；PG/容量/验证依赖暂不可用、
内部 schema 初始化失败则延迟重试。未查到完成事实不等同于可信地确认无权完成；错误分类
须由 Execution 验证协议冻结。接入初版不以 Term 代替拒绝事实，单条业务拒绝不封禁整个路由。

若 Reply 采用 WorkQueue，正常 ACK 会删除消息，不能复制 Routing 完整保留日志的连续水位
算法；应校验可信源身份/subject/sequence 并逐条保存 receipt，源重建走显式恢复。上述区别
依据 [NATS 官方保留策略](https://docs.nats.io/learn/jetstream/retention-policies)（2026-09-05 查阅）；
它不提供外部 IM exactly-once 保证。当前拓扑仍仅 Route/Run 两个 stream；新增 Reply 的资源、
窄 ACL、拒绝容量与保留上限以及故障测试须随该切片交付，不新增 Connector Workload。

## 8. 两条完整流程

### 8.1 Telegram 文本消息

```text
Telegram HTTPS Webhook
→ telegramadapter 验证 ingress key/secret header 与 Body 上限
→ 映射 AuthenticatedInbound
→ Admission 先查 ExternalEventKey
→ 首次 admit-run 才调用 Routing.Resolve
→ PG 原子提交 Inbox + Decision + Admission + RunRequested Outbox
→ 提交后返回 HTTP 2xx
→ Relay 发布 RunRequested
→ Worker 执行并产生 ReplyIntent
→ Delivery 持久接纳、claim、MarkCalling
→ telegramadapter 直接调用第三方 Go SDK
→ Delivery 保存确定性与 RetryPolicy 结果
```

普通 Telegram 路径不经过 Connection。生产入口也不直接使用会早于业务持久提交返回 200
的 SDK 默认 Webhook Handler。

### 8.2 企业微信文本消息与 Final

```text
可信 ChannelAccount 运行配置投影（Control 完整快照；fixture 可用账户文件，独立于 Routing 的 Agent 路由投影）
→ Connection Supervisor 尝试取得 owner grant
→ 解析该 credential generation 的企微凭据
→ 创建 platform/im/wecom.Client
→ Client.Run：connect/subscribe/ping/read/reconnect
→ wecomadapter 将原生 Event 映射为 AuthenticatedInbound
→ 进入共用的 Admission 用例；旧 Receipt 重放不重新解析 owner
→ 新企微事件在接纳事务内额外通过 Connection owner guard，固定原 ReplyContext + ReplyOrigin
→ Worker 产生有不可变完成事实依据的 ReplyIntent Final
→ Delivery 持久接纳，并在当前 owner 资格下领取
→ WeCom Sender 只预留与原 ReplyOrigin 匹配的 Client；A2 提交后才发送
→ Client.Reply 返回明确 ACK、拒绝、确定未发出或 UNKNOWN
→ Delivery 按 attempt/owner epoch CAS 记账
```

企微远端不验证平台 Owner Epoch。旧 owner 在途发送可能已经成功，因此本地 fence 只阻止
后续新调用和过期本地提交，不能把远端副作用撤销。

## 9. 编译期依赖和装配

```text
cmd
  → bootstrap
  → module/wiring.go
  → adapter
  → application
  → domain
```

固定规则：

1. Domain/Application 不 import Telegram SDK、Gin、pgx、NATS 或 `platform/im/wecom`。
2. Provider SDK 只进入相应 Adapter 与明确的纯装配入口；Bootstrap 可以构造/注入
   Client，但协议转换、业务重试、账户生命周期必须放回对应 Adapter/Application。
   “允许装配 import”不允许 SDK 类型进入 Domain/Application 的 Port。
3. Application Port 由使用方定义；Bootstrap bridge 显式注入。
4. `platform/im/wecom` 只拥有外部协议，不依赖 `services/`、PG、NATS、Tenant 或 Run。
5. 跨 Workload 协议只进入 `api/` 与 `gen/`；SDK DTO 不进入业务事件或数据库 Row。
6. 不建立全局 `common/types`、巨型 Connector Interface 或 Service Locator。
7. 一个根 `go.mod`；公开企微库无独立 Dockerfile、迁移或部署单元。

### 9.1 目录怎样分工

以下是职责目录草图，不创建占位目录，也不要求每种 Adapter 都有一个空包：

```text
services/channel-gateway/internal/
├── bootstrap/                         # 组装模块、bridge、启动和有界关闭
├── routing/
│   ├── domain/                        # generation、snapshot、tombstone 不变量
│   ├── application/                   # Apply/Resolve、消费方需要的 Port
│   └── adapter/                       # Control event 解码、投影持久化和同步
├── admission/
│   ├── domain/                        # EventKey、Decision、Receipt、Admission
│   ├── application/                   # 查重/分类/原子接纳、Outbox relay 用例
│   └── adapter/                       # Telegram/企微入站、账本、发布传输
├── connection/
│   ├── domain/                        # OwnerGrant、epoch、账户连接资格
│   ├── application/                   # Supervisor、失租/轮换/关闭状态机
│   └── adapter/                       # lease store、公开企微 Client、内存 registry
├── delivery/
│   ├── domain/                        # ReplyIntent、DeliveryAttempt、Certainty
│   ├── application/                   # 接纳意图、发送资格、调度、恢复和重试
│   └── adapter/                       # Worker event、账本、Telegram/企微 Sender
└── infra/                             # PG/NATS/HTTP/telemetry 技术连接
```

各 Module 的 `wiring.go` 是自己的装配入口，不是业务服务定位器。Module 内部的 `application`
可有多个文件，但对调用方只暴露少量用例。Relay 的技术 Publish Adapter 可复用 NATS 连接，
其事件身份、Outbox 事务、claim/ack/retry 策略仍由产生事件的 Module 拥有，不转交给 infra。

若四个人并行开发，应按四个 Module 分工，各自交付完整用例、Adapter 和测试，**不是**一人
只写所有 Handler、一人只写所有 Repository。公开企微库单独承担协议实现工作，但它与
Connection/Delivery 共用测试契约，不增加部署单元。第一批端到端闭环仍先串 Routing 与
Admission；Delivery 需要真实 Worker 契约，Connection 需要已验证的企微 Client。

<a id="92-跨-module-的同事务只读校验-seamd0-04-提案"></a>

### 9.2 跨 Module 的同事务只读校验 seam（已落实的切片与剩余扩展）

不能在 Application 先调用 `Resolve`/Sender lookup，然后就假定稍后 Commit 时版本未变。
当前 Admission/Routing/Connection/Delivery 切片已采用**基础设施层、使用方定义的只读 transaction guard**，下面按其职责描述，不新增第二套事务方案：

- Admission 的 PostgreSQL Adapter 拥有接纳事务，接收 Routing 提供的 guard；guard 用
  **同一事务**核验账户/路由 generation，并将对应读锁持有至接纳提交。Routing 更新与 guard
  使用同一把锁/一致的锁顺序，锁还要覆盖“尚无投影行”的情况，不能只依赖锁已存在的行。
- Admission 对**新企微事件**还注入 Connection 提供的只读 guard：EventKey 事务锁与
  Receipt 查重之后、事实写入之前，核验账户/实例/owner epoch/连接配置代次与租约期限，
  并持锁到接纳提交。`ignore / interaction` 也检查 owner；旧 Receipt 不重查，Telegram 绕过。
  当前统一锁序为 EventKey → 本地预算 → Connection owner → Routing projection（仅新 Run）；
  所有写入方须遵循兼容锁序，租约时间在获得相应锁后由 PG 采样，不能使用等锁前的旧时间。
- Delivery 的 PostgreSQL Adapter 对企微 claim、`MarkCalling`（A2）与活动状态结果提交采用相同
  模式，调用 Connection 提供的 lease guard；owner/epoch 检查必须包含 A2，而非只在 A1
  领取时检查。核验 owner、epoch、数据库时间和到期条件；Grant token 或内存 registry
  命中不能代替数据库校验。Telegram 只使用自身 attempt CAS，不调用该 lease guard。
  此 fence 不用于丢弃已失租调用的可信晚到 ACK；原 Attempt 的受限观察记录按 §7.3 单独保存。
- `pgx.Tx` 等类型仅存在 PostgreSQL Adapter 内的技术 seam，不进入 Domain/Application。
  Owner 的 Adapter 只实现自己数据的只读检查/锁定，使用方不复制 Owner SQL、不改 Owner 表。
  Bootstrap 显式注入；Module 的 Adapter 不 import 另一个 Module 的 Adapter。
- 跨 Module 只读锁定不改变写入拥有权，也不允许跨 Workload SQL/事务。若今后不共享本地
  Gateway 数据库，就需要重新设计版本验证协议，不能直接复用这个实现。

这些既有切片的锁序与并发验收见[实施状态 §9–11](implementation-status.md)，不整体重新
标为“尚未实现”。新增容量排空或 Reply transport 若引入新事务，仍须单独冻结锁顺序、
竞争重试及故障矩阵；既有 guard 不自动解决这些扩展。对 Provider 的网络调用始终
在数据库事务外；即使发送前 guard 成功，远端在途请求仍可能越过 lease 到期，结果需按
UNKNOWN 或原 Attempt 的明确结果证据收束（§7.3），不能靠把事务一直锁住来制造外部 exactly-once。

## 10. 健康与关闭语义

- `/livez` 只表示进程主循环存活；单个企微 Bot 的 auth/连接失败不拖垮整个副本。
- 路由来源观察超时与运行期 apply lag 是两种已接入门禁：前者为 5 分钟，后者为持续已知积压满 60 秒；见 §4.4，不以源 heartbeat 成功代替追平。
- `/readyz` 启动时要求 Gateway migration、PG 可事务、路由投影通过完整性水位判定已初始化（允许空集合），并确认 JetStream
  topology 已校验/应用。运行期 NATS 短时不可达可以在 Outbox 容量/最老年龄门限内保持
  ready 但报告 degraded；越过 D0-08 阈值后在 Admission Commit 拒绝新事件，而不只切换探针。它不要求“所有 Bot 都 ready”。
- 正常 shutdown 先摘 readiness，关闭新 Admission、claim、新发送与重连入口；在有界 drain、
  结果提交和 Client 关闭期间保持续租。Client 关闭、账本收束后再停止 renew 并释放 owner grant，
  最后关闭 NATS/PG，避免自己提前放弃 lease 扩大接管重叠。
- 失租、续租失败或 drain 超时走快速关闭：立即取消 Client、停止重连，未确定发送保留 UNKNOWN。
  cleanup 使用独立且有截止时间的 context，不复用已经取消的运行 context；保持续租也受同一
  shutdown 总期限约束，不无限拖延退出。

## 11. Helm 与运行形态

Compose 在各 Workload 实现时持续承担本地集成与单机验收。Helm 不属于 Gateway 的 P1/P2
或 G0-G4：它只在 Control、Gateway、Worker、Local IM 和前端等全部生产 Workload 的镜像、
端口、Probe、权限与 Secret 契约稳定后，进入 **FINAL-INTEGRATION**。

最终 Helm 中仍然只有一个 Channel Gateway Deployment；Routing、Admission、Connection、
Delivery 是同一容器内部 Module。不会产生 `wecom-connector` Deployment/Service，公开
`platform/im/wecom` 也没有端口或 Kubernetes 资源。

## 12. 常见错误设计

- 把四个 Module 拆成四个微服务或四个 Helm Deployment。
- 把 Telegram 强行纳入企微式 owner lease。
- 接收重复事件后重新解析当前 Binding。
- 用 `inbox_event_id + binding_id` 暗中支持未设计的多 Binding fan-out。
- 在 `AuthenticatedInbound` 中预带 TenantID/BindingID。
- 将每个 Update 都转换为新 Run。
- 把 Telegram HTTP 2xx、NATS ACK、企微 handler nil 和 Provider Reply ACK 当成同一种 ACK。
- 在 PostgreSQL 事务内等待 Telegram HTTP 或企微 ACK。
- 把 `ReplyIntent` 或 Worker Run 完成当成外部投递成功。
- 对 `UNKNOWN Final` 使用普通网络重试策略。
- 混淆 Owner Epoch 与 Connection Generation，或声称本地 fence 能撤销远端副作用。
- 把 Token/Secret 放入 NATS、路由投影、普通 JSON、日志或 Metric。
- 暴露 Acquire/Renew/Release，让 Bootstrap 编排 Connection 状态机。
- 使用空 `doc.go`、空 Module 和空 Adapter 目录冒充纵向切片。

## 13. 已形成方向与 D0

已形成方向：

- 四个业务 Module 同进程、单二进制、单镜像；
- V1 单外部事件固定一个首次 Decision/Receipt，不做隐式多 Binding fan-out；
- Telegram 普通路径不经过 Connection；
- Connection 外部 Interface 为 Supervisor，租约细节留在实现内部；
- Delivery 先记录确定性，再计算 RetryPolicy；
- Helm 延后至全部 Workload 完成后的 FINAL-INTEGRATION。

### 13.1 D0 议题与剩余范围

D0 是分切片的设计议题索引，不把已实现选择重新列为全新待办，也不因一个 Port 存在就
关闭整项。以下“已有”表示源码选择；既有运行证据保留原范围，新 Runtime 的本轮
最终验收见实施状态 §11，不以已有源码或旧日志声明新增实现已验收：

| 议题 | 本工作树已有选择 | 仍需补齐或冻结 |
| --- | --- | --- |
| 账户与凭据 | Control mTLS 完整目录、精确用途/版本凭据、失效资格、动态 Telegram 注册与轮换；fixture 显式选择 | 真实企微账户兼容性、进一步故障/容量验收 |
| 公开企微状态 | `State()` / `States()`、有序 snapshot 序号与有界队列已实现；慢读取可跳号 | 后续协议扩展与真实账号兼容性；累计 Final 身份容量耗尽的 Gateway 组合策略（CGR-37） |
| 交互 | Admission 可保存 interaction Decision，不物化 Run | Stop/取消命令、授权、即时反馈与 Worker 最终结果协议 |
| 持久存储 | Gateway 独立 database/role、自有 0001–0010、启动迁移与 SHA ledger；0007 仅新增五索引 | Worker 自有表/迁移、全平台权限与恢复验收；不替已有 Gateway 再指定第二迁移执行者 |
| 事件与传输 | Route/Run Schema、Subject/Retention/Durable、真实 Control publisher 与 Telegram 入站已验收；ReplyIntent Final 业务 Schema 已有 | Reply transport receipt/拒绝、topology/ACL/consumer 与恢复协议 |
| 运行阈值 | Connection/Outbox 与 Routing 门禁保留；新增独立维护、有限页/worker Runner 与当前业务 deadline 过期（CGR-35，本轮验收已通过） | ReplyIntent 生产输入、渠道有效截止与会话预算、生产负载参数/公平性验收 |
| Conversation / Session | 外部会话身份由 Gateway 固定传递，Session generation 与执行顺序归 Worker | Worker 物化、演进/重置与跨渠道身份规则；不让 Gateway 接管执行状态 |
| Final 授权与展示 | CommittedFinalVerifier Port、精确匹配、原 ReplyOrigin、每 Run 一个 Final 屏障已实现 | 可信 Execution 完成事实/认证/Outbox、生产接线，以及后续 Progress/多展示流 |
| 保留与恢复 | SourceDigest 与永久首次决定语义、固定容量、部分账本恢复已实现 | 去重墓碑与正文保留分离、GC、容量回收、传输超窗/源重建与观测 |

当前精确实现见[实施状态](implementation-status.md)和[Final V1](delivery-final-v1.md)；
剩余风险及本轮实现见[最新记录](design-review.md#18-delivery-runtime-v1-实现与剩余生产门禁)。

## 14. 用失败场景检查四者有没有混在一起

| 场景 | 谁负责 | 必须观察到的结果 |
| --- | --- | --- |
| 相同消息重投，但 Binding 已切换 | Admission | 返回第一次 Receipt；不重新路由或生成新 Run |
| 首次接纳与 Binding 停用同时发生 | Routing + Admission guard | 提交在线性化点看到一致 generation；冲突重试/拒绝 |
| Control 当前没有任何账户 | Routing | 完整空快照可初始化；新输入无路由被明确拒绝 |
| NATS 暂断，PG 可用 | Admission/Relay | 在持久预算内接纳；恢复后发布，不让 HTTP 等 Worker |
| 同一物理 Bot 被两个账户同时激活 | Control + Routing | 注册唯一性拒绝冲突；冲突投影阻止相关新接纳与建连 |
| 企微失租后旧排队 callback 才提交 | Connection + Admission | 新事件 owner guard 拒绝；旧 Receipt 仍可重放；Telegram 不受影响 |
| 临时接纳失败后恢复 | 入站 Adapter + Connection | 有界重试/退避，恢复不要求改配置；不无限重建 Client |
| 确认 replaced 但 Close 失败 | Connection | 仍尝试持久隔离，同代次其他副本不抢回；分别报告清理/隔离结果 |
| 同 owner 重连或换副本后才产生 Final | Admission + Connection + Delivery | 原 ReplyOrigin 不变；不把旧 req_id 重绑新 socket；准备失败不冒充 Provider 已调用 |
| Worker 完成后释放执行 lease，Final 延迟到达 | Execution + Delivery | 校验不可变完成事实；合法完成可接纳，未被接受的旧 Attempt 被拒绝 |
| 企微失租但旧 Reply 仍在途中 | Connection + Delivery | 停止新调用；不把旧结果误写为新 owner 成功 |
| 明确 ACK 晚于失租或 stale recovery | Delivery | 保存原 Attempt 的受限观察，不覆盖新状态、不自动重发 |
| 两次查空后赢家提交，输家 Resolve 失败 | Admission | 在期限内最终查 Receipt；不制造第二个 Run，不隐藏真正错误 |
| 领取完成、MarkCalling 前崩溃 | Delivery | expired CLAIMED 可重新调度；旧 claim token 通过发送门失败 |
| Provider 已接收但记账前崩溃 | Delivery | stale CALLING 进入 UNKNOWN，不盲重发 Final |
| 旧 Progress 晚于 Final 到达 | Delivery + Worker event contract | 展示 generation/sequence 和 Final 屏障阻止倒退 |
| 无效版本事件不断被重投 | 消费事件的 Module | 持久隔离/审计后受控处置，不静默 ACK 或无限热重试 |

### 14.1 传输与恢复不是第五个业务 Module

JetStream PubAck 表示 Stream 接收发布，不表示 Worker 已物化 Run；消费方 ACK 也不代替
Provider 的投递结果。[NATS 发布确认](https://docs.nats.io/learn/jetstream/publishing)

Stream 的保留策略、大小和时间上限会影响之后能否回放。[NATS 保留策略](https://docs.nats.io/learn/jetstream/retention-policies)
因此 D0-06/D0-10 必须同时冻结：最大消费者离线窗口、业务事实/Outbox 可恢复窗口、过期前
告警、消费水位，以及越过窗口后的重建/补发方式。不得把已收到 PubAck 当作可立即删除唯一
业务事实的理由。需要限制容量时优先显式拒绝新发布并让 PG Outbox 承接，而不是静默淘汰
尚未完成交接的事件；具体 Stream 配置仍须评审与故障验证。

当前 [streams.yaml](../../../deploy/nats/streams.yaml) 与
[拓扑校验器](../../../services/channel-gateway/internal/infra/nats/topology.go)已经分别固定
Route 为 Limits、RunRequested 为 WorkQueue；这两项不再是待任选的方案。目标 Worker
仍须在自己的 Consumer Inbox + Run 事务提交后 ACK；当前生产 Worker 未接线，传输层的
选型与真实消费闭环完成是两回事。路由快照/归档/受控重建、离线与容量窗口、GC、超窗恢复
仍属 D0-06/10；Reply stream 也尚未交付，不能从 Run stream 自动推导其协议。
具体当前限制以配置及实施状态为准，不把传输保留策略扩张为业务 exactly-once 保证。

坏 Schema、同版本不同内容和永久拒绝要有持久隔离记录与受控重驱动；隔离自身落库失败时
不确认消费。隔离不会自动授权继续使用受影响账户的旧路由；Routing 应把其完整性/可用性
降级。数据库暂时故障则延迟重试，不把所有错误都永久丢入隔离队列。


## 15. Control 账户接入的职责扩展（GCI2 已实现）

接入设计与实现见[control-integration-v1.md](control-integration-v1.md)。下面的账户目录、
内部 HTTP、资格 guard 与注册接线已在 GCI1/GCI2 完成，真实 Telegram 入站至 RunRequested
已验收；不把该结果扩展为真实 Worker 或完整回复链完成。

Connection新增独立RefreshAccounts用例、provider-neutral非秘密目录、SnapshotStore及每实例
资格管理；现有Supervisor仍只管理企微连接。Telegram入站/Delivery经各自定义的只读资格Port
消费目录，不导入Connection Repository。这是明确的Module职责扩展，不是“完全不变”。

Consumer-owned AccountUseGuard在Admission与Delivery的PostgreSQL Adapter事务内调用，
由Connection目录拥有方实现并注入。共同锁前缀为账户行→实例资格行；之后Admission沿原
幂等键/预算→owner→route顺序，Delivery沿part→owner→attempt顺序。快照写端按catalog→
账户稳定顺序→实例资格更新，不持目录锁调用SDK/其他业务Module。local precheck 与
Telegram 无企微 Owner 的语义仍保留，但不能绕过现已注入的账户事务 guard。

新Admission核对min_route_generation恢复屏障，A1/A2核对当前账户/用途/连接与实例资格。
既有Receipt重放、Finish/Observe/维护不因账户撤权加入新准入门禁，仍验证原证据和Owner历史。
停用与A2按Gateway DB提交顺序线性化，已进入CALLING的有限在途副作用不假装可撤回。

Telegram新增telegram_registration用途，同一连接版本批量取Bot Token+WebhookSecret，
独立注册fence且不依赖已存在Webhook/Binding/Delivery Claim；不复用企微OwnerGrant。
Snapshot SUPERSEDED分类、5秒凭据/2秒lease分离及Provider静态限制以接入设计和Control权威
文档为准。Module观测不负责业务授权；其上报状态不颁发AccountUseToken。
