# Channel Gateway：技术栈、代码结构与部署设计

> **当前替换切片**：[社交身份记录与用户连续对话](minimal-identity-session.md)。移除优化阶段的过度治理，保留正式 Worker／Session／Final 和独立 Web／Tracing。下方原设计及旧日期状态作为历史参考。

- **设计状态**：纯 Go / 进程内公开企微库方向已明确；详细接口与持久事务仍为草案
- **实现状态**：四 Module、Final/ReplyOrigin、Control mTLS 账户目录与凭据、动态 Telegram 注册、账户事务门禁、观测和 0001–0010 已实现。Control 模式启动有界 Delivery Runner，由 Runner 独占 Maintenance 生命周期；ReplyIntent Consumer、真实 Final verifier/Worker 与完整回复链仍待接入。真实 Telegram 入站至 RunRequested 已验收，见[实施状态 §13–14](implementation-status.md)。
- **工作树 / 分支**：`trpc-agent-service-channel-gateway` / `codex/channel-gateway`
- **核验日期**：2026-09-06；当前接线见 GCI2/GCI3，历史切片保留原日期和证据边界。分支提交与集成状态以 Git 历史为准。
- **上游规范**：[架构级约束](../constraints.md)、[Deployment V1](../control-api/deployment.md)
- **入门说明**：[从一条消息理解四个 Module 的分工](module-introduction.md)；先读此文，再按需查详细规范。
- **关联文档**：[四个业务 Module](module-boundaries.md)、[本轮设计复审](design-review.md)、[公开 Go Connector](public-go-connector.md)、[术语](CONTEXT.md)、[机器人行为调研](im-channel-sdk-semantics.md)、[部署设计](../operations/deployment.md#7-channel-gateway-部署扩展草案)

本轮按用户方向替代先前的 Node 独立 Connector 提案：使用公开的 Go 协议库，直接编译进
Gateway，不启动 Node 子进程，也不创建独立 Connector 镜像。完整设计继续保留；本轮已由 Gateway Adapter/bootstrap 直接使用企微 P0 库；当前已另有 Final 模块与本地投递纵切，但默认生产 Final 链路仍未启用。

介绍文档已整理 Module/内部层次/Workload 的区别、完整消息链路、非线性协作关系、人员
分工与分层验收口径。最新文档复审见[§19](design-review.md#19-四-module-讲解补充与文档一致性复审)，历史运行切片见[§18](design-review.md#18-delivery-runtime-v1-实现与剩余生产门禁)。
CGR-35/36 已补源码、本轮最终验收已通过；CGR-37 与有效发送截止策略仍开放。精确规则见
[Final V1](delivery-final-v1.md)与[Runtime V1](delivery-runtime-v1.md)，不把维护接线扩张为生产发送。

## Telegram 接入预检增量

[Telegram预检设计与Gateway实现](telegram-preflight-v1.md)已在本工作树落盘：
独立诊断授权、只读SDK Adapter、claim/resolve/complete Client、四槽Runner和Bootstrap；
仍是同一个Go进程，Compose增加可关闭开关。当前本地HTTP/mTLS及Gateway回归与
真实Control Schema/Handler、Web、真实Bot联合验收分开记账，后者与main合并尚待协调。

## 1. 一句话结论

**目标：Gateway 使用 Go；Telegram 直接 import 第三方 Go SDK，并用公开 Go 协议包补足
单次有界轮询；企业微信自研
`platform/im/wecom` 公开协议库并直接 import。两种渠道运行在同一个 Gateway Workload，
PostgreSQL 保存事实，NATS JetStream 负责传输，Agent 执行仍在 Worker。**

Connector 是库的角色，不自动形成部署单元。公开包不放在 Gateway 的 `internal/` 下，
但平台专属 Adapter、租户路由、Admission、Delivery，以及有状态 Provider 的跨副本租约
继续留在服务内部；企微长连接与 Telegram 物理 Bot receiver 均需要 owner，分布式
Webhook HTTP 收件不要求独占 owner。

## 2. 技术栈与当前状态

| 层 | 当前设计 | 当前实现证据 |
| --- | --- | --- |
| 语言 / module | 统一 Go；根 module，toolchain `go1.25.14` | 根 go.mod 为 Go 1.25.0、toolchain 1.25.14 |
| HTTP | 第一切片使用标准库 net/http，自有入站 ACK 与独立管理 listener | 代码已有 Webhook、livez/readyz；Control 既有 Gin 保持不变 |
| PostgreSQL | pgx/v5；事实、幂等、投影、Outbox | 当前 12 个 Gateway 自有迁移；0001–0010 后保留两份已发布的 0011（Telegram receive modes 与 Reply transport），按完整文件名记账；不改历史 SQL |
| 异步传输 | NATS JetStream | 代码已有版本化事件、Relay、拓扑校验及独立授权配置；Control publisher 与真实 Telegram 入站已联合核验；Worker 执行仍单独推进 |
| Telegram | `github.com/go-telegram/bot v1.25.0` 直接导入 | 根 module 已加入依赖，入站 Adapter 使用 models；发送、双模式接收及 ReplyIntent 消费已实现；真实消息与 Worker 回路按独立验收记录确认 |
| 企业微信 | 公开包 `platform/im/wecom`；P0 WebSocket + JSON，媒体后续 | 公开 P0 已有历史验证；本轮实现已接 Connection、企微入站及 bootstrap，Delivery 与真实账号另验收 |
| WebSocket 库 | `github.com/coder/websocket v1.8.15` | P0 固定官方稳定 Release；不手写 RFC6455，见[协议核验](wecom-protocol-implementation-notes.md) |
| 可观测性 | OTel traces/metrics + 结构化日志 | 沿用现有设计；尚未接入 Gateway |
| 构建与部署 | Gateway 一个 Go 二进制、一个镜像 | 当前已有入口、Dockerfile 和 Compose 增量；镜像/容器结果见实施状态 |

Node 不再是该 Gateway 的运行依赖。自研 Go 协议库节省进程、镜像、跨语言 RPC 与独立运维，
相应成本转为协议维护和兼容/故障测试；首版只覆盖文字闭环，而非一次移植官方 SDK 全部方法。

## 3. 运行关系与事实所有权

```text
Telegram ──HTTPS webhook──> Gateway telegramadapter ───────┐
                                                        │
企业微信 <──WebSocket──> Gateway wecomadapter               │
                         └─ import platform/im/wecom ─────┤
                                                        v
                                                  Gateway Admission
                                                        │ Gateway PG + Outbox
Control API → 自有 Relay → NATS → Gateway 路由投影          │
                                                        v
                                                 Gateway Relay → NATS
                                                                   │
                                                                   v
                                                              Agent Worker
                                                                   │ ReplyIntent
                                                                   v
                                        NATS → Gateway Delivery → 渠道 Adapter
```

企微库与 Gateway 是函数调用，不经过 Go↔Node HTTP。目标拓扑不是当前 Compose 的实际状态；
Local IM 后续仍经过 Provider → Gateway，不增加客户端直达 Worker 的执行入口。

| 事实 / 能力 | 拥有方 | 说明 |
| --- | --- | --- |
| ChannelAccount 管理归属、Binding、生效 Revision | Control 的 channelbinding 模块；需保证同一物理 Bot 的唯一有效账户映射，已冻结账户/Binding 契约并完成实际 Control 接入 | Gateway 不在热路径查 Draft/latest |
| 最小绑定/Manifest Ref 投影 | Gateway routing | 不编译 Manifest，不复制整个 Profile |
| Inbox、Admission、稳定 RunID 与 ReplyContext | Gateway admission（精确表归属提案） | 接纳成功不等于执行方已物化 Run |
| Run/Attempt、执行 fence、Session 执行顺序与终态 | Agent Worker | 不投递 IM，不承担 Gateway Relay |
| Delivery、业务限流/重试、UNKNOWN | Gateway delivery | 不回写 Worker Run 终态 |
| 有状态账户 owner lease、epoch、实例分配 | Gateway connection | V1 仅企微；Telegram webhook/HTTP 不经过 owner lease |
| WebSocket、subscribe、ping、req_id ACK、原生协议 DTO | `platform/im/wecom` | 仅实例内协议状态，无平台业务持久化 |

物理 PostgreSQL 实例可复用；第一切片采用 Gateway 独立 database/role 和进程启动迁移，
具体当前选择见[实施状态](implementation-status.md)，全平台权限与迁移仍需集成验收。逻辑表所有权与事务不因
共享实例而合并，Gateway / Worker 不做跨服务事务或跨 owner SQL。

## 4. 建议代码结构

以下是完整目标结构，不是逐文件存在清单。当前已有四个业务 Module、bootstrap/infra、
10 个迁移与事件契约；Maintenance 已接默认 App，Control 模式另启动有界 Runner。
ReplyIntent Consumer、可信执行验证与真实 Worker 等接线仍待交付。精确源码入口见
[实施状态](implementation-status.md)。公开库与私有服务实现分开，沿用一个根 `go.mod`。

```text
platform/                                # 目录命名空间，不是全局工具大包
└── im/
    └── wecom/                           # 公开 Go 包，纯外部协议职责
        ├── client.go
        ├── websocket.go
        ├── subscription.go
        ├── heartbeat.go
        ├── reconnect.go
        ├── event.go                     # 企微原生事件，不是平台业务实体
        ├── reply.go                     # req_id 相关性与协议回执
        ├── stream.go                    # P1
        ├── media.go                     # P2
        ├── errors.go
        └── *_test.go

services/channel-gateway/
├── cmd/channel-gateway/main.go
├── internal/
│   ├── bootstrap/                      # 配置、装配、信号、生命周期
│   ├── routing/                        # 控制事件与本地运行投影
│   ├── admission/                      # 去重、分类、首次接纳与 RunRequested
│   │   └── adapter/inbound/
│   │       ├── telegramadapter/        # import 第三方 models；自有 HTTP ACK
│   │       └── wecomadapter/           # import 公开库；原生事件→平台输入
│   ├── delivery/                       # 投递账本、会话预算、重试
│   │   └── adapter/outbound/
│   │       ├── telegramadapter/        # 直接调用第三方 SDK Client
│   │       └── wecomadapter/           # 原 ReplyOrigin 匹配 + 当前资格；A2 后调用 Client
│   ├── connection/                     # stateful provider owner/lease/epoch；V1 为企微 Client
│   └── infra/                          # HTTP/PG/NATS/telemetry 连接
├── migrations/
├── integration/
└── Dockerfile

api/events/{control,execution}/v1/       # 已有 RouteProjection/RunRequested/ReplyIntent Final；ReplyIntent transport 待装配
api/openapi/                            # 真实跨服务 HTTP 协议；不新增 Connector RPC
gen/                                    # 由 api 生成的代码
go.mod                                  # 唯一根 module
```

业务 Module 内保留 `domain / application / adapter / wiring.go`。Application 定义使用方 Port，
Adapter 依赖 Application/Domain，bootstrap 装配；公开库只由 Adapter/技术装配使用，
不把 SDK 类型带进 Domain 或数据库 Row。

`routing / admission / connection / delivery` 是一个 Gateway Workload 内的四个业务 Module，
用于隔离事实、事务和失败语义，不对应四个微服务。先看 Module §0.1 的需求归属表；输入输出、依赖方向以及 Telegram/企微
两条完整流程、代码/人员分工、跨模块事务 seam 和故障归属见[四个业务 Module](module-boundaries.md)。

Telegram 发送及 DTO 直接导入 `github.com/go-telegram/bot` 与其 `models` 子包；
`platform/im/telegram` 按[双模式设计](telegram-receive-modes-plan.md)提供 SDK 未暴露的
单次有界 `getUpdates` 等协议能力，而不是透明转发 SDK 或拥有后台 polling 循环。
Gateway 专属 Adapter 转换 ReplyIntent、结果和错误；Connection 拥有持久 cursor、
receiver owner 和启停协调，Admission 继续固定持久接纳事实。

## 5. 关键 Interface 与两条持久链路

以下是 Gateway 内部 Module Interface 的语义草案，不是公开企微库 API：

| Interface | 必须隐藏的实现复杂度 | 返回含义 |
| --- | --- | --- |
| `AcceptInbound(AuthenticatedInbound)` | Adapter 已完成协议鉴权/DTO 转换；Module 内查重、分类，新 Run 才解析绑定并原子接纳 | decision + 可选 AdmissionID/RunID；admit-run 只在本地事务提交后成功 |
| `ApplyRoutingEvent` | 版本验证、持久幂等；旧 generation 持久回执后忽略，同 generation 异内容冲突；投影就绪状态 | 已持久处理，不表示旧版本被重新应用，也不冒充业务发布回执 |
| `AcceptReplyIntent` | 可信 Worker 事件、归属校验、幂等生成投递记录 | 已持久接纳投递意图，不等于外部发送成功 |
| `RunDeliveryDispatcher` | 隐藏 claim/CALLING、额度、deadline、Provider 调用、stale recovery 与重试调度 | 过期 CLAIMED 可重领，stale CALLING 为 UNKNOWN；明确调用结果后 RetryPolicy 再决定动作 |
| `RunConnectionSupervisor` | 隐藏 account 扫描、Acquire/Renew/Release、epoch、Client 启停与失租恢复 | 只管理有状态 Provider；V1 为企微，不要求调用方编排租约状态机 |

### 5.1 触发新 Run 的用户消息：接纳与执行分成两个本地事务

所有输入先区分 `ignore / interaction / admit-run`。按钮、停止请求、服务消息不默认创建 Run；
交互反馈走短路径，业务操作另做授权、幂等和时限检查。下面的 Admission / RunRequested
事务只适用于 `admit-run`，不把每个 Provider Update 都转成一次 Agent 执行。

1. Gateway 验证 Telegram 请求，或使用已认证企微连接的可信 account 上下文；不信任入站 tenant_id。
2. **先查既有事件，再解析当前路由。** 按 provider + account + external_event_id 查既有
   Inbox/决策/Admission。已接纳输入重投返回原 receipt 和固定身份，不重新读取当前 Binding
   或改投新版本；绑定后来停用、切换或投影暂缺也不改变已接纳事实。此步骤仍检查当前请求
   的来源与 account 授权，不向未认证调用方暴露 receipt。
3. 仅对首次输入执行触发分类；需要新 Run 时，Tenant 从当前可信 ChannelBinding 派生，
   并通过本地投影固定 账户路由代次 RouteGeneration、DeploymentRevision、Manifest Ref/Digest。
   读投影与接纳提交需要一致性检查，切换竞争时重试，不产生半新半旧的记录。
4. Gateway 在一个本地事务写入 Inbox/决策、Admission 和 RunRequested Outbox，首次接纳
   生成稳定 RunID。并发创建冲突时读取赢家已提交的 receipt，不重新路由；去重不含
   DeploymentRevision 或连接 epoch。无 Run 的决策也需明确审计与重投行为，但不写 RunRequested。
5. Gateway 持久提交后，Telegram 返回 HTTP 成功；企微完成本地 handler 交接，
   不称为服务端持久消费 ACK，也不据此承诺补发。Relay 后台重试发布 RunRequested。
6. Worker 消费后，在自己的事务里写 Consumer Inbox + Run（及必要 Outbox），提交后 ACK NATS。
   重投只物化一次；后续 Attempt/完成事务由 Worker 自己拥有。

外部 Inbox identity 为 provider + stable account_id + external_event_id。V1 中一次外部事件
固定一个首次 Decision/Receipt 与至多一个 RouteSnapshot，不隐式 fan-out 到多个 Binding。
持久接纳、事件发布、Worker 物化、Run 完成必须分别可观测。Gateway 不等待模型执行完成
才确认输入，也不承诺接纳时 Worker 已在线。

### 5.2 ReplyIntent 与 Delivery 独立

Worker 完成 Run 的事务同时写 ReplyIntent Outbox；Gateway 将其幂等转为 Delivery。
Gateway 记录外部 message ID、分段结果、重试时点和 UNKNOWN。Progress、交互反馈、Final
是不同操作，不用一套无条件重试策略。Delivery 需核验原 Admission 的不可变回复目标，
并遵循 execution generation/sequence 与 Final 屏障；这些协议仍是 D0-09。发送成功后本地
记账前崩溃不具备外部 exactly-once 保证。

## 6. 公开 Go 库与 Gateway 的有状态连接管理

- 公共库暴露 Client、原生事件、请求/回执和结构化协议错误；不接收 TenantID、BindingID、
  RunID、平台租约或数据库对象，不依赖 PG、NATS、Gin、服务 internal 或 Agent Runner。
- Gateway connection 仅为有状态 Provider 工作；V1 在获得某企微账户的 owner grant 后创建
  受 context 控制的 Client。同进程不等于单副本，其他 Gateway 实例不得同时订阅同一企微
  Bot。失租/停用/轮换先取消该 Client 生命周期并停止重连和新发送，再完成有界关闭。
- Telegram 采用 webhook 入站与 Bot API HTTP 出站，任意健康 Gateway 副本都可以处理，
  不经过 Connection owner lease；进程内 SDK Client 缓存只是 Adapter 技术细节。
- 任意 Gateway 实例可消费 ReplyIntent 并持久生成 Delivery；企微实际投递由当前 owner
  按 account 领取。初版用 Gateway 自有账本和 owner CAS 定向领取，不另造 Gateway-to-Gateway
  Connector RPC。通知只负责唤醒，不取代持久待办。
- 库只管理连接内 req_id / pending response / connection generation；Gateway 在领取与
  记账时核验自己的 attempt + epoch。两种代次不混用，也不加入外部事件去重 key。
- 企微远端不验证平台 epoch，旧实例的在途发送仍可能成功。超时归 UNKNOWN，不无条件
  重发 Final。旧连接的 req_id/stream 是否能跨重连续用尚待实测，不假定新 owner 能继续旧流。
- 入站 reader 与业务 handler 隔离：慢 PG 操作不阻塞心跳或发送 ACK 读取；缓冲有界，
  溢出/交接失败有明确状态。handler 返回 nil 只表示本地交接，不制造企微离线补发保证。

详细 API 生命周期、迟到 ACK、异常测试和阶段范围见 [公开 Go Connector 设计](public-go-connector.md)。
ChannelAccount 凭据管理仍需独立冻结：渠道 Token/Secret 不是模型/工具 Profile 凭据，
不套用 Worker Attempt 的 `RuntimeCredentialResolver`，也不复用
`CONTROL_PROFILE_CREDENTIAL_KEY`。`origin/main@bf107766` 已有的 Profile 凭据管理与可选
runtime HTTP Adapter 只服务 Worker Runtime Profile；真正 Run/Attempt 授权和 Worker 接线仍
未完成。企微库只接收调用方提供的必要协议凭据，不读取业务配置、不向日志输出值。

## 7. deploy 的修订

目标新增 **channel-gateway 和 nats**；不新增 `wecom-connector` 服务、Node 镜像、子进程、
sidecar、独立 Connector 配置端口或 Go↔Node RPC。企微启用成为 Gateway 账户/渠道配置，
不是一个独立 Compose profile。`agent-worker` 仍作为完整执行链路的另一独立交付。

目标 Gateway 镜像同时包含 Telegram Adapter 和公开企微 Go 库；只在取得企微账户 owner
grant 时运行对应连接。本轮实现已直接 import 企微库，按可选账户配置启动；
没有新增 Connector 部署单元，实际工作树应用与新镜像验收已完成，见实施状态 §9.5。Gateway 自有 Relay、投影消费者与独立 Delivery Maintenance 已由同一 bootstrap 管理；
历史 Runtime 切片只显式组合 Runner；GCI2 已在 Control 模式启动 Runner，ReplyIntent Consumer/真实 Final verifier 仍未接入，见实施状态 §11、§13–14。
部署设计详见 [第 7 节](../operations/deployment.md#7-channel-gateway-部署扩展草案)。
NATS 的拓扑 reconciliation 与 Server 权限配置加载采用两条路径，前者成功不代表后者
已生效；具体部署/验收责任见部署设计 §2.2。当前已有第一切片 Compose、Dockerfile 和依赖
增量，实际验证结果以实施状态和审计记录为准。

## 8. 实施顺序与待冻结项

| 阶段 | 交付 | 验收证据 |
| --- | --- | --- |
| 0 契约 | 公共库 API/错误/取消语义；Admission/Run 表归属、事件、账户凭据、权限 | 评审 + Schema/Fixture；API 不是空代码占位 |
| 1 Gateway + Telegram | 路由投影、真 PG 持久接纳、Relay、镜像与 Compose | 真实 PG/NATS + 故障重投 + 实际镜像验收 |
| 2 执行与 Final | 对接真实 Worker 与 ReplyIntent、投递账本 | 真 Worker E2E；发送失败只重试投递 |
| 3 企微 Go P0 | `platform/im/wecom` + Gateway Adapter/owner：文本输入与 Final | 模拟 WS 异常测试 + race + 真实企微账户 |
| 4 渠道扩展 | stream/卡片/主动消息，再媒体；Gateway 观测与账户级故障恢复 | 能力矩阵、故障注入与部署恢复验收 |

Local IM 是独立 Workload/平台集成对象，最终复用 Admission/ReplyIntent 契约，不作为
Gateway 渠道功能塞进阶段 4。Helm 属于平台 `FINAL-INTEGRATION`：仅在 Control、Gateway、
Worker、Local IM、前端等全部生产 Workload 完成，且镜像、端口、Probe、权限和 Secret
契约稳定后统一设计；它不属于 Gateway P1/P2 或上述阶段 0-4。

已明确的公开库 P0、状态 API、Gateway 自有迁移/role、Route/Run 传输和本地运行阈值，
不再整体重复列为“待冻结”。已有选择与真正剩余范围统一见
[Module §15](module-boundaries.md#15-control-账户接入的职责扩展gci2-已实现)与最新
[实施状态](implementation-status.md) §13–14。Control账户/托管凭据、Gateway生产接线和
账户观测、真实Telegram入站已验收；当前剩余重点是Execution完成证明、Reply transport、
完整发送/回复链、CGR-37、有效发送截止、交互/保留/GC/配额，以及真实企微账号与Worker E2E。
D0按纵向切片收敛，不因一个Port存在整体关闭；历史测试按原时间点保留。


### 2026-09-06 最新联合验收

Control mTLS 账户目录、托管凭据与 Gateway 动态注册/接收、事务门禁、观测已落地。真实
Telegram 私聊文本已产生唯一 Receipt/Admission/Outbox，并在 NATS FileStorage 中读取到
同一 RunRequested；见[真实入站验收](telegram-real-inbound-20260906.md)。
这不是 Worker/模型/Storage 可执行性或机器人回复验收。
