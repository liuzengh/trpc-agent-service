# 公开 Go Connector：企微协议库与 Telegram 直接引用

- **方向**：企微使用公开 Go 库，由 Gateway 直接导入，不独立部署；2026-09-06 状态对齐
- **状态**：公开 P0、Connection/企微入站与 Sender reservation 已实现；当前默认 Control 来源已接托管凭据、轮换和 Delivery Runner，Gateway 共 10 个迁移（0001–0010）。真实 Control 发布与 Telegram 入站已验收；ReplyIntent Consumer、真实 Worker 完成证明、真实企微账号和 Final 回复闭环仍单独交付。历史 Runtime 验收见[实施状态 §11](implementation-status.md#11-delivery-runtime-v1实现与本轮验收)，当前接线与真实入站见实施状态 §13–14。
- **上游**：[ARC-101/102/103](../constraints.md)、[Gateway 总览](README.md)、[四个业务 Module](module-boundaries.md)、[本轮设计复审](design-review.md)、[行为证据](im-channel-sdk-semantics.md)

## 1. 可行性与命名

企微智能机器人长连接是 WebSocket + JSON 帧以及相关 HTTP/媒体操作，没有必须依赖 Node
进程的协议环节。可以按公开协议独立实现 Go Client；官方 Node/Python SDK 用于行为对照，
不是运行时桥接，也不是经 Go 启动 Node 子进程。[官方长连接协议](https://developer.work.weixin.qq.com/document/path/101463)

当前公开包路径：

```text
github.com/liuzengh/trpc-agent-service/platform/im/wecom
```

`platform/` 和 `platform/im/` 是目录命名空间，实际 package 为 `wecom`。它不放在
`services/channel-gateway/internal/` 下，当前 P0 可以被其他包直接 import。
“公开”是可导入与 API 导出，不意味着所有字段都导出，也不意味着独立镜像、独立仓库或
独立 Go module。当前沿用根 go.mod；未来确有独立发布需求再决定是否拆 module。
[Go 官方模块组织说明](https://go.dev/doc/modules/layout)

## 2. Telegram 可以直接 import

选定的 `github.com/go-telegram/bot` 是普通 Go 库，可以在 Gateway Adapter 中直接引用，
其 models 子包提供 Telegram 原生 DTO。它是第三方/社区实现，不是需要另行部署的服务。
[固定 SDK 的导入与使用示例](https://github.com/go-telegram/bot/blob/3d38d39d8ee18d926b64cb808c78f0927ea6d045/README.md)

下面表示完整目标 Adapter 导入关系；当前入站 Adapter 已导入 Telegram models，
企微协议库已有 P0；Gateway 已直接装配入站 Adapter、Connection 与 Delivery Sender，
Control 来源默认启动有界投递 Runner。ReplyIntent Consumer 与真实 Worker 仍待接线；
此前独立实验结果保留在第 9 节：

```go
import (
    telegrambot "github.com/go-telegram/bot"
    "github.com/go-telegram/bot/models"
    "github.com/liuzengh/trpc-agent-service/platform/im/wecom"
)
```

不为目录对称性增加 `platform/im/telegram` 转发层。Gateway 私有 Telegram Adapter 负责
DTO→平台输入、ReplyIntent→SDK请求、错误→Delivery状态，仍然有明确业务价值。
直接 import SDK 不等于直接使用其 WebhookHandler 作为持久接纳入口；已有固定版本实验
确认它早于模拟业务完成返回 200，Gateway 应自行控制 HTTP 返回点。

## 3. 公开库与 Gateway 的职责

下表是完整职责边界，不是首版一次性交付全部能力；reply/stream/send/media 按第 7 节的
P0/P1/P2 capability 阶段逐步提供。

| `platform/im/wecom` 负责 | Gateway 私有实现负责 |
| --- | --- |
| WebSocket 拨号、帧读写、subscribe、应用层 ping | Bot 对应哪个 Tenant/Binding，运行版本和权限 |
| 原生事件/请求/回执 DTO、未知类型与字段处理 | 事件分类、Inbox 去重、Admission、稳定 RunID |
| req_id 对应的内存 pending response、连接 generation | 持久 Outbox、Delivery、业务幂等和重试决策 |
| context 取消、有界缓冲、有限且可停止的重连 | 有状态账户跨实例 owner lease / epoch、失租/停用/轮换；V1 为企微 |
| 企微原生 reply/stream/send/media 方法 | capability、会话额度、Final优先、超时后的业务取舍 |
| 协议错误、明确拒绝、回执成功、结果不确定 | UNKNOWN 的恢复/审计；原运行事实与外部结果的关系 |

公开库不接收 TenantID、BindingID、RunID、Admission、平台 lease、数据库事务、
RuntimeCredentialResolver；不依赖 services、PostgreSQL、NATS、Gin 或 tRPC-Agent-Go。
它可以依赖标准库、已审查的 WebSocket 库和注入的窄 transport/clock/logging 能力。

库可以维护外部协议所需的短期状态，但这些状态不是持久队列，也不是 Gateway 业务实体。
API/生成 DTO、外部协议 DTO 与平台 Domain 类型分别有拥有方，不创建通用 `connector/types`
或共享业务 `contract/common`。暂不设计同时覆盖 Telegram/企微的巨大通用 Connector Interface；
业务侧通过自己的小 Port 面向能力，具体 Adapter 各自转换。

## 4. API 轮廓与生命周期

当前 P0 已提供具体 Client、原生事件、typed 状态与确定性错误；精确签名、配置默认值、
取消/关闭与状态流规则以[公开库 README](../../../platform/im/wecom/README.md)及源码为准，
本文不维护第二份容易漂移的完整 Config 或 Option 清单。

- 库构造只做本地校验，Run 才联网；一个 Client 的运行/关闭边界由公开 API 明确约束。
- P0 覆盖 subscribe/auth、应用层 ping、text/event callback、单次文字 Final、取消/关闭与有限重连。
  Final 使用 `msgtype:stream` / `stream.finish:true`，不等于已提供 Progress 流式刷新。
- `ReplyRequest` 保存原 callback req_id 和连接关联；ACK、NOT_SENT/ACCEPTED/REJECTED/UNKNOWN
  只表达协议调用结果，不表示 Delivery 已持久记账。库不自动重发 UNKNOWN Final。
- reader 与有界 handler 交付分离；状态 snapshot/通知只表达本地连接状态，不提供平台 owner 资格。
- Gateway 已实现 Connection lease/epoch、配置切换、Final 账本、CLAIMED/CALLING 恢复、
  Observation 与原连接 reservation。当前默认 Control 来源已接凭据 Owner、解析轮换与
  Delivery Runner；文件/环境引用只保留为显式 fixture。ReplyIntent Consumer、真实 Worker
  和完整审计入口仍待交付。库本身不接收 Tenant、Run、PG/NATS 或平台 Secret 引用，
  也不读取全局业务环境配置。

固定官方协议、SDK 类型差异、req_id 错配风险与待实测边界见[本轮协议核验](wecom-protocol-implementation-notes.md)。
P1 再增加 Progress/欢迎语/卡片/主动发送，P2 增加媒体；当前不据官方 SDK 方法全集宣称 Go P0 已全部移植。

### 4.1 累计身份容量不等于瞬时背压（Gateway 组合门禁）

当前公开库同时限制在途请求与每 socket generation 已尝试 Final 的身份数量，默认
MaxRequestIDs 为 4096。后者用于阻止同 generation 重复尝试/串用 ACK，尝试完成后仍保留
标记。因此累计容量耗尽不会因为等待在途调用完成而恢复，socket/心跳却仍可能 READY。

当前两种限制都返回 CodeCapacity，Gateway 又统一映射为 temporary。生产 Final 启用前
须冻结 typed 区分、账户级发送容量状态、准入/发送资格影响、告警和有界恢复策略；
具体是排空后换代、其他有证明的容量管理，还是停止账户等待处理，仍需联合设计与验证。
不靠无限短重试、静默删除同 generation 标记或把旧 ReplyOrigin 重绑新 socket 解决。

小容量联合测试应验证：耗尽后 reader/心跳仍工作但发送不可用；账户状态准确；恢复不
重发旧 Final、不丢已观察结果、不误用旧 generation。该组合策略尚未实现，见
[复审 CGR-37](design-review.md#162-三项需要代码处理的剩余风险)。数值与行为依据
[配置](../../../platform/im/wecom/config.go)和[命令实现](../../../platform/im/wecom/command.go)，
这是本地库选择，不是企微服务端配额。

## 5. ACK 与错误语义是主要工程难点

### 5.1 入站回调不是远端持久消费确认

官方 SDK 对 callback 进行本地事件派发，没有对应“本业务已落库”的独立消费 ACK。
Go handler 返回 nil 只能表示本地交接完成；它不能让服务端自动获得新的补发保证。
Gateway 应等待自己的持久接纳用例返回，库则维持 reader/心跳不被该等待阻塞。
[固定官方消息处理实现](https://github.com/WecomTeam/aibot-node-sdk/blob/80615b987ef69c6028ad764924609247c0725955/src/message-handler.ts)

Gateway 接线还必须区分暂时接纳失败与协议终态：当前库的活动 handler 错误会终止 Run，
不由 SDK 自动重试。业务有界重试与账户恢复类别由 Gateway Adapter/Connection 配合定义，
不得直接把一次 PG/路由暂时故障变为永久封禁，也不得无限重建 Client 绕过协议重连预算。
精确组合规则见[Module §6.4](module-boundaries.md#64-入站失败与连接恢复的组合契约)。

### 5.2 同 req_id 的迟到 ACK

官方回复回执按 req_id 相关，同一流式回复又需要复用 req_id。固定 Node 实现在超时后推进
该 req_id 队列，再按 req_id 匹配 ACK；从源码推断有把旧迟到 ACK 关联到下一项的可能，
这是待测试风险，不是本轮发现的线上事故。
[固定队列/超时/ACK 实现](https://github.com/WecomTeam/aibot-node-sdk/blob/80615b987ef69c6028ad764924609247c0725955/src/ws.ts#L518-L579)

Go 版至少要求：同 req_id 单在途；回执超时进入 UNKNOWN 并暂停该关联流，不能立即把
下一个同 req_id 请求送出再接受模糊 ACK。恢复策略必须考虑连接 generation、重复/迟到 ACK
和旧 req_id 的可用性；本地添加 attempt_id 不会让远端自动回传它。

错误必须区分参数无效/确定未写出、服务端明确拒绝、服务端明确接受，以及已写出但
未得到确定结果。重试策略由 Gateway 的 Delivery 决定；库只执行明确且有界的协议恢复。

## 6. 同进程、多 Gateway 副本

```text
Gateway A 获得 Bot X owner → 启动 wecom.Client X
Gateway B 未获 owner      → 不连接 X，但可处理其他 Bot/Telegram
Worker ReplyIntent       → 任意 Gateway 持久生成 Delivery
Bot X 的 Delivery        → 当前 owner 资格 + 原 ReplyOrigin 匹配 → A2 提交 → 调用原关联 Client
```

Gateway 的 owner lease / epoch 与库的 connection generation 是两套概念。前者保护
跨实例资格与数据库旧写，后者隔离连接内 pending/回执；两者都不属于外部消息去重 key。
失租后 Gateway 取消 Client context；新 owner 连接企微时，旧实例仍可能已有在途外部发送，
因此保持 UNKNOWN 和不盲重试 Final 的规则。旧 req_id/stream 的跨连接延续仍需真实验证。

Gateway 还需在首次 Admission 保存原 ReplyOrigin；同一 owner 重连也会更换 socket
generation，不能用当前值补旧回调。该来源与 callback req_id 配对，留在 Gateway 自有事实，
不进入 Worker 事件或外部去重键。本切片已由 Admission/0006 保存来源并提供 Connection ReserveFinal；生产 Final 入口未装配；
精确目标契约与验收见[Module §7.5](module-boundaries.md#75-企微原回复关联不等于当前发送资格)。

新企微入站接纳也需要 Connection owner guard；同内容旧 Receipt 不重新校验 owner，
Telegram 绕过该租约。确认 replaced 后的跨副本隔离由 Gateway Connection 持久管理，
不能仅靠库终止一个 Client，也不能以 Close 成功作为记录隔离的前提，见 Module §6.5。
Owner 校验在 Gateway 持久事务内通过同事务只读 guard 完成，具体 seam 见
[Module §9.2](module-boundaries.md#92-跨-module-的同事务只读校验-seamd0-04-提案)；库本身不持有 PG 事务。

Telegram webhook 与 Bot API HTTP 不经过该 owner lease；任意健康 Gateway 副本都可以处理
Telegram 输入/输出。Telegram Client 的本地缓存不构成 Connection Module 的持久所有权。

## 7. P0 范围与验收

| 阶段 | 功能 | 必测事项 |
| --- | --- | --- |
| P0 | subscribe/auth、应用层 ping、文本/事件解码、文字 Final、取消/关闭 | 订阅拒绝、心跳丢失、被替换断连、重复/迟到ACK、队列满、慢handler、Close释放pending |
| P1 | stream、欢迎语、卡片更新、主动推送 | 时窗、同req_id相关性、finish后调用、已有会话条件 |
| P2 | 下载/解密、分片上传、素材过期 | 类型/大小/有效期、校验、取消、失败分片与恢复 |

先使用本地可控 WebSocket 服务端做契约/故障/race 测试，再使用真实企微账号完成私聊/群@、
Final、抢占与断线窗口测试。当前 Go P0 代码及本地真实 WS 契约/故障测试已存在，
SDK 全矩阵与最终整合状态另记[实施状态](implementation-status.md)；尚未完成企微真机验收，
本轮实现通过显式 Gateway Adapter/Connection 装配获得企微入站，不是仅凭库目录存在就自动接通。

官方 Node SDK 的元数据声明 MIT，本设计按公开协议独立实现；不把尚未移植的源码
当成 Go 能力。若后续直接移植文件，应记录固定来源版本与原有通知。
[固定 SDK package 元数据](https://github.com/WecomTeam/aibot-node-sdk/blob/80615b987ef69c6028ad764924609247c0725955/package.json)

## 8. 部署结果

目标由 Gateway Go 镜像承载两种渠道接入；本轮实现已组装公开库与企微入站，
实际工作树与新镜像验收已通过。公开企微库没有自己的 Dockerfile、端口服务、
Compose 单元或 Helm Deployment。目标部署新增 Gateway 与 NATS；Agent Worker 独立。
当前 Control 来源已接账户 Owner、托管凭据解析/轮换、状态 observations 与 Delivery Runner；
文件/环境引用保留为 fixture，完整出口控制/可观测性和 ReplyIntent Consumer 仍单独交付。
Runtime Profile 凭据管理、`ResolveForAttempt` 与 runtime HTTP Adapter 服务 Worker Profile；
ChannelAccount Token/Secret 使用已实现的独立账户 Owner 和 mTLS 解析协议，见
[Control 接入](control-integration-v1.md)，不通过 `RuntimeCredentialResolver` 取得 Bot 凭据。

## 9. 历史 Telegram 直接导入验证（独立实验）

在独立实验 module 固定 `github.com/go-telegram/bot v1.25.0`，直接导入 bot 与 models，
创建跳过 getMe 的 Client，再调用一次 SendMessage。注入的 HTTP transport 在内存中返回
固定 JSON，未建立网络连接；这验证 Go 导入/类型/Client 调用，不是新一轮真实 Bot API 测试。
该次独立实验未改变根 go.mod / go.sum；后续入站第一切片代码已经加入 Telegram SDK
依赖，两者不是同一次验证。以下路径是被 `.gitignore` 排除的 **local-only evidence index**，
不是可随仓库提交的文档附件；共享评审前应将最小脱敏证据迁入 tracked `docs/evidence/`。

- 输入：本地固定配置、ChatID=1、Text=fixture；transport 返回 message_id=42。
- 源码：`/Users/jfs/Projects/trpc-agent-service-channel-gateway/artifacts/channel-gateway-go-connector-20260904/telegram-import/main.go`
- 输出：`/Users/jfs/Projects/trpc-agent-service-channel-gateway/artifacts/channel-gateway-go-connector-20260904/telegram-import/output.txt`
- 退出状态：`/Users/jfs/Projects/trpc-agent-service-channel-gateway/artifacts/channel-gateway-go-connector-20260904/telegram-import/exit-status.txt`

```sh
cd /Users/jfs/Projects/trpc-agent-service-channel-gateway/artifacts/channel-gateway-go-connector-20260904/telegram-import
GOWORK=off GOPROXY=off GOSUMDB=off GOMODCACHE=/private/tmp/channel-sdk-research-20260904/module-cache GOCACHE=/private/tmp/channel-sdk-research-20260904/go-cache go run .
```

字面输出与退出状态：

```text
DIRECT_IMPORT PASS: sendMessage result=42 calls=1 external_network=0
DIRECT_IMPORT_EXIT_STATUS=0
```
