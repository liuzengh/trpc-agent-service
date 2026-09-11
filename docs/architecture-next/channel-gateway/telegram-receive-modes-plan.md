# Telegram 双接收模式：总体修改计划与三方分工

状态：原始设计与评审记录；用户于本轮授权进入开发。当前已采纳的契约、实现和测试进度见 [开发记录](telegram-receive-modes-implementation.md)，下文原计划不作为完成声明。

- 日期：2026-09-07。
- 总负责人：现有任务「设计 Channel Gateway」。
- 核对基线：`origin/main` = `f61d49b09b8ae534f39f8065ddec699aa35ee10e`。
- Gateway 工作树：`/Users/jfs/Projects/trpc-agent-service-channel-gateway`。
- 本轮计划分支：`codex/channel-gateway-receive-mode-plan`，从上述基线创建；原 `codex/channel-gateway` 保留。
- 本轮只编写和对齐计划。实施、提交、推送、部署及真实 Bot 模式切换另行执行。

## 1. 目标和范围

Telegram 账户支持 `long_polling` 和 `webhook` 两种接收方式，新建账户默认 `long_polling`。升级时将已有 Telegram 账户显式标记为 `webhook`，保持原来的接收行为；不能通过“缺省字段解释成新默认”偷偷切换旧账户。

接收方式是 **ChannelAccount 的期望连接配置**，不是 ChannelBinding 的路由属性。Control 保存并发布配置，Gateway Connection 管理接收生命周期，两个入站适配器进入同一 Admission 持久接收链路。Routing、Delivery、Worker 不新增按接收方式分叉的业务流程。

本次范围包括 Control/API/共享契约、Web、Gateway、迁移、预检、部署配置及联调。继续使用一个 `channel-gateway` 部署单元，不新增 polling 服务、独立 connector 或 Node 镜像。Helm 仍等全部 workload 完成后再做。

长轮询免去的是公网可达的入站回调地址，不是出站网络。Telegram API 出站、代理、Control mTLS、数据库、NATS 和内部健康检查仍是相应链路的依赖。

## 2. 三个任务怎样分工

| 任务 | 主责 | 交付与边界 |
| --- | --- | --- |
| **设计 Channel Gateway**（本任务） | 总体协调、Gateway Connection/Admission、部署配置与联合验收 | 编写本总计划，整理共同决议，审核另两份计划；实施阶段负责接收策略、owner/cursor、切换、协议适配器和 Gateway 测试；不代替 Control 定义一份私有 wire contract |
| **control-api** | ChannelAccount、共享契约与管理 API | 模式字段、默认值、存量迁移、允许/必需凭据、修订与授权、内部快照/凭据消费者/观测、预检版本兼容、OpenAPI/Schema/fixtures、Control 测试 |
| **优化 Web 页面框体对齐** | Web 账户接收方式选择及运维体验 | 创建/详情/编辑、停用后切换、凭据展示、启用确认、运行状态和预检历史、权限/CAS/幂等/BFF、真实后端和响应式验证；不直接调用 Telegram API |

### 2.1 各自的工作树和计划

- Control 工作树：`/Users/jfs/Projects/trpc-agent-service-channelbinding`。
  - 计划分支：`codex/control-telegram-receive-mode-plan`。
  - 计划：`/Users/jfs/Projects/trpc-agent-service-channelbinding/docs/architecture-next/control-api/telegram-receive-mode-plan.md`。
- Web 工作树：`/Users/jfs/Projects/trpc-agent-service/.codex-worktrees/control-tenant-rbac`。
  - 计划：`/Users/jfs/Projects/trpc-agent-service/.codex-worktrees/control-tenant-rbac/docs/architecture-next/web/telegram-receive-modes-v1-plan.md`。
- Gateway 总计划：本文。

已经向两个现有任务发送“只制定计划”的委托；两方均已确认从 `f61d49b` 核对源码，并交换了发现。两份独立计划均已落盘；初稿回读后，两方最终交叉审阅结论也已回传，本任务已核对并纳入。Control 与 Web 均明确停止于本轮计划完成；三份文档方向一致，具体 wire 仍待共同冻结。

共享文件由 Control 单一归口修改，包括 `api/schemas/channel/`、相应 `api/openapi/control/`、公共 fixtures 和公共预检规格。三方先共同评审，再冻结；单一归口不等于单方决定。Web 与 Gateway 使用同一份契约和 fixtures，不复制第二套私有枚举、摘要规则或请求协议。

## 3. 基于当前代码的修改清单

以下路径相对各自仓库根目录；它们描述核对到的现状和拟修改方向。

| 当前实现位置 | 已核对的问题 | 拟修改方向 |
| --- | --- | --- |
| `services/control-api/internal/channelbinding/domain/account.go` | Config 只有 WebhookPath/BotID；Telegram 新建与校验绑定 webhook path；RequiredPurposes 同时充当允许保存的集合 | 增加账户接收方式；拆分 AllowedPurposes 与按 mode 计算的 RequiredPurposes；保留可选 Secret |
| `services/control-api/migrations/0001_baseline.sql` | 物理身份仅在 `(scope_id, provider, provider_account_id)` 内唯一；Telegram observation 禁止 owner_epoch | 新增迁移，不修改已发布 baseline；明确跨 scope Bot 归属和 Telegram polling 观测规则 |
| `api/schemas/channel/v1/`、`api/openapi/control/v1/` | 账户、快照、凭据消费者、观测、预检均是闭合契约 | 由 Control 统一提出兼容/版本方案，增加正反 fixtures，三方共同消费 |
| `services/channel-gateway/internal/connection/domain/accountcatalog/` | Config 无 mode；启用 Telegram 要求两份凭据；消费者仅 webhook/delivery/registration；快照去重仅覆盖 scope | 模式感知校验、修订比较、授权和状态；不能把 scope 内校验当成全平台 Bot 独占 |
| `services/channel-gateway/internal/connection/application/telegramruntime/runtime.go` | 构造要求 origin；生命周期只安装 Webhook 并注册远端 | 变成模式协调器，分别委托 Webhook 策略和 Poller 策略，统一切换互斥 |
| `services/channel-gateway/internal/connection/adapter/outbound/telegramregistration/`、`registrationpostgres/` | 现有注册 fencing 是账户作用域，不能直接等价于长轮询 Bot owner | 协调注册与 polling 的物理 Bot 控制权，重用适当机制而非创建互不相知的两套 owner |
| `services/channel-gateway/internal/admission/adapter/inbound/telegramadapter/handler.go` | Webhook 专有入口内包含私有 normalize | 抽取原始 Update 规范化；两种入口复用同一实现 |
| `services/channel-gateway/internal/admission/domain/admission.go` | ConnectionFence 仅允许 WeCom；Telegram 带 fence 会校验失败 | 设计 Telegram polling 的本地授权证据，并在持久提交边界检查；不把 WeCom ReplyOrigin 塞给 Telegram |
| `services/channel-gateway/internal/bootstrap/control_config.go` | Control URL 和 PublicOrigin 均无条件要求 HTTPS origin | 保留 Control URL 要求；PublicOrigin 按账户模式校验；纯 polling 无 origin 可启动 |
| `services/channel-gateway/internal/connection/application/preflight/`、`adapter/outbound/telegrampreflight/` | Webhook 前提、固定检查结果和配置摘要 | mode/policy 感知检查、N/A 语义、历史解释和逐任务事实绑定 |
| `web/lib/channel-api.ts`、`channel-editor-state.ts`、`channel-preflight-api.ts` 与 Channel 页面 | 闭合 DTO/pending storage 可能丢弃或拒绝 mode；现有凭据和固定预检状态规则绑定 Webhook | 严格消费新契约，保留旧 pending 操作语义；按 mode 展示，不用 UI 自行补协议 |
| `deploy/compose/compose.yaml`、`compose.local.yaml`、`compose.gateway-fixture.yaml` 及部署说明 | 现有启动说明以 Webhook 为前提 | 同一服务补充纯 polling 与混合模式配置；保留代理，移除非必要公网入口前提 |

## 4. 账户配置、默认值和修改规则

拟议保存形态如下；实际字段约束以 Control 共享契约评审结果为准：

```json
{
  "provider": "telegram",
  "config": {
    "receive_mode": "long_polling"
  }
}
```

1. 持久记录与发给 Gateway 的快照必须有明确模式。新建默认 LP 与旧记录兼容是两个不同入口，不能共用一个模糊的缺省推断。
2. 旧客户端的无 mode 新建请求、旧 Web pending 请求重放和持久历史记录要分别定义兼容行为。尤其不能把升级前尚未完成的 Webhook 创建操作按 LP 默认重放。仅给历史 receipt 增加契约标记仍不够：旧请求可能从未落库、根本没有 receipt；这类 pending 必须锁定，按明确的兼容请求标记/恢复路径或结果核对处理，不能重发缺 mode 的 body，也不能换 key 静默重建。
3. 模式变化推进 `connection_revision`，并按照现有账号 CAS/幂等语义推进账号修订；纯展示名称变更仍不能无故重启连接。
4. 两种模式都需要 Bot Token；Webhook 需要其入站 Secret；LP 不要求 Secret，但允许保存、保留并显示其配置状态。切换不清空 Secret、cursor 或积压消息。
5. WebhookPath 是保留但暂不适用，还是在 LP 下不投影，由 Control 明确统一规则；不得让两个后端对同一快照作相反校验。
6. **建议首版仅允许账户 disabled 时保存模式。** 保存是管理面变更，不访问 Telegram、不自动启用、不执行远端接管。运行中编辑返回明确冲突，前端引导先停用。
7. 下一次显式启用才执行接收协调。启用意图绑定最新已保存模式与连接修订；禁用请求成功不代表远端在途请求已停止。
8. ChannelBinding 不增加 receive_mode；如果其启用前置条件当前也检查账号凭据，应改为复用账号的 mode-aware readiness，而不是继续硬编码 Secret 必需。

## 5. Gateway 代码结构与依赖方向

以下是实施骨架，不代表这些文件已创建。已有模块就地深化，新增名称在实施前以代码审查确定，不为目录对称而制造空层。

```text
platform/im/telegram/                         [新增公开 Go 协议包]
  client.go                                  有界 HTTP、代理、超时、错误分类
  updates.go                                 单次 PollOnce，保留原始 Update JSON
  webhook.go                                 显式 set/delete/info 操作
  *_test.go                                  协议级测试；无 Control/SQL/NATS 依赖

services/channel-gateway/
  internal/connection/
    application/telegramruntime/             [重构现有]
      runtime.go                             按 mode 协调接收生命周期
      webhook.go                             安装/撤销本地入口与注册协调
      polling.go                             owner→poll→持久接收→cursor
      transition.go                          停旧、远端切换、核验、启新
      ports.go                               消费者定义小接口
    domain/telegramreception/                [候选新增]
      state.go                               owner/cursor/transition 约束
    domain/accountcatalog/                   [修改现有]
      catalog.go / permit.go                 mode、修订、凭据用途和调用授权
    adapter/outbound/
      telegramregistration/                  [重构现有适配器]
      registrationpostgres/                  [评估与新接收状态共用/衔接]
      telegramreceptionpostgres/             [候选新增，持久 owner/cursor]
      controlhttp/                           新共享 DTO/消费者/观测
      telegrampreflight/                      只读、按 mode 的预检
  internal/admission/
    adapter/inbound/telegramadapter/
      handler.go                             Webhook 鉴权和 HTTP 协议
      normalize.go                           [抽取] 两种方式共用原始 JSON
    domain/                                  本地接收 fence；不改变来源摘要
    application/                             复用 Receipt→Admission→Outbox
    adapter/outbound/                         最终持久提交时验证接收授权
  internal/bootstrap/                        组装依赖、mode-aware 启动/健康
  migrations/
    0011_telegram_receive_modes.sql           [暂定编号，冻结前检查冲突]
```

公开包负责“如何与 Telegram 通信”；Gateway 内部负责“哪个账户/实例现在有权通信、何时确认、如何持久化”。领域/应用层不依赖具体 SDK、HTTP、SQL；出站 adapter 实现应用层需要的接口，bootstrap 组装。

项目固定的 `github.com/go-telegram/bot@v1.25.0` 有私有 `getUpdates` 循环，未提供可直接导入的公开单次 GetUpdates；其循环在移交内存 channel 前推进 lastUpdateID。实施不能写成调用一个不存在的 SDK 方法，也不能把 `bot.Start()` 当持久消费框架。可以复用合适的 SDK 类型/现有能力，补一个小型受控 HTTP 协议实现，不 fork 整个 SDK。

建议的边界示意（不是本轮交付的可执行实现）：

```go
type PollRequest struct {
    Offset         int64
    Limit          int
    TimeoutSeconds int
    AllowedUpdates []string
}

type UpdatePoller interface {
    // 不启动后台循环，不维护 cursor，不把 raw Update 重序列化。
    PollOnce(context.Context, PollRequest) ([]json.RawMessage, error)
}
```

## 6. 长轮询可靠接收与 cursor

Telegram 用后续更大 offset 确认此前 Update，而不是以本地 handler 开始执行作为确认。因此顺序必须是：

```text
确认当前 owner、配置修订与剩余请求预算
  → getUpdates(saved next_offset)
  → 对返回批次按顺序处理原始 Update
  → normalize
  → 同一 AcceptInbound 持久提交 Receipt / Admission / Outbox
  → owner + revision + cursor CAS 保存 next_offset
  → 下一次请求才使用推进后的 offset
```

- 101 成功、102 失败、103 尚未处理：保存的 next_offset 最多是 102，不能直接置为 104。
- Update ID 有跳号时，以实际返回且已持久处理的有序前缀为准，不等待不存在的整数编号。忽略事件和交互事件同样需要持久回执。
- Receipt 已提交但 cursor 提交前崩溃：重读并复用 Receipt，允许重复，不允许提前远端确认。
- Admission 提交超时且结果不确定、路由暂不可用、owner 已失效：不推进游标。不得靠送入内存 channel 或直接发布 NATS 替代持久接收。
- 两种 transport 产生同一 `(provider, stable account_id, update_id)` 和原始规范化摘要；mode、owner、接收实例不是来源内容的一部分。保留未知 JSON 字段，避免 SDK 重序列化制造假冲突。
- Telegram polling 的本地 fence 应在新 Receipt/Admission 持久提交和 cursor 更新处检查；来源去重成功不等于过期 owner 获得推进权限。沿用 WeCom 专用 ReplyOrigin 会破坏边界。
- Poll timeout 必须小于剩余 owner 执行预算，并预留网络/提交余量；建立在途调用记录或可证明的 deadline 协议，避免持有数据库事务跨网络等待。
- 对 malformed/poison Update 明确有界重试、可观察阻塞和人工处置，不静默 skip；支持的 ignore 事件与格式损坏要区分。
- `allowed_updates` 显式传递且与 Webhook 一致；不依赖 Telegram 保留上次设置。控制响应体、批次、并发和退避，处理 429/retry_after、5xx、超时和 409。

### 6.1 不能略过的长期游标边界

Telegram 文档说明至少一周无新 Update 时下一个 update_id 会随机选择，更新最多保存 24 小时。因此不能无限期假设 ID 单调，也不能把长轮询描述成无限期无损消息日志。

实施前需补明确的长空闲游标恢复方案和测试，包括判断证据、无破坏性探测、避免旧高 offset 提前确认较小新 ID、ID 冲突及运营处置。不得用 negative offset 或 drop_pending_updates 自动“修复”。当前使用稳定账号 ID 的 EventKey 也要求明确物理 Bot 解绑/重新建账号后的重放归属；这属于设计冻结项，而非宣称现有账本已覆盖。

## 7. 多副本和模式切换

### 7.1 控制权的真实边界

HTTP Webhook 接收可以分布在多个副本；同一个物理 Bot 的远端注册/删除与 polling 控制必须协调。不能保留一套独立 registration owner，再另外加一套彼此不可见的 poller owner。

当前 Control 仅约束 scope 内 Bot 身份唯一，Gateway 快照同样只覆盖一个 scope。若要平台内跨 scope 独占，需共同选择：收紧身份唯一约束，或引入跨 scope 的物理 Bot ownership registry。历史重复项要先检测并确定归属，不能让迁移随意选赢家。其他平台/进程的 poller 不受本平台 DB lease 控制。

租约与 fencing 约束本平台的调用许可及写入；Telegram 不校验我们的 fencing token。进程暂停后恢复、失联 owner 的在途 getUpdates、远端写超时仍需要请求截止、旧路径静默期和状态不确定时的阻塞策略。不能仅用 `enabled=false`、一次 DISABLED observation 或 `getWebhookInfo.url == ""` 证明绝对独占。

### 7.2 推荐首版流程

```text
用户停用账户
  → Control 推进连接修订并发布
  → 用户在 disabled 状态保存新模式（CAS，仅保存期望配置）
  → 用户显式启用（针对最新 mode/revision 确认切换意图）
  → Gateway 获得该物理 Bot 控制权并确认旧路径已静默
  → 核对 getMe 身份和远端实际状态
  → 执行显式模式变更
  → 核验远端状态并启动新路径
```

Gateway 在收到停用配置后即异步撤销旧接收授权、停止新调用并处理/等待在途请求。上图不要求 Web 在保存纯期望配置之前等待或证明全局 quiescence；旧路径已静默是 Gateway 启动新 receiver 前的强门槛，即使用户快速保存并重新启用也不能跳过。

- Webhook → LP：按已授权意图调用 `deleteWebhook(drop_pending_updates=false)`；核验 URL 清空后开始 polling，不清 cursor。
- LP → Webhook：停止/排空旧 polling，在准备好本地鉴权入口后按已授权意图 `setWebhook(drop_pending_updates=false)` 并核验；不把管理面保存成功当运行 READY。
- 远端已有不属于当前配置的 Webhook：预检只报告，启用前明确接管确认。确认需绑定已保存连接修订；若远端事实改变，重新确认/拒绝的令牌、摘要与错误协议由三方冻结，不能在 UI 用一个不受服务端验证的复选框替代。
- 409、重复外部 poller、远端写结果不确定：报告冲突/未知，停止新竞争性动作，不自动改变用户选择，不反复抢占。通过只读事实核验后按同一意图继续协调。
- 停用后的后台清理动作范围、外部 Webhook 是否仍保留、如何保留 backlog，由生命周期契约写清。不得把“停用”“删除 Webhook”“丢弃队列”当同一个操作。

## 8. 预检与运行观测一起演进

| 项目 | Webhook | Long polling |
| --- | --- | --- |
| Bot Token / getMe 身份 | 必需 | 必需 |
| Webhook Secret | 必需 | 不适用但允许保留 |
| PublicOrigin 可用性 | 适用 | N/A，不伪装 PASS |
| getWebhookInfo | 核对期望配置 | 报告阻挡 polling 的现有 Webhook |
| 实际接收状态 | 本地入口与远端注册 | owner、轮询健康、游标推进/错误 |
| 真实消息投递验证 | 未做则 NOT_TESTED | 未做则 NOT_TESTED |

预检仍是只读诊断：允许 getMe/getWebhookInfo；不调用 getUpdates，不 delete/setWebhook，不启用账户、不发送测试消息。空 Webhook URL 只能说明未配置 Webhook，不能证明没有外部 poller。

当前预检固定状态、检查项顺序、首批检查聚合、DTO/Schema/DB 与 Web parser 需要成套修改。新增 N/A 需明确机器值、reason codes、适用性、整体 outcome 和历史渲染版本；旧结果按原策略解释，不回填伪通过。

当前领取 preflight 时携带批级 Gateway origin/config digest。建议保留该摘要表达共享运行配置，另以逐任务字段绑定账户 receive_mode、connection_revision、diagnostic policy 及适用事实；不把不同账号的 mode 填入同一个批级摘要。兼容端点/版本判别、完成校验和历史查询由 Control 统一提案，Gateway/Web 一起审核。

运行观测须能区分配置期望与事实：等待旧接收退出、模式协调中、正在 polling、远端冲突、配置不满足、disabled 等语义。具体 wire 枚举不在三份文档各自发明；Telegram owner_epoch 与对应 DB 约束/允许消费者一起改。

## 9. Web 的交互要求

1. 创建页展示“长轮询（默认）/ Webhook”；说明前者无需公网入站，后者要求公网 HTTPS 回调入口。
2. 详情页展示已保存模式、connection_revision、运行事实和最近预检，不混淆“已保存”“已启用”“正在收件”。
3. disabled 时允许保存模式；enabled 时提供先停用的明确入口。保存不触发 Telegram 副作用。
4. LP 隐去“不配置 Secret 就不能继续”的校验，但保留已有 Secret 的配置状态，不清除服务端值；Webhook 模式恢复必需校验。
5. 启用时基于最新模式/修订展示实际切换与 backlog 保留说明，接管远端配置的协议接受服务端校验。
6. 预检 N/A、NOT_TESTED、失败、历史结果原义分别展示；保留已有各层预算及鉴权边界，不为掩盖错误任意延长超时。当前 `CHANNEL_TIMEOUT_MS = 10_000` 位于 Web 客户端 `web/lib/channel-api.ts`；BFF `web/app/api/control/[...path]/route.ts` 的上游 fetch 尚无显式 deadline，不能把客户端 10 秒描述成 BFF 已有的截止。若补 BFF deadline，需独立定义取消传播与结果不确定的恢复语义。
7. pending storage、创建重试、刷新恢复、CAS 冲突、幂等键、OWNER/MEMBER 权限都进入测试；UI 隐藏不能替代服务端权限。
8. 不在浏览器直接调用 Telegram、不在客户端存储 Token 值，不用 mock PASS 替代真实 Control 联调。

## 10. 实施顺序与合入门槛

### P0：本轮计划与共同冻结

- 三份计划完成，核对同一基线，彼此回读。
- 冻结第 12 节跨端事项，确定共享 Schema/fixtures 的唯一来源与文件 owner。
- 先评审后实施；此阶段无生产代码、运行配置或 Bot 副作用。

### P1：Control 主导契约与兼容基础

- 新增迁移和存量显式 webhook 回填；定义旧请求/pending/历史兼容。
- 更新 Domain/Application、公共 API/内部快照、credential consumer、观测、预检契约及正反 fixtures。
- 给 Gateway/Web 可验证的同一契约基线和真实 API；实际新默认的发布时机与 P4 联动，不能先让旧 Gateway 接收未知模式。

### P2：Gateway 与 Web 并行实现

- Gateway：公开协议包、raw normalize 共用、owner/cursor/切换、Admission fence、预检和 bootstrap/Compose。
- Web：消费冻结契约，实现选择/停用编辑/启用确认/历史和状态展示；无需等待所有 Poller 故障测试完成才开始 UI 工作。
- 契约变化先通知 Control 更新公共 fixtures，其他端同步，不临时绕过严格校验。

### P3：三方联合验收

- 真实 PostgreSQL 迁移/并发/授权/CAS；真实 Control mTLS 账户与预检接口；真实 NATS 持久出站。
- 两 Gateway 副本测试 owner 切换、停止恢复和不确定请求；模拟 Telegram 故障，不把 SDK mock 成功等同于可靠接收。
- Web 用实际 Control BFF 测试创建、编辑、冲突、预检和权限，补桌面/移动端、键盘及可访问性核对。
- 受控真实 Telegram 验收在实施阶段安排：无公网 origin 的 polling 收件、跨模式重放去重、显式双向切换、Receipt→Admission→Outbox→RunRequested 证据。真实 Agent Worker/回复链完成度单独说明，不冒充本功能验收。

### P4：部署、兼容发布与合入

- 先发布能够理解两种模式与新诊断契约的读取方/执行方，再放开新模式写入与默认值；精确顺序由兼容测试确认。
- 新模式 feature gate 在旧 Gateway 仍工作时保持关闭，阻止滚动升级产生快照拒绝或接收争用。
- 验收通过后按协调结论分支提交/推送并合入；计划完成不代表已进入 main。
- 无需新增工作负载；更新原 Gateway 镜像/环境示例/启动检查和部署文档。纯 LP 不要求 PublicOrigin，混合模式要逐账号报告 Webhook 配置错误，不能整体静默放行。
- 回退必须识别新 mode/迁移后的状态；旧二进制不一定能读取新快照。优先兼容前向修复，若降级则先受控停用/静默和转换配置。代码回退不自动回滚远端 Webhook，不删除 cursor/积压消息。

## 11. 验收矩阵

| 类别 | 必须证明的结果 | 负责人 |
| --- | --- | --- |
| 默认/迁移 | 新账户默认 LP；旧账户显式 Webhook；旧客户端和 pending 请求不误切换 | Control + Web |
| 凭据 | LP 只需 Token；Webhook 需 Secret；来回切换保留可选 Secret；Binding 前置条件一致 | Control + Web + Gateway |
| 修订/授权 | enabled 编辑拒绝；CAS/幂等/权限正确；旧 permit/source/revision 无法启动新调用 | Control + Gateway |
| 原始数据 | 同一 raw Update 的 EventKey/digest 在两模式一致，未知字段/ignore/interaction 不丢 | Gateway |
| 游标 | 批内部分失败、Receipt 后崩溃、cursor CAS 失败、DB 结果不确定均不提前确认 | Gateway |
| 协议边界 | timeout/429/409/5xx、allowed_updates、跳号、长期空闲随机 ID、poison 更新有明确结果 | Gateway |
| 多副本 | 物理 Bot 跨 scope 归属、租约到期、暂停恢复、旧在途请求和并发切换有证据 | Control + Gateway |
| 双向切换 | 停旧再启新，显式确认，保留 backlog，远端事实变更不盲目接管 | 三方 |
| 预检 | 真正只读，N/A 不等于 PASS，批级/任务级摘要正确，旧历史按旧策略展示 | 三方 |
| 部署 | 无 PublicOrigin 的纯 LP；混合模式；代理/mTLS/DB/NATS/健康检查正常 | Gateway |
| Web | 真实 BFF、pending 重放、角色/CAS/错误/响应式/a11y，保存与运行状态区分 | Web |
| 回归 | 旧 Webhook、WeCom owner/ReplyOrigin、Delivery 和稳定去重语义保持 | 三方 |

执行时分别报告单测、真实数据库集成、模拟外部协议、真实 Telegram 四类证据，不把可能跳过的普通 go test 称作真实联调。

## 12. 实施前需要共同冻结的事项

以下问题由本任务负责组织两方给出明确方案，暂不把候选规则当作已发布协议：

1. 账户 config 精确字段、WebhookPath 在 LP 下的投影、新默认/旧创建请求/pending 重放策略。
2. disabled-only 修改的服务端错误、停旧与启用的可观察语义，以及远端接管确认的修订/事实绑定协议。
3. 物理 Bot 跨 scope 归属规则、历史重复账户处置、平台外竞争 owner 的处理。
4. Telegram polling/transition 所需 credential consumer、允许 owner_epoch 的观测和内部权限，及本地 Admission fence。
5. cursor/owner/transition 的持久化模型，旧在途请求的截止证明、长期空闲随机 update_id 恢复、Bot 重新建账号的去重归属。
6. 预检 N/A 机器枚举、聚合规则、诊断策略版本/历史解释、共享配置摘要与逐任务事实摘要。
7. 扩展读取方与新模式写入的发布顺序、feature gate、迁移编号和可执行回退条件。

### 12.1 两份独立计划回读后的收敛建议

Control 与 Web 各自的 D1–D12 是局部编号，不能按编号直接互相等同；本总计划按上述七个主题归并。下面是三方已提出的优先候选，不是新的已冻结接口：

| 主题 | 收敛建议 | 仍需确认的具体边界 |
| --- | --- | --- |
| 配置写入 | 复用 Account PATCH 和现有 account CAS；disabled-only；服务端可原子更新 metadata 与 mode，Web 将模式变更单独确认 | 精确请求、NOOP/幂等规范化和错误 fixtures；是否额外要求 connection CAS |
| 可选凭据 | 保留两条稳定元数据，初始 version=1；LP 的 Secret 可为 configured=false；保留服务端生成的 WebhookPath | 首次 replace CAS、旧快照严格校验、disabled 保存缺 Secret 的 Webhook 配置与 enable 前置规则 |
| 旧请求 | 新创建命令应用 LP 默认；老幂等 key 按原规范化/原回执处理，旧 Web pending 不补 mode | command receipt 契约标记、原响应形状、原 MAC 校验与无 receipt 旧请求的发布门槛 |
| 平台内归属 | 优先评估 Telegram 平台目录跨 scope 唯一、disabled 保留占用，历史重复先审计 | 唯一索引与 ownership registry 取舍；不扩大到 WeCom 或宣称约束外部平台 |
| 迁移修订 | 倾向回填显式 Webhook 时推进 account/connection/catalog 修订，保持 enabled/凭据/Binding 原值 | Gateway 同模式重授权行为；若保持 connection_revision 不变，必须证明兼容 reader 接受同版本 config 表示变化，当前 CheckSuccessor 不支持直接这样做 |
| 预检兼容 | 保留八项 ID，候选机器状态 NOT_APPLICABLE；共享配置与逐任务有效诊断摘要分离；旧策略/新策略双读 | N/A 合法位置、Webhook 阻碍 severity、混合任务、旧 claim receipt、进行中任务排空和可读兼容 |
| 副作用授权 | 保存不触 Telegram；显式启用后由 Gateway 协调；保留 pending | LP 的 deleteWebhook 需要可用的 transition/receiver 授权，不能被 webhook-only registration consumer 条件误阻；启用确认仍需精确服务端验证 |

另外，Web 指出“预检必须全 PASS 才允许启用”会令 LP 用户永远无法通过启用来处理已有 Webhook。共同契约应区分诊断阻碍与经明确确认后可执行的协调动作，不能把 UI 的全绿条件当作额外业务门槛。

Control 最终校验又补充了三项约束，本任务已回读并纳入共同冻结范围：

- **双摘要失效矩阵**：LP 任务不应仅因无关 PublicOrigin 改变就 STALE；同一 lease 内的 grant 不得被静默替换，完成仍精确匹配该 lease 固定的摘要。新 lease 是否可在有效摘要相同时接受新共享摘要，要明确重领规则，旧 claim receipt 保持原样。
- **结果自身可识别**：诊断策略的内部版本设计必须投影为 Web 能区分的结果 mode/policy 形态或明确 legacy 分派依据，不能只在数据库加标记、却让 GET 消费者猜测。
- **灾备不是普通应用回退**：恢复备份要同时校验 Control catalog/账号/凭据/连接版本、路由下限与 Gateway owner/cursor/Receipt 水位；不得通过换 source_epoch 或清除 Gateway 水位掩盖回退。执行前需要独立的停止接纳、权威核验与信任恢复方案。

归口顺序不变：Control 提交共享字段/Schema/fixtures 草案，本任务核对执行语义，Web 核对可交互与恢复语义，再形成统一冻结结论。本轮到此止于计划，不分派实施。

## 13. 协议资料与本轮证据边界

核对日期为 2026-09-07。Telegram 官方资料：

- [Getting updates](https://core.telegram.org/bots/api#getting-updates)：两种收件方式互斥、同一 Update 格式、24 小时保留及长期空闲 ID 规则。
- [getUpdates](https://core.telegram.org/bots/api#getupdates)：offset 确认、limit/timeout/allowed_updates 的行为。
- [deleteWebhook](https://core.telegram.org/bots/api#deletewebhook) 与 [setWebhook](https://core.telegram.org/bots/api#setwebhook)：显式切换和 drop_pending_updates。
- [getWebhookInfo](https://core.telegram.org/bots/api#getwebhookinfo)：只读远端 Webhook 状态。
- 已核对本机 SDK：`/Users/jfs/go/pkg/mod/github.com/go-telegram/bot@v1.25.0/get_updates.go`。

本文的源码差距来自 `f61d49b` 实际实现。本文没有宣称双模式已实现、单测已通过或真实 Bot 已切换；本轮验证对象是设计文档、源码基线及两方任务委托。实施后的验收应另附可复现命令与实际结果。
