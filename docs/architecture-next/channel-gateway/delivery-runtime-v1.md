# Delivery Runtime V1：独立维护、有界调度与生产装配边界

> 2026-09-05 Runtime 历史切片的源码与验收见[实施状态 §11](implementation-status.md#11-delivery-runtime-v1实现与本轮验收)；本文运行装配口径已于 2026-09-06 按 GCI2 更新。
> 本文补充 [Final V1](delivery-final-v1.md)，不修改其 wire、唯一 Final、原 ReplyOrigin 或 A1/A2 契约。
> **当前默认 Control 来源启动 Delivery Runner，并由 Runner 独占 Maintenance 生命周期；显式 fixture 来源仍只独立维护。ReplyIntent Consumer 与真实 Worker 完成验证器仍未接入。**
> 当前 Gateway 共有 10 个迁移（0001–0010）；Control/Telegram 真实入站验收见[独立报告](telegram-real-inbound-20260906.md)，不等于真实 Final 回复闭环。

## 1. 这次解决什么

先前 Dispatcher 已能对一个指定账户执行领取、准备、发送和记账，但没有完整后台发现与
维护循环。只在 ClaimDue 中顺便检查 deadline，会漏掉无人 owner 的企微账户，以及被前段
UNKNOWN 阻塞的后续到期 part。CGR-35 因此同时要求独立维护与有界、公平的账户调度。

当前实现把两种用途分开：

| 用途 | 实现 | 需要的能力 | 默认 App 是否启动 |
| --- | --- | --- | --- |
| 维护已持久事实 | `Maintainer.Run` / `Sweep` | Delivery 自有账本 | 是；Control 由 Runner 拥有，fixture 由 App 独立拥有；空账户也维护 |
| 寻找并执行可发送工作 | `Runner.Run` | 账户发现、eligibility、Dispatcher、一个独占的 Maintainer 生命周期 | Control 来源已启动；fixture 来源不自动启用 |
| 接纳新的 Final | Acceptor / eventadapter | 原 Admission 快照、可信不可变完成证明 | 不新增默认入口 |
| Provider 调用 | 原 Dispatcher / Sender reservation | 已确认 A2、原目标、当前发送资格 | Control 已接可信凭据与 Sender；没有持久 Final 意图时不制造发送工作 |

维护不访问模型、不生成 Final、不接受 Worker 自报的成功，也不取得企微连接。四 Module
仍在一个 Go Gateway Workload 中；不新增维护服务、Sender 服务或 Connector 部署单元。

## 2. Interface 与所有权

[Runtime Ports](../../../services/channel-gateway/internal/delivery/application/runtime_ports.go)由
使用方 Delivery Application 定义；PostgreSQL Adapter 实现持久发现与维护，Connection 通过
窄 bridge 提供本地资格快照。Application 不 import pgx 或公开企微 SDK。

- `DueAccountReader.ListDueAccounts`：按 provider 与账户 keyset 返回候选，不授予发送权限。
- `AccountEligibility.InspectAccount`：本地调度预检查。Telegram 不带 Owner；企微必须带
  有效 OwnerFence，实际 A1/A2 仍在数据库事务中核验。
- `AccountDispatcher.DispatchAccount`：复用既有完整发送用例；每次请求领取一个 part。
- `MaintenanceStore`：过期 CLAIMED 回收、stale CALLING 恢复、未发送项过期、观察候选发现
  与显式一致证据收束。所有写入仍只修改 Delivery 拥有的事实。

这些是内部能力，不是跨 Workload RPC 或第五个业务 Module。账户发现页、状态 snapshot
也不是外部 IM 成功回执。Bootstrap 只构造并启动生命周期，不逐账户编排 claim/retry。

## 3. Maintenance 不依赖可发送账户

[Maintainer](../../../services/channel-gateway/internal/delivery/application/maintainer.go)每轮：

1. 有限批次 `RecoverExpiredClaims`：A2 未发生的旧领取可按已有规则重新调度。
2. 有限批次 `RecoverStaleCalling`：已通过发送门而未确认结果的调用保守进入 UNKNOWN。
3. 有限批次 `ExpirePending`：使用数据库时钟收束到期 PENDING 和仍可重试的 NOT_SENT。
4. 分页 `ListResolvableObserved`，逐项调用已有 `ResolveObserved`；只在原当前 Attempt
   的证据唯一一致并满足已有恢复条件时收束，不触发 Provider 调用。

维护不会把 CALLING/UNKNOWN 改成 EXPIRED 来声称“确定未发送”，不会修改终态 NOT_SENT，
也不删除 Intent、Part、Receipt、Attempt 或 Observation。**Expiry 不是 GC：总行数容量
不会因为记录变成 EXPIRED 自动释放。** 正文/墓碑、审计与容量回收仍是独立保留策略。

观察游标跨 Sweep 保留，经过冲突、尚不能收束或暂时失败的候选后继续后页，下次完整
轮转再访问。一个可恢复步骤失败不阻止其他独立维护步骤；失败以受限状态记录，不输出
原始 Port 错误或凭据。取消后停止启动新步骤，Port 必须遵守传入的截止时间。

当前默认值：1 秒轮询、每个 Port 操作 2 秒、批次 100、每 Sweep 最多 4 个观察页。
这是源码默认预算，不是固定总 Sweep SLA，也不是已证明的生产吞吐。构造器校验范围，
`Snapshot` 记录运行状态、Sweep 次数、最近统计与归一化失败类别；尚无新增管理 HTTP 或 OTel 输出。

## 4. Runner：有界发现与每账户串行

[Runner](../../../services/channel-gateway/internal/delivery/application/runner.go)轮转 provider，
并为每个 provider 保留账户 keyset。每 tick 的起始 provider 独立轮换，不由页预算取模
决定；否则默认 4 worker / 4 页可在每 tick 都先由 Telegram 占满，企微即使持续 due 也
可能一直得不到槽位。跳过非本地 owner、不可用或正在处理的账户时仍推进游标；工作池
满时不预先领取大批任务堆在本地等待。

- 固定 worker 数；单个 Runner 实例对同一 `(provider, account_id)` 最多一个在途 dispatch。
- discovery 只返回持久候选，eligibility 只减少无效调度；两者都不替代 ClaimDue/MarkCalling。
- 每次传给 Dispatcher 的 ClaimRequest 限定一个 part；原分段顺序、准备/发送预算和
  UNKNOWN 屏障继续由既有账本控制。不同 Runner/副本之间仍依赖 PG claim/CAS 防止同一 part 冲突，而非共享本地 active map；
  这不额外保证同一账户的不同 part 跨 Runner 串行。
- Provider 和页轮转避免第一页不可用账户一直阻塞后页，但不承诺租户配额或硬延迟 SLA。
  生产负载公平性与会话级额度仍待验证。

本轮按公开 Interface 补 `TestRunnerDefaultPoolSharesCapacityAcrossContinuouslyDueProviders`：
两个 Provider 都持续 due，验证默认池仍向两者提供调度机会，同时保持每账户单在途与
worker 上限。该用例针对持续积压，不把“偶尔遍历到两个 Provider”当作完整公平证据；
本轮分项红绿与最终运行结果由实施状态 §11 记录。

默认参数为：1 秒轮询、单操作 10 秒、claim lease 15 秒、drain 15 秒、4 个 worker、
每页 32 个账户、每 tick 最多 4 页。配置通过内部 RunnerOptions 明确传入；当前 Control
App 直接装配一个 Runner，SDK Client 经 Control 账户用途和托管凭据接入，
不凭 Provider 列表自动构造未认证客户端。

`Run` 单次生命周期；`Quiesce` 与 worker 接纳在同一锁下确定停止点，停止接纳新的 dispatch。
此前已接纳的工作仍使用独立有界 context 完成已有用例和结果证据收束，不宣称撤销在途
网络副作用。`Drain` 等实际工作结束；超时报告未完成，不把仍活跃的工作伪装成退出成功，
调用方可再次等待。

Runner 自己持有一个 Maintainer 生命周期。**同一个 Maintainer 不能既交给 App 独立 Run，
又交给 Runner 再次启动。** 当前 Control App 选择 Runner 拥有 Maintenance；
fixture App 才独立运行 Maintainer，两个分支互斥，不能叠加两次 Run。

此外，当前生产装配采用一个 Runner 管理本进程的发送账户，避免多个 Runner 的调度范围
重叠。`active` 属于 Runner 对象，不是进程级共享表；两个 Runner 使用各自的 Maintainer
仍可能同时调度同一账户。若以后确需多个 Runner，先设计并验证互斥账户分区或额外协调，
不要把目前仅有的 provider 配置当作已经实现账户分区。重复装配/重叠范围的拒绝与测试
属于未来多 Runner 装配门禁；当前单 Runner 选择不等于已经实现通用的分区/重叠拒绝机制。

## 5. PG RuntimePorts 与 0007

[PostgreSQL Runtime Adapter](../../../services/channel-gateway/internal/delivery/adapter/outbound/postgres/runtime.go)
实现三项新增操作：

| 操作 | 数据库边界 |
| --- | --- |
| `ListDueAccounts` | 去重账户、provider 隔离、业务 deadline/next_attempt_at/尝试预算及前段 ACCEPTED 筛选；不核验 owner |
| `ExpirePending` | `FOR UPDATE ... SKIP LOCKED`，获锁后重新读取 PG 时间并复查状态/deadline；确认提交后才返回计数 |
| `ListResolvableObserved` | 只列当前 part 的 UNKNOWN Attempt 且已存在 Observation；列表自身不推进状态、不绕过已有证据校验 |

两个发现查询使用 `COLLATE "C"` 和严格 ASCII ID keyset，与 Go cursor 比较一致；读取
`limit+1` 决定是否还有下一页，返回页不超过 limit，Next ID 是本页最后实际返回项。
Application 还复核页长、provider、单调性与 cursor，坏页不进入下游调度。
PG 三项操作各有独立 5 秒上限，保留更短的父 context；limit 接受 1–1000。

[0007_delivery_runtime.sql](../../../services/channel-gateway/migrations/0007_delivery_runtime.sql)
仅新增五个索引，继续由 Gateway 启动迁移与 SHA ledger 执行：

| 索引 | 用途 |
| --- | --- |
| `gateway_delivery_runtime_accounts` | provider、C 排序账户、deadline 与 intent 的候选发现 |
| `gateway_delivery_runtime_deadlines` | deadline 驱动的到期候选 |
| `gateway_delivery_runtime_pending` | PENDING/NOT_SENT 的分段与尝试预算查询 |
| `gateway_delivery_runtime_unknown` | 当前 UNKNOWN Attempt 的关联 |
| `gateway_delivery_runtime_attempts` | C 排序 Attempt keyset 与 part 关联 |

0001–0006 原 SQL 不重写；0007 不创建新的业务事实、不清理旧行、不释放容量。迁移执行者
和独立 Gateway database/role 不变，没有第二个迁移 Job 或数据库向下回退承诺。

## 6. LocalOwner 不是原 Sender，也不是授权证明

[Connection.LocalOwner](../../../services/channel-gateway/internal/connection/application/local_owner.go)
返回真实当前 localLease 的 OwnerGrant **值拷贝**，不从 eventually-updated Status 或原
ReplyOrigin 拼接 grant；不 Acquire、Renew、查凭据或访问数据库。

它按 Supervisor → lease 锁序核验：生命周期/配置源健康、账户存在启用、配置 revision
一致、未隔离/排空、grant 全部身份、Client READY 且非终态/被替换、context 与本地保守
截止时间。修改返回值不会修改内部 owner。

[Eligibility bridge](../../../services/channel-gateway/internal/bootstrap/delivery_eligibility.go)
只转换匹配 account/instance 的真实 grant。下面按实际调用顺序区分预检查、准备与持久发送门：

```text
LocalOwner：本地“值得调度”的当前资格快照
    → A1 ClaimDue：PG 核验当前资格并持久领取
    → ReserveFinal：匹配原 ReplyOrigin，预留 Sender，尚无 Provider 发送副作用
    → A2 MarkCalling：PG 再核验 owner / claim / 时间并确认提交 CALLING + Attempt
    → SendFinal：再次检查原关联并调用 Provider
    → Observe：保存原 Attempt 的受限结果证据
    → Finish：按当前 Attempt / owner CAS 收束
    → Release：释放覆盖调用和有界落账窗口的 reservation
```

A1 与 A2 分别核验数据库资格，Reserve 成功不替代 A2。准备失败走 FinishPreparation，
不先创建 CALLING；A2 提交失败或结果不确定时不调用 Provider。具体源码见
[Dispatcher](../../../services/channel-gateway/internal/delivery/application/dispatcher.go)，与
[Final V1 §5](delivery-final-v1.md#5-a1准备a2-与外部调用)保持同一顺序。

LocalOwner 通过不保证旧 Final 可发；同 owner 重连、换实例或配置切换后仍不得把旧
ReplyOrigin 绑到新连接。累计 Final 身份容量耗尽的账户策略（CGR-37）也尚未由 READY
或 LocalOwner 完整表达。

## 7. 默认 Control App：有界发送调度，不伪造 Final 入口

[App.New / Run](../../../services/channel-gateway/internal/bootstrap/app.go)在迁移、拓扑与
Routing 初始化后构造 Delivery Ledger/Maintainer；两个 listener 都绑定成功后启动
Relay、Routing consumer 与按账户来源选择的后台生命周期。

- Control 来源启动账户目录 refresh、WeCom Supervisor、Telegram 注册/动态入站、
  observations 和一个 Delivery Runner；Runner 独占同一 Maintainer，不重复启动维护。
- 显式 fixture 来源保留 App 独立 Maintenance；没有账户/本地 Sender 时也维护旧事实。
- Control Sender 已接凭据解析、轮换及 A1/A2 用途资格；没有持久 Final 意图时不制造发送任务。
  ReplyIntent Consumer、生产 Acceptor 入口和真实 CommittedFinalVerifier 仍未接入。
- 本地组合测试显式提供完成事实与 Sender fixture；这些帮助函数只在 `_test.go`，不是
  真实 Worker 的完成证明。真实 Telegram 入站验收也没有生成伪造 Worker 回复。
- App 取消后等待后台生命周期退出，再由进程入口关闭 PG/NATS。Control 目录新鲜度和
  实例资格已进入 readiness；Maintenance failure 仍没有独立 HTTP probe 映射。

Compose 使用同一个 Go Gateway workload，当前 migration 已到 0010；0007 仍只拥有
上述五个 Runtime 索引。Helm 仍在 Control、Gateway、Worker、Local IM、前端等全部生产
Workload 完成且运行契约稳定后进入 FINAL-INTEGRATION。

## 8. CGR-36 与剩余门禁

[eventadapter](../../../services/channel-gateway/internal/delivery/adapter/inbound/eventadapter/handler.go)
现只把 `errors.Is(err, ErrInvalidReplyIntent)` 的确定 wire 错误映射为 ErrInvalid；内部
Schema 初始化等其他 Decode 错误映射 ErrUnavailable，不再一律当成永久坏输入。
此修正没有创建 NATS Consumer、持久拒绝账本或 ACK 路径。后续传输必须保持先持久接管
再 ACK、内部/依赖失败延迟重试的契约，不能以分类修复关闭完整 D0-06/09/10。

仍开放：可信 Execution 完成事实/认证/Outbox、Reply transport/ACL/consumer、完整 Final
生产链路验收、CGR-37 累计身份容量、[有效发送截止](delivery-final-v1.md#31-业务-deadline-与有效发送截止剩余设计门禁)、
保留/GC、外部 IM/真实 Worker E2E。当前 expiry 使用既有 Intent.deadline，没有暗中实现
业务截止与渠道窗口的合成策略。

## 9. 验证入口与验收边界

本切片测试按 Interface 验证，而不是仅检查目录或编译：

- [Runner](../../../services/channel-gateway/internal/delivery/application/runner_test.go) / [Maintainer](../../../services/channel-gateway/internal/delivery/application/maintainer_test.go)：有界分页/工作池、失败后继续、取消/Quiesce/Drain 与生命周期独占。
- [LocalOwner](../../../services/channel-gateway/internal/connection/application/local_owner_test.go)：失租、期限、轮换、配置健康、drain、返回拷贝与并发读取。
- [真实 PG Runtime](../../../services/channel-gateway/internal/delivery/adapter/outbound/postgres/runtime_integration_test.go)：无人 owner、终态保护、锁后时钟、claim/expiry 竞争、并发 CAS、超时与 C 排序。
- [真实升级](../../../services/channel-gateway/migrations/runtime_upgrade_integration_test.go)：0006→0007 并发幂等、旧事实保留、故障 DDL 与 ledger 一起回滚。
- [EXPLAIN](../../../services/channel-gateway/internal/delivery/adapter/outbound/postgres/runtime_plan_integration_test.go)：10,000 行历史及稀疏/稠密 keyset 的计划回归；不作为任意生产规模的容量保证。
- [空账户 fixture App](../../../services/channel-gateway/internal/bootstrap/delivery_runtime_integration_test.go)：真实 PG/NATS 加独立维护；自动 Runner 的 Telegram HTTP/企微 WS 组合另在 Bootstrap 纵切中验证，不将 fixture 结果称为真实 Final E2E。

**2026-09-05 Runtime 历史切片验收已通过**：该切片实际工作树、全仓/race/真实 PG/NATS/本地 HTTP/WS、
七迁移镜像/Compose 与源码副本回退结果见[实施状态 §11.4](implementation-status.md#114-最终联合验收)。
2026-09-06 Control 装配与真实 Telegram 入站分别见实施状态 §13、§14；历史镜像数字不代替
新增接线验证，真实入站不表示真实 Worker 或完整生产 Final 发送已完成。
