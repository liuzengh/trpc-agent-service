# Channel Gateway 设计复审记录

- **复审日期**：2026-09-05
- **最新阅读入口**：[独立 Module 说明](module-introduction.md)；最新 Control 接入/真实收信状态对齐在第 20 节，上一轮文档复审在第 19 节。Runtime 历史实现与验收见第 18 节/实施状态 §11；第 1–18 节正文与实施状态全文保持原文，本次不重跑历史运行验收。
- **复审范围**：Gateway 总览、四 Module 说明、公开企微库、既有行为调研、术语、部署设计及架构约束/索引；实现状态按本工作树核对
- **初次复审基线**：`codex/channel-gateway@9a709d44`
- **实现前主线对齐**：2026-09-05 工作树已更新至 `codex/channel-gateway@bf107766`，Profile 事实按该基线复核
- **历史同步对照**：本机远端跟踪引用 `origin/main@bf107766`；不等同于当前本地 `main`
- **本次只读分支核验**：worktree HEAD=`bf107766`，本地 main=`ab315e4`；双向独有提交数 10/231，已分叉，本轮未合并或移动引用
- **初次复审影响**：初次复审只修订设计；后续实现状态单独记录，本文保留当时发现与修正依据

> 第 1–8 节保留文档设计复审的时间点与 CGR 修订记录，不作为新增运行实现的完成清单。
> 2026-09-05 后续第一切片已有代码；当前代码选择、尚未实现部分与验证状态统一见
> [实施状态](implementation-status.md)。第一切片不等于完整 Gateway 或生产验收完成。

## 1. 结论

纯 Go Gateway、Telegram SDK 直接导入、公开 `platform/im/wecom` 进程内使用、Gateway 自有
持久 Admission/Delivery，以及不部署独立企微 Connector 的主方向保持成立。

复审发现的问题主要集中在：Module Interface 过浅、把所有 Bot 泛化为有状态 owner、同一
外部事件的单路由与多 Binding 描述冲突、Delivery 确定性与重试策略混写、Helm 阶段过早，
以及初次复审时工作树落后远端跟踪引用导致 Profile 实现状态过期。当前本地 main 的新分叉另见第 7 节。

## 2. 已在本轮修正

| ID | 原问题 | 修正结果 |
| --- | --- | --- |
| CGR-01 | 四者被称为“子领域”但没有解释是否拆服务 | 统一为单一 Gateway Workload 内的四个业务 Module，并新增完整边界文档 |
| CGR-02 | 顶层 Interface 暴露 `Acquire/Renew/ReleaseConnection` | 改为 `ConnectionSupervisor.Run(ctx)`；租约流程成为 Module 内部 seam |
| CGR-03 | `connection` 泛称 Bot owner，容易把 Telegram 纳入独占 lease | 明确只服务 stateful provider，V1 即企微；Telegram webhook/HTTP 绕过 Connection |
| CGR-04 | `VerifiedInbound` 预带 `binding_id` | 改为 `AuthenticatedInbound`，只含可信 account 与外部事件/会话；首次新事件才解析 Binding |
| CGR-05 | 同一外部事件又允许以 `inbox_event_id + binding_id` 多次接纳 | V1 明确单 EventKey 对应单一首次 Decision/Receipt；删除隐式多 Binding fan-out |
| CGR-06 | Delivery 结果混用 `RETRYABLE/PERMANENT/UNKNOWN` | 先记录 `NOT_SENT/ACCEPTED/REJECTED/UNKNOWN`，再由 RetryPolicy 决策 |
| CGR-07 | Helm 被放入 Gateway 扩展阶段 | 移到全部生产 Workload 完成后的 `FINAL-INTEGRATION` |
| CGR-08 | 全局 Migration 文案暗示 Control 迁移整个平台表 | 收窄为每个 Workload 只迁移自己拥有的表；Gateway 精确表集合仍为 D0 |
| CGR-09 | Epoch 描述可能被理解为撤销远端副作用 | 明确只 fence 本地 claim/commit 与新调用；已写出 Provider 的调用仍可能成功 |
| CGR-10 | `Client.Close` 与单次 Reply 结果混在一起 | 明确 Close 只报告 drain/timeout；每个被中断 Reply 单独结束为 typed NOT_SENT/UNKNOWN |
| CGR-11 | Delivery 把 owner epoch 泛化到 Telegram | 所有 Provider 使用 attempt CAS；只有企微等 stateful provider 额外核验 owner epoch |
| CGR-12 | Telegram 出站缺少凭据/Client seam | Delivery Adapter 定义 purpose-scoped `TelegramSenderProvider`；解析/缓存/轮换仍由 D0-01 冻结，不进入 Connection lease |
| CGR-13 | NATS 短时故障下 readiness 与 degraded 表述不一致 | 区分启动时 topology 已校验/应用与运行期 connectivity；阈值内保持 ready + degraded，越阈值停止准入 |
| CGR-14 | 两份文档索引仍把 Profile 主线实现写成未实现 | 初次复审区分快照与主线；实现前已对齐 `bf107766` 并同步根/服务 README、约束、索引及 Deployment 文档，保留真实运行接线缺口 |

## 3. 仍需冻结的设计问题

| ID | 问题 | 当前建议 | 阻断范围 |
| --- | --- | --- | --- |
| D0-01 | ChannelAccount 凭据 Owner 与解析协议 | Control 保证物理 Bot 唯一有效账户映射并加密保存凭据；Delivery 通过 purpose-scoped `TelegramSenderProvider`、Connection 通过 `WeComConnectionCredentialResolver` 按 generation 取得内存能力；实施前冻结认证/缓存/轮换 | Telegram 出站、企微建连、凭据轮换 |
| D0-02 | 企微 Client ready/connected/authenticated 状态输出 | 为公开库增加 typed StateObserver 或有界状态流，定义顺序、丢弃与慢观察者策略 | Connection 状态、readiness、运维诊断 |
| D0-03 | interaction/Stop 命令协议 | Admission 保存 interaction Receipt；Provider 即时反馈与跨 Worker 业务命令分开 | callback、取消与幂等 |
| D0-04 | Gateway 表与迁移所有权 | 冻结表集合、独立 DSN/schema/role、ledger、同事务只读 guard 与锁顺序；一个迁移执行路径 | 持久纵切、部署 |
| D0-05 | 路由投影事件与初始化 | 自包含事件 + 完整性水位；空集合初始化、tombstone/重建、账户级 RouteGeneration 与持续 apply lag 门禁需要冻结 | Routing、Admission 一致性 |
| D0-06 | NATS topology 与权限部署 | 分别冻结 Stream/Retention/Durable Consumer/reconcile 与 Server auth 配置加载；包含隔离/重驱动、runtime 最小权限和拒绝测试 | Relay、恢复、部署 |
| D0-07 | 会话身份 | 定义稳定 ExternalConversationKey；Session generation 的 Owner 不落到 Gateway 猜测 | Worker Session 顺序、Binding 切换 |
| D0-08 | 积压与租约阈值 | 冻结 lease/renew、来源观察年龄与持续增量应用滞后、Outbox saturation/deadline；新接纳事务原子预算，测试定阈值 | readiness、故障切换 |
| D0-09 | ReplyIntent 授权与顺序 | Delivery 校验不可变 Admission 回复快照；Worker fence 失效 Attempt；冻结 intent 幂等、generation/sequence、Final 屏障、CLAIMED/CALLING 恢复和分段恢复 | Worker/Final/Progress |
| D0-10 | 幂等与保留/恢复 | SourceDigest、最小去重墓碑、正文清理、Stream/Outbox 离线窗口与超窗恢复；稳定 account_id 不复用 | 持久接纳、运行交接、审计 |

## 4. 主线同步后处理

初次复审时 Gateway 工作树落后当时的远端跟踪引用 5 个提交；此前已更新到 `origin/main@bf107766`。
以下实现事实只描述这个重构工作树，不描述当前本地 main，也不是本次从远端 fetch 的结果。
Runtime Profile 直接录入、私有加密存储、COW/live/CAS、11 个管理操作、`CheckUsable`、
`ResolveForAttempt` 和可选 runtime HTTP Adapter 已在本工作树存在。真实 Deployment
Checker Adapter、Run/Attempt 授权拥有方、可信 Workload 认证、默认路由与 Worker 接线仍待完成。

主线对齐逐段保留 Gateway/Deployment 深化设计，并以 Profile 主线文档及代码作为事实来源；
根 README、架构约束、两个索引、Services/Control API README 与 Deployment 专属文档已移除
“当前 ref-only”“直接录入未实现”和“本工作树仍落后主线”等过期现状描述。Profile 专属文档、
Schema、加密实现与 Fixture 保持主线版本，没有复制平行规格或把规划路由加入已实现 API。

另一个迁移注意点：`origin/main` 直接扩充开发期 Control `0001_baseline.sql`，而迁移器按文件
名判定已执行。执行过旧版 `0001` 的开发数据库不会因重启自动获得新增凭据表；开发期按当前
无兼容规则重建数据，确需保留数据时另行设计增量 Migration。

## 5. 证据可移植性

行为调研中的本机 `artifacts/` 绝对路径当前可在本工作树打开，但该目录被 `.gitignore`
排除，协作者或 GitHub 文档不会获得这些文件。本轮保留这些路径作为 local-only evidence
index，并取消“文档附有证据”的可移植性暗示。进入评审/提交前，应二选一：

1. 把最小、脱敏、允许共享的证据迁入 tracked `docs/evidence/` 并使用相对链接；或
2. 明确该证据只在本地审计包中提供，不把链接当作仓库附件。

## 6. 实施门禁

进入首个 Gateway 纵切前至少完成：

1. 文档已按重构基线 `bf107766` 对齐，但这不是“当前本地 main 已同步”。实施前单独确认
   分叉后的开发基线和整合方式，保留本工作树已有设计；本次不执行 Git 合并；
2. 按切片接受 D0：Telegram 仅入站需 D0-04/05/06/08/10 与可信账户配置；涉及凭据解析/发送需 D0-01，交互需 D0-03，Worker/Final 需 D0-07/09；
3. 固定事件 Schema/Fixture，再生成 `gen/` 类型；
4. 先交付真实 `routing → Telegram admission → PG Outbox → NATS` 纵切，不创建空服务目录；
5. 企微 P0 同时要求 D0-02、公开库协议/故障/race 测试和真实账号验收；
6. Helm 保持 FINAL-INTEGRATION，直到全部生产 Workload 的镜像、端口、Probe、权限与 Secret 契约稳定。

## 7. 本次再次复审：仍有问题，不作“设计全部通过”的结论

本次把已存在的 Module 说明补成可独立阅读的介绍：增加一条消息的职责追踪、目录/人员
分工、同事务技术 seam 和故障归属矩阵。原四 Module 划分继续保留，没有新增微服务。
下面的“已补设计”只表示文档明确了建议和验收条件，不表示 D0 已接受或运行实现已通过。

| ID / 严重度 | 原文定位与具体缺口 | 影响 | 本次处置与剩余门禁 |
| --- | --- | --- | --- |
| CGR-15 / 高 | Module §5.4/§7.3：Commit 校验 generation/epoch，但普通 Resolve/CurrentSender 不持有同一事务锁 | 读后发生切换，可能按过期资格提交 | §9.2 补使用方定义的同事务只读 guard；D0-04 仍需锁顺序与竞争测试 |
| CGR-16 / 高 | Module §7.1：ReplyIntent 与 Admission 固定 ReplyContext 缺少明确读 Port/一致性校验 | 错误目标或失效 Attempt 可能产生外部副作用 | §7.4 补 AdmissionReplyReader、执行授权与顺序规则；D0-09 待冻结 |
| CGR-17 / 高 | Module §4.2/§10：只写 initialized，未说明何时完成全量同步 | 收到一条事件就 ready，或空系统永远不 ready | §4.4 补完整性水位、空快照和重建；D0-05 待冻结 |
| CGR-18 / 高 | Admission 永久 EventKey 去重只比较身份，未约束内容与生命周期 | 同键异文被误认成功；删记录后再次执行；正文长期保留 | §5.5 补版本化摘要与分层保留；D0-10 待冻结 |
| CGR-19 / 高 | 部署 §7.4：越阈值停止准入仅体现为 readiness | 已连入流量和并发副本仍可越过容量限制 | Module §5.6/§10、部署 §7.4 增加 Commit 原子预算和重复回执例外；D0-08 待测试 |
| CGR-20 / 高 | Relay/Consumer 只描述 ACK 与重试，未闭合保留超窗和永久错误 | 已发布事件在业务接手前过期，或坏事件无限重投 | Module §14.1 补恢复窗口、隔离与受控重驱动；D0-06/10 待配置与故障验证 |
| CGR-21 / 中 | 设计复审 §4 将 origin/main 同步记录容易读成当前 main 对齐 | 后续可能误在不同架构上直接移植或合并 | 明确 HEAD/origin/main 与本地 main 分叉，不合并；实施前单独决策基线 |
| CGR-22 / 中 | Module §10 正常 shutdown 先停止续租，与部署关闭语义不一致 | 有界 drain 尚未完成就接管，增加 UNKNOWN 与连接重叠 | 两份文档统一正常 drain 期间续租，失租/超时快速取消；cleanup 独立 deadline context |

### 7.1 文档复审时只读核验的代码与 Git 事实（历史快照）

- 本工作树：`/Users/jfs/Projects/trpc-agent-service-channel-gateway`，分支 `codex/channel-gateway`。
- HEAD 与本机 `origin/main`：`bf107766be72cd7fcaa9e878428b97a6aa05260b`。
- 本地 main：`ab315e4232ee6240eff94116368cc4e530fe6695`。
- `git rev-list --left-right --count HEAD...main`：`10 231`；
  `git merge-base --is-ancestor HEAD main` 返回 1，不能直接快进。
- 本工作树的 `services/channel-gateway` 与 `platform/im/wecom` 尚不存在；当前 Compose
  仍只有 Control 与 PostgreSQL。当时的临时实验不算工作树已实现；后续第一切片状态见 §9。
- 本次仅修改文档；未改变 HEAD、索引、go.mod、运行代码、Compose 或 Helm。

### 7.2 实现之前要先完成什么

1. 独立确认重构工作树与本地 main 的后续关系，不能把这次文档修订当成同步授权或同步完成。
2. 先冻结最小入站纵切的数据库/路由快照/事件/预算/保留协议，再做真实 PG/NATS 纵向测试。
3. 再冻结 ReplyIntent 的执行授权、回复目标、顺序及 Final 分段恢复，接真实 Worker。
4. 企微接入同时验证 Client 生命周期、owner 竞争和真实账号；源码推论与模拟协议测试单独标记。
5. 全部生产 Workload 完成后才设计 Helm；Compose 随各真实 Workload 交付演进。

### 7.3 本轮验证覆盖

本轮执行文档基线检查、修订后链接/结构/设计断言检查、原始 SHA-256 保留和独立副本回滚。
这些验证不替代运行协议测试；上表“高”表示设计缺口的影响级别，不表示已发现线上故障。
本轮只复核与新增条款相关的 NATS 官方确认/保留语义，沿用此前标记日期的 SDK/企微研究；
没有重跑真实 Telegram 或企微账号实验，也没有把旧实验重新标成最新端到端验证。

## 8. 介绍文档补充后的复审

本节在第 7 节已补规则基础上继续核对接口示例与部署可执行路径；不把尚未冻结的 D0
重新报告为新发现，也不将临时目录中的实验代码算作本工作树已实现。

| ID / 严重度 | 本次发现 | 修正后的设计 | 尚需实施验证 |
| --- | --- | --- | --- |
| CGR-23 / 高 | Module §7.3 把 claim+CALLING 写成一次事务，但示例分开 ClaimDue/MarkCalling，且只有 stale CALLING 恢复 | 明确 PENDING→CLAIMED→CALLING；MarkCalling 是唯一发送门；补 RecoverExpiredClaims，过期 CLAIMED 重调度、CALLING 保守 UNKNOWN | 两个崩溃窗口、旧 claimant 迟到、恢复与 Finish 竞争；D0-04/08/09 |
| CGR-24 / 高 | 部署 §2.2/7.1/7.2 将 streams 与 permissions 一律描述为 reconciliation 应用 | 分成 JetStream topology reconcile 与 Server auth 配置渲染/加载，分离 runtime、管理和部署权限 | 真实 broker 合法操作、错凭据、越权发布与拓扑修改拒绝；D0-06 |
| CGR-25 / 高 | account_id 不复用只约束单向关系，未排除同一物理 Bot 被两个账户注册 | Control 保证物理身份到有效账户唯一；Gateway 对冲突投影阻止新接纳/建连；轮换不改身份 | 并发注册/激活、跨租户冲突、迁移/重注册与重放；D0-01/05/10 |

同时完成两处说明对齐：

- Module §0.1 增加“以后一个需求应该交给谁”，直接对应版本切换、重复消息、企微接管、
  Final 超时与 Agent 执行配置，避免把 Module 当四层或四个部署单元。
- §6 的“主线对齐已完成”改为明确的 `bf107766` 文档基线；本地 main 分叉仍是独立整合问题。

**复审结论**：保留四 Module、公开 Go 企微库、Telegram 直接 import SDK 和单 Gateway
Workload 的方向。介绍文档已具备独立阅读入口；上述三项描述已修正，D0-01～D0-10 仍按切片
冻结，不标记整份设计“实现就绪”。Helm 继续仅属于全部生产 Workload 完成后的 FINAL-INTEGRATION。

上述文档复审的验证为文档链接/锚点、结构、设计断言、受保护文件哈希与副本回滚检查；当时运行代码、
Compose、生产依赖与 Git 引用保持不变，未运行新的 Telegram/企微账号实验。


## 9. 后续第一切片与设计记录的关系

当前第一切片提供 Routing、Telegram Admission、PG Outbox、NATS 传输、版本化 Schema/DTO
以及启动/部署代码；精确能力、当前选择和本轮验证状态见[实施状态](implementation-status.md)。D0 只按本切片细化，不整体关闭。

- **CGR-23** 的 Delivery CLAIMED/CALLING 状态与恢复仍为设计；Admission Outbox 的发布
  claim 是另一种传输账本，不冒充 Delivery 实现。
- **CGR-24** 的两条配置路径已有实现：`reconcile` 管理 Stream/Consumer，
  `nats-config` 生成 Server auth 配置并由部署加载；真实 ACL 允许/拒绝测试结果按实施状态和审计记录单独核验。
- **CGR-25** 的 Control 物理 Bot 唯一有效账户管理及 Gateway 冲突投影流程尚未实现。
  本地 accounts.json 的 account_id 唯一只检查配置重复，不证明物理 Bot 唯一。
- Connection、Delivery、公开企微库、Control route publisher、真实 Worker 与完整
  ReplyIntent 链路尚待交付。Helm 仍只属于全部生产 Workload 完成后的 FINAL-INTEGRATION。


## 10. 本次说明文档与设计代码对照复审

### 10.1 本次交付与证据范围

新增[从一条消息理解四个 Module](module-introduction.md)，作为独立入门说明；原
[module-boundaries.md](module-boundaries.md) 继续保留详细 Interface、事务和故障规则。
两个仓库文档索引和 Gateway 总览均提供入口，不再让入门介绍与 700 多行详细规范混在一起。

本次同时只读核对实际工作树的 Routing/Admission、bootstrap、事件契约与 Compose，
并读取第一切片既有审计记录。代码、依赖、部署文件、Git HEAD/索引和 main 引用均保持原样。
本次验证为文档结构、链接、术语一致性、源码保护哈希与独立副本回滚；没有重跑真实
PostgreSQL/NATS/Telegram/企微账号实验，静态推导也不标为已复现运行事故。

### 10.2 两项新增实质问题

| ID / 影响级别 | 问题与具体场景 | 本次文档处置 | 剩余交付状态 |
| --- | --- | --- | --- |
| CGR-26 / 高 | 原 Module §4.1、§4.2 与总览把账户投影代次称为 BindingGeneration；账户从 A 的代次 20 换到新 B 的实体版本 1，会被现有消费者当旧事件忽略，同代异文则冲突 | 统一为账户级 RouteGeneration；wire/DTO 仍名 generation；改绑、停用、重启用不重置，Binding 实体 CAS 另列 | 与现有消费代码对齐；Control publisher 的账户序列分配与并发发布验收仍待交付，不改代码/Schema 字段 |
| CGR-27 / 高 | 原文把源 heartbeat 与同步新鲜度合称；初始化后 StreamInfo 可成功而消费持续失败，新停用事件长期未应用时仍可能接纳旧路由 | 拆开启动完整性、source-observation age 与 apply lag；明确 5 分钟只是观察超时，不是停用传播上限 | 持续增量应用滞后的准入阈值、告警、恢复与故障测试尚未实现；D0-05/08 继续开放；本轮没有以文档修订冒充修复代码 |

**CGR-26 源码依据：** [Routing Domain](../../../services/channel-gateway/internal/routing/domain/route.go)
的 RouteSnapshot 注释与 Compare 按账户代次判断；[投影 Store](../../../services/channel-gateway/internal/routing/adapter/outbound/postgres/store.go)
按 `(provider, account_id)` 查找与替换；[Control event 契约](../../../api/events/control/v1/README.md)
与 [Admission Store](../../../services/channel-gateway/internal/admission/adapter/outbound/postgres/store.go)
固定相同的 generation。所有候选平台输入、术语与总览同步修正，历史 Provider 试验结果不变。

**CGR-27 静态推导：** [Consumer](../../../services/channel-gateway/internal/routing/adapter/inbound/nats/consumer.go)
周期执行 StreamInfo 观察，消费错误单独重试；[Replay Store](../../../services/channel-gateway/internal/routing/adapter/outbound/postgres/replay.go)
的 ObserveSource 更新 highest/last_observed_at，不推进启动 target。当前健康判定比较
contiguous 与启动 target，并检查来源观察年龄；它没有“持续未应用水位”的时间门禁。

例如启动追到 100 后，新停用事件为 101，消费权限失效而 StreamInfo 权限仍有效，可能持续为：

```text
target_sequence=100
contiguous_sequence=100
highest_sequence=101
last_observed_at=持续刷新
```

这不是正常短暂的 Observe→Apply 窗口，也不等于源不可达。当前 Resolve 和接纳事务 guard
仍可能通过；不能据“每 30 秒观察、5 分钟过期”推断停用在 5 分钟内一定生效。
后续门禁需基于已观察但未连续应用的水位差及持续时间，在读取路由和接纳提交两处一致生效；
具体容忍窗口仍待冻结，不因瞬时存在一条积压就未经评审地拒绝全部流量。

该问题与 CGR-17 区分：CGR-17 是启动完整性；CGR-27 是初始化完成后的持续增量应用滞后。

### 10.3 五项现状与说明对齐

| 项目 | 修订内容 |
| --- | --- |
| 探针契约 | 部署 §6 不再把 Gateway 写成 Control 的 /healthz；明确 Control 8080/healthz、Gateway 管理 8091/livez 与 /readyz，public 8090 不承载探针 |
| 历史验收 | 实施状态表移除“本轮运行验收/真实 ACL 验证待完成”的过期项；既有证据继续保留在 §7，并标为历史验收，不声称本次重跑 |
| 装配位置 | Module §3 标明当前 routeBridge 位于 bootstrap/app.go；application_ports.go 仅为将来拆文件候选，不是已存在源码 |
| 路由能力 | 当前 Resolve(provider, account) 只支持账户级单个有效目标；ConversationSelector 是待冻结扩展，群/topic 分流没有被实现，也没有被本次永久排除 |
| 发布镜像 | 部署 §2.1 区分不可变发布要求与当前 :local 默认镜像；CI 固定发布引用及其校验仍待交付，不把本地镜像启动当发布验收 |

### 10.4 本次结论与下一步门禁

四个 Module、单 Gateway Workload、Telegram 直接 import SDK、公开 Go 企微库进程内使用
的方向保持成立；未找到需要另拆 Connector 部署或改回 Node 的依据。Helm 明确保留在
**全部生产 Workload 完成后的 FINAL-INTEGRATION**，本轮没有创建 Chart 或新部署单元。

文档已补独立介绍并修正上述歧义，但设计不标记为“全部通过”或“生产就绪”。下一步至少需要：

1. 在 Routing 后续切片冻结并实现 CGR-27 的持续 apply lag 门禁，以消费失败而来源观察成功、
   持续积压、追平恢复、旧 Receipt 重放进行真实故障测试。
2. Control publisher 按账户单调分配 RouteGeneration，验证改绑/停用/重启用与并发发布；
   ChannelAccount 的物理 Bot 唯一身份、凭据与轮换协议仍属于 D0-01/05/10。
3. Delivery/Worker 冻结 ReplyIntent 授权、顺序、Final 屏障与 UNKNOWN 恢复；Connection/
   公开企微库冻结状态流、owner 生命周期与协议故障测试，并取得真实账号验收。
4. Compose 随各真实 Workload 交付演进；全部完成后再统一设计 Helm，不将 Chart 提前归入 P1。

本次八项文档检查分别覆盖：独立介绍、两项新增语义澄清和上述五项说明对齐。
检查通过只证明这些文本/引用已对齐；CGR-27 的运行行为以及其他未实现能力仍是开放项。


## 11. 后续实现与当前状态：CGR-27 / 公开企微 P0

本节记录第 10 节文档复审之后的实现进展；第 1–10 节及 CGR 原始问题行逐字保留，
其中“尚未实现”“本次仅文档”等描述属于当时快照，不覆盖本节的新增代码事实。
**本切片已完成实际工作树与镜像核验**，精确执行状态集中在[实施状态 §8](implementation-status.md#8-2026-09-05-后续实施cgr-27-与公开企微-p0已应用并验证)。

### 11.1 CGR-27 的实现处理

本轮新增持久 `apply_lag_since` 与 60 秒连续已知积压门禁；来源观察成功、部分应用、
旧观察和重启都不延长同一 episode，完整追到已知最高水位才清除。Ready、Resolve 和
Admission 同事务 guard 共用判定，旧 Receipt 仍重放。0004 迁移升级已有 0001–0003 数据库，
不重写旧迁移；已有积压从升级 PG 时钟起计时，无积压保持 NULL。

真实 61 秒 PG 红绿验证、双副本 HTTP 门禁测试与旧库升级测试已通过。HTTP 测试在
Fetch 持续失败时等待实际后台 Consumer.Run 周期观察识别积压，不手动推进观察；
Gateway/API/gen 联合 race 已通过，SDK 最终全矩阵、实际工作树/镜像已核验。
实现门禁不等于端到端停用 SLA，
生产告警与阈值调优仍待后续；D0-05/08 不因单个缺口已修就整体关闭。

### 11.2 公开企微 P0 与未完成的运行接线

公开 `platform/im/wecom` 已有可导入 P0 源码与本地真实 WebSocket 契约/故障测试；
支持 subscribe/auth、应用层 ping、text/event callback、单次文字 Final、状态与取消/关闭。
固定官方协议核验见[协议说明](wecom-protocol-implementation-notes.md)，精确 API 与测试见
[公开库 README](../../../platform/im/wecom/README.md)。未知/迟到 ACK、replaced 与 UNKNOWN
的规则是有边界的 Go 策略，不宣称服务端提供持久消费 ACK 或 exactly-once。

这不是 Gateway 企微收发完成：当前二进制没有 import/运行公开库，Connection、Delivery、
企微 Adapter、真实 Control publisher / Worker 接线及企微真实账号验收仍未完成。
CGR-23 与 CGR-25 不因 SDK 存在而关闭；CGR-26 的 Control 账户序列发布仍待交付。
全部生产 Workload 完成后才进入 Helm FINAL-INTEGRATION，本轮不新增 Chart 或 Connector 部署。


### 11.3 CGR-28：bundled Compose 的 NATS 密码解析边界

| ID / 影响级别 | 真实发现 | 本轮处理 | 剩余交付 |
| --- | --- | --- | --- |
| CGR-28 / 中 | NATS 2.11.8 对环境变量值按配置语法再解析；某些数字/单位前缀的随机密码会导致 Server 配置失败，纯数字/布尔值也可能被解释为非字符串；Compose 当前仅校验非空 | 独立实测保留失败日志，换用字母前缀的合成测试密码后镜像验收通过；补生成建议和边界说明；server.conf/runtime Auth 未变 | bundled deploy preflight 需验证三角色密码格式并提供明确诊断；通用 Gateway Auth 不应因此限制外部自管 NATS 的合法密码 |

依据：[官方变量规则](https://docs.nats.io/reference/config/#variables)与
[v2.11.8 固定解析源码](https://github.com/nats-io/nats-server/blob/v2.11.8/conf/parse.go#L434-L440)。
环境变量替换不是原样字符串插值；将配置引用写成 `password: "$ENV"` 会成为字面密码，
不是正确修复。给服务端环境值额外嵌入引号而客户端继续复用同一环境变量，还会产生
服务端解引号/客户端保留引号的认证不一致。

这是前一切片已存在的部署配置契约缺口，不是新 Routing/SDK 导致；本轮没有因更换测试
输入就声称预检已实现。详细合成样本 `nats-server -t` 结果在本轮审计日志中，后续归属
bundled Compose/preflight，不增加 Connector 或 Helm 部署单元。


## 12. 四 Module 介绍补充与再次设计复审（文档变更）

### 12.1 本次对象与结论

本次在已有 `module-introduction.md` 上补充独立讲解，不另建重复文档。新增四类事实的
写入责任对照、一次版本切换的分工、运行时调用与编译时依赖两张图，以及企微/Telegram
路径为何不同。详细规则继续集中于 `module-boundaries.md`，术语与已完成代码分别见
CONTEXT 与实施状态，不让入门文档成为第二份完整协议规格。

核验对象是实际 `codex/channel-gateway` 工作树：HEAD 为 `bf107766`，本地 main 为
`ab315e4`，两侧仍有 10/231 个独有提交。实际源码仍只有 Routing/Admission 入站切片、
4 个 Gateway 迁移和公开企微 P0；临时 staging 中的 Connection 代码不计入已落地能力。
本轮只修订 Markdown，没有应用临时实现、合并 main、改动依赖、Compose、镜像或 Helm。

**结论：主方向成立，但设计细节仍有缺口。** 不需要拆出 Node Connector，也不需要
将四个 Module 拆成四个服务；Helm 始终仅在全部生产 Workload 完成后的 FINAL-INTEGRATION。
以下“补充”指目标设计与验收条件，不表示代码缺口已修复或 D0 已整体关闭。

### 12.2 新发现与设计处理

以下定位使用本轮修改前的详细规范节号及实际代码入口，便于与本次 diff 对照。

| ID / 影响级别 | 原设计缺口与最小失败场景 | 本次文档处理 | 实现验收门禁 |
| --- | --- | --- | --- |
| CGR-29 / 高 | Module §5.4/§8.2/§9.2 仅明确 Admission 的 Routing guard、Delivery 的 owner guard。A 收到企微 callback，失租被 B 接管后，A 的旧排队 callback 仍可能通过未变的路由并提交新事件 | §5.4/§9.2 增加新企微接纳的 Connection 同事务 guard；检查实例、epoch、连接配置代次和 PG 时间，持锁至提交；旧 Receipt 豁免，Telegram bypass，fence 不入去重/wire | D0-04/08：失租、配置切换、等锁跨过到期点、ignore/interaction、跨 owner 重放与 Telegram 不受影响 |
| CGR-30 / 中 | 公共库的 handler 错误是 Run 终态，而 Gateway 未定义业务失败到生命周期的转换。若原样传播临时 PG/投影错误，再把终态封禁到配置升级，恢复后仍收不到新消息；反向无限重建 Client 又会清零重连预算 | §6.4 将临时接纳、确定拒绝、认证/协议终态、预算耗尽与 replaced 分级；Adapter 有界重试，Connection 接收明确恢复类别；公开库不依赖业务错误 | D0-02/08：同配置的暂时故障恢复、队列/期限/退避上限、关闭取消，以及认证拒绝不能无限重建 |
| CGR-31 / 高 | Module §6 仅强调终止被替换 Client，缺少跨副本恢复事实。A 被替换后停止，lease 到期，B 按同配置重新拨号；若隔离只放在 Close 成功之后，清理失败又会跳过记录 | §6.5 要求被替换账户配置的持久隔离，按原 grant CAS；Close 失败仍尝试独立有界记录；隔离失败与清理失败分开报告，不用 Release 加速未经隔离的接管 | D0-01/04/08：两副本、进程重启、Close 超时、隔离写失败、旧通知与更新配置竞争；不把 DB 不可写说成隔离成功 |
| CGR-32 / 中 | Module §7.3/§9.2 将当前 owner 的提交权限与 Provider 结果混合。ACK 在失租后明确到达，但 Finish 被拒，恢复只留下 UNKNOWN，丢失原调用的确定证据 | §7.3 区分状态推进与受限 Observation；晚到 ACK 关联原 Attempt/请求，不覆盖新 Attempt、不自动重发；缺少证据才保留 UNKNOWN | D0-09/10：ACK 与失租/恢复/新 Attempt 并发；观察记录幂等和授权；禁止伪造成功与跨 Attempt 覆盖 |
| CGR-33 / 中 | Receipt-first 只处理首次命中与 Commit 内竞争，未覆盖查空后、事务前动态依赖失败。A/B 查空，B 提交，路由停用后 A Resolve 失败，未进入 Commit 的二次查重 | §5.7 与 Deployment §12.3 补期限内一次最终 Receipt 查询；同摘要重放、异摘要冲突、无记录保留原错误；不跳过鉴权，不承诺未提交结果可见 | D0-04/10：确定编排上述竞争、PG 失败/期限用尽、异摘要；这是返回契约缺口，不是双 Run 缺陷 |

CGR-30 的事实依据是实际公开库 [Client dispatch/retryable](../../../platform/im/wecom/client.go)
与[生命周期说明](../../../platform/im/wecom/README.md)：handler 错误结束活动 Run，内置
重连仅覆盖特定传输错误。本次不改变该公开库策略；需要补的是 Gateway 组合契约。
CGR-33 的当前 Gateway [Application](../../../services/channel-gateway/internal/admission/application/service.go)
在 Resolve 失败后直接返回 unavailable，最终 Receipt 复查尚未实现。其余 Connection/Delivery
场景是目标设计压力测试，不能当作已部署系统事故或新运行测试结果。

### 12.3 三处文档一致性修正

1. **装配依赖规则**：总览原本允许 Adapter/技术装配使用 SDK，详细规范却写 Adapter-only。
   现统一允许显式的纯 composition root 构造/注入；Domain/Application 不接收 SDK 类型，
   协议转换、业务重试和租约状态机仍属于对应 Module。Bootstrap 不是业务流程总控。
2. **Deployment 现状**：修正 §2 中“整个工作树只有一条迁移、事件只有 README、services
   只有 Control”的过期陈述。区分已存在的 Gateway 4 个迁移/事件/Outbox，与尚未完成的
   Control Deployment 发布/Relay、Worker 执行。这是现状对齐，不证明部署发布已实现。
3. **状态与阅读入口**：实施状态 §6 不再把已核验的 SDK 本地全矩阵列作待完成；本报告
   顶部指向本节。历史测试/评审段落原文保留，不重新解释为本次重跑。

### 12.4 当前 D0 怎样使用

不要把早期 D0 表中“待冻结”的历史表述理解为所有代码仍是空白，也不要因某个切片通过
就关闭整项。下一步按以下剩余交付推进：

- **Routing / Admission**：第一切片已有表、版本化事件和真实持久入站；Control 的账户管理/
  路由发布、全局身份唯一、超窗恢复与 CGR-33 仍待交付。
- **Connection**：公开协议库已有本地验证；账户配置/凭据 Owner、跨副本 lease、接纳 owner
  guard、分级恢复与 replacement 隔离仍需在真实 Gateway 中实现和验证。
- **Delivery / Worker**：原 Admission 回复目标、执行授权、ReplyIntent、CLAIMED/CALLING、
  Final 屏障及 CGR-32 结果证据必须一起冻结，不能靠测试订阅者代替 Worker。
- **部署**：Compose 随实际 Workload 演进；CGR-28 的 bundled NATS 密码预检仍开放。
  Helm 等全部生产 Workload 的镜像、端口、Probe、权限和 Secret 契约稳定后再统一设计。

### 12.5 本次验证的含义

本轮验证只覆盖 Markdown 链接/锚点、章节结构、上述设计规则的一致性、修改文件范围以及
工作树/旧目录保护。文档断言通过不代表故障测试已实现；没有新增真实 Bot、WS、PG/NATS、
Worker 或镜像验收声明。本次目标是把分工讲清楚并补全设计契约，不扩大为代码实现交付。


## 13. Connection 与企微入站实施：已应用并验收

本节承接第 12 节的设计，不覆盖其历史“尚未实现”结论。当前新增实现已应用到实际工作树，
完整联合测试与新镜像验收通过；精确代码与默认策略见
[实施状态 §9](implementation-status.md#9-connection-与企微持久入站已应用并验收)。

| 复审项 | 本轮实现处理 | 剩余边界 |
| --- | --- | --- |
| CGR-29 | Admission 的企微新事件在同一 PG 事务调用 owner guard，检查 instance/epoch/配置 revision/锁后 PG 时间；旧 Receipt 先返回，Telegram bypass | 已通过本轮联合验收；不把本地 fence 当远端取消或 Control 注册管理 |
| CGR-30 | Adapter 6 次/2s 有界重试；临时耗尽显式映射 Retryable；Supervisor 1/2/4s 最多 3 次快速重建，之后 60s 半开，Ready 不清预算 | 本进程账户 revision 预算；跨副本/重启全局限制与真实 IM 恢复仍未交付 |
| CGR-31 | PG blocked_revision 持久隔离；旧 grant CAS，只有更高配置恢复；Close 失败仍尝试隔离；Status 独立记录隔离成功/失败，失败不 Release 加速接管 | PG 不可写时残余 lease 到期风险；本轮故障套件已通过；真实账号仍待验收 |
| CGR-33 | 准备失败后在剩余期限内一次新快照查 Receipt，同摘要重放，异摘要冲突；查询失败或无记录不伪造成功 | 当前只落实 Gateway；Control Deployment 仍为设计，未实现其发布用例 |
| CGR-32 | 保留晚到 Observation 与状态推进分离的设计 | Delivery、Attempt 账本、Sender lookup、Final/UNKNOWN 恢复仍未实现 |
| CGR-28 | 保留 NATS bundled 密码格式建议 | 自动预检与明确诊断仍未实现 |

Connection Store 与 Admission owner guard 的真实 PG 分项，以及 0004→0005 并发升级/
旧事实保留/注入 ledger 故障回滚测试已有通过证据。历史分项成功本身不替代联合验收；
本轮完整联合套件、实际工作树与新镜像验收已另外完成，统一记录于实施状态 §9.5。
历史公开 SDK 测试同样不替代此次新增 BodyDigest/组合生命周期的当前验收。

本轮继续单 Gateway Go 二进制、单镜像，SDK 直接 import，不增加 Node Connector、RPC
或额外 Workload。账户文件只保存五字段配置与环境引用；真实 Control 账户/凭据 Owner、
Worker、Delivery、完整机器人收发尚未交付。全部生产 Workload 完成后才进入 Helm
FINAL-INTEGRATION。


本轮最终证据见实施状态 §9.5：实际工作树 19 个 Gateway/API/SDK race 测试包、208 个
顶层测试通过，全量 Go 42 个测试包及 vet 通过；双 Gateway 本地真实 WS/PG/NATS 重复
验证、5 迁移实际镜像、原子文件替换与正常退出均通过。新增 managed Client 的终态分类
原子发布与错误候选隔离已有确定性 RED→GREEN，不以一次绿色重试掩盖原失败。
完整目标仍包含上表的 Delivery/Worker/Control 与真实 IM E2E，未整体关闭 D0。

## 14. 介绍文档与原回复关联复审

### 14.1 本次交付与判断

继续维护独立的[四 Module 入门说明](module-introduction.md)，不另建同题副本。补齐两种
Attempt、成功层级、企微原连接与当前资格的区别；保留四类事实、消息流程、开发分工、
运行时/编译时依赖，以及公开库和部署的解释。四个 Module、单 Go Gateway、Telegram
SDK 直接导入与公开企微库进程内使用的方向保持成立。

本次核验实际 `codex/channel-gateway` 工作树：HEAD `bf107766`、本地 main `ab315e4`，
源码已有 Routing/Admission/Connection、公开企微库及 5 个 Gateway 迁移；Delivery 目录、
生产 ReplyIntent 消费链路与真实 Worker 仍未交付。临时实现副本不计作本工作树功能。
本次只修改 Markdown，不应用实验代码、不合并 main，不新增部署资产或真实机器人实验。

### 14.2 新的实质缺口：CGR-34

| ID / 影响级别 | 原问题与最小场景 | 本次设计处置 | 实现验收门禁 |
| --- | --- | --- | --- |
| CGR-34 / 高 | Module §7.3 只用 account/epoch 找当前 Sender，§8.2 未保存原 socket 来源；A 在 generation 7 接纳，重连到 8 后旧 Final 到达，即使 owner 没变也会拿到错误连接 | 新增 §7.5：首次 Admission 固定本地 ReplyOrigin，预留原连接并另查当前资格；原 req_id 不重绑，重复事件不改来源，历史 NULL 不猜测；信息不进 Worker wire 或 EventKey | 同 owner 重连、跨 owner/配置切换、旧 NULL、重复不改来源、A2 前后取消、一次性发送与 drain；原来源表/迁移和 Sender Port 尚待实现 |

**源码依据**：公开库 [Event / ReplyRequest](../../../platform/im/wecom/types.go) 要求原
RequestID 与 Generation；实际 [入站 Adapter](../../../services/channel-gateway/internal/admission/adapter/inbound/wecomadapter/handler.go)
保存的 ReplyContext 只有原 req_id 等回复字段，没有 socket generation；[Admission Domain](../../../services/channel-gateway/internal/admission/domain/admission.go)
的 ConnectionFence 是不序列化的提交授权上下文。现有 owner guard 证明当前接纳资格，
不能替代未来投递所需的持久原连接关联。

上述为静态设计/源码对照得出的缺口；本次没有把它标成已复现的线上误投，也没有宣称
通过文档修订完成代码修复。旧 SDK 文档已提示跨连接延续待验证，本次补的是 Gateway
如何保存、交接和拒绝错误来源的完整契约，而非新增远端协议保证。

### 14.3 既有 D0-09 的精化，而非重复发现

原 §7.4 已要求执行授权与可信终态证明，不重新编号成“完全缺少授权”。本次补充其
易被实现错的时间语义：**Final 应验证不可变的已提交完成事实，而不是当前活跃 lease**。
正常结束后释放 lease、Final 稍后才被消费时，仍应接受合法完成；未被 Execution 接受的
旧 Attempt 不能借新事件取得发送权。固定原 Admission/Manifest/内容身份，同摘要 Receipt
重放不依赖当前路由或当前验证服务；新意图在证明缺失/不匹配时拒绝。

验收还须覆盖完成与发布竞争、过期旧 Attempt、证明服务暂不可用、重复接纳、跨租户 /
Run 绑定、Final 唯一性与新 generation 的竞争。这些条件写入 Module §7.4；最终 wire、
证明接口、保留期与真实 Execution Owner 仍是 D0-09，未以草案取代跨 Workload 协议冻结。

### 14.4 现状与术语一致性

- 入门目录不再把已有 Connection 标成未来；Sender lookup 和 Delivery 仍明确待实现。
- CONTEXT 增加 ExecutionAttempt / DeliveryAttempt，发送恢复不重跑 Agent 或创建 Run。
- 根 README、Services README、两个文档索引统一为“企微库已由 Connection/入站直接装配”，
  删除“尚未 import”“Connection 未实现”等过期现状；这些更新只依据当前源码存在性。
- 部署说明不再把已有 Connection 恢复/隔离统称待实现；Delivery、跨重启全局预算和真实
  账户验收仍开放，沿用既有验收引用而不声称本次重跑。
- 企微建连的可信 ChannelAccount 配置与 Routing 的 Agent 路由投影分开描述；当前来源是
  可选账户文件，不声称真实 Control 账户 publisher 已交付。
- Helm 保持 **全部生产 Workload 完成后的 FINAL-INTEGRATION**；Compose 随实际 Workload
  演进，不把 Helm Chart 放到 Gateway P1/P2，也不新增 Connector Workload。

### 14.5 剩余实施门禁与本次验证含义

优先闭合原 ReplyOrigin + Sender 资格 + Delivery 账本，以及真实 Execution 的 Final 授权。
此外仍开放：Control 账户/路由发布与凭据 Owner、交互命令/反馈、NATS 保留窗口与受控恢复、
CGR-28 bundled 密码预检、生产观测/配额、真实 Telegram/企微 E2E。沿用既有 D0 按切片
关闭，不因当前本地验证或一份说明文档而标记完整 Gateway“生产就绪”。

本次验证只覆盖 Markdown 结构/相对链接/锚点、设计一致性、修改范围、源码与 Git 引用
保护，以及独立副本文档回退。原运行代码、依赖、Compose 和历史评审 §1–13 正文保持不变；
没有新的 Go、PG/NATS、SDK、Worker、镜像或真实 IM 运行验收声明。

## 15. Delivery Final V1 实现与独立复核

本节承接 §14，不改写历史问题行。当前 Final/text 实现选择见[Final V1](delivery-final-v1.md)，
实际应用与最终联合验收见[实施状态 §10](implementation-status.md#10-delivery-final-v1源码纵切与剩余运行接线)。

| 项目 | 本切片代码处理 | 仍未交付 |
| --- | --- | --- |
| CGR-23 | 独立 PG Ledger、A1/A2、过期 CLAIMED 回收与 stale CALLING→UNKNOWN | 默认生产 Dispatcher/NATS Consumer |
| CGR-32 | 随机 capability 绑定原请求；受限 Observation；显式一致结果恢复，保留原 UNKNOWN 时间 | 完整审计入口与保留/GC |
| CGR-34 | Admission 单独保存原 ReplyOrigin；重复不覆盖、旧 NULL 不猜；Connection ReserveFinal 匹配原 lease/config/socket 并参与 drain | 真实账号/跨 Workload Final E2E |
| D0-09 Final | 严格 wire、原目标读模型、逐字段 CommittedFinalVerifier、Run 唯一 Final 与完整分段账本 | 真实不可变完成事实拥有方/认证接口、Worker Final Outbox/生产传输 |

独立复核未发现新的 Final 唯一性、原目标、可信证明、A2/Observation 不变量缺陷；仍按
测试覆盖范围解释证据，不称整个 Gateway 已完成。另有两项异常 Port 返回先复现后修正：

- Reserve 返回非 nil handle + error 仍需释放；nil,nil 必须消耗准备预算，避免无限重新领取。
- Connection→Delivery bridge 保留 ProviderCode，矛盾的 ACCEPTED 或携回执的 NOT_SENT
  归 UNKNOWN/permanent，避免丢掉证据后误记成功或允许普通重试。

当前生成契约、PG Ledger、HTTP/WS 纵切以显式 committed Final fixture 连接，不注入默认
许可到 App.New，也不把本地测试订阅者称作真实 Worker。生产凭据配置、有界 Telegram
transport、ReplyIntent NATS ACL/Consumer、调度生命周期及完整 E2E 仍按后续目标继续。

一个 Go Gateway Workload/镜像不变。Helm 始终在全部生产 Workload 完成后的 FINAL-INTEGRATION。


## 16. 介绍归档与设计实现对照复审

### 16.1 本次范围与结论

用户请求是把四个子领域介绍落成文档，并再次审查原设计。本次直接维护已有
[module-introduction.md](module-introduction.md)，补充业务 Module、内部层次和 Workload
三个维度，以及按完整用例分工的交付/测试矩阵；没有新增同题副本或执行实现切片。

只读核对的工作树为 `codex/channel-gateway@bf107766`，本地 `main@ab315e4`；本轮不合并、
不提交，也不修改运行代码、依赖、迁移或 Compose。四 Module 的方向仍成立，Telegram
直接 import、公开企微 Go 包进程内使用、单 Gateway 镜像/Workload 的决定保持不变。
Helm 始终等全部生产 Workload 完成后才进入 FINAL-INTEGRATION。

### 16.2 三项需要代码处理的剩余风险

以下风险来自源码控制流检查，本次没有运行故障复现或修复代码；“已补设计”不等于已关闭。
严重度描述启用生产投递/接入后的影响，不声称已发生线上事故。

| ID / 严重度 | 源码依据与场景 | 影响 | 本次文档处理与验收门禁 |
| --- | --- | --- | --- |
| CGR-35 / 高 | [ClaimDue](../../../services/channel-gateway/internal/delivery/adapter/outbound/postgres/claim.go) 先过滤前序 part、核验 owner，再处理 deadline；[Recover](../../../services/channel-gateway/internal/delivery/adapter/outbound/postgres/recovery.go) 仅覆盖 CLAIMED/CALLING | 无 owner/停用账户，或前段 UNKNOWN 阻塞的到期 PENDING 不会由现有路径完整清理；到期未发送项长期停留待办状态，终态/运维统计不完整；容量释放另属保留/GC 设计 | Module §7.6 补独立 expiry 与公平有界 Runner；需真实 PG 验证无人 owner、前段阻塞、claim/expiry 竞争、全部账户不可用时仍恢复。代码仍开放 |
| CGR-36 / 中 | [eventadapter.Handle](../../../services/channel-gateway/internal/delivery/adapter/inbound/eventadapter/handler.go) 将全部 Decode 错误变为 ErrInvalid；[Codec](../../../api/events/execution/v1/reply_intent.go) 还会返回内部 schema 初始化不可用 | 后续 Consumer 若将 ErrInvalid 直接永久拒绝/ACK，可能把内部故障当坏输入终结；当前生产 Consumer 尚未启用 | Module §7.7 明确确定坏输入与内部/依赖错误分类；需测试后者仅延迟重投，拒绝落账失败不 ACK。代码仍开放 |
| CGR-37 / 高 | [SDK config](../../../platform/im/wecom/config.go) 默认 MaxRequestIDs=4096；[command](../../../platform/im/wecom/command.go) 保留每 generation 已尝试 Final 身份，耗尽与瞬时 pending 满均报 CodeCapacity；[Gateway 映射](../../../services/channel-gateway/internal/delivery/adapter/outbound/wecomadapter/results.go) 都归 temporary | 长期健康 socket 可持续收消息却不能再发新 Final，短重试不释放累计身份容量；不能只靠 READY 判断完整收发可用 | 公开库 §4.1 补 typed 区分、账户容量状态/准入影响/告警与有界恢复门禁；小容量联合验证耗尽和恢复，不静默删标记、不重绑旧 ReplyOrigin。组合策略仍开放 |

### 16.3 已修正文档表述，不重复编号既有问题

| 对照问题 | 现在的准确说法 | 核对依据 |
| --- | --- | --- |
| 授权仍笼统标为待冻结 | Gateway CommittedFinalVerifier 与精确字段校验已实现；真实 Execution 完成事实/认证/Outbox/生产接线待交付 | [Port](../../../services/channel-gateway/internal/delivery/application/ports.go)、[Acceptor](../../../services/channel-gateway/internal/delivery/application/service.go) |
| 缺少 ReplyOrigin 和原 Sender 失效都写成准备失败 | 历史 NULL 在首次接纳前返回 ErrUnsupported，尚无 Delivery；已接纳后 Sender 失效才走 FinishPreparation | [ReplyTarget bridge](../../../services/channel-gateway/internal/bootstrap/delivery_bridges.go) |
| 有可信 ACK 就必须退出 UNKNOWN | Observation 与 UNKNOWN 可以并存；显式恢复满足当前 Attempt/一致证据条件才收束，冲突保留 | [ResolveObserved](../../../services/channel-gateway/internal/delivery/adapter/outbound/postgres/observation_resolution.go) |
| Routing 的账户投影与 Connection 配置混写 | Routing 拥有路由目标；Connection 拥有非秘密连接配置与 revision，当前账户文件直接供 Supervisor | [0005](../../../services/channel-gateway/migrations/0005_connection.sql)、[装配](../../../services/channel-gateway/internal/bootstrap/app.go) |
| 目标 Dispatcher.Run 容易误读为现有能力 | 当前有定账户 DispatchAccount/Recover；账户枚举、Runner 和生命周期仍是目标设计 | [Dispatcher](../../../services/channel-gateway/internal/delivery/application/dispatcher.go) |

另外两处状态说明同步收紧：行为调研的 P0 表明确为目标阶段，当前 Telegram 入站仅支持
通过校验的真人私聊文字；群触发实现与真实群验收分别开放。公开 Connector 当前描述不再
笼统声称 Delivery 恢复完全未实现，区分已有账本恢复与未交付的完整后台维护。

相应修正在介绍、详细规范、Final V1、总览、实施状态、术语及部署说明同步体现；旧评审
§1–15 的正文/问题行保留原文，以免把历史“未实现”改写成当时已实现。

### 16.4 既有接线门禁的精化

D0-06/09/10 与 CGR-20 已覆盖传输/授权/隔离议题，本次不把“Consumer 尚未实现”另编成
新缺陷，而是补清 ACK 前持久责任、业务幂等与 transport receipt 的区别，以及事务间隙：

- 当前拓扑只支持 Route/Run 两个 stream，Reply topology/ACL/consumer 仍待交付。
- 接纳/永久拒绝须持久接管后才 ACK；PG、容量或可信验证依赖暂不可用不应永久拒绝。
- Final 接纳与 transport receipt 的同事务/分事务选择须显式设计；分事务必须测试中间崩溃
  与 receipt 重建，不能用“可靠消费”几个字替代确认点，也不能把测试 verifier 装为默认许可。
- 如果 Reply 使用 WorkQueue，正常 ACK 后删除消息，不能照搬 Routing 完整历史连续水位。
  这项区别来自 [NATS 官方保留策略](https://docs.nats.io/learn/jetstream/retention-policies)，
  于 2026-09-05 核对；逐消息 receipt 和显式源重建是本设计据此提出的要求。

D0-01 的真实账户/凭据拥有方、D0-03 交互命令、Control publisher、Execution 完成事实、
CGR-28 bundled NATS 密码预检、保留/GC、观测/配额与真实 IM E2E 继续保持开放。
D0 是分切片议题，不因为接口或本地 fixture 存在就整体关闭，也不把已实现部分再次当作空白。

### 16.5 本次文档验收

本次仅检查 Markdown 链接/锚点/围栏、介绍完整性、上述状态表述和历史正文保护；保存
变更前 SHA-256，在副本修改并验证，再应用到正确工作树，另在独立副本验证回退。
运行代码/Go 依赖/六个迁移/Compose/Git 引用与索引保持原哈希；本次不重跑 Go、PG/NATS、
SDK、镜像或真实 IM 验收。此前运行证据仍按实施状态第 7–10 节的日期和范围解释。


## 17. 说明文档补充与剩余设计门禁复核

### 17.1 本次文档交付

继续维护独立的[四 Module 介绍](module-introduction.md)，新增非线性协作关系、三个变更
归属例子，以及骨架/Module/Gateway/平台 E2E/最终 Helm 的验收层级。该文说明四者是
按业务事实与恢复责任组织的 Module，而不是四层目录或四个部署单元。

本次基线为实际 `codex/channel-gateway@bf107766` 工作树及其已有未提交内容，本地
`main@ab315e4`；只修订文档，不合并分支。临时修改副本中的其他实现不计入实际交付。
四 Module、Telegram 直接 import、公开企微 Go 库进程内使用、单 Gateway 镜像/Workload
的设计方向维持；Helm 仍等全部生产 Workload 完成后才进入 FINAL-INTEGRATION。

### 17.2 复核后修正的两处状态残留

| 位置 | 问题与修正 | 性质 |
| --- | --- | --- |
| [部署 §7.2](../operations/deployment.md#72-部署资产与后续增量) | 旧表把已有 Dockerfile/Compose/NATS/justfile 统称“待实施”；改为当前已有与后续增量两栏 | 文档状态一致性，不是新的部署缺陷 |
| [Module §13.1](module-boundaries.md#131-d0-议题与剩余范围) | 旧 D0 列表将表集合/role/迁移执行者、状态 API、阈值与 Final 授权统称“待冻结”；拆分已落实选择与剩余跨 Workload 门禁 | 既有 D0 的进度说明，不重复新增 CGR |

对照来源包括实际 Dockerfile/Compose/NATS 配置、justfile、0001–0006、公开库 State/States、
Bootstrap、Final Acceptor 与 CommittedFinalVerifier Port。历史评审 §1–16 原文保持不变；
新摘要不会把历史“当时未实现”改写为“当时已完成”。

### 17.3 新补清的设计问题：有效发送截止与时间来源

既有行为调研描述了企微长连接回复窗口，但 Final V1 只明确 Worker 的业务 deadline，
尚未冻结两者如何组合。源码 [Intent.Validate](../../../services/channel-gateway/internal/delivery/domain/policy.go)
只校验合法时间，[Delivery 接纳](../../../services/channel-gateway/internal/delivery/adapter/outbound/postgres/accept.go)
按 Intent.deadline 判断是否已过期；[bridge](../../../services/channel-gateway/internal/bootstrap/delivery_bridges.go)
虽然传递 ReceivedAt，当前并未据此计算模式相关截止。

这会使“业务授权有效”与“渠道仍可回复”缺乏明确分工，也使 deadline 的最大驻留预算
由上游输入决定。它不表示已经观察到外部发送失败，更不表示服务端一定按原 req_id 精确
计时。官方长连接窗口已重新核对，模式与限定见[Final V1 §3.1](delivery-final-v1.md#31-业务-deadline-与有效发送截止剩余设计门禁)。

本次将其列为 **D0-08/09 的精化**：区分业务截止与持久有效发送截止，明确时间来源、
不延长旧窗口、不改业务摘要、不把未知调用改成过期，以及临界/排队/重连的验收要求。
具体策略尚待接受与实现；不新增重复 CGR 编号，不把文档方案等同代码修复。

### 17.4 仍开放的实质风险与接线门禁

| 项目 | 本次只读复核结论 | 完成门禁 |
| --- | --- | --- |
| CGR-35 | 实际工作树仍只有定账户 DispatchAccount/部分恢复，无独立完整 pending expiry/公平 Runner | 真实 PG 竞争与无人 owner、前段 UNKNOWN、多页公平、关闭语义验收 |
| CGR-36 | eventadapter 仍将内部 Schema 初始化错误统一映射 ErrInvalid | 区分坏输入与内部故障；持久拒绝失败不 ACK，内部故障延迟重试 |
| CGR-37 | SDK 累计 Final 身份容量与瞬时 pending 满仍共用容量分类 | typed 原因、账户状态/准入影响、有界恢复与小容量组合验收 |
| 完整回复链路 | 默认 App 未启用 ReplyIntent Consumer/生产 Dispatcher，可信 Execution 与 Telegram 发送凭据 owner 仍待接线 | 完成事实/认证/Outbox、Reply transport/ACL 与实际 Worker/IM 验收 |
| 有效发送截止 | 业务 deadline、渠道窗口与时间来源的合成规则未冻结 | 按 §17.3 / Final V1 §3.1 明确规则并验证，保留原授权摘要 |

结论是**总体架构方向成立，但生产闭环仍有明确门禁**；不是“所有设计问题已关闭”。
本次未发现需要推翻四 Module 或恢复独立 Connector 部署的依据。

### 17.5 验证方式

本次验证 Markdown 相对链接、锚点、围栏、介绍覆盖及状态表述；保存变更前 SHA-256，
在修改副本完成检查后应用实际工作树，并用独立副本测试回退和 diff 重放。
所有非文档源码、依赖、六个迁移、Compose 与 Git HEAD/main/index 保持原哈希；本次没有
重跑 Go、PG/NATS、SDK、镜像或真实 IM E2E，不引用临时实现测试作为本次交付证据。
既有运行证据仍按[实施状态 §7–10](implementation-status.md)的日期和具体覆盖范围解释。

## 18. Delivery Runtime V1 实现与剩余生产门禁

### 18.1 接续历史设计，不改写过去验收

第 1–17 节正文与既有 CGR 行保留原文。当前接续 CGR-35/36 实现独立维护、有界账户调度、
PG 发现/过期、LocalOwner 与 Decode 错误分类；精确规格见[Runtime V1](delivery-runtime-v1.md)，
本轮实际应用与最终验证见[实施状态 §11](implementation-status.md#11-delivery-runtime-v1实现与本轮验收)。
本节不把上一轮临时副本的测试直接计作本轮最终验收。

### 18.2 本轮源码处理

| 项目 | 已提供的实现 | 尚未证明或交付 |
| --- | --- | --- |
| CGR-35 | Maintainer 独立恢复与过期；PG RuntimePorts/0007；Runner 有界 keyset/worker/每账户单 dispatch；Connection.LocalOwner | 本轮最终验收已通过；生产发送装配、负载公平性/会话配额仍待交付 |
| CGR-36 | eventadapter 仅把 ErrInvalidReplyIntent 视作确定坏输入，其他 Decode 错误为不可用 | 本轮最终验收已通过；Reply transport receipt/永久拒绝/ACK 消费路径仍未实现 |
| 默认启动 | 无论本地是否有账户，App 都只新增独立 Maintenance | 没有新增 Sender、发送 Runner、ReplyIntent Consumer、Final Acceptor 或默认许可 |
| 数据迁移 | 0007 新增五个 Runtime 索引，沿用启动迁移/SHA ledger | 不修改旧 SQL、不删业务事实、不释放账本容量；旧库升级需真实验证 |

Maintenance 不依赖 owner/凭据/执行授权；它只维护已提交的 Delivery 事实。候选发现不等于
发送授权；LocalOwner 返回当前真实 grant 拷贝，不能替代 PG A1/A2 或原 ReplyOrigin 检查。
Observation 的列表不修改状态；维护调用既有 ResolveObserved 才按一致证据规则收束。
当前单 Maintainer 只有一个生命周期拥有者，不允许默认 App 与 Runner 重复启动同一对象。

本轮代码复审另修正默认 4 worker / 4 页配置的跨 Provider 饥饿：旧按每页推进起始位置，
页预算整除 Provider 数时可使 Telegram 每轮先占满池；现在每 tick 独立轮换起始 Provider，
不改变本地每账户单在途、worker 上限或 PG 发送门。新增公开 Interface 回归
`TestRunnerDefaultPoolSharesCapacityAcrossContinuouslyDueProviders`，两个 Provider 持续 due，
不依赖上一个 Provider 积压先耗尽。这是 CGR-35 有界调度的实现修正，不新增重复 CGR；
分项红绿及最终验收由实施状态 §11 统一记录，不宣称已验证任意生产负载公平性。

### 18.3 明确保持开放

- CGR-37 的累计 Final 身份容量不同于瞬时背压；当前未交付账户级 typed 状态、准入影响
  或换代恢复策略，不能仅以 SDK READY/LocalOwner 判定完整发送可用。
- Final V1 §3.1 的有效发送截止仍待冻结。本 Runtime 的 ExpirePending 只用 Intent.deadline，
  未暗中引入新回调时间、窗口延长或业务摘要变更。
- 真实 Execution 完成事实/认证/Outbox、Control publisher、Reply topology/ACL/Consumer、
  Telegram 发送凭据和生产 Runner 接线、真实 IM E2E 尚未交付。
- 保留/GC、容量回收、OTel、全局恢复预算、生产负载配额和 CGR-28 密码预检继续开放。

四 Module 同进程、一个 Gateway 镜像和无独立 Connector 的方向不变；Compose 随真实
Workload 交付。Helm 仍在全部生产 Workload 完成后进入 FINAL-INTEGRATION。

### 18.4 最终验收边界

本轮实际工作树全仓、真实 PG/NATS race、本地 Telegram HTTP/企微 WS Runner、七迁移
镜像/Compose、认证 NATS ACL 与源码副本回退已通过，完整证据与首轮审计布局错误的修正
见[实施状态 §11.4](implementation-status.md#114-最终联合验收)。默认 App 仍只新增 Maintenance，
测试 verifier/Telegram eligibility 未进入默认装配；本结果不扩大为实际 Worker 或外部 IM 验收。


## 19. 四 Module 讲解补充与文档一致性复审

### 19.1 文档交付与审查口径

2026-09-05，按“把介绍写成文档，再审查之前设计”的请求继续维护
[独立介绍](module-introduction.md)，不复制第二份同题讲解。补充 §1.6 的同一消息四类事实、
各自可变/不可变的关系，以及 §8.4 的跨 Module 交接清单，明确成功确认点和故障拥有方。
四 Module、内部 Domain/Application/Adapter、单 Gateway Workload 是三个不同维度。

审查使用实际 `codex/channel-gateway@bf107766` 及当前未提交内容，本地 `main@ab315e4`；
本次只修订文档，不合并分支，不继续实现新的协议或运行切片。检查 Gateway 文档及相邻
Control/Execution/部署入口，并只读对照 Routing Compare/PG receipt、Dispatcher、Runner、
Final 验证用例、Bootstrap、七个迁移和 NATS 配置。外部 SDK 能力沿用既有调研的日期与
限定，本次不把它们当作重新进行的官方协议或真实账号验证。

**结论：总体方向成立，存在需要修正的契约表述与进度残留；修订说明不等于完成生产门禁。**
没有将已记录的 CGR-37、有效发送截止或真实 Worker 接线包装成新发现，也不为文案修正
重复新增 CGR 编号。以下级别表示误读后对后续实现的影响，不表示本次发现对应运行故障。

### 19.2 本次修正的契约表述

| 级别 / 位置 | 原说明的问题 | 修订及源码依据 |
| --- | --- | --- |
| 高 / [Runtime §6](delivery-runtime-v1.md#6-localowner-不是原-sender也不是授权证明) | 将 PG A1/A2 画在 ReserveFinal 前，读者可能先进入 CALLING 再发现原 Sender 不可用 | 改为 A1 → Reserve → A2 → Send → Observe/Finish → Release；准备失败仍走 FinishPreparation；与 [Dispatcher](../../../services/channel-gateway/internal/delivery/application/dispatcher.go) 及 Final V1 §5 一致，现有代码无需变更 |
| 中 / [总览 §5](README.md#5-关键-interface-与两条持久链路) | 将旧路由 generation 的正常重放写成拒绝 | 旧 generation 持久记录后忽略；同 generation 异内容才是冲突；依据 [Compare](../../../services/channel-gateway/internal/routing/domain/route.go) 与 [PG receipt](../../../services/channel-gateway/internal/routing/adapter/outbound/postgres/store.go)，不让正常乱序进入坏事件处置 |
| 中 / [Runtime §4](delivery-runtime-v1.md#4-runner有界发现与每账户串行)、Module §7.6 | 把单 Runner 的账户单在途扩大为整个进程的保证 | 明确 [Runner.active](../../../services/channel-gateway/internal/delivery/application/runner.go) 属于每个对象；生产先单 Runner，多个 Runner 需要互斥分区或协调与重复装配验证，当前尚无这项装配检查；PG 同 part CAS 不保证同账户跨 Runner 串行 |
| 高 / [Module §7.4](module-boundaries.md#74-回复目标执行代次和展示顺序) | “没有完成事实就拒绝”可能被误读成一次查空即可永久拒绝并 ACK | 与 §7.7 对齐：暂时未查到/依赖不可用与可信永久否决分开；[Final 接纳用例](../../../services/channel-gateway/internal/delivery/application/service.go) 传播 verifier 错误，取得证明后才逐字段校验；真实验证协议、transport receipt 与 Consumer 仍待交付 |

Runner 的修订同时新增生产装配验收前提，不声称通过一段文档已经拥有进程级串行锁。
Final 的修订也不允许默认授权；暂不可验证仍不得发送，只是不把暂时性读结果写成永久否决。

### 19.3 对齐已实现选择与历史验收状态

- 总览、部署前置和 Module §9.2 不再将已存在的 State/States、独立 database/role、启动
  迁移、同事务 guard 与本地阈值整体列为待冻结；剩余范围集中到 Module §13.1。
- Module §14.1 按实际配置说明 Route=Limits、RunRequested=WorkQueue；未交付的 Reply
  transport、真实 Worker、路由归档/重建与超窗恢复继续分开，不沿用“Run 若选择…”的旧草案。
- 相邻 Control Deployment 的当前事实由 Gateway 0001–0004 更新为 0001–0007，并区分
  Delivery 已有 Module/Runtime 与生产发送尚未启用；不改 Control Deployment 的设计选择。
- Compose 与 Execution event 入口改为引用实施状态 §10/§11 的既有验收记录，不再同时
  写已完成和待根集成。旧测试数字、镜像 ID、日期和历史结论保留在原记录，不重新背书。

### 19.4 仍开放的生产门禁

| 门禁 | 责任与后续完成条件 |
| --- | --- |
| 可信账户与发布来源 | Control 完成 ChannelAccount/凭据拥有方与发布；Gateway 处理认证分发/轮换；文件/环境引用不是完整管理闭环 |
| Execution 与 Reply 交接 | Execution 提供可信不可变完成证明/认证/Outbox；Delivery 接入 Reply topology/ACL、transport receipt、永久拒绝与 Consumer；先持久再 ACK |
| 生产发送装配 | 默认 App 仍只新增 Maintenance；接线必须选择唯一维护生命周期、无重叠 Runner 调度范围、可信 Sender/凭据，并完成真实 Worker/渠道验收 |
| CGR-37 | 区分累计 Final 身份容量和瞬时背压，提供账户状态/准入与发送影响、有界恢复；旧 ReplyOrigin 不重绑，小容量联合测试仍待实现 |
| 有效发送截止 | 冻结业务 deadline、渠道/本地窗口及可信时间来源；不改授权摘要、不延长旧窗口、不把 UNKNOWN 当 EXPIRED |
| 长期运行 | 保留/GC/容量回收、全局恢复预算、CGR-28 密码预检、会话配额/负载公平与观测仍待交付 |

单 Go 二进制、单 Gateway 镜像、Telegram 直接 SDK、公开企微 Go 库进程内导入维持不变。
Compose 随真实 Workload 交付；Helm 必须在全部生产 Workload 完成、运行契约稳定后进入
**FINAL-INTEGRATION**，不属于 Gateway P1/P2，本次也没有创建 Chart 或 Connector 部署单元。

### 19.5 本次文档验证

本次保存完整源码清单及原始 SHA-256，在独立副本修改；执行 Markdown 相对链接/锚点/
围栏校验和六组文档契约回归（发送顺序、Runner 范围、旧路由重放、验证错误分类、已有/
剩余选择、验收状态）。基线保留原六组不一致，修订版要求全部通过；这些是文档检查，
不是 Go 业务、真实 PG/NATS、镜像或外部 IM 测试。

仅应用本次 Markdown 差异；所有非文档源码、依赖、七迁移、Compose 配置及 Git 引用/索引
保持原哈希。复审第 1–18 节正文与实施状态全文保持原文。独立副本回退应恢复全部原始
源码哈希及原六组文档问题；实际工作树保留修订。最终结果、精确命令与退出码、diff 重放
及四项工件见本地 `artifacts/channel-gateway-doc-review-r19-20260905/VERIFICATION.txt`。


## 20. Control 实际接入与真实 Telegram 入站（2026-09-06）

本轮按实际 GCI2 源码和运行结果清理 `control-integration-v1.md`、Module §15 与总览中的旧
“均未实现/待审查/事务 guard 缺失/注册属于后续”表述。credentialContext、容量配置、
Handler 安装顺序、公开错误映射均对齐现有实现；不再把 Gateway ManifestResolver 当本次
入站的既有能力或必要同步前置。历史第1–19节仍保留原时间点，不用于覆盖最新状态。

真实用户 Telegram 消息已通过实际 Control/Gateway 进程至唯一 RunRequested，脱敏字段与
PG/NATS 交叉证据见 `telegram-real-inbound-20260906.md`。原始 PubAck 帧未另存，明确使用
“有效 PubAck 后才写 published_at 的代码路径 + 已发布 Outbox + 可读持久消息”交叉证明，
不将运行推断伪装成抓包。Worker、Manifest 正文执行与实际模型/Storage 能力仍后续验收。
