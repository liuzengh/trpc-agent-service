> **历史记录，已被替换。** 本文中的授权／预算实现已从当前运行路径撤出。当前业务范围、代码接线及迁移说明见 [最小身份与连续对话](minimal-identity-session.md)。以下保留原日期和证据，不表示当前仍启用。

# Channel 优化实施账本

## 目标与状态

完整目标保持为 [优化设计](channel-optimization-design.md) 的 F01–F12 及 §19 验收矩阵。
开始日期 2026-09-08，基线 `7fc79234853884780b1b99abb12bbd9af56213c0`，
分支 `codex/channel-optimization`，仍在 Channel Gateway 工作树开发。

这不是缩减版验收：阶段 A/B/C、跨数据 owner 协作及明确列出的故障门禁分别跟踪。
依赖其他 owner 的审批、Artifact、Memory、多后端迁移不冒充本轮已实现；Helm 保留
FINAL-INTEGRATION 的全部 Workload 完成前置条件。既有部署、Bot 和 main 不变。

## 最新交付边界（2026-09-08 审查）

下文是追加式实施历史；“本轮”“尚未”描述对应记录时点，当前状态以末尾验收记录为准。
本次提交仅交付 F01 的身份/策略/定义、发布及历史投影基础，不宣称 F01–F12 完成。
策略投影默认关闭、policy_scopes 默认空；现有 Admission/Worker 授权路径不切换。
用户已要求审查后提交远端 main；此操作不启动部署或真实 IM 联测。

## A0：边界与默认值

按原提案记录待冻结的默认值：DENY_ALL、账户范围 ExternalPrincipal、群会话
per_user_in_conversation、共享 PG 额度、初始授权投影新鲜度评估值 30 秒。
产品默认值与行为测试接缝的确认问题已发送；没有收到回答前不将这些值启用于账户。

接缝：Control 主体/策略发布用例、Gateway Admission、Worker intake/当前授权核验、
真实 PG/NATS 联合链。数据库约束、Schema、HTTP 和 UI 的负例随对应纵向切片补齐。

## 本轮已落代码

- Control domain 新增 ExternalPrincipalIdentity / ExternalPrincipal 数据模型；身份键包含
  tenant/provider/account/external_user。模型不包含 TenantMember/OWNER、Bot Secret 或展示名。
- 新增 `0005_channel_principals.sql`：复合账户/租户/Provider 外键、完整身份唯一键、
  ACTIVE/REVOKED 闭合状态、版本上限及租户账户分页索引；撤销后保留身份唯一键。
- 这些是 **F01 持久化基础**，不是已接线的授权功能。尚未新增主体管理 HTTP、
  策略发布、运行投影、Admission 拒绝或 Worker 核验；现有运行行为没有改变。

## 阶段与剩余工作

| 项 | 状态 | 完成所需的权威证据 |
| --- | --- | --- |
| F01 外部主体/策略 | 进行中：主体生命周期、OWNER 命令与事务持久化 | OWNER/CAS/幂等管理、原子发布/撤销、双端完整投影、新鲜度与事务 fence、Worker 核验、A01–A04 |
| F02 群触发 | 未实现 | 可信 conversation/mention/reply/command 元数据、CapabilitySet、G01及双方 Provider 矩阵 |
| F03 Session 分区/命令 | 未实现 | scope 契约、registry 与 intake/reset 同锁事务、旧 generation head 隔离、S01 |
| F04 Callback/命令/审批 | 未实现；审批依赖 Worker Tools | Receipt/CommandOutbox、独立 ProtocolAck、命令结果、handle/CAS/权限/到期，I01 |
| F05 Progress/卡片 | 未实现 | 独立意图、TTL/sequence、Final barrier、在途 UNKNOWN、Provider 实验与 P01 |
| F06 附件 | 未实现；依赖 Artifact owner | 授权先于下载、受控引用、有界下载/扫描/发布、出站/回收，M01 |
| F07 限流/恢复/分片 | 未实现 | retry_basis、共享 PG 多层额度/公平性、恢复审计、不可变分片计划，D01–D03 |
| F08 状态/诊断 | 未实现 | ObservationOutbox、mTLS/epoch/sequence、期望实例集聚合、分页/隐私，O01 |
| F09 观测/审计 | 未实现 | 有界 OTLP/trace carrier、事务审计 outbox、HMAC/脱敏、usage 幂等，O02 |
| F10 多节点/真实 IM | 未验收 | 同一最终 build、双 Gateway/Worker、恢复/失租/原 Origin、N01/N02/L01/L02 |
| F11a 不可变版本/路由回退 | 已实现基础能力 | Binding CAS 指回仍有效旧 Revision、RouteGeneration 单调、原 Admission/Run/Reply 目标不变；这不是流量灰度 |
| F11b 双目标流量灰度 | 已实现核心链路 | Control stable+canary/基点/显式 sender 策略、Route事件、Gateway确定性用户分桶、Admission固定目标与trace维度；正式结果指标窗口和自动停止/回退仍未实现，R01仅部分验收 |
| F12 容量/归档/部署 | 未实现 | 热点与恢复压测、保留 tombstone/UNKNOWN、可重建归档、C01；Helm 后置 |
| 数据 owner 协作 | 契约待收敛 | Session/Memory/Summary/Artifact/Knowledge/Audit 独立 Port；正式后端切换与回灌 B01 |
| Worker 治理/预算 | 待所属能力落地 | 原子预留、未知不退款、当前工具授权与审批复查，Q01 |

## 后续执行顺序

1. 完成主体/策略 Control 用例与数据库约束负例；策略默认拒绝，PUBLIC_LIMITED 必须
   绑定隔离 Session、额度和受限工具能力，不留开发 bypass。
2. 同步 v1 Schema、管理/内部 API、发布 outbox 与完整运行投影。
3. 接 Gateway receipt-first 授权与同事务 policy fence；同步 RunRequested actor/policy。
4. 接 Worker intake / Attempt 的当前授权核验，再补 Web 管理与诊断。
5. 按设计 A/B/C 顺序完成其余优化；按原矩阵逐项积累当前构建的验收证据。

重复事件不增加策略版本到 ExternalEventKey。暂时依赖故障不写成永久 DENIED。
来源正文、临时凭据和 trace carrier 不参与业务身份；不跨 owner 共用业务事务。

## 继续执行提示

```text
继续 channel-optimization：先核对当前分支、未提交修改和这份实施账本。
保留 F01–F12 原目标，核验上一轮代码和测试结果后实现下一条未完成接缝。
默认策略与测试边界以用户确认结果为准；不得用主体 ACTIVE 代替已发布使用授权。
源代码单测、实际 PG/NATS、真实 Provider 与运行部署分别计证；未验收项保持未完成。
```

## 本轮验证记录（基础，不代替 F01 验收）

- 原基线与修改版：既有 Control channelbinding/domain、migrations 两包测试通过。
- 隔离 PostgreSQL 17：既有 `services/control-api/integration` **23 pass / 0 skip / 0 fail**。
- 独立临时 schema 按顺序应用 0001–0005，检查新增主体表约束目录，退出码 0。
- 上述结果证明当前代码可编译、迁移可应用且既有集成未回归；**没有证明**新主体管理、
  跨租户授权行为、撤销传播窗口或 A01–A04 已完成。新行为测试仍需随接缝实现。
- 证据放在 `artifacts/channel-optimization-20260908/`；临时 PG 容器及匿名卷已清理。

## 2026-09-08 续作：主体生命周期

本次前一轮分类为 progress（已有主体模型与迁移，并有真实 PG17 应用证据）。
继续在同一工作树补齐 Control domain 的公开创建/校验/状态转换边界：

- `NewExternalPrincipal` 从已加载账户派生 tenant/account/provider；Telegram 数字身份
  去除前导零，拒绝空白、控制字符、无效 UTF-8 与超长身份；错误不回显外部 ID。
- `Validate` 拒绝损坏的持久状态、非规范化身份和非法版本/时间。
- `SetState` 保持 ID/身份不可变；撤销、显式重新启用均校验 expected revision；重复
  当前状态不分配版本；过期 CAS、未知状态、时间倒退、版本耗尽均拒绝。
- 先运行缺失入口的失败测试，再实现；新增生命周期、跨租户/账户身份和非法状态矩阵。
  domain race 测试为 **32 pass / 0 skip / 0 fail**；整个 channelbinding 与 migrations
  既有源码回归通过。此次没有重新运行真实 PG；上一轮 PG 证据仅证明迁移基础。

这些是领域规则，不是应用层 OWNER 授权、事务 CAS 或撤销传播已经接通的证明。
策略默认值仍未启用于账户；管理用例、HTTP、发布 outbox 和两端运行投影继续待实现。
本轮证据：`artifacts/channel-principal-lifecycle-20260908/VERIFICATION.txt`。

## 2026-09-08 续作：OWNER 主体命令与真实 PG 持久化

上一目标轮次只复核已合入的 WeCom，未推进优化开发；本轮回到优化分支继续 F01。

- 新增 `RegisterExternalPrincipal` / `SetExternalPrincipalState` 应用用例，复用当前
  OWNER 检查、请求 MAC、幂等回执及事务内 OWNER 再校验。响应为 NOT_EMITTED，
  不伪装成策略发布或已向 Gateway/Worker 传播的撤权。
- PostgreSQL Transaction 增加 LoadPrincipal / SavePrincipal：账户先锁定，主体后锁定；
  tenant/account/provider 范围一致；创建不 upsert；状态改变复核原身份、创建者、CAS。
  复用已经锁定的账户，不改变原有 Aggregate 禁止重复 LoadAccount 的约定。
- 仅修改尚未发布的 0005 草稿，增加命令回执闭合 operation 集合；0001–0004 不变。
- 先运行新用例得到缺少 API 的编译失败，再实现。真实隔离 PostgreSQL 17 + race 测试
  覆盖同 revision 两 writer 竞争、撤销后重复注册拒绝、跨租户/账户隔离、真实事务 OWNER、
  伪造身份拒绝、回执写失败整笔回滚并可用相同 key 重试、不自动增加 TenantMember。
- 最终 Channel + migrations race：211 pass / 1 skip / 0 fail；其中 PG adapter
  36 pass / 1 skip / 0 fail。唯一测试 skip 是已有的真实 JetStream relay 测试，未配置 NATS。
  Control 全服务常规回归 exit 0；该命令没有配置真实 PG，不把其跳过项计作集成验收。
- 主体命令没有暴露新 HTTP 路由。策略 revision/digest、发布和审计 outbox、运行投影、
  Gateway/Worker 授权及 Web 接线仍待实现；主体 ACTIVE 仍然不是访问授权。

证据：`artifacts/channel-principal-commands-20260908/`。源码回滚仅验证隔离副本回到
本轮前的主体生命周期基础，不是数据库回滚，也不撤销活跃工作树的实现。

## 2026-09-08 续作：访问策略候选、revision 与 digest

上一轮完成主体命令与 PG 事务验证，属于开发进展。本轮继续 F01，尚不发布策略。

- 新增 `AccessPolicyBody`、精确 `PolicyRevisionReference` 和
  `ChannelAccessPolicyRevision` 候选模型；`PrepareAccessPolicyRevision` 从已加载账户
  派生 tenant/account/provider，禁止把调用者自报的租户当成授权来源。
- `DefaultAccessPolicy()` 显式 DENY_ALL、空主体/会话/操作集合。ALLOWLIST 和
  PUBLIC_LIMITED 结构上要求精确 Session/Quota ID + revision + digest；PUBLIC_LIMITED
  不携带会被公共分支忽略的主体 allowlist，避免配置语义误导。
- 集合复制后排序去重，不修改输入切片；JCS + SHA-256 覆盖 schema version、租户、
  账户、Provider、policy ID/revision、body、发布者和时间。计算 digest 时省略 digest 字段。
  修改内容必须由未来发布事务生成新 revision，不能把旧 revision 的 body 原地改写。
- 解码拒绝重复/未知/大小写变体字段、缺失字段、null 替换、无效 UTF-8、篡改和超长文档；
  校验收到的 body 与规范化 body 相同，拒绝“摘要一致但集合解释不同”的非规范表示。
- 本地准备上限：主体/会话输入各 4096 项，单条 Provider 会话 ID 1024 字节，完整文档
  1 MiB；授权新鲜度配置上限暂按设计建议 30000 ms。这些不是已测 SLA，也不是最终
  分页投影实现。大集合关系存储/完整性分页和 F10 校准仍待完成。
- PUBLIC_LIMITED 的结构有效只说明引用格式正确；隔离 Session、低配额、无敏感工具
  仍必须由发布用例调用可信 owner Port 解析并校验。候选不是已发布授权，不可用于接纳。
- 领域 race 回归 53 pass / 0 skip / 0 fail（包含既有测试和 fuzz seeds）；解码器 fuzz
  20 秒完成 1,367,894 次执行并 PASS。Control 常规回归与 domain vet 均 exit 0。
  本轮没有运行真实 PG/NATS 或 Bot 测试，没有新增迁移、发布 outbox 或 HTTP 路由。

证据：`artifacts/channel-access-policy-20260908/`。下一步仍是 Control 发布事务中的
OWNER/CAS、引用解析、策略持久化和审计/发布 outbox，然后才允许接运行投影与授权消费。

## 2026-09-08 续作：策略 revision 存储与事务 outbox 基础

上一轮策略候选及 digest 校验属于进展。本轮继续 F01，新增 0006 迁移与 PG 存储接缝。

- 每个 tenant/account 一个稳定 policy head；revision 文档追加写，head 精确 CAS，
  policy ID/account/provider 不原地替换。延迟外键保证提交后的 head 指向真实 revision。
- 规范化主体成员表同时引用同一 tenant/account/provider 的 Principal 与策略 revision；
  延迟约束触发器保证提交时成员集合与不可变 body 精确一致。历史 revision/member
  UPDATE/DELETE 被拒绝，head 只允许单调加一；不存在删除旧授权来伪造历史的更新路径。
- `PolicyRevisionStore.WithPolicyWrite` 复用当前事务 OWNER 再校验；专用 Transaction
  暴露精确读取、追加及命令回执方法。账户锁、当前主体 ACTIVE 状态和 revision 都在
  同一事务复查，和既有主体撤销命令互斥。读取 JSONB 后重新验证完整 digest 与身份列。
- 每次追加把 revision、head、成员关系和两条 reference-only 发布/审计 outbox 一起提交。
  审计保留 Control actor、action、decision、reason 和策略引用，不携带完整用户列表。
  这两个新 event type 尚未接 relay/共享 Schema，旧 route relay 不消费它们。
- 真实 PG17 测试顺序执行 0001–0006；验证并发 CAS 单胜者、历史文档不变、head 回退拒绝、
  审计 outbox 失败整笔回滚和同参数重试、OWNER/跨租户拒绝、未知/撤销主体拒绝、列表不入
  outbox、提交不完整成员集合失败、发布后补入额外成员失败。
- 首轮夹具错误使用公开 AccountView（ScopeID 已脱敏）构造候选，产生 SourceIntegrity；
  保留失败日志，改为读取内部账户，不放宽产品校验。随后真实 PG 测试通过。
- 最终 Channel + migrations race：234 pass / 1 skip / 0 fail；PG adapter 38 pass /
  1 skip / 0 fail。唯一 skip 是既有 JetStream relay 测试，NATS 未配置。Control 常规
  回归与 Channel vet exit 0；常规回归未配置 PG，不冒充额外真实依赖验收。

本轮接的是 **发布存储基础，不是完整发布用例**。Session/Quota/tool 的可信 owner
引用解析、发布命令的幂等编排和 API、正式事件契约/relay、审计背压和保留管理仍待接线。
尤其测试的 Session/Quota 引用只是存储夹具，不是 PUBLIC_LIMITED 能力证明。
主体撤销向运行端传播、完整投影、新鲜度 fence、Worker 检查及剩余 F01–F12 仍未完成。
证据：`artifacts/channel-policy-storage-20260908/`。只在一次性 PG 中应用新迁移，未部署。

## 2026-09-08 续作：策略发布应用用例与幂等编排

上一轮策略存储和 PG 测试属于进展。本轮将发布应用用例接到该事务接缝。

- 新增 `PolicyPublisher.Publish`：共用 OWNER 校验，规范化策略后绑定 actor、tenant、
  scope、account、expected revision 与内容 MAC；同一事务优先查幂等回执，重试不再
  调用当前依赖、不重复生成 revision/outbox。不同内容复用 key 返回冲突。
- 首次发布生成稳定 policy ID；后续发布复用 ID，按当前 head CAS 生成新 revision。
  返回体只含精确引用、event/audit ID 和 PENDING，完整列表不放入成功回执。
- `PublishedPolicyValidator` 是必须注入的可信 owner Port，没有默认放行实现。
  非 DENY_ALL 必须通过该 Port；明确拒绝返回稳定代码，未知/私有错误映射为依赖不可用。
  传给适配器的是签名输入的分离副本，防止适配器修改切片改变实际发布内容。
- 追加策略和两条 outbox 后，成功命令回执仍在同一事务写入。回执或 commit 失败返回
  零结果，不泄漏“尚未提交但看似成功”的引用。0006 草稿增加 PublishChannelAccessPolicy
  到回执闭合集合，已发布 0001–0004 和既有 0005 保持本轮前字节。
- 测试覆盖并发首次同 key 发布只生成一份结果、集合顺序/重复项等价重放、修改请求冲突、
  stale CAS、撤销 OWNER 后拒绝重放、依赖未知/拒绝不落库、适配器输入隔离、回执失败整笔
  回滚后重试。含 1024 个会话 ID 的策略仍产生小于 2 KiB 的成功回执。
- 最终 Channel + migrations 真 PG/race：244 pass / 1 skip / 0 fail；PG adapter
  40 pass / 1 skip / 0 fail。唯一 skip 为旧 JetStream relay 测试，未配置 NATS。
  Control 常规回归、Channel vet 均 exit 0。新建的依赖缺失与入库前拒绝单测不依赖 PG。

生产 Session/Quota/tool owner 适配器、HTTP/Bootstrap/Web、正式事件 Schema/relay、
运行端投影与授权仍待实现。PG 发布用例测试使用显式 owner 校验替身，不将替身的接受
结果当成生产 PUBLIC_LIMITED 的隔离、低额度或工具权限证明。本轮没有部署新发布入口。
证据：`artifacts/channel-policy-publisher-20260908/`。完整 F01–F12 目标仍在进行中。

## 2026-09-08 续作：具体策略依赖校验器

上一轮发布编排及真实 PG 事务测试属于进展。本轮实现 `PolicyReferenceValidator`，
不再仅定义一个由测试替身整体返回成功/失败的验证接口。

- Session/Quota owner reader 必须返回已校验文档 digest 的不可变发布快照。校验器检查
  返回 tenant 及 ID/revision/digest 与请求精确匹配，拒绝跨租户、旧 revision、替换引用。
- Session 分区只接受 per_user_in_conversation/shared_conversation；PUBLIC_LIMITED
  必须前者。停用 Session/Quota 拒绝发布，未知分区/损坏字段/私有错误视为依赖不可用。
- 公共策略必须引用明确 PublicLimited 的额度配置，同时满足正数 concurrency/rate
  和显式注入的公共额度上限；零值（含无限额度表示）、超限均拒绝。没有猜测的“低额度”
  默认值，生产配置尚未启用；测试上限 1 concurrent/5 starts per minute 仅为夹具。
- 公共策略必须通过独立 PublicToolIsolation owner Port；这个 Port 需证明运行时公共
  路径排除敏感工具，不以客户端声明替代。缺少 reader/工具 owner/额度上限则构造失败。
- 普通 ALLOWLIST 可选择已发布的共享 Session，不错误套用公共额度上限。DENY_ALL
  不读取不存在的授权依赖；取消上下文保留取消结果；依赖私有错误不进入错误响应。
- 真实 validator + publisher + PG 联合测试依次拒绝共享 Session、超额公共配额、工具
  隔离拒绝，三种失败均不产生策略/outbox/回执；条件满足后只写 1 revision、2 outbox、
  1 receipt。owner I/O 本身仍为显式夹具，不当作生产 owner 适配器验收。
- 最终 Channel+migrations 真 PG/race：273 pass / 1 skip / 0 fail；PG adapter
  41 pass / 1 skip / 0 fail。唯一 skip 为已有 JetStream relay，NATS 未配置。
  Control 常规回归、Channel vet exit 0。本轮未修改迁移或产品 Bootstrap。

下一步仍需 owner 发布数据的实际存储/读取与工具隔离适配器、公共 HTTP/Bootstrap/Web、
正式事件契约和 relay、完整运行投影以及 Worker/Gateway 授权。完整 F01–F12 未完成。
证据：`artifacts/channel-policy-references-20260908/`；只在一次性 PG 中执行联合测试。

## 2026-09-08 续作：真实 Session/Quota owner 文档与读取

上一轮具体校验器与联合回归属于进展。本轮进一步替换 Session/Quota 读取夹具。

- 新增 Control 内部 channelpolicy owner，区分“Session/Quota 定义”和 Worker Session
  事实/额度消耗。没有新增 Workload，Channel adapter 不直接查询另一个 owner 的业务表。
- 0007 新增 tenant/kind/id/revision 精确键的不可变定义存储；类型闭合、身份列和文档
  一致，历史 UPDATE/DELETE 被拒绝。未修改本轮前迁移。
- owner domain 对 Session/Quota 分支、数值边界、元数据和完整文档计算并验证 JCS/SHA-256；
  严格解码拒绝重复、未知、大小写变体、非法 UTF-8、超长输入和篡改。构造时复制定义指针。
- 真 PG Reader 只读精确 tenant/kind/id/revision，重新校验文档和索引 digest，没有 latest
  fallback。Channel policyowner adapter 经 owner Port 读取并映射已验证的定义。
- 联合测试中的 Session/Quota 文档插入是显式夹具，但读取、digest 验证、适配映射、
  具体校验器和发布器均使用真实实现，并完成策略/回执/outbox 事务。工具隔离仍为夹具。
  跨租户、kind 替换、错误 digest、缺失精确版本和伪造文档分别拒绝；数据库私有错误不泄漏。
- 最终 Channel + owner + migrations 真 PG/race：277 pass / 1 skip / 0 fail；Channel
  PG adapter 42 pass / 1 skip / 0 fail。唯一 skip 为旧 JetStream relay，NATS 未配置。
  Control 常规回归、两模块 vet exit 0。

owner 定义的发布命令（OWNER/CAS/幂等/outbox）、API/Bootstrap、真实工具隔离及
运行投影仍待完成；不能把 SQL 插入夹具当成生产发布流程。Enabled 是不可变定义配置，
不是即时撤权信号。本轮没有证明运行时额度执行或工具授权。完整 F01–F12 保持进行中。
证据：`artifacts/channel-policy-owner-readers-20260908/`。临时 PG 中顺序应用 0001–0007。

## 2026-09-08 续作：Session/Quota owner 的事务发布命令

- 新增 channelpolicy Publisher 与 PostgreSQL PublicationStore，OWNER/CAS/MAC 幂等
  必须显式注入依赖。事务内重新核对当前 OWNER，并持有租户/成员锁；精确
  tenant/kind/id advisory lock 也覆盖首版本不存在时的并发发布。
- 定义 revision、reference-only 发布和审计 outbox、紧凑成功 receipt 一起提交。
  事务或 receipt 失败不返回成功结果；底层适配器也拒绝漏写 receipt 的发布。
- 0007（当前未发布迁移）补充不可变 owner receipt。复用 control_outbox，不新增
  Workload，不在事件中携带完整策略内容。Distribution=PENDING 不代表运行投影生效。
- Session/Quota 的正向集成数据改走真实 owner Publisher，然后精确读取、校验 digest、
  owner port 适配与 AccessPolicy 发布；SQL 仅用于恶意文档夹具。工具隔离仍为夹具。
- 同键并发只产生一次 publication；不同键并发同一 expected revision 只能一个成功，
  另一个返回 CAS 冲突且结果为空。旧版本保持不变；receipt 故障注入整体回滚后可重试；
  跨租户无 OWNER、OWNER 撤销后重放均拒绝。
- 最终真实 PG/race 测试：279 pass / 1 skip / 0 fail。
  唯一 skip 为已有 JetStream relay（没有配置 NATS），不计为通过。
  Control 全模块常规回归和 Channel/owner vet exit 0。

管理 HTTP/API/Bootstrap/Web、生产工具隔离、正式定义/访问策略事件契约与 relay、
运行投影及 Gateway/Worker 授权链仍待完成。F01–F12 全部目标保持进行中；本轮只验收
定义发布的内部事务能力，不宣称机器人运行时额度、工具授权或完整多租户验收完成。
证据：`artifacts/channel-policy-owner-publisher-20260908/`。没有推送、部署或操作真实 Bot。

## 2026-09-08 续作：ExternalPrincipal 的真实管理 HTTP 入口

- 本轮接通现有 Channel Module，不新增 Workload。实现租户下 channel-principals
  POST 注册和 /{principal_id}/state POST 状态变更；复用原 Session、OWNER、事务和
  MAC 幂等回执。账户归属与 Provider 从可信账户解析，不接受正文 tenant/provider/role。
- 新增三份闭合 JSON Schema、正向 fixtures、公开 OpenAPI 分片及主 API 引用；
  body 16KiB，必需 Idempotency-Key，拒绝 query、压缩正文、未知/重复/大小写变体字段。
  OWNER 在解析正文前校验，事务内再次校验。没有新增外部主体枚举接口。
- 响应只投影 principal 与 NOT_EMITTED，重放保持原状态码和原内容；含认证失败在内
  no-store。返回值与 tenant/account/principal 路径不一致时拒绝，内部账户/路由字段不输出。
- 真实 NewModule + Session middleware + Tenant OWNER + PG + MAC receipt 联合测试
  已执行 principal-http 子测试：创建/重放、撤销/重放、重复身份、旧 CAS、MEMBER
  拒绝、错误账户404，只有两条成功回执。不是仅 handler mock 验证。
- 最终 schema/OpenAPI/Channel/Control integration 真 PG + race：551 pass / 1 skip / 0 fail。
  未配置 NATS 的既有 relay 测试跳过，不计通过。Control 常规回归与 vet 另存证据。

该入口管理 Control 身份事实，不把 ACTIVE 作为运行授权，也不把 REVOKED/NOT_EMITTED
作为撤权已传播的确认。策略定义/AccessPolicy 管理入口、生产工具隔离、事件契约/relay、
Gateway/Worker 当前授权及 F02–F12 仍待完成。完整目标保持进行中。
证据：`artifacts/channel-principal-http-20260908/`。未改 SQL、未推送、未部署、未操作 Bot。

## 2026-09-08 续作：Session/Quota 定义发布 API 与生产装配

- 新增 channelpolicy 模块装配，在现有 Control bootstrap 的 Channel 配置启用分支中
  使用真实 DB、Session middleware、Tenant OWNER reader/transaction authorizer、
  已配置的 request-MAC signer；不是单独服务，也没有 permissive production fallback。
- 新增 POST /v1/tenants/{tenant_id}/channel-policy-definitions/{kind}/{policy_id}/revisions，
  发布可复用的租户定义；账户具体绑定/策略引用仍由账户管理流程另行发布。
  expected_revision=0 表示初次发布。201 和幂等重放都返回原 revision/digest/event IDs。
- 发布器抽取 AuthorizeWrite，使 HTTP 在解析正文前校验当前 OWNER；事务内仍重新校验。
  闭合 Schema 拒绝缺失 enabled、重复/未知字段、混合分支；owner domain 验证 path kind
  与定义分支相符。必需幂等键，16KiB，禁用 query/Content-Encoding，全链 no-store。
- 主 OpenAPI 引用发布分片并通过全文件校验、精确路由清单测试。真实 NewModule +
  Session + Tenant + PG 联合 definition-http 子测试通过：Session/Quota 各一份定义，
  共 2 revisions、2 receipts、4 outboxes；重放、CAS、MEMBER、非法跨分支等负例通过。
- 最终 owner/bootstrap/integration/schema/OpenAPI 真 PG/race：338 pass / 6 skip / 0 fail。
  skip 逐项列在 COUNTS.json，不计为通过。Control 常规回归及 vet exit 0。

PENDING 仅表示待分发 outbox；没有激活额度或 Session 分区。定义查询/Web、AccessPolicy
管理发布、生产工具隔离、正式事件契约/relay、运行投影和 Gateway/Worker 授权仍待完成。
F01–F12 保持进行中。证据：`artifacts/channel-definition-api-20260908/`；没有新增部署或推送。

## 2026-09-08 续作：定义的精确管理读取

- 新增 owner QueryService 和 GET .../channel-policy-definitions/{kind}/{policy_id}/revisions/{revision}，
  接入同一 Channel-enabled Control 模块；使用真实 PostgreSQL owner Reader。
- 当前 unrestricted Session + 当前租户 OWNER 后才读取数据。已知 reference 不是读取权限；
  返回 tenant/kind/id/revision 必须精确相符，完整定义 digest 再校验，内部错误不透出。
- 正整数 canonical decimal revision，无 latest/前导零/正号/指数/越界别名；拒绝 query、
  GET body 和 Content-Encoding。所有响应 no-store，不新增枚举或隐式最新版本替代。
- 真 Session/Tenant/PG 联合测试发布第二版后读取 v1 仍返回原定义/digest；v2 精确返回新
  定义；缺失版本和错误 kind 404，MEMBER/无权限租户403，未登录401。新增读操作不增加
  发布回执/outbox；场景中 3 次发布对应 3 revisions、3 receipts、6 outboxes。
- 最终 owner/Control integration/OpenAPI 真 PG/race：75 pass / 0 skip / 0 fail。
  Control 全模块常规回归与 vet exit 0。首次路由清单测试因期望数组顺序失败；调整新增
  GET 项的词典序位置后重新执行全套，保留 ROUTE_ORDER_RED 原始日志。

定义列表/Web、AccessPolicy 管理发布、生产工具隔离、正式事件契约/relay、运行投影
及 Gateway/Worker 授权仍待完成。读取定义不等价于运行策略生效。完整 F01–F12 保持进行中。
证据：`artifacts/channel-definition-read-20260908/`。本轮没有 SQL 迁移、部署、推送或 Bot 操作。

## 2026-09-08 续作：正式策略通知/审计 v1 契约与真实生产者

- 新增共享 AccessPolicyEvent、PolicyDefinitionEvent，分别覆盖 published/audit 两个
  闭合变体；2 份 Schema、4 份 wire fixtures、编码/解码/篡改负例测试。
- published 变体拒绝 actor/audit 字段；audit 必需 CONTROL_USER actor、固定 action、
  decision/reason。两者均只含不可变引用，不携带 allowlist、正文、external_user 或凭据。
- Shared CanonicalJSON 先验证完整 Schema 再输出 JCS + transport SHA256；transport
  digest 与引用的 policy digest 分开。解码失败返回零结果，拒绝重复/未知字段、错误
  类型/版本/epoch/digest、缺失 audit 字段、零时间，不做宽松 JSON map 消费。
- Control AccessPolicy 与 Session/Quota owner 的实际 PG outbox 生产者改用共享 DTO
  与规范编码；不是仅增加未调用的协议包。真实联合测试从 JSONB 读回全部6条通知/
  审计事件，用共享解码器逐一验证 row event/tenant/revision，并重算 transport digest。
- 最终 shared Schema + Channel + owner + Control integration 真 PG/race：541 pass / 1 skip / 0 fail。
  唯一 skip 为旧 JetStream relay（未配置 NATS）；不计通过。Control 常规回归与 vet exit 0。

本轮事件仍是 reference-only notification，不是授权完整快照、Gap recovery 或 runtime fence。
新的 relay、完整快照获取、Gateway 投影/新鲜度、Worker 授权仍待接通；PUBLIC_LIMITED
联合场景的工具隔离仍为既有显式夹具，不宣称生产工具守卫完成。完整 F01–F12 持续进行。
证据：`artifacts/channel-policy-event-contract-20260908/`；未部署、推送或操作 Bot。

## 2026-09-08 续作：访问策略通知的 PG→JetStream relay

- 新增 AccessPolicyRelay、PG claim/finish 与真实 NATS publisher，Channel Module 提供
  固定自身 scope/source epoch 的 relay 工厂；没有新增 Workload。
- PG 只领取访问策略 published 事件，join 精确 owner revision/account 约束 scope；
  SKIP LOCKED、15s lease、随机 token/attempt fencing，旧持有者不能覆盖新领取者。
  audit/定义/route 不混入该队列。完成回写还匹配 aggregate/revision。
- 发布前校验完整共享 schema、scope/epoch、行身份及 transport digest；16KiB。
  损坏事件保留 FAILED，暂时错误按固定脱敏代码指数退避（最大1min）；broker call 5s。
- NATS 专用 subject/stream，显式 ExpectStream + 非零 durable ACK 才完成回写。
  Msg-Id 使用 tenant/event，防止不同租户相同本地event ID错误去重；不宣称 exactly-once。
- 真 PG+NATS 测试通过 ACK→PUBLISHED、同消息去重、跨租户不同消息、旧 lease拒绝、
  重试、损坏claim拒绝。初次故障注入被数据库事件不可变触发器拒绝；保留失败日志，
  改为 claim 传输层故障夹具，不关闭/绕过数据库触发器。
- 最终 Channel+sharedSchema 真 PG/race：513 pass / 1 skip / 0 fail。
  新策略 JetStream 测试实际执行通过；唯一 skip 是旧 Route relay 专用配置未提供。
  Control 常规回归与 vet exit 0。

当前 relay 已有可调用的模块工厂，但 Bootstrap Run/shutdown 和部署 stream/ACL reconcile
尚未接通，Gateway consumer/完整快照/gap recovery/freshness/Worker授权仍待完成。
审计与定义事件有独立责任，不由该 relay 消费。完整 F01–F12 继续进行，不作全链完成声明。
证据：`artifacts/channel-access-policy-relay-20260908/`；真实依赖均为一次性测试资源。

## 2026-09-08 续作：relay 生命周期与声明式 Stream/ACL 接线

- Control 的 Channel-enabled 装配实际创建 AccessPolicyRelay，并由 App.Run 启动；
  主 context/listener 结束时取消，shutdown deadline 内等待 relay退出后关闭共享资源。
  生命周期测试覆盖开始/取消、listener失败无孤儿任务和截止时间；不新增独立服务。
- deploy/nats/streams.yaml 新增 retained CHANNEL_ACCESS_POLICIES_V1（64MiB/16KiB），
  topology validator/reconciler 支持可选第五条流，保留 FileStorage/DiscardNew/禁止
  Delete/Purge/无自动TTL的既有规则。其他四条流和 durable 保持原用途。
- permissions.yaml 只给 Control 增加精确 policy发布subject，Gateway增加 INFO以兼容
  全拓扑核验；没有新增 policy消费权限或宽泛拓扑写权限。server.conf 已由正式 CLI
  重生成，声明一致性测试通过；Helm 未提前实施。
- 使用本轮生成的 server.conf 启动一次性带鉴权 broker，正式 CLI reconcile exit0，
  字面输出 NATS_RECONCILE=PASS。真实 ACL 测试证明 Control取得该流持久ACK，向Run
  发布/修改policy stream被拒绝；Gateway向policy发布亦被拒绝。
- 最终 Bootstrap/NATS race：66 pass / 8 skip / 0 fail。
  跳过6条专用PG角色测试、initial-operator PG和TLS broker测试；本轮不把它们计为通过。
  Control + Gateway 全模块常规回归、vet exit0（普通回归未设置外部依赖）。

声明与生命周期已接线，但没有修改现有部署。Gateway consumer/pull/ACK、完整策略快照、
Gap恢复/投影新鲜度、Worker授权和剩余F01–F12仍待完成；通知发布成功不代表授权生效。
证据：`artifacts/channel-policy-relay-lifecycle-20260908/`。仅使用一次性 broker验证。

## 2026-09-08 续作：Control 精确不可变访问策略读取

- 既有内部 mTLS listener 新增 POST `/internal/v1/channel-access-policies:resolve`；
  SPIFFE workload 映射必须显式授予 `channel_policy_projection`，接收器/preflight
  权限不隐式升级。公开 OpenAPI 不引入该路由，部署映射未自动授权。
- 严格 schema1/account/reference 请求；reference 精确匹配 id/revision/digest，
  不接受客户端 tenant/scope 字段。授权先于解析，限制16KiB、拒绝压缩/重复字段。
- PG repeatable-read 只读事务读取 catalog epoch，再以受信 scope 与账户租户/provider
  联合查询；重验落库完整文档摘要和行身份。不存在、越scope和精确引用不可用统一404，
  不回退latest。响应有界且no-store，不返回凭据。
- 真实PG通过先发布rev1/rev2再精确取回rev1、摘要/版本/账户/策略缺失、catalog epoch
  不符及另一个合法scope无法读取现有账户的负例；返回文档通过共享闭合schema。
- 真实TLS测试覆盖已映射证书可到达服务、未知SAN403、无客户端证书握手失败；该测试
  使用业务fake返回404，与真实PG/runtime测试分层，不宣称一次TLS→PG全栈联合请求。
- 最终 Control Channel/bootstrap/sharedSchema/OpenAPI 真PG/race：614 pass / 8 skip /
  0 fail。跳过2条专用NATS relay和6条专用PG角色测试；不计通过。Control全模块常规
  回归与vet exit0。只使用本轮一次性PG，不部署、不操作真实Bot、不推送。

此接口只提供投影构建所需的精确历史文档，不是完整当前授权快照，不携带Principal撤销、
水位/Gap完整性证明或freshness fence；不能逐消息调用或把历史策略当作当前授权。
Gateway消费者/快照恢复/持久投影与Admission/Worker授权、管理发布API及真正的工具
隔离守卫仍待完成，完整F01–F12目标保持进行中。
证据：`artifacts/channel-policy-resolve-20260908/`。

## 2026-09-08 续作：Gateway 策略读取适配器与跨服务文档校验

- Admission 下新增 outbound/controlpolicy：固定受信scope/epoch、HTTPS origin、私有
  CA和客户端证书；独立有界TLS1.3传输、不走环境代理/重定向、5秒截止时间、header/
  connection/body限制。只接受published通知，不把audit当读取触发器。
- 请求只发送account+精确reference；响应逐一核对scope/epoch/tenant/account/provider/
  policy/revision/digest。未知身份、文档篡改、越界、压缩、错误Content-Type均拒绝；
  所有失败返回零文档，服务端诊断正文与URL/TLS细节不进入错误文本。
- shared policy_document.go 新增独立wire DTO与完整解码器：闭合schema、规范有序集合、
  opaque群ID字符/字节界限、发布时间表示和完整文档JCS/SHA256；不导入任一服务内部包。
- Control真实PrepareAccessPolicyRevision产出的三种策略均通过shareddecoder，实际PG
  resolver响应也通过；重算摘要后的乱序/重复主体、空白/control/超字节群ID仍被拒绝。
- Gateway真实loopback mTLS覆盖正常读取、全部身份错配、正文篡改/unknown/duplicate/
  null/超大响应、403/404/409/503、redirect/compression/content-type和取消；错误事件
  在IO前拒绝，无客户端证书握手失败。业务响应为夹具，不称为正式Control→Gateway联测。
- 最终SharedSchema+Admission+Control domain/PG，双方真实PG+race：560 pass / 2 skip /
  0 fail。仅跳过未配置的两条Control NATS relay；初轮未设置Gateway专用PG环境的日志
  另存CONTROL_ONLY，最终已补跑。Gateway+Control常规回归与vet exit0。

当前读取适配器尚未接入Bootstrap消费者。它是构建投影的依赖，不会授权、建Run或刷新
授权新鲜度；通知来源认证仍由后续broker adapter负责。持久投影、完整快照/水位/Gap
恢复、撤销及Admission/Worker fence均待接通；完整F01–F12目标保持进行中。
证据：`artifacts/channel-policy-client-20260908/`；本轮无部署、远端推送或真实Bot操作。

## 2026-09-08 续作：Gateway 策略历史持久投影与连续性

- 新增Gateway迁移0012_access_policy_projection.sql（不改历史SQL）：account-local
  projection head与不可变document history。新迁移由既有embed executor自动纳入。
- Admission outbound/policypostgres的Apply重验通知、scope/epoch及完整文档；首次
  INSERT/ON CONFLICT和head FOR UPDATE串行化同账户并发，文档与版本水位同事务提交。
- observed_revision固定已见最大版本，contiguous_revision仅固定从1开始的连续历史；
  乱序3/1/5/2/4的连续水位为0/1/1/3/5，重复旧版不回退。缺口保留为可检测状态。
- 同版本异摘要/账户tenant-provider-policy身份冲突提交blocked_reason后返回失败；
  后续重放/重启/重新应用migration不清除。数据库触发器守住不可变文档、身份和单调
  水位；重复版本还重验已存正文，不仅信任digest索引。
- ReadExact在repeatable-read只读事务中按scope/epoch/tenant/account/ref读取历史，
  重算文档完整性；阻断或缺失不返回文档。5秒操作deadline与独立有界rollback。
- 真实PG/race通过乱序补洞、24次并发重复→12份历史、租户/scope/epoch隔离、冲突
  持久化、SQL不可变约束、延迟commit失败零结果、锁等待取消和损坏首写重放拒绝。
  损坏夹具仅INSERT，未关闭不可变触发器。最终Admission+migrations：262 pass /
  0 skip / 0 fail；Gateway+sharedSchema常规回归、vet exit0。

这一层仅持久化历史内容和账户局部连续性，没有生成fresh_until、当前Principal状态或
授权资格。连续历史仍不证明Control没有更新/撤销；不能用于新Run授权或刷新新鲜度。
读取器与存储尚未接入Bootstrap消费者，完整快照/全局水位/gap恢复、Principal撤销、
Admission事务guard和Worker授权仍待接通，完整F01–F12保持进行中。
证据：`artifacts/channel-policy-projection-20260908/`；未部署、推送或操作真实Bot。

## 2026-09-08 续作：JetStream通知→读取→持久化→DoubleAck

- Admission新增policynats.Step/Run，以Reader/Store接口串接精确mTLS客户端和PG历史
  存储。固定stream创建身份、subject、完整retained历史与显式ACK pull配置；metadata
  来自JetStream而非正文。64MiB/16KiB、禁止裁剪/TTL/变换，变化拒绝继续消费。
- Durable名称由scope完整SHA256派生并强制匹配，避免不同scope共用durable后互相ACK
  丢失通知；同scope副本共享。每durable只保留1个pending ACK，未设有限MaxDeliver。
- 当前scope严格核对source epoch；依次fetch→Apply提交→DoubleAck。其他scope只在
  完整事件/来源校验后ACK，不调用本scope reader/store。audit、坏schema和旧epoch
  不静默跳过；read/store/source失败停止Run并保留未ACK事件，无TERM。
- 真实JetStream+loopback mTLS Gateway reader+真实PG一条测试链证明：HTTP503后
  ACK pending=1/floor=0；deferred commit失败仍pending且无文档；重试同一实际delivery
  成功后pending=0/floor=1。后续Step处理重复1/乱序3/补洞2，3份文档与ACK floor=4。
- 负例验证跨scope不读取、同scope旧epoch不ACK、stream创建身份不符、错误durable
  配置和poison事件不ACK。最终Admission真PG/NATS+race：254 pass/0 skip/0 fail。

HTTP端是策略响应夹具，不是正式Control进程；失败后重试的是同一已取得的真实消息，
未声称broker计时重投验证。NATS使用本轮一次性broker，未证明生产ACL。consumer可调用，
但Bootstrap生命周期、reconciler/ACL、持久跨重启source pin、完整snapshot/cursor恢复
尚未接通。历史ACK不产生freshness或Principal授权，Admission/Worker授权和完整F01–F12
继续进行。证据：`artifacts/channel-policy-consumer-20260908/`；无部署/推送/真实Bot操作。

## 2026-09-08 续作：跨重启持久来源绑定与写入fence

- 新增Gateway迁移0013_policy_source_identity.sql：scope固定source epoch/stream name/
  精确创建时间，重复相同绑定幂等。变化提交SOURCE_CHANGED，历史已存在但从未绑定时
  提交UNBOUND_HISTORY；不通过自动“接受当前来源”掩盖历史缺失。SQL禁止改身份/删除/
  清除阻断，恢复需要后续显式协议。
- PG BindSource取FOR UPDATE；Apply必须已绑定，并持有source FOR SHARE直到提交。
  source阻断与投影写入串行化，未绑定写入拒绝。ReadExact在只读快照内检查已存来源。
- consumer verify接入实际BindSource：拉取前、处理时及ACK前都验证来源。真实测试
  删除并重建同名stream，用新StreamInfo创建新消费者+新Store，仍被持久旧pin拒绝，
  数据库SOURCE_CHANGED且未调用reader；不是仅伪造时间参数的单元测试。
- PG验证12次并发首次相同绑定、两个不同来源竞争后持久阻断、重启不清除、未绑定
  历史不被收编、source共享锁阻止阻断越过写事务，取消的重绑定不留下半完成状态。
- 最终Admission+migrations真PG/NATS/race：270 pass/0 skip/0 fail。现有数据库不变，
  仅使用一次性测试依赖。证据：`artifacts/channel-policy-source-20260908/`。

持久source身份已接consumer，但仍不是处理cursor、完整当前快照或授权新鲜度证明。
Bootstrap/reconcile/ACL、snapshot/缺口恢复、Principal撤销、Admission/Worker授权及
剩余F01–F12继续进行；无部署、推送或真实Bot操作。

## 2026-09-08 续作：连续处理游标、逐位置重放与ACK恢复

- 新增Gateway迁移0014：source processed_sequence/observed_sequence与不可变逐位置
  canonical event digest。旧迁移不变，SQL禁止游标回退和receipt篡改。
- Processed按精确position+digest校验，不仅依赖sequence<=cursor；同位置异内容持久
  阻断POSITION_CONFLICT。RecordProcessed检查本scope已提交策略文档，再原子写位置
  receipt与连续cursor；其他scope仅记录已验证通知摘要，不导入其文档。
- 缺少前序时只提高observed，返回GAP，不推进processed或ACK。ObserveBroker在空闲
  时也比较broker ACK floor与数据库，识别数据库恢复后broker已经领先的遗漏历史。
- consumer已接上述调用：已提交位置重放跳过Control读取，未提交先fetch/Apply，再
  RecordProcessed，最后DoubleAck；文档先提交但checkpoint未提交的崩溃由Apply幂等恢复。
- 真PG/NATS/mTLS测试模拟ACK丢失（实际消息wrapper仅让DoubleAck失败，broker仍pending），
  然后重建consumer+store、令HTTP返回失败，重放仍凭checkpoint成功ACK；并非broker
  计时重投。另实测提前ACK但数据库cursor=0的空闲durable被拒绝且observed=1。
- PG覆盖缺口补齐、foreign scope位置、重启重放、位置冲突、deferred checkpoint commit
  失败不留receipt/cursor、SQL单调/不可变fence。最终Admission+migrations真依赖/race：
  273 pass/0 skip/0 fail；Gateway+sharedSchema常规回归、vet exit0。

处理cursor已接入消费者，但完整当前快照、缺口恢复执行器与retention/compaction仍待
完成；cursor不产生Principal授权或fresh_until。Bootstrap/reconcile/ACL、Admission/
Worker授权与完整F01–F12继续进行。证据：`artifacts/channel-policy-checkpoint-20260908/`。
本轮无部署、推送或真实Bot操作。

## 2026-09-08 续作：Gateway Bootstrap与策略投影生命周期

- 新增GATEWAY_POLICY_PROJECTION_ENABLED，默认false；严格true/false解析，启用时要求
  Control账户模式和policy stream。Compose与.env.example透传默认关闭，不新增Workload。
- App.New实际装配独立policy mTLS reader、PG source/history/checkpoint、scoped消费者；
  Initialize在后台任务启动前校验来源/游标。只读取既有durable，缺失时报错，不创建拓扑。
  初始化失败关闭新HTTP transport；App.Run纳入errgroup，错误触发既有shutdown，App.Close
  幂等释放资源。此开关不等同Principal授权开关，不改变现有Admission资格判断。
- 扩展真实Gateway App.New/Run/Close集成夹具：正式运行生命周期驱动实际NATS通知、
  mTLS策略读取、PG文档/checkpoint=1，再完成既有账户轮换/收件回归及停止。HTTP对端为
  Control/Worker协议夹具，Telegram为已有synthetic remote；不是正式Control或真实Bot。
- 启动缺失durable负例证明不创建consumer；配置/禁用不取依赖及cancel/Close-once测试
  通过。本轮专用Bootstrap真PG/NATS/race：3 pass/0 skip/0 fail（含一项完整runtime集成）。
- 首轮测试HTTP夹具缺少JSON Content-Type，严格reader返回POLICY_CONSUMER_READ；
  修正夹具header后通过，不放宽产品校验。失败日志RED_CONTENT_TYPE保留。

生产生命周期已接通，但reconciler/精确ACL及Control映射尚未更新以自动支持此开关，
默认不启用。当前快照/恢复、Principal撤销、Admission/Worker授权与完整F01–F12仍待
完成。证据：`artifacts/channel-policy-bootstrap-20260908/`；未部署、推送或操作真实Bot。


## 2026-09-08 续作：声明式 scoped durable、精确 ACL 与合入审查

- topology.policy_scopes 显式声明最多 64 个 scope；reconciler 为缺失项创建确定性
  durable，对已有配置做严格兼容检查，不重置 ACK 或覆盖不兼容 consumer。
- Gateway role.policy_scopes 只展开该 scope 的 INFO/NEXT/ACK 权限，不授予策略发布、
  其他 scope 消费或拓扑写权限；默认列表空。运行实例只验证自己的 durable，不要求跨
  scope 权限。启用策略投影时必须声明本实例的 Control.ScopeID。
- Control 工作负载映射仍需操作者显式添加 channel_policy_projection，保留原 scope、
  audience、instance 和已有能力；本次不猜测或自动提升现有 workload 权限。
- Standards 审查发现并修复 P2：先读取 ConsumerInfo，再读取 StreamInfo，避免另一个
  副本在两次读取之间推进 ACK 造成旧 stream tail 的误判。确定性交错测试覆盖正常提交、
  数据库确实落后以及来源重建；保留全部 source/checkpoint fail-closed 校验。
- Spec 审查没有发现基础提交的额外阻塞代码问题；已补最新状态，历史记录保持原样。
- ACL 联合测试首次重复执行暴露测试夹具错误：假设首个 pending 就是本次 publish。
  修正为在有界期限内确认到精确发布 sequence，并核对该序号 payload；不是放宽产品校验。
  ACL fixture 只证明 NATS 传输权限，不冒充 schema-valid 策略或真实授权通过。
- 先前 topology 专用 CLI reconcile、真实 ACL 三项及 Bootstrap 三项均 exit 0。
  本次重跑证据另存 artifacts/channel-main-review-20260908，合入前回归结果见该目录
  VERIFICATION.txt 和 COUNTS.json，包含跳过项清单与首次失败记录。

当前快照/恢复、Principal 撤销传播、有界授权新鲜度、Admission/Worker 事务授权及
完整 F01–F12 验收仍待完成。Bootstrap 使用真实 PG/NATS 和 HTTP 协议夹具，不是正式
Control/Worker 或真实 Bot 联测。本次不新增 Node connector 或部署单元，不提前实施 Helm。

## 2026-09-08 续作：Control 当前授权快照与分页完整性

- 在已合入 8080eca 的基础上新增 0008 数据库 generation：覆盖既有/新建账户，
  principal/head/account 变更在同事务递增，回滚不推进、上限溢出不留部分写入。
- 正式 RuntimeService 和 mTLS listener 接当前 manifest/page；tenant/scope 派生，
  复用显式 policy projection capability。不同于历史 resolver，manifest 读当前策略头、
  account gate 及完整主体集合，含 REVOKED。未向公开 Session router 添加接口。
- 同一 repeatable-read 事务流式生成主体 count/digest；每页最多 128 条并校验 SQL
  generation。索引、排序和游标均用 C collation。共享 proof 检查完整身份、游标链、
  顺序、终止、count/digest，拒绝拼接、截断、篡改和额外页。
- 首轮真实 PG 发现复用 Tenant owner 的 FOR SHARE 与 READ ONLY 冲突；保留 owner
  校验和 repeatable-read，取消 READ ONLY 标志，不写业务数据，保留 5 秒事务上限。
- 真实测试包括 257 主体/三页、两租户相同局部主体/外部 ID、撤销/最新策略/账户门禁、
  未提交写不可见、回滚、scope/epoch、generation 耗尽。另经真实 mTLS 调用正式
  RuntimeService/PG，验证 manifest -> page -> shared complete proof，再经 OWNER
  撤销获得旧 page HTTP 409；非仅 HTTP fake。安全负例单独验证无证书/错误映射和
  缺 capability 在解析前拒绝。精确计数、命令与首次失败保留于本轮证据目录。

captured_at 不晚于最初 MVCC 快照；分页不续期。SQL generation 不是 NATS watermark，
proof 成功不直接赋予当前授权。Gateway 完整安装/新鲜度、撤销传播、恢复执行和
Admission/Worker 同库授权 fence 仍待完成，全部 F01–F12 目标保持。当前修改仅在工作树，
本轮未提交/推送、部署或操作真实 Bot。证据：artifacts/channel-current-snapshot-20260908。

## 2026-09-08 续作：Gateway 当前快照 reader 与原子 PG 安装

- 新增 ReadAuthorization：真实 mTLS manifest -> 精确策略 -> 顺序分页 -> 全量 proof，
  身份/epoch/generation/cursor/count/digest 全匹配；要求当前响应 no-store。整次读取
  和 stage 消耗最初请求起点的 monotonic 年龄预算，409 不隐式重试或续期。历史 Fetch
  行为保留，精确读取不伪造 publication event。
- 新增 Admission domain reader port 与 authorizationpostgres.Store.Refresh；0015
  分离当前 snapshot/head、主体表和事务暂存表。默认运行开关及现有接纳路径不变。
- PG clock anchor 在调用 reader 前取得；暂存不锁活跃 head，完整校验后才短暂串行化
  账户安装。较早开始但晚到达的读取返回 SUPERSEDED；之后开始的版本回退、epoch
  冲突或同版本异内容持久阻断，重启不清除。SQL guard 拒绝回退、篡改及解封。
- 暂存和实际安装行均独立重算摘要；半份读取、错误被吞掉、取消、过期、Copy/trigger
  改写和 deferred commit 失败都不替换旧集合或续期。持久阻断分支首轮漏清 stage 的
  真实测试失败已修复，保留 RED_BLOCK_STAGE 日志；成功/阻断提交不留暂存行。
- 对齐现有数据库 CONNECT-only 约束，采用迁移创建的暂存表和普通 DML，不执行 TEMP
  DDL，不增加 runtime 权限。真实角色测试检查无 TEMP/CREATE 仍可安装 257 条主体。
- 联合测试实际运行 Gateway mTLS reader、精确文档、三页 proof 与 PG 安装，保留 128
  个 REVOKED 主体；HTTP409 回滚 stage 且原 fresh_until 不变。HTTP 对端为协议夹具，
  不冒充本轮正式 Control/真实 Bot/Worker 联测。命令、计数和回滚证据在本轮目录。

当前读取与安装已具备实现，但生产刷新调度/Bootstrap、新鲜度和 blocked 的 Admission
同库检查、Worker 复核、完整依赖解析及全部 F01–F12 仍待完成。数据库时钟稳定性
需要后续测量，不宣称 30 秒撤销 SLA；generation 不是 NATS watermark。无提交/推送、
部署或真实 Bot 操作。证据：artifacts/channel-snapshot-reader-20260908。

### 2026-09-08 — Current snapshot lifecycle and merge review

- Added opt-in `GATEWAY_AUTHORIZATION_REFRESH_ENABLED` (default false) to existing
  Gateway Bootstrap and Compose. No new workload. The qualified Control account
  directory supplies enabled and disabled targets; stale/unhealthy/closed sources
  cancel outstanding refreshes. Four bounded workers refresh without per-account
  overlap; revision changes cancel old work; errors use bounded backoff.
- Added scheduler race tests, qualified-directory clone/outage tests, strict flag
  and disabled-construction tests, plus a real PostgreSQL/NATS/mTLS Bootstrap test
  for ACTIVE installation and periodic REVOKED installation without a catalog
  mutation. The Control endpoint in the Bootstrap test is a protocol fixture;
  the separate Control HTTP integration exercises its real service/store.
- Merge review found and fixed same-policy-revision reinterpretation across newer
  snapshot generations. Application installation persistently blocks it; SQL also
  rejects changes to the immutable revision's digest/body. Fixtures now reuse a
  stable published document; the negative test preserves the prior installed set.
- This is a current-state foundation slice, not completion of F01–F12. Admission
  receipt/authorization transaction fences, Worker independent authorization,
  source recovery and the remaining optimization gates stay open. No real IM or
  deployment acceptance is asserted by this merge. Test evidence and exact source
  rollback are in `artifacts/channel-snapshot-main-review-20260908-0537/VERIFICATION.txt`.

### 2026-09-08 — Admission local current-authorization transaction fence

- Added a same-PG AuthorizationGuard to Admission Store, with current snapshot
  head FOR SHARE versus installer FOR UPDATE, exact immutable policy validation,
  principal/operation/group checks and final DB-time freshness recheck.
- Preserved Receipt-first replay. Determinate denial stores `denied` without a
  Run; unknown/expired/blocked state rolls back and remains retryable. Both local
  ALLOW/DENIED decisions persist actor, route/policy/principal versions plus audit
  intent atomically with the original Receipt/Admission/Outbox transaction.
- Added 0016 and real PG tests for allow/deny/revoke/replay, unknown/expired/tenant
  mismatch, allowed/disallowed groups, PUBLIC_LIMITED dependency hold, concurrent
  deduplication, SQL head lock, final-time expiry and audit-failure rollback.
- Not production-enabled: full policy dependencies and quotas, versioned execution
  fact transfer, Worker current checks, audit relay/retention remain required.
  `postgres/AUTHORIZATION.md` records these explicit next integration boundaries.
- Evidence: `artifacts/channel-admission-authorization-20260908-0545/VERIFICATION.txt`.
  F01 and the full F01–F12 goal remain unfinished; no commit/push/deploy this turn.

### 2026-09-08 — Authorization fact in RunRequested and Worker retention

- Added optional closed authorization wire object and generated DTO. The codec
  checks actor/tenant/account/provider/route/conversation relations and recorded
  observation interval; only ALLOW facts can accompany a Run. Old events remain
  valid, but old strict consumers require upgrade before new emission is enabled.
- Gateway's transaction now writes the actual authorization fact into RunRequested
  Outbox; Worker wire/domain/JSON persistence retains it and includes it in both
  event and logical Run digests. Mutation or stripping changes the identity.
- Worker Claim currently holds such requests as AUTHORIZATION_NOT_READY, preserving
  Receipt replay and allocating zero Attempts. New migration 0005 also prevents
  old/direct SQL INSERT from bypassing the hold. This is a transitional guard,
  not completion of the independent Worker authorization projection/checks.
- Added closed-shape/identity/digest/round-trip tests, real Gateway PG Outbox fact
  assertion, and real Worker migrator/runtime-role PG intake/replay/zero-attempt
  tests including direct SQL bypass rejection. Evidence directory:
  `artifacts/channel-authorization-wire-20260908-0600/VERIFICATION.txt`.
- Next: full policy dependencies/quotas, independent Worker current-state fence,
  tools/approvals, audit relay and mixed-consumer rollout. Production Gate remains
  unassembled; F01 and all remaining F01–F12 requirements remain open.

### 2026-09-08 — Worker-owned current projection and local Claim checks

- Extracted the existing complete-snapshot installer and source port into public
  `platform/channel/authorization` packages. Only compiled Gateway/Worker table
  families are allowed; each owner retains its own pool/schema and migrations.
  Gateway API/errors and integrity/supersession/expiry behavior remain unchanged.
- Added Worker migration 0006 and NewAuthorizationProjection. Claim reads and
  locks Worker-local current policy/principal state, checking epoch/identity,
  revision floors, group/operation scope and DB time. Historical Admission ALLOW
  no longer substitutes for a newer local REVOKED principal.
- Wait reasons now distinguish unknown state, known denial and pending policy
  dependencies. Even a passing local subject/policy check allocates zero Attempts
  until SessionPolicy/TenantQuota and execution gates are complete. Migration0005
  remains intact; production source lifecycle is not assembled by this increment.
- Real Worker migrator/runtime PG tests install a complete independent set, check
  local ALLOW, tenant/epoch/generation/group negatives, update to REVOKED without
  changing Admission, and verify replay, wait reasons, zero Attempts/staging.
  Gateway regression rechecks the shared install implementation.
- Evidence: `artifacts/worker-authorization-projection-20260908-0610/VERIFICATION.txt`.
  Next: independently authenticated Worker source lifecycle, exact referenced
  policy dependencies/quotas, completed execution fence/tools/approvals, audit and
  rollout gates. Full F01–F12 remains open; no commit/push/deployment this turn.

### 2026-09-08 — Worker independent source and main integration review

- Extracted the existing strict mTLS reader and bounded scheduler to public
  `platform/channel/authorization/controlhttp` and `refresh`; Gateway retains
  compatibility adapters. Worker opt-in bootstrap uses its own database, source
  URL, scope/epoch and verified Control identity. Omission disables acquisition.
- Control adds exclusive `worker_channel_authorization`: only the three current
  snapshot/page/exact-policy read routes are allowed. Mixed capabilities are
  rejected; account catalogue, credentials and observations remain denied at both
  HTTP and service boundaries. No IM credential is delivered to Worker.
- Migration 0007 indexes Worker nonterminal authorization target discovery.
  Real Worker migrations + Ledger.Accept tests validate stored JSON keys, scope,
  target deduplication, terminal filtering and lifecycle cancellation/Close.
  A separate real Control RuntimeService/PostgreSQL mTLS test observes ACTIVE then
  REVOKED with the public Worker reader and checks forbidden credential access.
- Standards review requested direct Worker bootstrap tests and current docs;
  these are added. Spec review found no blocking deviation for this deliberately
  default-disabled increment and requested capability/documentation alignment.
  Component integration evidence is not a deployed App/real IM acceptance claim.
- Admission production emission remains disabled. Worker Claim and migration 0005
  retain the zero-Attempt hold, including when local principal/policy checks pass.
  SessionPolicy/TenantQuota, full execution/tool/approval fences and the remaining
  F01–F12 rollout/acceptance gates are not completed by this main integration.
- Review, exact commands, baseline/modified regression and marked-copy rollback:
  `artifacts/main-review-20260908/VERIFICATION.txt`. Source submission is authorized;
  this review does not deploy or change the live Bot configuration.

### 2026-09-08 — Exact runtime policy dependency read path

- Added closed, digest-verified complete SessionPolicy/Quota definition contracts,
  exact dependency binding and detached public Go values. References cannot be
  substituted by kind, tenant, ID, revision or digest; disabled values are not
  silently converted into enabled authorization or quota capacity.
- Added account-authorized Control dependency resolution via the policy owner's
  published read port, production module composition, an internal mTLS route and
  the public bounded/no-store client. Requests name only an exact AccessPolicy;
  tenant and both dependency references are derived from its verified document.
  The exclusive Worker capability permits this fourth read route, not credentials.
- Real owner publication/PostgreSQL -> RuntimeService -> verified Worker mTLS ->
  public reader exercises complete content, cross-tenant/revision rejection and
  credential denial. This is a real dependency-read chain, not a quota reservation
  or an authorized Attempt. New wire corruption/shape tests cover both kinds.
- Evidence: `artifacts/policy-dependencies-20260908-0700/VERIFICATION.txt`.
  Next: atomic dependency installation with current snapshot expiry, runtime
  enabled/session/low-quota checks, actual shared reservations, then complete
  execution/tool/approval fences. Production emission and zero-Attempt hold remain
  unchanged; full F01–F12 is unfinished. No commit, push or deployment this increment.

### 2026-09-08 — Atomic current snapshot dependency installation

- Added CompleteReader and wired both opt-in refresh lifecycles to resolve a full
  exact Session/Quota pair within the original current snapshot deadline. No new
  freshness interval starts after historical dependency reads; DENY_ALL skips them.
- Added Gateway migration 0017 / Worker migration 0008 with paired dependency
  JSONB, account/tenant/kind/reference constraints and same-policy immutability.
  Complete pairs may enrich an old generation; incomplete/old writers cannot
  erase an installed pair for that exact policy.
- Shared installation verifies incoming complete content and reads actual JSONB
  back inside the policy/principal transaction before commit. Invalid, partial,
  downgraded or DB-trigger-mutated dependencies leave the prior head unchanged.
  The DB anchor still precedes network reads, locking and bulk installation.
- Real PostgreSQL tests exercise both compiled owner namespaces, same-generation
  enrichment, deadline preservation, SQL guards and corruption rollback. Gateway
  namespace uses its restricted runtime role; Worker namespace corruption tests
  use the isolated test owner. Separate Worker ledger regression uses its actual
  migrator/runtime split. No live IM or single deployed App claim is made.
- Evidence: `artifacts/dependency-install-20260908-0720/VERIFICATION.txt`.
  Next: require/interpret these dependencies in Admission/Worker current checks,
  enforce session partition/low-quota semantics and shared reservations, then
  complete execution/tools/approval fences. Zero-Attempt hold and production
  emission state are unchanged; F01–F12 remains unfinished. No push/deployment.

### 2026-09-08 — Locked runtime policy dependency checks

- Admission and Worker now read the paired dependency JSONB under the same current
  head lock as account/policy/principal state, then verify complete content and
  exact references before a local allow. Missing/corrupt/substituted dependencies
  stay unknown; complete disabled definitions are a known denial.
- Admission records POLICY_DEPENDENCY_DENIED plus the usual authorization audit,
  creates zero Runs, and replays the first decision. Missing dependencies persist
  no acceptance state. Worker missing/disabled checks stay zero-Attempt waits;
  complete enabled definitions still do not replace actual session/quota gates.
- Shared semantic checks retain explicit partition/limit/reference values. Public
  definitions require per-user partition, explicit positive quota and a provided
  valid ceiling; missing ceilings are not-ready. No default quota ceiling, implicit
  unlimited capacity, process-local budget counter or tool isolation is invented.
- Tests now use real signed complete dependency fixtures instead of placeholder
  references. PostgreSQL tests exercise Admission unknown versus disabled, denial
  replay and zero Run; Worker tests exercise missing -> complete -> disabled ->
  revoked while preserving historical Admission facts and zero Attempts.
- Evidence: `artifacts/dependency-checks-20260908-0740/VERIFICATION.txt`.
  Next: actual Session partition application and shared reservation semantics, then
  complete runtime/tool/approval gates. PUBLIC_LIMITED/production emission remain
  unenabled. Full F01–F12 remains open; no commit/push/deployment this increment.

### 2026-09-08 — Worker conversation registry transaction foundation

- Added policy-bound, tagged per-user/shared scope and generation-derived SessionID
  values. Exact policy revision/digest changes isolate history; tenant/topic/
  deployment/principal isolation and switch-back behavior are covered.
- Added Worker migration 0009 and a transaction-owned registry adapter. New intake
  callbacks select generation while holding the registry lock; reset uses command
  identity replay, expected-generation CAS, immutable result and audit outbox in
  the same transaction. Deferred SQL enforcement rejects naked generation updates.
- The adapter requires current authorization through an explicit transaction port,
  rechecks before commit and distinguishes shared reset operation. No default
  grant, Gateway-owned generation or process-local registry is introduced.
- Real Worker-role PostgreSQL tests cover 8 concurrent duplicate commands, distinct
  CAS contenders, replay after response loss/denial, final authorization failure,
  audit rollback, intake/reset ordering and reopening the durable registry.
- Evidence: `artifacts/session-registry-20260908-0800/VERIFICATION.txt`.
  This is not yet actual Execution intake or `/new` routing: those need the real
  current-authority adapter plus pending authorized-intake/refresh discovery, to
  avoid a projection/intake initialization cycle. The existing hash path is not a
  chosen compatibility fallback; replace it when wiring this registry. Session
  accepted-head/reset integration and full F01–F12 acceptance remain uncompleted.
  No commit, push, deployment or live IM operation in this increment.

### 2026-09-08 — Session current projection authority

- Added a concrete Session authorizer over Worker's current locked authorization
  tables: pinned epoch/scope, account state, complete immutable policy/dependency
  proof, exact SessionPolicy reference/partition, current principal, operation and
  group scope plus DB-time checks. It makes no per-command Control request.
- Added explicit `session.reset_shared` publication/wire enum support; it is not
  inherited from session.new or granted by default. Authenticated conversation kind
  is now part of Session scope and validation, not guessed from provider IDs.
- Registry tests now additionally compose real installed Worker snapshots with
  this authorizer. Both partition modes succeed only with the correct grants;
  missing/disabled/revoked/reference/tenant/group/epoch failures create no command.
  After current revocation, a prior immutable result replays but a new reset fails.
- RouteAuthority is a mandatory separate port, not a default grant. The tests use
  a bounded route-owner fixture; production route/intake assembly and provider
  command routing remain pending. The complete execution/reset chain is not claimed.
- Evidence: `artifacts/session-authority-20260908-0820/VERIFICATION.txt`.
  Next: pending authorized intake and refresh discovery plus the actual route-owner
  adapter; then replace old Session selection, integrate registry/Run transaction
  and reset results, and continue quota/execution/remaining F01–F12 gates.
  No commit, push, deployment, or live IM operation in this increment.

### 2026-09-08 — Pending intake discovery and main integration review

- Worker migration 0010 stores unassigned authorized inputs with immutable first
  request, policy and deadline. Retry does not renew expiry. Pending entries create
  no Execution Receipt, Run, Session or broker ACK; expired entries stop requesting
  authorization refresh but occupy retained capacity until explicit cleanup.
- The refresh directory discovers both active Runs and unexpired pending inputs.
  This provides the discovery primitive before Session assignment, not a complete
  broker intake pipeline. Staging is not yet called by the production consumer;
  atomic registry/Run promotion and retention remain future work.
- Pending and ordinary intake share capacity and identity locks. Main review found
  ordinary Accept could cross a pending identity when capacity was available.
  A reproduced regression now protects same/event/run/admission identities after
  receipt replay: ordinary Accept returns retryable ErrNotReady, never terminal
  ACK or a new receipt. Actual conversion requires the future promoter transaction.
- Review also moved the policy-owner PostgreSQL reader composition to Control's
  process bootstrap and injected the published-definition port into ChannelBinding
  (ARC-206), and aligned Session adapter package naming with Worker (ARC-104).
- Review/test evidence: artifacts/main-submit-20260908/VERIFICATION.txt. This
  integration packages the dependency, Session and pending-input foundations above;
  it does not complete F01–F12, enable production authorization emission, deploy
  services, or establish a new real Telegram/WeCom acceptance result.

### 2026-09-08 — Production receipt-first pending intake

- Worker bootstrap now gives Acceptor the production NewIntake adapter through a
  narrow IntakeLedger port. Existing receipts/accepted Run aliases replay before
  capacity or current authorization acquisition. New authorized input stages in the
  same identity-locked transaction and returns ErrNotReady without Run/Session/
  Receipt allocation. Staging and explicit StageAuthorization share one SQL path.
- The existing NATS consumer retries this non-terminal result with delayed NAK;
  repeated delivery is not ACKed or reclassified as a durable conflict. Inputs
  without AdmissionAuthorization retain their existing wire/intake behavior; new
  policy-bearing inputs never select the old Session hash as a fallback.
- Real PostgreSQL tests compose the production application/adapter path and prove
  pre-Run discovery, eight concurrent duplicate deliveries across adapter instances,
  immutable initial pending state, zero execution facts, historical receipt replay
  at capacity and unchanged non-authorization wire intake. The broker protocol test
  uses a message/consumer fixture, not a real NATS server or live IM account.
- Evidence: artifacts/production-pending-20260908-0758/VERIFICATION.txt.
  Pending-to-Run promotion, concrete RouteAuthority, expiry/conflict terminalization
  and retention remain pending. No release/deployment/push is performed this turn;
  F01–F12 remains active. Next is atomic registry/Run promotion with real route proof.

### 2026-09-08 — Pending route and transaction-bound runtime target proof

- Added a concrete per-event pending RouteAuthority over Execution-owned rows.
  Session scope/actor cannot substitute tenant/account/binding/revision/conversation/
  topic or historical principal provenance; pinned scope/epoch and current external
  mapping/revision floors are checked before the separate current Session grant.
- RouteAuthority now receives the operation. A pending message only proves
  message.send provenance; it cannot be reused as a reset/new-session command.
- Manifest owner provides a narrow transaction Reader which holds a shared advisory
  identity lock against publication/conflict writes until caller transaction end.
  Bootstrap composes it with the existing full manifest+Worker-contract verifier;
  no cross-owner SQL, synchronous Control lookup or permissive target stub is used.
- Real Worker-role tests compose pending intake, current policy projection, complete
  Manifest and Session registry. Caller substitutions, missing/foreign input,
  identity remapping/revocation, and target conflict serialization fail closed;
  the proof creates zero Execution Run/Session/Receipt facts.
- Initial test fixtures used a current generation below the admitted floor and a
  non-numeric Telegram external ID; both were rejected. Corrected fixtures passed.
  Evidence: artifacts/pending-route-20260908-0810/VERIFICATION.txt.
- The composition helper is not yet called by a production promoter. Next: add
  the owner transaction API to select Registry generation, create Run/Receipt and
  consume pending only after final proof, then quota/execution and remaining F01-F12.
  No commit/push/deployment/live IM operation this increment; full goal stays active.

### 2026-09-08 — Registry joins the intake owner's transaction

- Added WithCurrentInTransaction: registry selection, callback writes and both
  authorization checks run in an owner-held transaction savepoint. Successful proof
  releases only the savepoint, not the owner's transaction or registry/proof locks.
  WithCurrent delegates to the same implementation for existing callers.
- Failed callback/final authorization rolls back savepoint writes even if the owner
  incorrectly commits its outer transaction. After successful final proof, the owner
  may consume pending and commit without attempting to reread a deleted input.
- Worker-role PostgreSQL tests cover outer commit/rollback, zero visibility before
  commit, savepoint failure isolation and competing reset blocked until outer end.
  The real pending/current-policy/Manifest proof test also exercises post-proof
  pending deletion followed by outer rollback/restoration; it creates no fake Run.
- Evidence: artifacts/registry-owner-tx-20260908-0820/VERIFICATION.txt.
  Next: actual policy-bound Run writer and guarded pending consumption in this owner
  transaction, plus required reservation and production promotion. No commit/push/
  deployment this increment. Full F01–F12 goal and acceptance remain active.

### 2026-09-08 — Policy-bound Run writer and guarded pending consumption

- Added actual transaction-owned Run/Session/Receipt writes using the Registry
  selection and exact current SessionPolicy/partition. Shared SQL insertion is
  reused by the previous intake path without changing its Session contract.
- Migration 0011 stores immutable historical scope/generation links and defers
  the pending-consumed invariant until commit. ConsumeRunInTransaction verifies
  actual Receipt/Run/request/link identities before deleting the source pending.
  Omitting consumption fails commit, rather than double-counting retained input.
- A mandatory reservation port participates in the SAME transaction; nil/denied
  reservations fail. Tests use a transaction fixture, not production quota logic.
  Production reservation/promoter assembly is still absent; no default grant exists.
- Real Worker-role tests cover missing/denied reservation, forged selection/receipt,
  commit without consumption, outer rollback, successful real Run/link/Receipt,
  receipt replay at capacity, original deadlines and zero Attempt allocation.
  A nanosecond timestamp regression was reproduced and fixed by matching the
  stored PostgreSQL timestamp at microsecond precision, without changing JSON input.
- Evidence: artifacts/policy-run-writer-20260908-0830/VERIFICATION.txt.
  Next: real bounded reservation owner and receipt-first promoter composition,
  expiry/conflict/alias handling and the rest of F01-F12. No commit/push/deployment.

### 2026-09-08 — Shared tenant Run-quota reservations

- Added Worker budget owner and migration 0012 for immutable stable-Run reservation
  records. Complete quota content is independently validated; tenant-wide counts
  span policy revisions/IDs, with explicit zero/disabled denial and retained bound.
- Shared DB locks serialize Run identity, tenant quota and retained storage. Exact
  retries replay the first policy/timestamp before capacity checks; owner savepoints
  let real intake roll back quota with its other writes. No process-local counter.
- Worker-role PostgreSQL tests race eight inputs through two independent pools at
  concurrency1 (one succeeds), and verify identity/tenant/policy isolation, rolling
  minute limits, full-storage replay, malformed content and transaction rollback.
  Historical timestamp fixtures demonstrate no timeout/age-based concurrency refund.
- This is real Run-count quota, not model/tool-cost budget. It intentionally does not
  implement the full PendingRunReservation port or enable the production promoter.
  Settlement/release and retention are not implemented; holds conservatively remain
  held. Next: terminal-proof release plus bounded cost/usage reservation composition.
- Evidence: artifacts/run-quota-20260908-0845/VERIFICATION.txt. Full F01-F12 remains
  active; no commit/push/deployment or live IM operation this increment.

### 2026-09-08 — Execution-proven Run quota release

- Added migration 0013 and a mandatory terminal-proof reader composed with the real
  Execution Ledger. Immutable settlement releases concurrency only for NO_ATTEMPT
  or one SUCCEEDED_ATTEMPT with matching completion and session commit facts.
- Unknown/failed attempts and terminal status without completion remain held.
  Minute-window and retained-history counts remain charged; replay is idempotent.
- PostgreSQL tests cover rollback isolation, concurrent settlement, forged identity,
  immutable settlement and next-Run concurrency versus minute-rate behavior.
- This is component-level Run-count settlement, not model/tool usage reconciliation
  or a production settlement scheduler. Promoter, bounded cost reservation and full
  F01-F12 acceptance remain unfinished; no live IM claim follows from these tests.
- Evidence: artifacts/quota-terminal-20260908-0900/VERIFICATION.txt.

### 2026-09-08 — Main integration review

- Reviewed the accumulated pending-intake, Session owner-transaction, policy Run
  writer and Run-count budget increment against main ae39aed. Standards review
  identified swallowed infrastructure errors; terminal readers now preserve those
  errors while absent facts alone produce NOT_READY. Cancellation, deadline and
  DB-failure regressions cover that distinction. Spec review corrected the budget
  lifecycle description to distinguish implemented components from missing wiring.
- Submission does not enable a production promoter, cost reservation or settlement
  scheduler. Earlier entries describe their execution-time state, not the current
  publication status. Full design completion and live IM acceptance are not claimed.
- Main-integration evidence: artifacts/review-main-20260908-0920/VERIFICATION.txt.

### 2026-09-08 — Managed Run-quota reconciliation

- Worker bootstrap now constructs the real Budget/Execution settlement owner and
  starts it in the managed work lifetime. Existing poll interval, operation timeout
  and scan batch bound its work; shutdown cancels and joins the loop.
- Keyset pagination skips unknown/faulty candidates without refunding them, wraps
  at the tail and revisits late inserts. Multiple Worker instances share immutable
  settlement facts rather than relying on local cursor state for correctness.
- Added bounded quota-reconcile telemetry and non-logging quota-wait metrics.
  Tests cover database pagination, late input, replicas, restart, fault/unknown
  preservation, loop backoff, operation deadlines and in-flight cancellation.
  The real bootstrap composition/loop test frees concurrency after Ledger terminal
  commit and successfully reserves the next Run; it does not exercise IM or models.
- No admission promoter/cost reservation or F01-F12 completion is claimed. This
  increment wires the previously manual Run-count release owner, not billing usage.
- Evidence: artifacts/quota-reconcile-20260908-0950/VERIFICATION.txt.

### 2026-09-08 — Shared operation-consumption reservation and settlement

- Added Budget operation ledger and migration 0014. Stable operation identity binds
  tenant/Run/input/unit/maximum/bound digest; shared locks make maximum consumption
  reservations atomic across nodes. Exact retries replay original facts; new calls
  consume new operation IDs. Tenant/unit totals do not reset on policy-ID changes.
- Mandatory current-policy/bound and final-usage owner ports fail closed. Unknown
  usage keeps the maximum; final usage replaces that charge. Overruns remain recorded
  and fence further tenant/unit reservations rather than disappearing on rollback.
- Append-only reservation/usage records, tenant-unique evidence and bounded storage
  protect replay/retention semantics. Owner savepoints support atomic caller rollback.
- Real PostgreSQL tests cover eight concurrent requests, two pools, final-vs-unknown
  usage, identity isolation, rollback, overrun, reused evidence and dependency faults.
  The proof sources are explicit fixtures: production policy/bound/usage adapters,
  runtime filters, full PendingRunReservation and promoter still need implementation.
  This does not change public Quota policy JSON or claim monetary billing enforcement.
- Evidence: artifacts/consumption-budget-20260908-0920/VERIFICATION.txt.

### 2026-09-08 — Preserve actual SDK usage provenance

- Source inspection found the pinned SDK sums usage chunks and suppresses explicit
  zero/empty-choice reports. Added a bounded, byte-transparent HTTP/SSE observer
  instead of treating normalized SDK counters or default zero as final proof.
- Actual stream reports distinguish absent usage from verified zero, deduplicate
  identical cumulative usage and reject conflicting/incomplete/invalid evidence.
  UsageKnown now propagates through runtime to Processor usage observations.
- Real SDK plus local HTTP/SSE tests cover zero, missing/null, usage-only chunks,
  duplicate/conflicting counts, invalid values and missing DONE. Early usage and
  mixed-response reports remain unknown. Byte-level tests
  cover fragmentation, multiline CRLF, oversized events and multiple requests.
- This does not yet persist a stable operation usage fact or wire ConsumptionReader;
  current budget policy/bound verification and Before/After dispatch remain pending.
- Evidence: artifacts/sdk-usage-20260908-0930/VERIFICATION.txt.

### 2026-09-08 — Persist known model usage before Session Stage

- Added deterministic per-Attempt single-LLM operation identity and migration 0015
  for immutable Execution model-usage facts. Digest binds input/Manifest/parent head
  and counters; unknown usage never becomes a final zero fact.
- Processor now calls the real Ledger persistence method before Stage. Failures stop
  Stage/Completion; later execution/session failures cannot remove committed usage.
- New writes require an authenticated live EXECUTING grant; exact terminal replay
  still authenticates the original Attempt identity, rejects changed counters and
  retains the first timestamp. Eight concurrent real-PG writes produce one fact.
- SDK provenance, Processor order and PostgreSQL owner tests remain distinct evidence;
  this is not full Control/SDK/DB integration or automatic Consumption settlement.
  Current policy/bound proof, exact reservation join and production admission gates
  remain pending. No F01-F12 completion claim.
- Evidence: artifacts/model-usage-facts-20260908-0945/VERIFICATION.txt.

### 2026-09-08 — Bind model reservations to actual Execution usage

- Migration 0016 adds immutable pre-call links. Actual Budget verification binds
  exact operation/input/bound/maximum to a live PREPARING Execution grant using
  the caller transaction; rollback removes both reservation and link atomically.
- Actual Execution ConsumptionReader joins its own persisted usage/link/Attempt,
  recomputes identity digests and supplies final proof to Budget settlement.
  Known consumption survives Stage failure; absent or unlinked zero stays held.
- Real two-pool PostgreSQL tests replace usage/verifier fixtures, cover substituted
  identities, exact replay, settled-receipt denial and late-binding rejection.
  Current policy/enforceable maximum authority remains a fixture. Production
  dispatch, settlement reconciliation and full admission remain unfinished.
- Evidence: artifacts/consumption-link-20260908-1010/VERIFICATION.txt.

### 2026-09-08 — Managed recovery of model consumption settlements

- Separated settlement-only construction from reservation authorization. Bootstrap
  now composes the actual Execution usage reader without a dummy policy authority.
- Worker lifecycle launches bounded consumption reconciliation alongside Run quota:
  operation-ID pages advance past unknown/errors, wrap, and replay safely on restart.
  Poll delay, operation deadline, shutdown cancellation and join share existing limits.
- Real PostgreSQL covers concurrent scans, late keys, held unknowns and failed owner
  reads. Actual bootstrap loop settles persisted model20 after Stage failure and
  allows next80 under a fixture cap100. No model/provider or live IM test is implied.
- Production new-call authority, Before-model dispatch and complete admission still
  remain pending; recovery only settles existing proved facts and never grants calls.
- Evidence: artifacts/consumption-reconcile-20260908-1030/VERIFICATION.txt.

### 2026-09-08 — Preserve completed usage across failed runtime output

- Fixed a real failure path: invalid Final/local snapshot errors discarded complete
  model usage before it reached durable accounting. SDK cleanup now retains only
  final accounting evidence on error and clears output/candidate data.
- Runtime forwards accounting-only failures without allowing Stage. Processor
  persists known usage before FailAttempt, while the original execution still fails.
- Red/green actual SDK tests cover empty Final with reported/zero/missing/incomplete
  usage; runtime and Processor tests separately enforce no successful promotion.
  Cancellation/incomplete drain remain unknown. Production authority still pending.
- Evidence: artifacts/failed-model-usage-20260908-1050/VERIFICATION.txt.

### 2026-09-08 — Publish explicit cumulative model-token budget

- Added optional max_total_model_tokens to quota owner and consumer contracts,
  JSON schemas and public/internal OpenAPI. Missing is unconfigured; zero remains
  explicit. Existing definitions omit the field and retain their prior digest.
- Control candidate cloning detaches the optional scalar. Signed owner-to-consumer
  tests check bounds, malformed values and digest stripping/substitution. Actual
  PostgreSQL publication checks cap-bound receipts, immutable revisions and outbox.
- This supplies the policy configuration source, not current authorization or a
  maximum-call proof. Worker admission/Before-model budget authority remain pending.
- Contract: model-token-budget-contract.md.
- Evidence: artifacts/model-budget-policy-20260908-1110/VERIFICATION.txt.

### 2026-09-08 — Read model budget under pending/current authorization

- Added PendingRouteAuthority.ReadModelBudget, binding the published cap to a
  persisted pending event and current principal, exact Session policy and Manifest.
  Snapshot locks remain in the caller transaction; DB freshness is checked again
  after target/decode work. Missing cap is not-ready and zero remains explicit.
- Real PG17 projection/pending/Manifest tests cover newer policy caps, scope/actor
  substitutions, revocation, missing fields and stale snapshots; no Run/Attempt
  allocation occurs. Source reader is a fixture, not real Control HTTP evidence.
- This is a current-policy evidence port, not a maximum-cost proof or completed
  ConsumptionAuthority. New-call reservation and dispatch composition remain pending.
- Evidence: artifacts/current-model-budget-20260908-1130/VERIFICATION.txt.

### 2026-09-08 — Gate the actual SDK request before model transport

- Added attempt-local ModelCallGate with final serialized request bytes/digest,
  model, output maximum and stable tenant/Run/Attempt identities. No credential
  headers enter the gate. Inspection is bounded, detached and non-mutable.
- Actual SDK tests verify gate-before-send, exact wire equality, zero sends after
  denial/mutation, sanitized errors and no second send on 307 redirect. Invalid
  model/limit, oversized body and cancellation are rejected before authority.
- Nil gate retains legacy behavior; this seam is not production ConsumptionAuthority
  or a token-count bound. Processor transition/atomic pre-call reservation must be
  composed before enabling managed model dispatch. No hard budget claim.
- Evidence: artifacts/model-dispatch-gate-20260908-1150/VERIFICATION.txt.

### 2026-09-08 — Sequence governed reservation before EXECUTING

- Added explicit ModelAdmission and GuardedAttemptRuntime ports. Authorization-
  bearing Runs require both; neither missing component falls back to legacy Execute.
- Processor validates actual request digest/model/output/identity, rechecks the grant,
  calls admission while PREPARING, then marks EXECUTING before network send. Repeat
  callbacks are fenced; transition failure sends nothing and retains any committed hold.
- Runtime forwards the actual SDK gate and rejects direct legacy calls on governed
  Runs. ModelCall is now an execution-domain type, not an application-to-SDK dependency.
- Tests cover ordering, denial, transition failure, missing ports, repeated callback,
  digest substitution and actual SDK forwarding. Admission/Ledger tests use fixtures;
  real maximum-bound/policy/reservation implementation and bootstrap wiring remain pending.
- Evidence: artifacts/governed-dispatch-20260908-1210/VERIFICATION.txt.

### 2026-09-08 — Make model dispatch transition single-use across nodes

- Reproduced eight successful MarkExecuting calls sharing one valid grant across
  two pools. Replaced idempotent status UPDATE with PREPARING/null-start CAS.
- Migration0017 rejects repeated old-code EXECUTING updates, backward state changes
  and start timestamp rewrites; legitimate renewal, usage and terminalization remain.
- Real PG race test verifies one permit/seven fenced, restart denial and token
  forgery rejection. This is a one-time transition, not exactly-once HTTP delivery;
  uncertain commit remains conservative and never authorizes automatic resend.
- Real maximum-bound/admission composition still remains pending. No goal completion.
- Evidence: artifacts/single-dispatch-20260908-1230/VERIFICATION.txt.
