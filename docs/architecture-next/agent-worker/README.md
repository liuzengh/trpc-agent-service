# Worker V1：持久执行、固定快照与最终回复

> 多后端数据迁移的当前实现、冻结/校验/切换边界见
> [Worker 多后端迁移 V1](backend-migration-v1.md)。

- **设计状态**：V1 范围已按 2026-09-07 评审收敛；Session 接缝已取得真实 PostgreSQL spike 证据。
- **实现状态**：Worker、Control 分发/授权、Gateway Reply 与部署接线已有代码；真实 PG/NATS/SDK fixture 纵切已通过。真实模型与 Telegram 联合验收尚待完成，精确门禁见 [实现状态](implementation-status.md)。
- **基线**：`worker` 分支，提交 `661e4a826ce8f539d8f610b3f1ef99cdbe4a4fdd`。
- **编写日期**：2026-09-06；**修订日期**：2026-09-07。
- **术语**：[Execution 术语表](CONTEXT.md)。
- **阅读顺序**：先读 §1–3 的所有权和运行路径，再读 §4–10 的契约与事务；实现按 §15 推进，
  验收使用 §16，设计决策与未决门禁集中在 §17；SDK 只读证据见 §9.5。

本文区分三种陈述：**已有约束**来自当前规范与代码；**V1 草案选择**是本次建议的确定方案；
**实现门禁**是进入相应切片前必须取得的协议或实验结果。文档写成不等于 Worker 已实现，
测试替身成功不等于真实模型、Storage 或外部渠道已经通过验收。

## 1. 目标、现状与交付终点

Worker 接管一个已固定运行目标的请求，将它变成可恢复、可隔离、可验证的执行结果。
它不是“订阅 NATS 后调用一次 SDK”的循环，也不是 Control 配置的另一个解释器。

**已确认的首版完成定义：Telegram 文本输入 → 单 LLM → 正式 Session → Final 回复。**
Manifest 获取、LLM 装配、执行与会话接受作为一条纵向切片，不先建设独立的调度、组合或预算平台。
首版交付路径是：

```text
真实 Telegram 文本 → Gateway Admission → RunRequested
→ Worker 持久接管 → 固定 Manifest → 有效 Attempt → 真实模型与 Session
→ 已提交 Completion / Final → ReplyIntent → Gateway Delivery → 原会话回复
```

### 1.1 当前已经具备什么

| 部分 | 当前事实 | Worker 交付需要补齐的接缝 |
| --- | --- | --- |
| Control Publication | 不可变发布与 PENDING Outbox；新增 Manifest Relay、投影与 Owner Export | 与真实模型/渠道的同次联合验收 |
| Gateway 入站 | ChannelAccount/Binding、路由 Relay、真实 Telegram 到持久 RunRequested 已有验收；Worker 接管已实现 | 真实两轮消息与 Worker 账本关联 |
| Profile 凭据 | CheckUsable/Resolve、真实 Execution 在线回查、mTLS 与完整批次校验已接线 | 实际 Control/Provider 联合轮换和故障验收 |
| Gateway Delivery | Final、Acceptor、Sender/Runner；新增 Reply Consumer 与已提交证明查询 | 真实 Telegram 完整回复与重启验收 |
| Worker | 已新增生产目录、tRPC-Agent-Go v1.11.2、持久账本与正式 Session Adapter | 真实模型/Telegram 联合验收与完整首版故障矩阵 |

证据入口：[当前架构路线](../README.md)、[Deployment 阶段表](../control-api/deployment.md#16-实现状态后续顺序与门禁)、
[Gateway 最新实施记录](../channel-gateway/implementation-status.md)、[NATS 当前拓扑](../../../deploy/nats/streams.yaml)。
当前拓扑已有 Route、Run、Manifest、Reply 四个 Stream 与独立 runtime ACL；新增接线的本地验证与真实渠道验收分别记录。

### 1.2 V1 的范围与本次减法

- 一个独立 `agent-worker` Workload，两个业务 Module：`manifest` 与 `execution`。
- 首版只运行单个 `llm` 根节点、`openai_compatible` 模型和必需的 `postgres_state/session`。
  Telegram 首版验收使用私聊文本、两轮会话；Run/Attempt/Fence、持久恢复和 Final 是同一闭环的保证。
- W1 从真实已发布 Manifest 一直打通 LLM 装配、执行、正式 Session 与 Completion/Final Outbox；
  W2 接通真实 Telegram 的入站与回复，并验证关键故障窗口。W1 与 W2 通过即达到本文的 V1 完成定义。
- Memory 整体后置：不做 preload、读取策略、自动抽取、写入、导入或 Memory Tools。
- sequence/parallel/loop 后续按 SDK 已有能力接入；不补一层分支隔离、确定合并或自定义调度器。
- 累计 Token 预占与结算后置。首版不增加累计 Token Policy、不私设 Token 总额、不暗中缩短模型输出。
- Session 采用“外部不可变候选内容 + Execution 原子接受引用”的最小方案进行 spike；
  删除双份正式正文、完成后投影、外部连续水位及下一 Run 补投协议，详见 §7。
- Worker 不管理 Agent/Profile/Deployment/Binding，不读 Control 私有表，不接收 Bot Token，
  不承担 Control Outbox Relay，不直接向 Telegram/企微发送消息。
- 后续能力、开放条件与验收单列于 §15.2/§16.2，不再作为首版的隐含完成条件。

当前发布门禁、Session Adapter 与 SDK 已有代码和本地验证；本文的范围确认不替代 §16 中尚未完成的真实联合验收。

## 2. 所有权、Module 与 Interface

已有架构约束见 [ARC-002/004](../constraints.md#arc-002配置运行热路径与凭据启动接口分离)。

| 拥有方 | 拥有的事实 | 协作方式 |
| --- | --- | --- |
| Control Deployment | 已发布 Revision、完整 Manifest、发布 Outbox | 受信事件和非热路径回填 |
| Control Profile | 私有凭据、当前值与用途授权 | 内部批量 Resolve，不开放表读取 |
| Gateway Admission | 首次输入、固定 RouteSnapshot、原 ReplyContext/ReplyOrigin | RunRequested；Gateway 内部回复只读 Interface |
| Worker Manifest | 已验证、不可变的运行快照投影 | Execution 使用自己定义的只读 Port |
| Worker Execution | Inbox、Run、Attempt、Session 顺序、Completion、SessionCommit、Reply Outbox | 内部事务与受认证证明查询 |
| Gateway Delivery | Final 接纳、分段、外部调用与发送结果 | 验证 Completion 后独立交付 |

### 2.1 两个业务 Module，不先拆出调度平台

**V1 草案选择**：先建 `manifest` 和 `execution` 两个业务 Module。

- `manifest` 的 Interface 提供固定身份读取、不可变投影应用和恢复状态；快照的严格校验、
  去重、冲突隔离、持久化与水位留在 Implementation 内。
- `execution` 的 Interface 提供持久接管、有限执行推进、当前 Attempt 授权和已提交 Final
  查询；Run/Attempt/Fence/Session/Completion 的一致性集中在同一个 Module 内。
- tRPC-Agent-Go 是 Execution 的 outbound Adapter。Application 在使用方定义
  `AgentExecutor`，Adapter 返回有界的执行结果和 staged Transcript，不直接宣布 Run 成功。
- 调度循环、凭据客户端、Reply Relay、Session 持久化都是明确职责的 Implementation，
  不因存在一个 goroutine 就新增业务 Module。

需要真实变化点时才引入 Port：Manifest 只读来源、执行账本、Profile HTTP、SDK 执行与外部
Session Store 各有不同故障/测试实现；不为每个 Domain 方法另造一层透明转发接口。

### 2.2 编译依赖与运行调用不同

```text
编译依赖：cmd → bootstrap → wiring → adapter → application → domain
运行调用：Consumer → AcceptRun → ExecutionLedger
          Scheduler → ExecuteAttempt → AgentExecutor → tRPC-Agent-Go
```

Domain/Application 不 import NATS、Gin、pgx 或 SDK；跨 Workload 只共享 `api/` / `gen/` 的协议。
Worker 不 import Control/Gateway 的 `internal`。尤其不直接复用 Control 的
`deployment/domain.PlatformExecutionContract` 或 Profile 的内部 DTO 来绕开服务本地 `internal`。
Worker Consumer Contract 通过版本化 Schema、生成 DTO、Golden Fixture 与一致性测试对齐。

## 3. 一条 Manifest → LLM → 执行纵向切片

```text
Control Publication Tx
  └─ Manifest Outbox → Control Manifest Relay → Worker Manifest Consumer
                                                   └─ 完整不可变 projection Tx
Gateway Admission Tx
  └─ RunRequested Outbox → NATS → Worker Run Consumer
                                      └─ 接管 Tx：Inbox + SessionSequence + Run → ACK
Execution（同一条纵切，非独立平台阶段）
  └─ 读取固定 Manifest / 校验首版运行矩阵
      └─ Claim Tx：Attempt + 当前 Grant + Session 执行资格
          └─ Profile 一次批量解析 Model/Session 凭据
              └─ 读取已接受 Session → 构造单 LLM + Runner + Attempt overlay
                  └─ SDK 执行 → 收束输出与错误 → 持久写入不可变 SessionCandidate
                      └─ Completion Tx：复核 Fence + 接受 candidate_ref/digest
                                         + Run/Attempt 终态 + Final/Reply Outbox
                          └─ Reply Relay → NATS → Gateway Reply Consumer
                              └─ 完成证明 → Delivery 接管 → transport receipt → ACK
                                  └─ 既有 Delivery Runner 发送与恢复
```

W1 测试从真实 Control 发布和真实 NATS RunRequested 进入，不以直接构造 SDK Agent 代替纵切；
W2 将该入口连接到真实 Telegram，并观察最终回复。每个切片同时包含所需的 Domain、
Application、Adapter、迁移、wiring 和故障测试，不先逐层做完全部模块再集成。

网络调用不占用 Execution 事务。外部候选内容先耐久，Execution 再原子接受引用；两库不是
分布式原子提交。外部成功、本地未接受的内容不可见于正式历史，故障规则见 §7.3。

## 4. RunRequested：先接管，再执行

### 4.1 不新增另一套输入协议

沿用 [RunRequested Schema](../../../api/events/execution/v1/run-requested.schema.json) 和
[严格 Codec](../../../api/events/execution/v1/schema.go)，Subject 为 `execution.run-requested.v1`。

- 输入固定 `event_id / admission_id / run_id / route / input`。
- Route 已固定 Provider、Account、Tenant、Binding、RouteGeneration、DeploymentRevision、
  Manifest Ref/Digest；执行期间不改选最新版本。
- `input.kind=text`，文本、外部会话身份与 typed ReplyContext 遵守现有跨字段约束。
- `source_digest` 是 Gateway 原规范化输入的摘要；Worker 不从缩窄后的 RunRequested 重算它。
- 现有上限是完整事件 1 MiB、文本 65,536 UTF-8 bytes，保持未知字段/重复 key/非法 Unicode
  等严格拒绝规则。Schema 校验本身不授予 Tenant 或执行授权。

Consumer 使用受限 NATS 身份，信任经过授权的 Gateway Publisher，而不是信任任意自报 Tenant。
协议 DTO 在 inbound Adapter 中转换为 Execution 自有值，不直接作为 Domain 或数据库 Row。

### 4.2 接管事务与幂等

**V1 草案选择**：一个接管事务写入：

1. ConsumerReceipt：稳定事件身份、完整已验证事件的 canonical 业务摘要、处理结果和 RunID。
2. SessionScope/Session：取得 Session 行锁，给新 Run 分配单调 SessionSequence。
3. Run：保存不可变输入、RouteSnapshot、Session 身份、初始状态和已固定的执行策略版本。

同一 `event_id` 同业务摘要重放 Receipt；同身份不同内容持久记录冲突，不覆盖首次决定。
`run_id` 与 `admission_id` 各自约束到唯一逻辑 Run；换一个 event_id 重新指定已有 Run 的输入
也不创建新执行。Broker source/sequence 与 raw byte digest 只标识传输观察，不代替业务幂等键。

事务提交后 ACK；提交结果不确定时先以稳定身份重查，确认接管事实后再 ACK。重复输入不因
今日 Manifest 缺失、Profile 不在线或 Session 已推进而丢失原 Receipt。

确定坏 wire/确定冲突先持久隔离，再确认传输；PG、容量、内部 Codec 故障延迟重投。
隔离写入失败时不 ACK。未知字段拒绝与内部 Schema 初始化失败须区分。

这里按 [现有精确交接约束](../channel-gateway/module-boundaries.md#141-传输与恢复不是第五个业务-module)
解释 ARC-004：ACK 确认的是 Inbox + Run 的持久接管，不等待模型结束。

## 5. Manifest Distribution 与运行投影

### 5.1 发布与消费的职责

沿用 [RuntimeManifestPublished Schema](../../../api/events/control/v1/runtime-manifest-published.schema.json)，
Subject 为已有协议预留的 `control.runtime-manifest.published.v1`。

- Control 自己的 Relay 负责发送当前 PENDING Manifest Outbox。Worker 不读/更新 Control 表。
- Manifest Consumer 需要独立的严格 decoder、DTO 与应用事务。现有 Control Route decoder
  是路由专用，16 KiB 的路由限制不适用于 Manifest。
- Content 默认平台上限 512 KiB；完整发布 Event 默认上限 1 MiB，最终以发布与运行双方
  同一版本的 Consumer Contract 为准，包含 Envelope/Trace 的大小也要核对。
- 投影应用事务同时保存 ConsumerReceipt 和完整内部快照。相同身份同内容幂等，不同内容
  标记冲突并阻止首次执行；不以最后到达覆盖已验证快照。

### 5.2 执行前验证

验证 [完整 Envelope 与 Content](../../../api/schemas/deployment/v1/runtime-manifest.schema.json)：

1. 各层已有的重复身份逐项一致：Event/Manifest Envelope 的 DeploymentRevision、Manifest
   身份与 Run 匹配；Content 的 Tenant 与 Envelope/Run 一致。Content 不新增后两种身份。
2. 按既定规范重新 canonicalize Content，计算 RFC 8785 SHA-256，与固定 Digest 一致。
3. Schema、Compiler/Runtime Contract、平台 Contract 身份与所需 Adapter Version 受支持。
4. 计划 root/节点/边、资源闭包、Storage roles、节点 callable entries 完整且类型一致。
5. 满足固定 Manifest 的显式执行限制与 §9.1 首版能力契约；不叠加未声明的累计 Token 或模型参数限制。

公开 `manifest_view` 是脱敏视图，不是可执行 Manifest，也不是内部 Digest 的重算输入。
Worker 不在运行时重新做 Slot 名称匹配，不追随 Profile live 配置；live 凭据值另按 §8 解析。

### 5.3 乱序与最小恢复

Run 可先于 Manifest 到达：先持久接管，记录 `wait_reason=MANIFEST`，不开始模型调用。
Manifest 应用后由有限扫描唤醒，不依靠一次性内存通知保证正确性。

首版限定一个共享 Execution DB 的运行组，Manifest 只新增、不做历史 GC。初始化固定一种
协议：先建立保留增量的 durable，再由 Control Owner 分页导出完整不可变 Manifest，按稳定
身份和 Digest 幂等写入，随后追赶已保留增量。W1 必须固定导出分页/完成标记、增量起点、
保留窗口和追赶检查；跨窗时从 Owner 重建，不拿公开 View 或 Draft 补正文。
空集合也是需确认的初始化结果。现已实现受认证的分页/完成接口与 Worker 初始化接线，
字段和边界见 [Control Worker Runtime](../../../services/control-api/WORKER_RUNTIME.md)。实际验收分开记录
空导出/仍保留的增量重叠，以及唯一 Manifest 增量源丢失后从 Owner 重建；当前 MaxAge=0，
后者不声称经历了按时间淘汰。重建时其余 Stream/durable、原 Run 和正式 Session 保持原事实，
不将整个 broker 灾难恢复或多个运行组协议混入首版。

首版不建多运行组恢复、在线删除或通用快照切换框架。缺失等待超过显式期限后结束 Run；
容量接近上限时报警并拒绝新增工作，保留已接管事实及其恢复来源。

## 6. Execution 模型、状态机与 Fence

### 6.1 最小持久事实

下列按逻辑事实分组；已实现的物理表/索引见 `services/agent-worker/migrations`，
Grant 保存在 Attempt 记录内，不另建通用授权账本。

| 记录 | 核心不变量 |
| --- | --- |
| ConsumerReceipt / Rejection | 首次决定稳定；正文清理不删除幂等身份 |
| Run | RunID、AdmissionID、Tenant、输入、目标、SessionSequence 固定；至多一个 Completion |
| ExecutionAttempt | 属于一个 Run；AttemptID 每次重试新建；ExecutionGeneration 单调递增 |
| ExecutionGrant | Worker、LeaseEpoch、有效期、Run/Attempt/Manifest 绑定；只保存令牌摘要 |
| Session | Scope 唯一；接纳/终结序号连续；accepted_head 指向已接受会话内容 |
| Completion | Run 的不可变终态；可带一个 Final 或明确的无回复原因 |
| SessionCommit | 与 Completion 同事务记录接受的 candidate_ref/digest；失败仅推进队列，不改 accepted_head |
| ReplyOutbox | 固定 IntentID、完整已授权 wire 与 Digest；传输状态不改变业务内容 |

业务唯一键、外键、读写条件都保留 Tenant 范围。全局 RunID 唯一也不取消租户条件。

### 6.2 Run 状态与等待原因

```text
QUEUED ──取得执行资格──> RUNNING ──完成事务──> SUCCEEDED
   │                       │                   （终态）
   │                       ├─可重试失败──> RETRY_WAIT ──到期──> QUEUED
   │                       └─永久失败/期限或重试次数耗尽──> FAILED
   └─未开始即过期/确定无效────────────────────> FAILED
```

`wait_reason` 与业务状态分离，可取 MANIFEST、SESSION_HEAD、CAPACITY、
RETRY_BACKOFF；不要把每个依赖的排列组合制造成新状态。重试不改变 RunID、输入或 Manifest。

Attempt 状态：`PREPARING → EXECUTING → SUCCEEDED | FAILED | ABORTED`。
PREPARING 包含资源校验、凭据初始化、Session 读取与装配；`agent_started_at` 只在真实执行
开始时设置。失租/进程关闭造成的 ABORTED 与用户取消不同，V1 不把它包装成已实现的 Stop。

### 6.3 Claim、续租、完成与恢复

- Claim 在短事务中锁定所属 Session 与目标 Run，确认最早未终结序号、前驱均已终结、
  Run 状态、容量和期限，分配新 AttemptID/ExecutionGeneration/LeaseEpoch。PREPARING 只读取
  已接受的 Session head；读取依赖故障按有限重试处理，不修复另一 Run 的外部写入。
- 同一 Session 同时只有一个可调用 Agent 的有效 Attempt；同一 Run 也只有一个有效 Grant。
- 续租只延长当前 Grant 的有效期，不改变 Epoch。LeaseEpoch 与 ExecutionGeneration 独立命名，
  即便首版经常一起递增也不构成语义别名。
- 所有终态提交在取得锁后使用数据库时间复核 Run 当前 Attempt、Worker、Epoch、LeaseUntil。
  仅在事务开始前预检一次，或仅依靠 context 没有取消，都不足以授权提交。
- lease 失效立刻取消 SDK 和后续外部调用资格；已写出的外部请求仍可能产生结果。
- 恢复扫描只接管已过期/已终止资格；绝不在另一有效 Grant 存在时重分配。恢复新建 Attempt，
  不跨进程复活原 Attempt 的内存凭据。
- 全部事务采用稳定锁序 `Session → Run → Attempt/Grant → Completion/Outbox`；批量任务按
  稳定 Session key 排序，查询授权不反向修改 Session。不得持有这些锁等待 Profile/SDK。

### 6.4 Completion 是唯一提交点

有效 Attempt 的完成事务复核 Fence，提交 Run/Attempt 终态与唯一 Completion，按策略写入
Final/ReplyOutbox，释放执行资格并推进 Session 队列。仅成功 Completion 接受 SessionCommit
的候选引用并更新 accepted_head；FAILED 保持原 head，不制造空候选引用，可按策略产生错误 Final。

可重试 Attempt 失败只结束该 Attempt 并令 Run 进入 RETRY_WAIT，不创建 Run Completion。
事务失败保留原状态；事务响应不确定时先查稳定 Completion/Intent 身份，不重新调用模型。
迟到旧 Attempt 的候选输出不得替换已提交结果。数据库的每 Run 唯一 Completion 与 Final
约束是最后防线，NATS 去重窗口不是业务唯一性保证。

无有效 Attempt 的终结走单独的 **SystemTerminalizer**，覆盖从未开始即过期，以及最后一个
Attempt 已失租/崩溃、重试额度耗尽或 RETRY_WAIT 到期等情况。它按相同锁序取得
Session/Run/最后 Attempt/Grant，使用 DB 时间和 CAS 确认不存在有效 Grant、Run 仍未终结
且满足到期/永久失败/期限或重试次数耗尽条件，必要时把旧 Attempt 标记 ABORTED，再原子提交 FAILED
Completion、Session 队列推进、稳定原因和 `reply_disposition=NONE`，accepted_head 保持不变。它与 Claim 相互排斥，
不因拥有系统恢复权限而接受旧 Attempt 的候选输出。

从未创建 Attempt 时 Completion.attempt_id 为空；已有 Attempt 时最后身份只作审计引用，
`completion_kind=SYSTEM_TERMINATION` 不产生 Final 授权，不伪造一个新 Attempt 绕过最大次数。
队列终结序号必须连续推进，非队首过期也不跳过更早 Run。失败通过受认证运维查询/告警可见，
V1 草案不承诺这类输入必有 IM 错误回复。

## 7. Session：隔离、顺序与最小接受协议

### 7.1 会话身份的保守首版选择

**V1 草案选择 D07**：

```text
SessionScope = (tenant_id, provider, account_id, conversation_id,
                normalized_thread_id, binding_id, deployment_revision_id)
```

使用明确编码后的元组，不使用有歧义的字符串拼接。数据库为 Scope 分配内部 SessionID。

- 不同 Tenant、机器人、群/私聊、topic、Binding、DeploymentRevision 相互隔离。
- 群内同一 topic 的参与者共享该会话历史；不使用 sender_id 偷偷制造每人独立群会话。
- 切换 DeploymentRevision 创建另一 SessionScope；切回旧 Revision 恢复该 Revision 的历史。
  Profile/Agent 更新只有经新 DeploymentRevision 才改变 Scope，live 凭据轮换不重置历史。
- RouteGeneration 不进入 SessionKey，账户停用/恢复不自动清空历史。
- V1 不提供 Reset 命令，SessionGeneration 固定初始代次；未来显式 Reset 的授权、在途 Run
  与历史保留需要独立协议，不借进程重启实现重置。

这是偏向版本隔离的选择，代价是升级版本不自动继承对话；D07 接受前需确认这个产品行为。
本文不新增 Environment、跨渠道身份合并或配置迁移语言。

### 7.2 顺序的精确定义

SessionSequence 按 Execution 接管事务的持久提交顺序分配；它不声称恢复 Provider 原始发送
顺序，也不把 JetStream 并发拉取顺序当成提交顺序。相同事件重放不分配新序号。

同一 Session 串行执行；不同 Session 通过全局并发上限与简单轮询有界并行，首版不做加权公平
调度平台。存在 RETRY_WAIT 的队首时后续 Run 等待；Run 重试与排队有显式期限，防止永久阻塞。
失败只推进队列终结序号、不修改正式会话内容；终结序号也必须连续推进，
禁止以 `max(sequence)` 越过未完成前驱。若未来要求精确上游顺序，应扩展受信接纳协议。

### 7.3 内容先耐久，引用后接受

`storage.session` 仍是 Manifest 必需角色，指向固定 Profile 的 PostgreSQL 目标。
V1 部署允许同 database 的 `worker`/`runtime_session` 两个 Schema；账本与 Session 连接身份
仍分开，不静默绕过 Profile 改用账本连接，也不强制不同物理数据库。首版不选择 `storage.memory`。

**D08 修订草案**：Profile Session Store 保存不可变会话内容；Execution 只保存接受事实及
内容引用/摘要，不另存完整正式 Transcript，不建立跨库复制与 projector。

1. Claim 后，从 Execution 的 accepted_head 取得唯一已接受快照引用；空 head 表示新会话。
   使用本 Attempt 的已授权批次读取固定 Store，核对 Tenant/Session、内容版本与摘要。
2. 使用 Attempt-local `session.Service` overlay 装载该快照。SDK 的事件追加、状态修改和
   完成事件全部写入 overlay；禁用异步摘要和 Memory 派生任务。任何写入错误保持 sticky。
3. SDK 收束且仍有有效 Grant 时，在外部 Store 写入一次不可变 SessionCandidate。首版存完整
   会话快照，以避免增量链与连续投影水位；键绑定 Tenant/Session/Run/Attempt，记录父 head、
   内容版本与 Digest。同键同摘要可重查/重放，同键不同内容拒绝，禁止覆盖既有候选。
   先用一张候选记录表实现，不扩展为通用对象存储或分布式提交协调器。
4. 外部耐久结果确认后，Completion Tx 锁定并复核当前 Fence 与原 accepted_head，原子提交
   Run/Attempt 终态、SessionCommit(ref/digest)、新的 accepted_head、Final 和 ReplyOutbox。
   ref 是 Adapter 生成的受信身份，不是 SDK/用户提供的任意路径。正文不复制到 Execution。
5. 失败/失租 Attempt 的候选不被接受。终态 FAILED 只推进 Session 队列，保留原 head；
   失败用户轮次与部分输出不进入下一轮上下文，错误信息留在 Completion/运维事实中。
6. 后续 Run 只按已接受引用读取历史，不扫描“最新候选”。因此外部写入成功而本地提交失败
   仅产生不可见孤儿；失租的迟到外部写入也不能改变正式 head。

| 故障窗口 | 处理 |
| --- | --- |
| 外部候选写入失败 | 不提交成功 Completion/Final；结束 Attempt，按明确类别有限重试 |
| 外部写入响应不确定 | 当前 Grant 有效时按固定键/摘要重查；失租则结束，不重新解析原 Attempt 凭据 |
| 外部候选已耐久、Completion 未提交 | 候选保持不可见；相同完成请求先查 Completion，旧 Attempt 不越过 Fence |
| Completion 已提交、返回响应丢失 | 按 Run/Completion 身份重查；不重跑模型、不重复接受会话 |
| 已接受内容暂不可读 | 后续 Run 有限等待/重试；不回退到旧 head，不重跑已完成 Run |

成功 Completion 接受时，正文已在 Profile Store 耐久，不依赖下一条消息补写；Completion 后
原 Attempt 不再发起 Session 写入或取凭据。首版删除外部 applied_sequence、失败空投影、
下一 Run 补投与后台 StorageGrant 协议。accepted_head 的接受权属于 Execution，内容的耐久
后端仍是 Profile 选定的 Session Store；两者不是两份竞争的正式历史。

**实现门禁**：W0 做 SDK/真实 PostgreSQL spike，W1 集成上述顺序。SDK 默认 Postgres
AppendEvent 并不直接提供此协议；需要小型 overlay + candidate Store Adapter，验证序列化、
并发幂等、同键冲突、Fence 与四个故障窗口。spike 未通过时重新评审 D08，不自动回到旧 projector。

首版完整快照会重复占用外部空间，孤儿也需要保留，这是用存储量换协议简化的明确代价。
候选 Schema/权限由显式迁移或准备命令管理；容量边界公开配置、监测并明确报错，不静默
截断历史。首版不自动 GC；后续在证明引用可达性与恢复保留期后再加清理，见 §15.2。

### 7.4 Namespace 与后置 Memory

SDK 的 app/user/session identity 从受信 Tenant 与内部 SessionID 推导；终端输入不提供
任意 namespace、SQL/schema/table 名称。相同物理 Store 也保持 Tenant/Session 隔离。
SessionGeneration 在首版固定初始值，不为尚未提供的 Reset 建独立生命周期。

**Memory 整体后置**：首版不初始化 Memory Service，不读取或导入条目，不 preload、不抽取、
不写入、不自动注册 Memory Tools；删除原 recency-v1、条数/扫描上限及排序协议。
Control Profile 仍可保存既有 Schema 允许的未来资源；若 Deployment 选择了首版不支持的
Memory role，应在发布时明确拒绝，不悄悄忽略。后续用途、数据生产和读取语义重新设计后
才开放运行契约，不能以 Session 存储已接入来宣称 Memory 已具备。

## 8. Attempt 凭据授权与初始化

### 8.1 已有消费 Interface

Profile 内部路径为 `POST /internal/v1/runtime-profiles/credentials/resolve`。请求体只接受
`execution_token / manifest_id / manifest_digest / uses`，身份由可信 middleware 提供。
[现有 Application](../../../services/control-api/internal/runtimeprofile/application/credential_consumer.go)
和 [成对注册规则](../../../services/control-api/internal/runtimeprofile/wiring.go) 是接线依据。

**V1 草案选择 D09**：Claim 事务创建有界寿命的高熵 opaque ExecutionToken，仅保存摘要；
原值随 Grant 返回本实例内存，不进事件、日志或 Session。Execution 所有者的受认证内部
Query 用数据库当前资格验证它，不依赖 Worker 自报 Tenant 或单纯签名未过期。

- Worker→Profile 和 Profile→Execution 都使用受认证加密内部传输。
- Profile 的工作负载 middleware 身份与 Grant.WorkerID 精确绑定。
- Execution Query 核对当前 Run/Attempt、Epoch、期限、固定 Manifest，并从可信投影推导
  AllowedUses；不把 Worker 提交的任意 uses 当成授权来源。
- 多副本 Query 从共同的 Execution 账本读取当前事实，不要求回调碰巧落在执行实例。
- Profile 获得自身锁后再次核验当前授权；Worker 收到整批后、开始执行前再次检查本地 Grant。
  这不是跨库瞬时撤销协议，在途网络响应仍存在需要丢弃的竞争窗口。

### 8.2 Single-flight 与不确定结果

每个 Attempt 从固定 Manifest 构造全部凭据闭包，一次 single-flight 解析；无凭据闭包不调用。
验收 Tenant/Profile/Run/Attempt/Worker/Epoch/Manifest 与请求完全一致，use 集合无缺失、
额外或重复，实际 endpoint/DSN destination 与固定 audience 一致，然后再构造客户端。

Session Storage 的 `purpose=dsn` 保留既有 Profile 契约：管理 API 接受完整受限 PostgreSQL
URI，Control 仅加密其中的 password，非秘密 destination 固定发布。内部 Resolve 批次的
`value` 是该密码，不是完整 DSN。Worker 必须从已验证 Manifest 的
`host/port/database/username/sslmode` 与授权密码按 URI 规则转义组装 Session 连接；
密码中的 URI 保留字符不能改变目标，不从 Worker 部署 DSN 推导目的地，也不引入自由
`options/search_path`。同 ID 密码轮换只改变密码，目标变化仍需新配置与发布 Revision。
测试用 Profile 响应也必须返回 password-only，不能用完整 DSN fixture 掩盖消费契约差异。

超时、响应丢失、部分响应、批次身份不符、失租、崩溃或缓存丢失结束该 Attempt。
关闭内部 Resolve HTTP 透明重试；只有可重试类别在显式期限与次数内分配新 Attempt 并
重新取得当前值，批次身份不符等稳定拒绝终结该 Run。迟到响应丢弃，不继续装配。
成功初始化的 Attempt 复用内存批次，不在每节点/工具调用时回查，不在同 Attempt 追加批次。

值只用于适配器初始化；不写入 Manifest、Transcript、Session、业务 Outbox、Trace 或错误正文。
按资源关系关闭客户端、释放响应缓冲与批次；第三方 SDK/Go 内存释放不被描述成物理抹除证明。

### 8.3 已接线的错误分类

当前 `ExecutionAuthorizationVerifier`、`ResolveForAttempt`、内部 HTTP 与 Worker Adapter
已区分“可信永久否决”和“Execution 查询暂不可用”：真实 owner 403 对应
`ErrExecutionUnauthorized`，Resolve 返回 403；网络/读取故障或其他未证明的 owner 状态
对应 `ErrExecutionDependencyUnavailable`，Resolve 返回 503。

Worker 把明确无权/用途不符或完整批次验收失败记为 `CREDENTIAL_DENIED`；把上述 503
记为 `DEPENDENCY_UNAVAILABLE`。任一初始化失败仍结束当前 Attempt，只有可重试类别
在 Run 固定期限与显式重试次数内创建新 Attempt，不重用旧 Attempt Resolve，也不对全部
403 盲重试。稳定失败在 Reply 期限内产生固定失败 Final，与无执行结果的
`SYSTEM_TERMINATION/NONE` 分开。联合测试的故障注入和实际响应证据见
[实现状态 W9](implementation-status.md#w9凭据整批拒绝与在线证明错误分类)。

## 9. Agent Runtime Adapter 与 SDK 语义

### 9.1 首版发布门禁，而非先建能力管理平台

当前 [PlatformExecutionContract](../../../services/control-api/internal/deployment/domain/platform_contract.go)
仅有资源 Adapter map，没有节点类型/Storage role 门禁；Compiler 直接接受组合节点并选择已配置
Memory。因此下表是 W1 必须新增的发布/消费共同校验，不是当前 allowlist 已能完成的配置。

| 能力 | 首版运行契约 | 后续开放条件 |
| --- | --- | --- |
| AgentPlan | 恰好一个 `llm` 根节点 | 组合按 SDK 原生语义实测后显式更新契约 |
| Model | `openai_compatible`；使用固定 endpoint/model/generation | 新 Adapter 单独验收 |
| Storage | 必需 `postgres_state/session`；不选择 Memory | Memory 独立后续计划 |
| Callable entries | 首版为空，不选择 Tool/Knowledge | MCP/Knowledge 与节点权限测试通过后开放 |
| 输出 | 单个完整文本 Final | Progress/媒体独立协议 |

最小实现使用静态、版本化首版规则，不新增运行能力配置中心。Control Deployment Validate 与
Publish 共用这些编译校验；规则进入平台 Contract identity/digest 和 Fixture。Worker 使用
相同版本协议独立校验，不 import Control internal。Agent/Profile Draft 的既有 Schema 不删减。
未支持配置在发布时给明确诊断，已经发布的旧 Manifest 不改写、不静默降级；上线前检查既有
Binding/Revision 与新运行版本的兼容性，Worker 对不支持的固定版本稳定拒绝。

### 9.2 SDK 原生执行与 Final 提取

**D10 修订方向**：Worker 负责固定 Manifest 装配、Attempt 生命周期、错误检查与最终接受；
组合运行交给所锁定 SDK，不重新定义分支事务、调度、合并语言或 Reducer。

首版只有单 LLM：按 Manifest 构造模型与 LLMAgent，将已接受历史注入 Attempt overlay，
由 Runner 执行。Adapter 从实际 SDK 输出流取最后一条有效完整 assistant 文本作为候选，等待流收束、
sticky error 检查和 Grant 校验后返回；Done、channel close 或 AfterAgent callback 都不是 Completion。
成功 Final 必须非空并满足现有 Reply codec；不把 tool call、控制事件或媒体当作文本。

下表属于**后续组合切片**，不阻塞首版：

| 节点 | 沿用 SDK 能力 | 不增加的承诺 |
| --- | --- | --- |
| sequence | ChainAgent 按 children 顺序执行；沿用 SDK 历史/角色传递选项与错误处理 | 不新增节点输入映射语言 |
| parallel | ParallelAgent 并发启动，独立 Invocation/FilterKey，按到达顺序合流；共享本 Attempt 的 Session/Service | 不做分支独立 overlay、声明顺序合并、固定最后声明分支 Final 或自动取消兄弟 |
| loop | CycleAgent 使用 Manifest max_iterations 与 SDK 停止机制 | 不新增成功 break 表达式或自有循环引擎 |

parallel 的共享范围仍在一个 Attempt 内，不共享到另一 Run 的正式 Session。Attempt overlay
须遵守 SDK 并发访问要求，但不为每个子分支复制可变会话。分支错误由 SDK 输出；当前版本
不会自动取消兄弟，Worker 记录错误并阻止成功接受，整体失租/deadline 仍取消根 context。

SDK parallel 仅合流，不自动生成综合答案；其 AfterAgent 响应候选不能冒充综合结果。
后续通用文本 Adapter 按实际 SDK 流选取最后一条有效完整 assistant 文本，根 parallel 因而
可能受完成顺序影响，不承诺确定输出。需要综合答案时由 AgentSpec 显式后接 LLM，Worker
不自动插入模型调用。SDK 升级或输出提取方式变化须重新验收，不为兼容一个例子重造组合器。

SDK Run 使用非 detached cancellation，Attempt context 绑定失租与执行 deadline；取消后
有界 drain，正常流关闭不覆盖错误。初始化失败按逆序关闭本 Attempt 拥有的资源，共享连接
由其 pool owner 关闭，不假设 Runner.Close 托管所有注入依赖。

### 9.3 首版关闭项与后续工具接缝

首版不初始化 MCP、Knowledge、Memory，不挂载 Skills、代码执行器或默认内建工具；显式
关闭 SDK 默认代码响应处理、自动 Memory、异步摘要。模型仍完整使用 Manifest 参数，不把
关闭未声明能力变成新的 Token/调用额度。

后续 MCP/Knowledge 切片复用已发布的节点 callable entries 和
[命名 Fixture](../../../api/schemas/deployment/v1/callable-name-v1.json)：EntryID 为
`tools/<resource_key>` / `knowledge/<resource_key>`，callable name 为 `fn_` 加 EntryID
SHA-256 前 60 个小写 hex 字符。仅装配节点声明的入口，真实调用仍校验 NodeID→EntryID。
MCP 在 Attempt context 中显式 Init 并核对唯一工具，不将刷新失败的缓存当验证成功；
Knowledge 自动派生入口须保持同一身份，Qdrant 只读初始化不暗中建 collection。
这些是后续接入已有资源契约的要求，不先为首版实现完整工具平台。

### 9.4 分切片验证 SDK，而非首版先验收全部矩阵

首版 W0/W1 只验证：

1. 锁定 SDK 依赖；Manifest 单 LLM/model/generation 能装配并真实返回文本。
2. Runner 全部 Session 读写进入 overlay；AppendEvent、状态与完成事件的错误能阻止成功接受。
3. 不可变候选保存与 Completion 接受引用符合 §7.3；取消、失租、崩溃不泄漏未接受历史。
4. 显式关闭默认代码响应处理、Memory、自动摘要；上下文、资源关闭和重复初始化行为可解释。
5. 不叠加累计 Token 额度、不暗中修改 generation 参数，实际 usage 仅作观测。

后续组合再覆盖 sequence 的角色/历史、parallel 共享 Session 与到达顺序、loop 终止行为；
Golden 测试记录 SDK 已有语义，不要求并行 Transcript 每次字节相同。工具/Knowledge 开放时
再覆盖初始化副作用、精确名称、节点授权和资源释放。SDK 私有 attempt wrapper 只作参考，
不作为公共平台提交接口，也不据此宣称其已有 Lease/Completion 协议。

### 9.5 本轮只读 SDK 源码证据

本轮参考本地 `/Users/jfs/Projects/trpc-agent-go`，分支 `feature/tool-safety-guard`，提交
`4a517fdbe7924066a5032e9707cc2aa5462f35fd`，核对时工作区干净。该提交不是本服务已锁定
依赖，也没有在本轮跑 SDK 或实际 Adapter；下面的相对路径均以该 SDK checkout 为根。
本地 origin 对应 [JFSAS/trpc-agent-go 的固定提交](https://github.com/JFSAS/trpc-agent-go/tree/4a517fdbe7924066a5032e9707cc2aa5462f35fd)；
该地址来自本地 Git remote/HEAD，本轮未在线验证链接可达性。

| SDK 源码位置 | 已观察事实 | 对 Worker 的要求 |
| --- | --- | --- |
| runner/runner.go:725–765,836–855 | 执行前/中直接 AppendEvent | 全部 Session 写路径进入 overlay |
| runner/runner.go:2725–2732,3142–3165 | 持久化错误可仅记日志；取消后仍可发 completion | sticky error + DB Fence，不认 Done 为提交证明 |
| runner/candidate_selector_session.go:28–55,307–358 | 私有 attempt-local Session，屏蔽 summary jobs | 仅作实现参考，非公共平台提交接口 |
| agent/invocation.go:1559–1580 | Clone 共享 Session/SessionService 指针 | 后续允许同 Attempt 共享，不实现分支写隔离 |
| agent/parallelagent/parallel_agent.go:325–384 | 到达顺序合流；AfterAgent 候选非综合答案；分支错误不主动取消兄弟 | 后续沿用 SDK，不增加确定合并/兄弟取消承诺 |
| agent/cycleagent/cycle_agent.go:79–123,366–390 | 默认错误停止、次数硬上限 | 普通 Done 不是标准成功 break |
| agent/llmagent/option.go:177–198 | 默认代码响应处理、角色改写与 preload 设置 | 显式固定选项，不继承隐式语义 |
| tool/mcp/toolset.go:99–126; agent/llmagent/llm_agent.go:882–929 | 初始化/刷新行为与自动 Knowledge tool 注册 | 精确入口、可取消初始化、错误门禁 |
| knowledge/vectorstore/qdrant/collection.go:18–36 | 缺 collection 时尝试创建 | 只读 Adapter 不暗中 provisioning |
| runner/runner.go:3169,4352–4360 | Runner 完成时可排自动 Memory job | 平台提交前禁止派生写入 |

实现选定的版本改变时重新核对这些位置和行为，不能把此次只读记录当作未来 SDK 的保证。

## 10. Final、证明查询与 Delivery 交接

### 10.1 完成事务产生完整逻辑 Final

沿用 [ReplyIntent Schema/Codec](../../../api/events/execution/v1/reply_intent.go)：
`execution.reply-intent.v1`，V1 只支持 `final/text`，`sequence=1`。

- 成功且仍在回复期限内的 Run 产生一个完整文本 Final。具有真实 Attempt 的终态失败可产生平台固定、脱敏的
  错误 Final；过期或无可信 Attempt 的结果按 §6.4 明确 NONE，不伪造 wire 字段。
- 固定 IntentID、AdmissionID、RunID、AttemptID、ExecutionGeneration、CompletionID、
  sequence、正文和 deadline；按现有 `ReplyIntentDigest` 计算所有稳定业务字段摘要。
- 每 Run 唯一 Final，不用 `(run_id, generation)` 允许另一 Attempt 再发一个 Final。
- 不携带 tenant/provider/account/chat/ReplyContext/ReplyOrigin，不按渠道先分段，不在
  完成后修改 deadline、正文或目标来绕过 Gateway 决定。

### 10.2 两类证明不可互换

| 消费方 | 查什么 | Lease 结束后 |
| --- | --- | --- |
| Profile Resolve | 当前 Attempt 是否有权读取固定闭包 | 已结束或过期应拒绝新解析 |
| Gateway CommittedFinalVerifier | 此 Final 是否已被正式提交 | 已提交且匹配的 Final 仍有效 |

Execution 的内部完成查询只能读已提交事实，认证调用方必须是允许的 Gateway；不存在
从消息中的“成功”字段直接构造许可的实现。查询返回现有 Port 要求的十个字段：IntentID、
Digest、AdmissionID、RunID、AttemptID、CompletionID、ExecutionGeneration、Sequence、
TenantID、ManifestDigest。与原 Admission 和 wire 逐项匹配。

格式合法不等于授权一致：HTTP Client 先验收完整 Final proof codec 与八个请求字段，
Delivery Acceptor 再将 TenantID、ManifestDigest 与原 Admission 的固定 target 比较。
合法但错配的身份返回稳定 `UNAUTHORIZED`；这与额外 wire 字段触发 `INVALID_WIRE`
是不同分支。执行已经成功并接受的 Session 不因 Gateway 拒绝这次交接而回滚。

读取暂不可用或暂未观察到证明，与可信永久否决分别返回；一次查空不直接授权永久丢弃。
同一个 Intent 已有 Delivery Receipt 时可重放原 Receipt，不依赖今天证明查询在线。

### 10.3 Reply Consumer 的确定交接方案

**V1 草案选择 D11**：Reply 使用 WorkQueue。Consumer 严格解码并以可信 Broker 传输身份
查询自己的 durable transport receipt，然后调用已有 Delivery Acceptor。

首版采用明确的两事务方案：Delivery 接纳事务保持已有 Interface；其后单独提交 transport
terminal receipt，记录 ACCEPTED 或持久 REJECTED，之后 ACK。

- “Delivery 已提交、transport receipt 未提交”崩溃后重放 Acceptor；其业务 Receipt-first
  返回原结果，补交 transport receipt 后 ACK。不把两次提交称为原子操作。
- 永久坏 wire、确定内容冲突、可信永久否决，以及现有 Delivery 的 EXPIRED/UNSUPPORTED
  结果先持久终结拒绝再 ACK。业务/transport Receipt-first 优先于重新检查 deadline，已接纳
  Intent 重放不因今天过期而改判；企微超长文本不作为未知依赖故障无限重投。
- PG/容量/内部 Codec/完成证明暂不可用只延迟重投；拒绝落账本身失败也不 ACK。
- Consumer 不等待 Provider 发送；发送重试、未知结果、原 ReplyOrigin 和连接恢复仍属于
  Delivery，不重新执行 Agent，不从当前路由重新选择回复目标。

Reply 期限单独决定是否交付：Worker 在回复期限后但有效执行资格内成功提交，仍接受
候选并保持 Run/Attempt `SUCCEEDED`，Completion 记录 `NONE/DEADLINE_EXPIRED`，
不产生 Final Outbox。若 Final 已在期限前提交，只是在 broker 排队到期限后才首次交接，
Gateway 记录 `REJECTED/EXPIRED` 后 ACK，不创建 Delivery 或调用 Sender。两者的后继
Run 均读取原已接受的成功历史，不因回复未发送而重新执行前一 Run。

Control 模式的 Delivery Runner 已实现；新增的是 Reply Consumer 与真实 verifier，不重复
建立第二个发送循环或第二个 Maintenance owner。

## 11. 期限、重试与错误分类

### 11.1 生命周期、执行窗口与回复截止

现有 RunRequested 只有 Gateway 首次 handler 的 `received_at`，没有下面这些新字段。
**V1 草案选择 D12**：接管事务按版本化 Worker Policy 固定内部 deadline，不改变现有 wire。

| 时间 | 草案规则 | 不变性 |
| --- | --- | --- |
| run_deadline | 以 received_at + policy.max_run_age 为上限，并记录首次接管的 DB 时间 | 重投、重试不续期 |
| execution_deadline | 首次 Claim 固定 min(run_deadline, first_attempt_started_at + Manifest.max_run_seconds) | 所有 Attempt 共用，不重置 |
| attempt_deadline | 不晚于该 Run 的 execution_deadline，可被显式配置的操作超时收紧 | 新 Attempt/续租不扩大 |
| reply_deadline | received_at + policy.max_reply_age，固定于 Run 接管时 | 写入 Final 后属于摘要的一部分 |

Worker Policy 为平台发布配置，不是用户 Environment。received_at 不是 Provider 发出时间；
需要最大未来时钟偏差门禁，对异常时钟持久记录失败/等待原因，不静默把时间改成当前时刻。
计时采用数据库接受的绝对时间，进程计时器只触发检查，取得业务锁后重新核对 DB 时间。

Final 生成时若业务回复截止已过，则仍提交执行终态，Completion 的
`reply_disposition=NONE`、`reason=DEADLINE_EXPIRED`，不人为延长 TTL；有效执行资格内
的成功仍是 `SUCCEEDED`。即使当时未过期，排队后 Gateway 仍可能拒绝过期 Intent，Run 成功不倒退。

Gateway 当前只实现 Intent.deadline，所选渠道模式的有效发送截止需另行持久计算并验收，
见 [Delivery D0-08/09](../channel-gateway/delivery-final-v1.md#31-业务-deadline-与有效发送截止剩余设计门禁)。
Worker 业务 TTL 不等于企微 req_id 的有效期；不将新的回调、重连或重投解释为旧关联续期。

### 11.2 只执行现有显式限制，不新增累计 Token Policy

`max_run_seconds` 是从首次 Claim 开始的 Run 执行窗口，包含准备、重试与退避，各 Attempt
不重新获得一份时间。`generation.max_output_tokens` 是单次模型响应上限；现有
Manifest.execution.max_output_tokens 是该上限的发布/运行帽值，不是 Run 累计 Token 配额。
节点显式提供该参数时按其值装配并检查不超过帽值；节点省略 generation 或 max_output_tokens
时，使用已经发布的 Manifest.execution.max_output_tokens 作为单次上限，写入装配 Golden
Fixture。其他可选参数遵守对应 Manifest/SDK 的已记录缺省语义；不借缺省参数引入私有额度。

当前锁定 SDK v1.11.2 的默认 known-model clamp 与上述显式参数契约不一致。Worker 使用
其公开 `WithChatRequestCallback`，在 SDK 转换之后同步设置 typed `MaxCompletionTokens`；
不更换模型名、不改 SDK、不在 HTTP transport 重写请求。该接缝只恢复已验证的单次参数，
不增加用户配置项或新的上限。当前 V1 不开放会覆盖该字段的 ExtraFields/JSON options。
Provider 对参数的拒绝仍走真实失败路径，不降低参数后透明重试，usage 仍只作观测。

**首版不引入 `max_total_output_tokens`，不设置 16,384 等私有累计阈值，不创建 RunBudget /
CallReservation，不做 Token 预占、退款或 usage 结算。** 模型参数按固定 Manifest 装配，
usage 仅记录为观测数据。SDK/Provider 的重试行为明确配置并记录，不在内部再藏一层输出
Token 或模型调用总额。未来费用配额必须有明确需求、公开语义与契约评审，见 §15.2。

首版无 Tool/Knowledge 调用；后续启用时仍遵守既有 `max_tool_calls`，在同 Run 的所有节点和
Attempt 间计数，失租/重试不清零。可以用最小持久调用计数实现，不由此引入 Token 账本。
SDK 的每 Invocation 工具轮数不是整个 Run 的真实工具调用次数，不能直接混同。

### 11.3 显式运维配置，不是隐含模型额度

运行需要排队期限、回复期限、Lease、重试和并发配置。它们必须在部署配置/运行说明中公开，
启动校验并记录 Policy version；此处不新增固定生产默认值，也不复用旧草案的候选数值。

| 配置 | 首版规则 |
| --- | --- |
| max_run_age / max_reply_age | 明确配置并以渠道/恢复验收确认；接管后不因重试延长 |
| max_attempts_per_run | 明确配置，计入 PREPARING 失败；耗尽后持久终结 |
| lease_ttl / renewal_interval | 明确配置并校验续租与网络超时关系 |
| max_active_attempts / consumer_batch / scan_page | 有界配置，简单轮询；不先做加权公平调度 |
| drain_timeout | 明确配置；关闭期间续租，超时取消并按 Fence 恢复 |
| 模型执行参数 | 使用固定 Manifest；不叠加未声明的 Token 或调用额度 |
| 事件/正文/存储容量 | 沿用现有协议限制；新增容量要求公开说明，明确拒绝而非静默裁剪 |

这些是故障恢复和容量控制，不得伪装成用户已配置的模型费用限制。新增或更严格的运行策略
需要显式评审，不在启动时自动缩小已发布 Manifest 的语义。

### 11.4 错误与恢复决策

| 故障 | Attempt / Run 行为 | 是否再调用 Agent |
| --- | --- | --- |
| Manifest 尚未应用 | 持久等待；到期终态失败 | 未开始调用 |
| Manifest 身份/摘要/版本确定不符 | 稳定失败、隔离证据 | 否 |
| Profile/授权查询依赖暂不可用 | 结束本 Attempt，在 Run 固定期限与显式重试次数内新 Attempt | 初始化未完成前无模型调用 |
| 明确无权、凭据已撤销/用途不符 | 结束 Attempt，稳定失败 | 否 |
| 模型/只读检索的可重试失败 | 新 Attempt，保留同 Run；承认可能重复计费 | 有界允许 |
| 外部操作是否发生不确定 | 保存操作证据，按 capability 的幂等/核验规则决定；无规则则终止自动重跑 | 不盲目重跑 |
| 失租/崩溃/Drain 超时 | ABORTED，新 Attempt 在旧资格失效后恢复 | 仍受副作用、固定期限与重试次数约束 |
| Completion 提交响应不确定 | 先查稳定 Completion 身份 | 不直接重跑 |
| Reply PubAck/投递状态不确定 | 原 IntentID / 原 Payload 重发 | 否 |
| SessionCandidate 写入失败/不确定 | 不接受成功结果；按 §7.3 重查或有限重试 | 不确定 Completion 先查，不直接重跑 |
| 已接受 Session 内容读取失败 | 不回退 head；当前 Run 按依赖故障处理 | 不重跑已经完成的前一 Run |
| Provider 发送 UNKNOWN | Gateway Delivery 原账本恢复 | 否 |

副作用分类覆盖 constructor 与 Storage 写入；Memory 首版关闭，工具副作用在后续开放时验收。

## 12. NATS 拓扑、权限与恢复窗口

以下 Stream/durable 已写入部署声明、runtime 校验与真实 ACL 测试；Route Stream 继续保持既有职责。

| Stream / Subject | Retention | Consumer / Publisher |
| --- | --- | --- |
| RUNTIME_MANIFESTS_V1 / control.runtime-manifest.published.v1 | Limits，保留配置配合回填协议 | Control 发布；共享 Execution DB 的 worker-manifests-v1 消费 |
| RUN_REQUESTS_V1 / execution.run-requested.v1 | 已有 WorkQueue | Gateway 发布；agent-worker-runs-v1 消费 |
| REPLY_INTENTS_V1 / execution.reply-intent.v1 | WorkQueue | Worker Reply Relay 发布；channel-gateway-replies-v1 消费 |

拓扑由显式 reconciler 创建；runtime 只验证所需拓扑和已授予的精确权限，不获取管理员通配
权限来自动建 Stream。Control、Worker、Gateway 各自只能发布所属事件族和消费指定 durable。
源码里的 subject 常量、生成配置、Server 实际 ACL 与拒绝测试必须共同验证。

同一 Execution DB 的多副本共享 durable 并共享 Manifest 投影；若未来部署多个独立运行
存储组，则每组需要独立完整 Manifest Consumer，不能共享一个 durable 后各留半份投影。

Stream 显式限制 bytes/message size，倾向 DiscardNew 让 PG Outbox 承接容量，不静默淘汰
未交接消息。有限 Broker 保留不代替 PG 原始事实；发送成功不立即删除 Outbox。
W1/W2 配置评审必须交付最大消费者离线窗口、source 保留窗口、报警阈值、跨窗重放/回填与
恢复身份；未证明 source 回填前不启用破坏唯一恢复来源的 MaxAge/GC。

## 13. 进程启动、授权查询与运维

### 13.1 Bootstrap 与关闭

一个轻量 main、一个 bootstrap；配置/连接/日志/信号统一由 bootstrap 管理，Module wiring
只接收显式依赖。执行账本使用 Worker 自有逻辑持久化空间、角色与迁移，不读 Control/Gateway 表。
**V1 部署采用同一 PostgreSQL/database 的 `worker` Schema**，Session 内容可使用同库
`runtime_session` Schema 与独立 Profile 身份；Control/Gateway 分别使用自己的 Schema。
迁移/运行连接分权与初始化实现见 [Database V1](../operations/database-v1.md)。Worker 账本、
Session 候选表/Adapter、显式 prepare 与进程 bootstrap 已有代码及真实基础设施 fixture，
真实模型/Telegram 联合闭环按 [实现状态](implementation-status.md) 继续验收。

建议启动顺序：静态配置与 Contract 校验 → 自有 DB/迁移 → 内部证明查询 → NATS 拓扑/ACL 校验
→ 建立 Manifest durable 与最小回填 → Consumers/Reply Relay → 初始化追赶达标后开放有界执行。

readiness 至少区分持久接管、Manifest 初始化和实际执行资格；受认证证明查询应独立于是否
允许新执行。暂停调度不应让 Gateway 失去已提交 Final 的读取能力。

关闭顺序：停止拉取与新 Claim → 保持证明查询/必要续租 → 有界等待完成事务与批次资源释放
→ 取消超时 Attempt → 记录可恢复状态 → 关闭服务和连接。关闭超时不提前释放一个仍在
执行外部调用的资格并同时让新实例开始；以过期 Fence 与副作用恢复规则收束。

### 13.2 可观测性与容量

遵守 [Telemetry 基线](../operations/observability.md)：Run/Attempt/Session/Manifest 内部身份
用于结构化日志和 trace 关联，不作 Prometheus 高基数标签。消息正文、完整 Prompt、凭据值、
DSN、ExecutionToken 不进入默认日志/Trace。

必须有：接管/拒绝/冲突、等待原因、队列 age、Attempt 初始化/执行时长、续租失败/Fence
拒绝、Session 候选写入/已接受内容读取失败、Completion/Reply Outbox 积压、Provider 调用与 usage、Drain 时长。
告警区分“入站成功但未执行”“执行完成但未产生可投递 Final”“Final 已交接但渠道结果未知”。

Reply wire 当前不允许任意 trace 字段；跨队列 Trace 使用单独明确的受信 metadata 契约，
过滤大小/来源，不把 Trace 混入业务 Digest。尚未接线前不宣称完整跨队列 Trace 已存在。

内存执行队列、Transcript、待执行 Run 和投影批次使用各自显式上限。持久 Run 接管区分
`max_queued_runs`（QUEUED/RUNNING/RETRY_WAIT）与 `max_retained_runs`（全部状态）；二者均为
必填部署配置，不是 Manifest 字段或模型 Token 额度。仅全新 Run 在现有容量事务锁内检查，
满额不创建 Session、不推进 sequence、不写接管 receipt，并让原 broker 消息延迟重投。
已有 receipt/同 Run alias 回放、Claim/续租、Completion 与 Final 恢复保持可用；下调限额
不删除历史，提高显式限额后可从原 broker sequence 恢复。

这不是整个数据库的严格行数或字节配额。Run 上限同时约束其 Session 账本、Completion、
Session commit 和 Final 主记录的增长；Attempt 数取决于各已接管 Run 冻结的 MaxAttempts。
额外 alias receipt、拒绝/冲突记录、Manifest receipt/tombstone 和独立 Session Store 累计
候选仍需后续保留/容量方案；`max_manifests` 只约束主投影，`max_snapshot_bytes` 只约束
单份快照。不得把这些局部限制标成“所有 DB 账本已有上限”。V1 不做事实 GC，保留已有
恢复事实、暴露已实现水位并明确记录剩余缺口，不无限 goroutine/热重试。辅助证据的容量
治理必须维持冲突隔离和重放契约，后续独立设计，不在首版补通用预算系统或跨 Schema 清理。

## 14. 建议代码结构与持久化演进

下列保留设计分层；实际目录中 NATS transport 统一位于 `internal/infra/natsadapter`，
装配位于 `internal/bootstrap`，另有 `sessionmigrations` 和 `integration`，详见
[Worker 源码入口](../../../services/agent-worker/README.md)。

```text
services/agent-worker/
├── cmd/agent-worker/main.go
├── internal/
│   ├── bootstrap/
│   ├── infra/                    # 连接、server、telemetry；无业务 SQL
│   ├── manifest/
│   │   ├── domain/
│   │   ├── application/
│   │   ├── adapter/inbound/natsadapter/
│   │   ├── adapter/outbound/postgresadapter/
│   │   └── wiring.go
│   └── execution/
│       ├── domain/               # Run、Attempt、SessionCommit、Completion
│       ├── application/          # 接管、Claim、执行、完成、恢复、证明查询
│       ├── adapter/inbound/      # Run consumer、受认证内部查询
│       ├── adapter/outbound/     # PG、Profile HTTP、trpcagent、Session Store、Reply Relay
│       └── wiring.go
├── migrations/
└── README.md
```

Migration 只在各切片有实质表/索引与集成测试时创建。Worker 独立管理自己的首次基线，
不接管 Gateway 的迁移账本，不把 Control 表当共享 Repository。Profile Store 的不可变候选
表由 Session 迁移管理；部署已预置 runtime_session Schema/role，其 runtime 默认仅 SELECT/INSERT。
不在业务请求中暗中建表，预置权限不等于候选内容协议已实现。
不新增 Session projector、Memory Module、组合调度器或通用预算 Module。

Controller/Domain/SQL/SDK 不共用一个万能 `model.go` 或 `contract/common`；文件按事实和
用例命名。Go 构建依赖与真实网络调用分别画图验收，不把分层重命名当作所有权完成。

## 15. 纵向实施顺序与后续计划

### 15.1 首版只有两个实现切片

| 阶段 | 同一切片内的工作 | 完成证据 |
| --- | --- | --- |
| W0 小型设计验证 | 确认首版静态能力规则、锁定 SDK；验证 Session overlay/candidate 接缝；确定必要显式运维配置 | Session 故障窗口的真实 PG spike；不是先验收全部 SDK 功能 |
| W1 Manifest → 单 LLM → 正式 Session → Completion | Control 发布门禁、Manifest Relay/最小回填；真实 RunRequested 接管；Claim/Fence；Profile 在线授权与错误分类；LLM 装配执行；候选持久化与接受；唯一 Completion/Final/Outbox | 从真实发布 Manifest 到真实模型输出，第二个 Run 读取正式历史；真实 PG/NATS、重投、失租、提交不确定验证 |
| W2 Telegram Final 闭环与首版验收 | 复用 Gateway 入站；Reply Relay/Stream/Consumer、完成证明、Delivery 接线；两轮会话、重启/Drain、基础并发与容量观测 | 真实 Telegram 输入到最终回复；Execution 与 Delivery 交叉证据；§16.1 首版矩阵通过 |

**W1 + W2 完成即为 Worker V1 完成；后续完整资源矩阵不再是首版完成条件。**
Manifest、LLM 装配、执行、Session 不拆成四个先行平台；跨服务接线在相应纵切内一起交付。
W0 只解决会阻塞这条路径的实际问题，不先建设通用恢复、授权或资源记账框架。

### 15.2 明确后续计划（均不计入首版完成定义）

| 计划 | 内容与顺序 | 开放门禁 / 不做什么 |
| --- | --- | --- |
| F01 SDK 组合 | sequence 优先，随后 loop/parallel；复用 ChainAgent/CycleAgent/ParallelAgent | 记录原生角色、共享 Session、输出与错误语义；更新发布矩阵；不造分支事务/确定合并引擎 |
| F02 Tool / Knowledge | MCP 精确工具、Qdrant 只读检索、固定 callable 身份与节点权限 | 真实资源验收及全 Run 工具次数计数；不默认挂载整个 toolset |
| F03 Memory | 先明确用途、Session/用户作用域、数据来源与读写流程，再选择 SDK 接缝 | Memory 整体后置；不继承已删除的 recency-v1/扫描参数，不以空 preload 冒充产品 |
| F04 费用与累计 Token | 有明确配额需求后再评估 Token 总额、调用预占与结算 | 显式产品/运行契约；不恢复私有默认额度，不与单次 max_output_tokens 混同 |
| F05 Session 演进 | 候选孤儿/历史保留与 GC；确有容量压力时评估增量内容；另评 Reset/历史迁移 | 证明接受引用与恢复窗口；不自动恢复双库全文复制、下一 Run 补投或后台 StorageGrant |
| F06 运行扩展与渠道 | 加权租户公平、进阶压测、多独立运行组恢复；企微模式专项 | 首版仍有基本并发/Fence/恢复；Telegram 成功不外推企微原连接与有效回复窗口 |
| F07 产品与其他输出 | Channel 页面、Local IM、Progress/媒体/编辑、用户 Stop、工作区工具 | 按各自产品/协议切片交付；Helm 在全部 Workload 后进入 FINAL-INTEGRATION |

现有已发布契约仍保持不可变。后续能力完成时同时修改发布门禁、Worker 实现、版本/Fixture
和验收，不通过“SDK 有一个类型”或“Profile 能保存一个资源”直接宣布运行支持。

## 16. 验收矩阵：首版与后续分开

保留 WV 编号便于追踪本次范围变更；移到后续的编号不表示已完成，也不阻塞 V1。

### 16.1 首版必需

| ID | 场景 | 必须观察的结果 |
| --- | --- | --- |
| WV-01 | 同 RunRequested 并发重投 | 一份 Inbox/Run、一个 SessionSequence，Receipt 重放 |
| WV-02 | 同 EventID 不同内容、不同 EventID 指向冲突 Run | 持久冲突，无输入覆盖/新执行 |
| WV-03 | 接管提交前后崩溃、ACK 丢失 | 未接管不 ACK；已接管重启后可执行 |
| WV-04 | Run 先到、Manifest 后到 | 持久等待再唤醒，无最新配置热路径查询 |
| WV-05 | Manifest 身份/摘要/版本不符、跨 Tenant、公开 View 冒充 | 隔离/拒绝，零 SDK 调用 |
| WV-06 | 最小导出/增量重叠、离线超窗、空集合 | 幂等无缺口；durable/导出完成/追赶证据明确 |
| WV-07 | 两实例争同一 Run / Session | 至多一个有效执行资格；同 Session 顺序执行 |
| WV-08 | 旧 Attempt 等锁到期、新 Attempt 接管 | 旧结果/续租/凭据响应不推进当前 Run |
| WV-09 | 同 chat 不同 Tenant/Account/Thread/Revision | 按 SessionScope 隔离 |
| WV-10 | Revision 切换再切回、RouteGeneration 变化 | 按 D07 复用/隔离，不意外串历史 |
| WV-11 | 凭据 use 缺失/额外/重复、批次身份不符 | 整批丢弃，零模型调用，结束 Attempt |
| WV-12 | Resolve 超时/丢响应、旧 Epoch、Profile 锁等待 | 新 Attempt 恢复，无原 Attempt 透明重取 |
| WV-13 | verifier 网络故障 vs 明确否决 | 可重试依赖错误与稳定拒绝分开 |
| WV-14 | live 轮换、clear、DSN 改目标 | 已初始化批次不混值；新 Attempt 按当前授权；改目标拒绝 |
| WV-15 | SDK AppendEvent 失败但 Done/channel close | sticky error 阻止成功 Completion |
| WV-16 | 失败/失租 Attempt 留下 overlay 或外部候选 | accepted_head 不变；下一轮不读取其部分输出 |
| WV-17 | 候选已耐久、Completion 未提交或响应丢失 | 未接受候选不可见；先查 Completion，不靠下一 Run 补投 |
| WV-18 | 候选同键重放/不同摘要、接受后 Store 不可读 | 幂等或拒绝；不覆盖、不回退 head、不重跑已完成 Run |
| WV-22 | Profile Session Store 缺 Schema | 明确准备错误，不在业务请求中建表 |
| WV-23 | 模型/constructor/Storage 操作结果不确定 | 记录事实，按固定身份核验，不承诺无条件重跑 |
| WV-24 | Completion Tx 任一步失败 | Run/Session 接受引用/Final/Outbox 一起有或一起无；外部孤儿不等于已接受 |
| WV-25 | Lease 结束后 Final 才到达 | 已提交证明仍有效，不复用活跃授权检查 |
| WV-26 | Final 正文/Generation/Completion/Tenant/Manifest 篡改 | 精确匹配失败，零发送 |
| WV-27 | 同 Run 不同 Intent/ExecutionGeneration | 每 Run 唯一 Final 屏障 |
| WV-28 | Delivery 提交后 transport receipt 前崩溃 | Receipt-first 重放、补 transport receipt 后 ACK |
| WV-29 | 完成证明暂不可用、拒绝落账失败 | 不永久丢弃、不提前 ACK |
| WV-30 | Reply PubAck 后标记前崩溃、Gateway 重投 | 原 Intent/Digest，Agent 不重跑 |
| WV-31 | 晚完成、时钟偏差、排队跨截止 | 不延长 deadline；执行结果与发送结果分离 |
| WV-32 | PG/NATS 容量、并发上限与有界扫描 | 明确退避/恢复；无无限队列或隐含模型额度 |
| WV-33 | Drain、证明 Query、续租与强杀 | 停新 Claim，已提交 Final 可读，过期后恢复 |
| WV-34 | 真实 Telegram 两轮对话、重启、重复输入 | 模型/正式 Session/Final/Delivery 交叉证据 |
| WV-36 | 最后 Attempt 强杀、失租同时到期、RETRY_WAIT 到期 | SystemTerminalizer 与 Claim 互斥，FAILED/队列推进/NONE，head 不变 |
| WV-37 | 跨 Attempt 重试与执行 deadline | Run 时间窗不重置；显式重试配置生效；不验收累计 Token 账本 |
| WV-39 | 发布组合/Memory/Tool/Knowledge 到首版契约 | Validate/Publish 明确拒绝；Worker 同版本校验；旧 Manifest 不改写 |
| WV-40 | Manifest generation 与真实模型请求、usage | 参数按契约装配；无累计 Token 阈值/预占/隐式输出裁剪；无 Memory 初始化 |
| WV-41 | 发布 Manifest → RunRequested → 装配 → LLM → Session → Completion | 一条真实纵切；不是分别通过若干 mock Adapter 就宣称完成 |

### 16.2 后续计划的专项验收

| ID | 计划 | 验收目标 |
| --- | --- | --- |
| WV-19 | F01 parallel | SDK 原生并发合流/共享 Session/错误行为；不要求声明顺序或独立分支快照 |
| WV-20 | F01 sequence/loop | SDK 顺序/角色与停止语义；Manifest 迭代数，不造组合执行器 |
| WV-21 | F02 Tool/Knowledge | 精确 callable 与节点授权、无默认工具扩展；补 Qdrant/MCP 初始化测试 |
| WV-35 | F06 企微专项 | 原连接、长文本与有效回复窗口，真实模式验收 |
| WV-38 | F03 Memory | 数据生产/作用域/读取写入协议确定后新增用例；旧 recency-v1 方案撤回 |
| 待定义 | F04/F05 | 明确需求后新增累计 Token 或存储 GC 验收；首版不预造账本/投影测试 |

协议单测、Application 替身、真实 PG/NATS、真实模型/Storage、真实 IM 分别记录。
设置命令不等于执行集成；必需变量缺失不得以 skip 代替成功。

## 17. 决策登记与未决门禁

| 决策 | 本次选择 | 状态/代价 |
| --- | --- | --- |
| D01 所有权 | 两业务 Module + SDK Adapter，不建调度/Secret 微服务 | 代码遵守现有 Workload 约束；真实纵切门禁另记 |
| D02 ACK | Inbox + Run 同事务后 ACK，DB 负责后续恢复 | 保持已有输入契约 |
| D03 Manifest | 完整投影、缺失等待、一个运行组的最小 Owner 回填 | 保持已有约束；已实现固定上界 Owner Export 与预绑定 durable 的重叠恢复 |
| D04 Completion | Fence + Run 终态 + Session 接受引用 + Final/Outbox 同事务 | 外部内容已先耐久；并非跨库原子提交 |
| D05 Generation | ExecutionGeneration 与 LeaseEpoch 含义分开；SessionGeneration 固定初始值 | 不先建 Reset 或多套生命周期 |
| D06 无执行回复 | 无可信 Attempt/过期结果明确 NONE | 保留运维可见与未必有 IM 错误回复的代价 |
| D07 SessionScope | 按渠道/Binding/DeploymentRevision 隔离，接管顺序串行 | 升级不继承历史，切回恢复；稳定 Scope 单测已实现 |
| D08 Session 提交 | 不可变外部候选 + 原子接受 ref/digest | 真实 PG spike 与 SDK fixture 已通过；撤回双份正文/projector/下一 Run 补投 |
| D09 授权 | opaque token + 当前 DB Grant；Profile 在线反查 | 保持在线核验；W1 接线与错误分类 |
| D10 SDK | 首版单 LLM；后续组合按 SDK 原生能力，Memory 后置 | 范围已确认；不增加分支合并引擎 |
| D11 Reply | WorkQueue + Delivery/transport 两事务 | 已复用 Delivery 并接入 Consumer/verifier；真实 Telegram 尚待验收 |
| D12 限制 | 既有 Manifest 限制 + 公开恢复配置；无新增累计 Token Policy | Token 预占/结算后置，不恢复隐含阈值 |
| D13 完成定义 | W1 纵向执行 + W2 Telegram Final；F01–F07 独立后续计划 | 2026-09-07 用户确认的首版范围；不是全矩阵首发 |

范围决定已反映到正文、阶段和验收；Session 接缝与静态能力门禁已有实现，联合验收按实现状态逐项记录。必要的架构约束
变化经单独 ADR/契约评审，不用本草案默默覆盖既有 Control/Profile/事件规范。

## 18. 文档校验、来源优先级与后续交接

本轮文档引用当前工作树的协议与源码；旧说明中的实现状态以新的 bootstrap 和切片验收
记录为准。已识别的漂移包括：凭据文档仍有 Deployment Checker 待接入的旧段落，实际
Control bootstrap 已注入；Execution events README 仍有默认 Delivery dispatcher 待接入
的旧段落，当前 Control 模式已有 Runner。这些历史状态不再作为当前 Worker/Reply Consumer 的实现结论；当前代码与验证见 [实现状态](implementation-status.md)。

当前可执行的协议回归入口：

```bash
go test -count=1 ./api/events/execution/v1 ./api/events/control/v1 ./api/schemas/deployment/v1
```

该命令只验证既有协议，不是 Worker 测试。Worker 实现前必须新增 §16 对应的执行 Domain、
Application、真实 PG/NATS、SDK Contract 与真实模型/渠道验收。下一步完成 W0 的小型
Session spike/能力规则确认，然后直接实施 W1 的 Manifest → LLM → 正式 Session 纵切；
本次仅修订文档，不新增空 Worker 代码树。
