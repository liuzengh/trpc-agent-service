# Worker V1 实现与验收状态

- 日期：2026-09-07；工作分支：`worker`；开发基线：`f61d49b`（Worker V1 独立交付基线）。
- 范围依据：[Worker V1](README.md)；代码入口：[agent-worker](../../../services/agent-worker/README.md)。
- 当前结论：首版纵向切片已取得真实 DeepSeek 与 Telegram 同次关联证据，Worker 重启与事件重投已验证；实际运行发现 Telegram 准备阶段失败，排查确认的错误分类缺陷已修复并重部署。保留第三轮修复前 `NOT_SENT` 事实，不将所有尝试都记为成功。
- 本文不把管理面发布、入站成功、readiness、单个 Adapter 测试或测试模型输出互相替代。

## 2026-09-09 数据能力增量（历史首版记录不覆盖此状态）

本文件后文的“首版没有 Memory/Knowledge”“继续后置”描述 2026-09-07 的历史基线，
不代表当前 `worker` 分支能力。当前按显式 AgentSpec → Profile → immutable Manifest →
Worker 装配逐包完成：

- [多后端迁移 V1](backend-migration-v1.md)：Session（含 Summary）与 Memory 已提供
  PostgreSQL/Redis 间显式当前快照迁移、不可变/Revision 校验及真实双向后端夹具；产品入口
  已收敛为 Memory 迁移、成功后发布新 DeploymentRevision、最后由 Web 对 ChannelBinding
  执行 CAS 切换。仍不包含在线双写或 Artifact/Knowledge 的伪通用迁移。
- [正式 Session Summary](session-summary-v1.md)：摘要与 boundary 随同 Session snapshot
  接受，下轮加载和消费；不新增摘要数据库。
- [PostgreSQL Memory](memory-postgres-v1.md) 与 [Redis Memory](memory-redis-v1.md)：
  SDK 六工具、正式接受后写入与隔离；真实后端及 Web 分别验收。
- [Redis Session + Summary](session-redis-v1.md)：同 Redis snapshot 保存原事件与摘要，
  PostgreSQL accepted head 仍是正式接受依据。
- [Artifact](artifact-v1.md)：S3 内容与既有 Worker PostgreSQL 元数据，四个 Worker
  薄工具及正式 Owner 上传/下载；不是 SDK 自带工具或跨存储原子事务。
- [Knowledge](knowledge-v1.md)：SDK 文本导入/Embedding/检索链与固定 Qdrant named
  vector，正式 HTTP/Web 同 Manifest 导入和检索。真实 Qdrant 已验；外部 Embedding
  尚缺可达 endpoint、model、dimensions 和独立凭据，fixture 不替代语义检索验收。

各包的实际模型、存储、GUI 和失败语义以对应文档为准；不把旧 Telegram 证据延伸为
新能力 Telegram 验收。各包功能完成也不自动等于四能力组合已通过。

## 2026-09-09 普通工具与编排增量

数据能力开发交付已完成，真实外部 Embedding 配置是单列遗留验收。
新任务从 [普通 MCP 工具](mcp-v1.md) 开始：既有 AgentSpec/Profile/Manifest 字段
驱动单 LLM 的显式 selected callable，保留正式 Session/Final 接受链路。
Control Capability 与既有 Agent 语法对齐，历史 `web.search` 仍有效。

MCP 已取得真实 Streamable HTTP server、正式 HTTP 发布 + Channel Lab 两轮 fixture
（含业务 IsError 纠正和下一轮历史消费），以及一轮真实 DeepSeek 选择并使用工具返回。
具体 GUI 与代码包验收记录见 MCP 文档；上述不代表真实 Telegram 或 live 错误纠正。
[Sequence](sequence-v1.md) 已接通有序嵌套 SDK Chain、节点级依赖选择和末叶 Final，
正式成功/终端失败/恢复联合及真实模型正常链路已验证。
[Parallel + 显式汇总](parallel-v1.md) 已接通共享发布门禁、Reader/Factory 和真实 SDK
并发装配；正式 fixture 两种相反完成顺序及分支失败取消、真实模型正常运行、既有 GUI
同 Manifest 发布/执行均通过。共享 Memory 的真实 SDK 并行 add/load 与候选无丢写
race 已验；PG/Redis Memory、Artifact 等真实后端并行组合仍留后续相关回归。
[Loop](loop-v1.md) 的既有 body/max_iterations 已完成 Reader/Factory 与真实 SDK Cycle
接线、定向 race、正式成功/终轮失败/恢复及 GUI 差异输出验收。真实模型两次场景均因
额外引用/标记指令未满足而保留 FAIL；第二次已证两次调用、完整前轮上下文、正式
接受与 Gateway/Lab 交付，不能因同文本响应独立证明末轮选择。无新增退出 DSL 或
隐式预算。[最终相关回归](orchestration-acceptance-v1.md) 已完成 Parallel 的真实 PG/Redis
Memory、MinIO/PG Artifact、Qdrant 双 resource/tenant scope 矩阵，实际发现并修复
组合 Plan 的 Knowledge 正式导入仍读取旧单叶字段导致403的问题；真实模型未验边界
和最终统一 Go/Web 结果在矩阵中逐项列明。

## 2026-09-10 危险工具二次确认增量

[危险工具二次确认 V1](../tool-approval-v1.md) 只治理 Capability
`test.ticket.status.update`：Worker 在真实 SDK Before/After Tool 回调之间持久等待，Control
提供租户 OWNER 决定，Web 显示目标与参数摘要。批准通过摘要和调用身份绑定；重复决定只重放
结果，工具结果未知及所属 Worker 重启均进入 UNKNOWN，调度器不以新 Attempt 重跑整轮 Agent。
当前自动化覆盖真实 PostgreSQL 状态机及 Web/HTTP/SDK 接缝；真实外部测试工单服务仍待联合验收。
这不是通用 Shell、任意 MCP 工具或 IM 卡片审批。

## 1. 本轮交付

| 位置 | 新增的实现 |
| --- | --- |
| Control Deployment | `worker-v1` 共享静态门禁、关闭的完整 Manifest wire、实际 Compiler 生成的 fixtures；历史发布不可变 |
| Control Runtime | Manifest Relay、固定上界分页 Owner Export、Worker mTLS 身份、Profile 活跃 Grant 回查与依赖错误分类 |
| Worker Manifest | 事件/导出投影，固定身份/Digest/pin 校验、并发重放、跨身份冲突隔离及持久 tombstone |
| Worker Execution | Run/Receipt、SessionSequence、Attempt/Lease、固定 deadline、数据库锁后 Fence、重试/SystemTerminalizer |
| SDK / Session | tRPC-Agent-Go v1.11.2 单 LLM，实际 per-response 参数，sticky overlay、不可变候选与 accepted head |
| Completion / Reply | 原子接受结果与 Final/Outbox，PubAck 后标记，lease 结束仍可查询的完成证明 |
| Gateway | 严格 Reply consumer，Delivery/transport 两事务，mTLS 完成证明；复用原有 Sender/Runner |
| Deployment | 同库四 Schema、八角色；Worker 二进制与显式 Session prepare；TLS Compose overlay、四 Stream ACL |
| 运行信号 | 有界结构化日志、可选显式 OTLP Metrics；阶段/积压/usage/Drain，不新增业务预算 |

首版没有 Memory 初始化、组合调度器、Tools/Knowledge 执行、累计 Token 账本、隐式总输出额度，
也没有 Session projector 或下一 Run 补投协议。Session username `session_runtime` 由新发布 gate
和运行 Adapter 一致校验；这项规则进入 Worker 平台摘要，不改历史 `platform-v1` 或 Draft schema。

### Tracing V1 当前状态（M1–M5 已实现，审查修复未部署）

[IM 运行链路 Tracing V1 计划](../operations/im-runtime-tracing-v1-plan.md) 的 M1–M5 已在
当前 `worker` 工作树实现；此前版本已部署到本机手测实例，验收记录见
[Tracing V1 联合验收](../operations/im-runtime-tracing-v1-acceptance.md)。历史阶段记录保留在 §6–§9，
其中“待实施”“部署未切换”均描述当时状态，不代表当前进度。

本次审查修复涉及 Consumer ambient 上下文隔离、callback HTTP 失败观测、父链验收和文档。
修复只更新源码与测试，尚未切换运行实例；提交状态以 Git 引用为准。此前 T15 的父链 PASS 不单独作为
完整图结构证明。加强后的验收器已重新校验此前保存的 29 Span 真实 Telegram Trace，
正常单根、关键 ancestry 与 creation Link 通过；这不是新一次实机请求或部署验证。
Git 提交、部署与验证状态分别记录。
Memory、生产 Tool/组合执行及预算继续后置。所有修改在当前工作树完成，不派发其他工作树。

## 2. 已取得的证据层次

### W0：SDK 与正式 Session 接缝

实际 SDK 对接 HTTP 模型 fixture；实际 PostgreSQL 用 `session_migrator` 准备候选表，用
`session_runtime` 读写不可变候选。验证候选键/摘要重放、目标身份、权限拒绝、sticky
AppendEvent、取消/无透明重试、模型参数和两轮历史；这不是 SDK in-memory Session 冒充持久存储。

### W1：执行纵向切片的基础设施 fixture

`TestWorkerVerticalPGNATSSDK` 运行真实 PG/NATS、严格 Run/Manifest wire、实际 Reader/Processor/
SDK/Session/Completion/Reply Relay，模型/Profile 为明确 HTTP fixture：

- Run 先到且 Manifest 缺失时，耐久接管但不调用模型/Resolve；Manifest 到达后执行。
- 两个顺序 Run 产生两个 Completion、两个不可变候选和两个发布 Final。
- 第二轮模型请求包含第一轮已接受的 user/assistant 历史。
- 两轮各一次模型调用与一次 Resolve；完成后的重复输入不重跑。
- 显式 20000 单次输出参数抵达实际 SDK 请求，没有隐式 16384 裁剪。

真实 Control PostgreSQL publication/distribution 单独验证新 gate、发布事务、完整不可变事件、
分页/重叠与 Relay。它与上述测试合起来仍不是“真实 Control 发布 → 外部模型”的一次联合运行。

### W2：进程装配与 Reply 交接的基础设施 fixture

`TestWorkerBootstrapRealPGNATSMTLSAndDrain` 启动真实 Worker App、固定角色 PostgreSQL、
认证 NATS 和临时证书 mTLS 监听器。Control/model 为 HTTP fixture；验证 Owner Export 先于
ready、保留增量追赶、两轮 SDK/Session、在线证明、Drain 期间持续续租和提交后 Final 发布。

`TestReplyHandoffPostgresNATSIntegration` 运行真实 Gateway Delivery 数据库/NATS：
Delivery 已提交但 transport receipt 未提交时重放，先读既有接纳结果，补齐 receipt 后 ACK。
另有 Worker/Gateway 真实 broker ACL 与证书链/主机名测试。

### W3：真实 Control/Gateway/Worker 进程联合与强杀恢复（首次联合证据）

`scripts/test-worker-v1-joint.py --race --faults` 在独立 PG/NATS、临时证书和八角色下运行实际
三个二进制。Agent/Profile/Deployment/Account/Binding 均经真实 Control 公开 HTTP API 创建并
发布，Manifest 经实际 Relay/Owner Export 到 Worker；只有模型、Telegram Bot API 和外部
webhook HTTPS 入口是明确 fixture，不手写 Manifest 或通过 SQL 修复产品状态。

本轮同一次执行已取得：

- 两轮真实 Gateway webhook → Admission → Run → 在线 Profile 授权 → SDK → 正式 Session →
  Completion/Final → Gateway Delivery ACCEPTED；第二轮真实 SDK 请求包含第一轮接受的历史。
- 错误 webhook secret 为 401、原输入重放只有一个 Admission；实际 Gateway SIGKILL 后，
  在 Worker proof 已离线时重投原始 Reply bytes 到新的 broker sequence，复用耐久 receipt，
  外部发送数保持不变。
- 两个独立 OS Worker 同时运行，同一 Session 只有队首取得活跃执行资格。阻塞模型后强杀
  持有者（真实返回码 -9），另一进程在最终耐久 lease 到期后接管，旧 Attempt ABORTED，
  唯一候选/Completion/Final，后续同 Session Run 正常完成并 Delivery ACCEPTED。
- 最后一个 Attempt 被强杀时，另一进程执行 SYSTEM_TERMINATION，得到 FAILED/NONE，
  不调用新模型、不写候选/Final、不推进 accepted head；下一 Run 正常执行且不包含失败输入。
- 实际进程 race detector 无报告，专用进程、容器、私有凭据文件清理完成。强杀发生在模型
  响应之前，不将这些断言描述成已覆盖“部分 SSE 输出后强杀”。

这次联合运行发现并修复了旧 HTTP Profile fixture 掩盖的契约偏差：Control 的管理 API
接受完整 DSN，但仅加密并在 runtime purpose=`dsn` 返回 password。Worker 现以固定已发布
StorageTarget 与 password 组装连接，不再把返回值当作完整 URI。独立回归用例在旧 Factory
上实际失败，在新 Factory 上通过，并验证特殊字符/IPv6及形似 URI 的密码不会覆盖目的地。
Control 既有凭据契约保持不变，旧 Profile fixtures 已统一为 password-only。

上述证明跨进程装配与恢复，不等于实际线上 Provider 或真实 Telegram Bot 的交付验收。
完整使用与证据说明见 [联合 Gate](../../../scripts/worker-v1-joint/README.md)。

### W4：live 凭据、Session/Reply 依赖与容量专项

在 W3 同一实际三服务 Harness 上增加 `--contracts`，与 `--race --faults` 同次执行通过。
本轮产品 Go 源码未改变；通过新增外部 fixture 钩子和真实故障场景检查已有实现，而非修改
产品状态或降低协议校验来得到通过结果。

- **WV-14**：实际 Control 公开 live rotate/clear 与 DSN 改目标 409；旧初始化 Attempt
  不混批次，新 Attempt 用新 key；两项 credential clear 后旧 Attempt 仍可提交 Session，
  新 Run CREDENTIAL_DENIED、零模型/零候选、head 不变，固定失败 Final 实际 Delivery ACCEPTED。
- **WV-16/18/23**：服务端实际 flush 非终态 SSE 后 OS SIGKILL，接管后的正式历史不含旧
  部分输出；Session 专用角色登录中断期间，既有 Completion/head/Final 不变、已完成 Run
  重放不执行模型，新 Run 先依赖失败，恢复后新 Attempt 读取原正式历史继续。
- **WV-26/27/29**：实际 proof 暂不可用时原 Reply 已消费并重投但不提前 ACK；恢复后同
  sequence 正式接纳。三种 Final 内容/身份变异得到真实 proof 409 与 durable CONFLICT，
  两个未知 wire 字段得到 INVALID_WIRE，两个未知 Intent 保留待证明且零重复发送。
- **WV-32**：显式入队/活跃/扫描容量均为 1，第二条 Gateway 已发布请求在容量满时尚无
  Worker Run 和模型调用；释放前项后原请求自动恢复，Gateway publication attempts 不增加。

本轮不把这些证据扩大为未观测的分支：SSE flush 不等于直接看到 SDK 内部消费；Session 登录
故障不等于 Put 提交后丢响应；Tenant/Manifest 未知 wire 字段拒绝不等于合法 proof 身份不符；
未知 Intent 的 404 重试未触达 SQL UNIQUE(run_id)；排队背压不等于累计历史或磁盘容量门禁。
实际外部模型和真实 Telegram 验收依旧未完成。

### W5：接管窗口、Manifest 恢复与 Revision 作用域

联合入口增加 `--recovery`，复用实际三服务与 PG/NATS，不增加产品运行模块或存储协议。

- **WV-03**：真实 receipt INSERT 后、接管事务提交前强杀 Worker；Run/receipt 未可见且
  broker 原消息未 ACK，重启后从同 sequence 恢复。另在接管/完成已提交时让私有 broker
  明确拒绝 Run ACK，强杀并恢复后重放原 receipt；唯一 Attempt/候选/Completion/Final 不变。
- **WV-06**：真实空 Owner Export 完成与 Worker ready；Worker 离线时公开发布 6 个
  Manifest，实际分页中途发布不越过固定 upper，新 Export 纳入新项；增量与导出重叠
  不重复投影/receipt，离线 Run 恢复，二次重启不重新执行。MaxAge=0；未测 broker 丢失重建。
- **WV-09/10**：同私聊真实公开切换 revision 1→2→1，按新 Revision 隔离、切回续接
  原历史；仅 RouteGeneration 刷新不新开 Session。群 topic 输入则真实持久 ignore、
  零 Admission/Run/SDK，与当前 Gateway 私聊文本首版边界一致，不声称 Thread 执行已覆盖。
- **WV-29**：实际 Worker proof 409 的变异 Final，在精确拒绝 receipt INSERT 故障时
  至少两次 PG 错误、零 receipt/额外发送、原 sequence 保留；恢复后 durable CONFLICT 才 ACK。

所有故障对象和 ACK 权限均恢复；私有 NATS 配置是源码拷贝，生产拓扑没有改变。
本轮还修复测试 HTTP response 的关闭路径；资源诊断不靠屏蔽警告通过。
真实 Provider/Telegram 与其他剩余 WV 专项继续独立记录；跨 Tenant/Account 执行补验见 W6。

### W6：提交响应丢失、跨 Tenant/Account 与显式保留 Run 上限

联合入口增加 `--durability`，与 `--race --faults --contracts --recovery` 同次执行。

- **WV-17/23**：实际 Session INSERT 的 PostgreSQL `INSERT 0 1` 与 `ReadyForQuery I`
  已被代理观察，27 bytes 成功响应全部未交给客户端。断开前独立读取已提交唯一候选，
  Completion/Final 仍为零且 accepted head 未变；丢响应后真实 Adapter 用新连接精确
  核验，同一个 Attempt、一次模型调用、同一候选完成接受，下一轮正式历史精确续接。
  故障代理仅作用于预先公开发布的 Session 目标，不代理 Worker/Control/Gateway 账本连接。
- **WV-09**：同一私聊 A1→A2→B1→A1→A2→B1，真实管理 API 创建三 Account、两 Tenant，
  同 Tenant 的 A1/A2 绑定相同 Manifest。六轮实际 SDK/Session/Final 保持三个独立正式
  历史，各自 sequence 1→2、parent 正确、无跨身份消息；Delivery 精确回到原 bot_id。
  完成后公开停用新增 Account/Binding 并恢复原身份。外部 Bot API 仍是显式 fixture。
- **WV-32**：产品新增必填正整数 `limits.max_retained_runs`，全部状态 Run 在原接管
  事务容量锁内计数；`max_queued_runs` 继续只限制未完成 Run。真实 PG 覆盖已完成/失败
  历史满额、零接管副作用、满额回放/alias/Claim/续租/Completion/Final、独立 pool 最后一槽
  竞争和下调/提高配置。实际进程 gate 在 active=0 时拒绝新接管，只提高显式配置并重启
  后从原 broker sequence 唯一恢复，保留旧历史、不重新发布、不用 SQL 修补业务状态。

该改动没有新增表、迁移、数据库实例、Token 额度、Session 协议或通用预算/GC。Run 容量
不是辅助 alias receipt、拒绝/冲突、Manifest receipt/tombstone、Session 累计候选或磁盘的
完整容量方案。§13.2 已明确拆开已实现部分与后续缺口，WV-32 不据此整项标为完成。

### W7：Resolve 不确定性、固定执行窗与发送 UNKNOWN

联合入口增加 `--uncertainty`，与此前全部矩阵同次运行；本轮产品改动只修正 SDK Adapter
对已发布输出参数的判断，其余新增为测试与验收记录，没有新增运行模块或存储协议。

- **WV-11/12/13**：真实 Control 完整200响应到达私有 mTLS relay 后，分别断开下游或
  等待 Worker 显式800ms请求超时。旧 Attempt 零SDK、不重复Resolve，新 Attempt 完成
  唯一候选/Completion/Final。实际 Profile 行锁等待期间强杀原 Worker，durable lease
  到期后解锁，同一授权组得到第一次真实proof200、锁后第二次proof403，Control拒绝旧
  授权，新Attempt恢复。代理保留真实身份且不合成成功业务响应。
- **WV-31/36/37**：实际 Manifest 的120s执行窗与显式150s Run窗分离；强杀接管不延长
  首次Claim固定的执行期限。在Run尚未到期、末次lease与执行窗同刻到期时，唯一
  SystemTerminalizer结束为FAILED/NONE。另验证8s Run期限先于20s退避，以及6s新排队
  Run在保留30s原策略的活跃队首后过期；不新建Attempt/SDK，不推进accepted head，
  后继输入继续正式Session。没有改写数据库业务时间或Manifest。
- **WV-26/27/29/30**：真实Telegram SDK已向外部fixture交付一次消息，HTTP响应前断开；
  Gateway耐久UNKNOWN、part attempt=1、无自动重发。Gateway真实强杀重启后，即使
  Worker proof离线，原Reply重放仍从耐久receipt接纳并ACK，没有第二次发送；后继
  输入仍读取原正式Session。transport ACCEPTED只表示耐久交接，不表示外部平台确认。
- **WV-40参数对齐**：移除把节点Schema的262144上界误套到execution.max_output_tokens
  的Worker判断；共享Schema本身保持不变。未知fixture模型的实际SDK请求验证300000
  fallback和18000节点override。另以真实SDK请求明确记录v1.11.2已知模型表的原生裁剪：
  `gpt-4o` 的18000变为16384。关闭tailoring不关闭该逻辑；没有改写SDK，也不将
  “Worker不新增限额”扩大为所有模型wire均原样传递。

测试辅助代码同时修复实际ID格式取证、截断HTTP响应与资源清理，并为Docker端口发布增加
状态诊断及有界等待；失败日志保留，失败运行不计为验收通过。原先一次docker port失败
缺少容器State/log，未将其武断归因为发布竞态或NATS启动失败。

W7未覆盖的credential uses负例、proof依赖传输错误由W9继续补验；主机/DB时钟偏移、
独立Reply期限由W10补验，实际外部Provider/Telegram仍单独记录。W7发现的SDK已知模型输出裁剪不是最终契约；其适配修复见W8，
不新增累计预算或以替换模型名称绕开SDK。

### W8：通过 SDK 公开接缝保持显式输出参数

按 §9.4、§11.2 和 WV-40 重新审查后，确认只记录 SDK 原生裁剪并未完成“不暗中修改
generation 参数”的要求。产品在已有 SDK Adapter 中增加公开 `WithChatRequestCallback`，
仅把已验证的有效上限同步写回 typed `MaxCompletionTokens`；仍固定 VariantOpenAI，
不开放 ExtraFields/JSON overrides，不 fork SDK，不修改模型名、HTTP transport 或历史。

- 实际 SDK/HTTP 测试覆盖已知模型的显式值、fallback、节点override、低于SDK cap的值、
  并发Attempt隔离，以及Provider拒绝后不隐式降额/重试。旧实现用同一目标断言实际失败；
  新实现请求参数按Manifest传递。未适配SDK的默认clamp保留为独立特征测试，不作Worker目标。
- 联合Harness新增显式 `--model-name` 选择，通过真实Control Profile公开发布模型名称；
  默认fixture名称保留。已知模型联合运行使用真实公开平台默认上限4096，并与实际发布
  视图逐项核对；它证明公开发布到实际请求的模型/参数接线，不冒称完整公开发布已测更高上限。
  超过SDK静态cap的参数由上述实际SDK边界测试证明。

本轮没有新增预算系统、运行模块、迁移或Session协议。请求参数原样传递不保证外部
Provider接受参数或输出满额；真实Provider和真实Telegram联合验收仍按WV-34/41独立完成。

### W9：凭据整批拒绝与在线证明错误分类

联合入口增加 `--authorization`，复用 W7 的真实 mTLS relay，在 live clear 前运行。
产品 Go 实现、数据库 Schema、八角色、SDK 参数与 Session 协议保持原样；本轮只新增
测试矩阵、加强现有 Factory 断言，并校正文档中已过期的“统一403/未接线”描述。

- **WV-11**：真实 Control 完整200批次经显式测试代理分别变异缺项、多项、等长重复use、
  合法形状的AttemptID错配。原始owner200和响应变异分开取证，不声称Control返回了错误授权。
  实际存活Worker每例只有一个`FAILED/CREDENTIAL_DENIED` Attempt，零SDK/候选，
  一个ATTEMPT Completion和固定失败Final；Gateway实际Delivery ACCEPTED。accepted head
  不变，seed→拒绝→follower同Session序号1→2→3，后继精确排除失败输入/失败Final。
  实进程身份错配代表项是AttemptID；其他批次身份、用途和值由Factory的16项负例覆盖，
  每项另证明runtime=nil、零Session构造器、重复Prepare不再次Resolve，不把两层证据混合。
- **WV-13**：真实Worker proof200完整到达代理后丢响应，实际Control返回503；存活Worker
  原Attempt以DEPENDENCY_UNAVAILABLE结束，Run经退避由新Attempt恢复，只有一次模型/候选/Final。
  另一场景只变异合法proof请求digest，实际Worker返回403、实际Control返回403，Worker稳定
  CREDENTIAL_DENIED并发送固定失败Final，不自动重试该Run。代理不合成403、不破坏Schema，
  也不把这个请求负例描述成有效Grant本身被否决或Final proof身份错配。

同库四Schema/八角色继续由真实数据库隔离gate验证。本轮不新增数据库实例、运行模块、
累计预算或隐式输出额度；实际Provider/Telegram、其他明确列出的首版专项继续独立保留。

### W10：合法 Final 身份错配与独立 Reply 期限

联合入口增加 `--finality`，复用真实 mTLS relay，先于保留待证明 Reply 的负例与 live clear
运行。本轮产品 Go、数据库 Schema/八角色、SDK 参数与 Session 协议没有变化。

- **WV-26**：真实 Worker 完整合法 proof200 经显式代理只改变 `tenant_id` 或
  `manifest_digest` 一个合法字段，原八个请求字段保持不变；通过严格 codec/client 后，
  Gateway Acceptor 对照真实 Admission 固定目标进入 `UNAUTHORIZED`。原 Outbox digest、
  stream incarnation/sequence 精确对应 durable `REJECTED` receipt，之后 ACK；两例
  victim 零 Delivery/外部发送，原成功 Attempt/Completion/candidate/outbox 不变。
- **WV-31**：实际 DB 同一时刻观察 Reply 的4s期限已过、Run的150s和Manifest执行120s
  尚未到期且lease仍活跃，再释放模型。Run/Attempt仍 `SUCCEEDED`，正式Session提交并
  接受候选；Completion 为 `reply_disposition=NONE`、`reason=DEADLINE_EXPIRED`，无Final
  Outbox。回复过期不把成功执行改成失败，也不撤销正式历史。
- **WV-25/31**：Gateway在模型释放前正常停止，Worker在20s Reply期限内提交并发布原
  Final；真实NATS保留其原字节到DB自然过期，再启动Gateway。原Attempt已耐久结束但
  真实Final proof仍为200；Gateway持久 `REJECTED/EXPIRED` 后ACK，零Delivery/发送。
  原stream incarnation/sequence/digest完整关联，不通过重发或修改业务SQL制造晚到。
- 四个victim之后的新Run均使用新Admission/期限，同Session follower准确包含未发送但
  已接受的原成功历史，正常Final被接纳。故障对象恢复、进程/容器/连接清理均独立验证。

测试恢复路径另外要求返回的Worker `ready=true`；恢复抛错或显式 `ready=false` 都把该case
与整体标为FAIL。以上是实际三服务与明确模型/Telegram fixture的证据，不代替真实外部渠道。

### W11：Manifest 拒绝、增量源缺口恢复与 NATS 容量

首版补验继续复用既有 Processor、Owner Export、Relay 与 Bootstrap，不增加产品运行模块、
数据库实例、迁移或 Session 协议。新增项进入显式命名门禁，缺失或 Skip 都不算通过。

- **WV-05**：`TestWorkerManifestRejectionPGNATSSDK` 用真实 PG/NATS Consumer、Projection、
  Reader/Processor、SDK 与正式 Session，检查12种负例和前后两个有效对照。负例涵盖
  重复身份、摘要、Schema/平台版本、release pin、固定 Route ref/revision/digest、跨 Tenant、
  合法公开 `manifest_view` 冒充 Envelope/Content，以及同身份不同合法内容的持久冲突。
  每例直接计数 Claim/Prepare/SDK Execute、Resolve/模型 HTTP，并读取 Attempt、候选与
  Final Outbox；负例均为零。错误发布的耐久拒绝加原 Run `MANIFEST` 等待，与 Reader
  在 Claim 前生成 `SYSTEM_TERMINATION/NONE` 分开记录，不把等待描述成 Run 已拒绝。
- **WV-06**：`--manifest-rebuild` 只重建私有 broker 的 Manifest Stream/durable，保留原
  配置和其他三类 Stream/durable；原 Control 已发布但 Worker 未见的增量确实消失。
  新 Worker 经真实 mTLS Owner Export 重建缺失固定投影，Manifest Stream 仍为空且
  Control 已 PUBLISHED 的原事实不重发；原未完成 Run 接管、新 Manifest Run 与二次重启
  幂等分别取证。没有改写业务 SQL、使用公开 View 或读取 Draft 补正文。
  这是实际增量来源丢失/替换的恢复，不冒称 MaxAge 时间淘汰或整个 broker 灾难恢复。
- **WV-32**：`TestWorkerReplyCapacityPGNATSSDK` 检查真实 NATS 字节容量拒绝期间原
  PostgreSQL Outbox 保留；只提高显式私有容量后，原 payload/digest/Intent 经真实 PubAck
  标记，Agent、Session 和 Completion 不重跑。App Relay 的显式 PollInterval 退避与
  取消由独立真实循环单测支持，不把手动 Tick 调用当作进程时序证据。

按 §16.1 逐项复核后，物理移动主机时钟、每条 SQL 的断连排列、每类故障都再接外部模型
并非追加首版条件；已有真实 PG、SDK 和跨进程故障证据按各自层次保留。外部联合验收
集中在 WV-34/40/41：同次真实 Provider 与 Telegram，两轮、重启、重投及参数/usage 关联。

### W12：最新 main 与真实 Provider / Telegram 联合验证

本轮将远端 `main=f61d49b09b8ae534f39f8065ddec699aa35ee10e` 与原 Worker WIP 合并。
Gateway 同时保留上游 Telegram preflight 和 Worker proof / Reply consumer 的启动、运行与关闭；
补齐 preflight 测试所需的完整 Worker-enabled 启动配置。Control 并发迁移验收由固定一行
改为逐项比对二进制嵌入的全部 migration，当前两项均恰好执行一次，runtime 权限不放宽。

实测由本机 Telegram 的 `JFS Channel SDK Lab` Bot 文本进入，Control 公开 API 发布固定
Manifest，实际 Worker 二进制运行 SDK 与正式 PostgreSQL Session，再由 Gateway 发送 Final。
模型为 `deepseek-v4-flash`；本地仅使用逐字节转发至真实 DeepSeek HTTPS 的观测代理，
没有生成模型 fixture 响应。四轮输入得到四个成功 Completion、四个 Session commit；
Session sequence/accepted head 推进到 4。第二轮及重启后的第四轮均回复原始标记
`WKR-20260907-0918`，正式历史与模型请求逐项关联。

- 模型共五次请求：四次 HTTP 200、一次代理上游 TLS EOF。第三轮首次 Attempt 失败后，
  Worker 的既有重试产生新 Attempt 并成功；没有隐式调整模型或输出参数。
- 实际请求均保持已发布的单次输出字段 `max_completion_tokens=4096`；每个成功响应的
  Provider input/output/total usage 与相应 Run/Attempt 的 Worker usage 完全一致。
- Telegram 共三个 Final 实际 `ACCEPTED`，并在本机客户端看见；第三轮旧 Gateway 在
  `getMe` 准备阶段出现 `NOT_SENT/permanent`，没有进入 `sendMessage`。当时日志未保留具体
  getMe 错误原因；排查确认该路径会把网络错误混同鉴权失败，并由实际 Reserve 调用接缝的
  EOF/500/429 回归复现并修复，不把该次网络根因当成已证实事实。401/403、
  错误 Bot 身份和已撤销 Permit 仍拒绝，原有准备阶段次数上限不变。
- 修复后 Gateway 重部署，第四轮 Final 成功。第三轮历史失败未用 SQL 改写或暗中重投。
- 保持原 RunRequested 内容及原 Final payload 重投，使用原工作负载 NATS ACL 和真实
  JetStream PubAck；停掉 Worker 后重启 Gateway，原 Final receipt 仍 ACK。两条 consumer
  的 ack floor 均推进到新 sequence=5，pending/ack_pending 均为零；模型、Run、Attempt、
  Completion、Session 与外部 Delivery 的原有计数/事实保持不变。
- 同库隔离 gate：119 个实际 SQL 断言、Control 集成 gate、Gateway 125 tests/subtests，
  命名依赖 gate 零 Skip。Go 全量回归90个含测试 package、Python helper138项、Web525项、
  TypeScript 与 Next.js production build 通过。

这是一次可回收的真实联合重部署，不是对旧数据卷做原地迁移。W12 验证时的 main 基线尚无 long polling
实现，因此临时按 webhook 验证，结束后恢复 Bot 空 webhook / 原 Gateway 接收进程，
不将其他未合并工作树的 receive-mode 实现混入本次基线。

原始证据位于 `/private/tmp/worker-v1-main-live-20260907-090318/VERIFICATION.txt` 与同目录
`live-evidence/acceptance-summary.json`、`restart-replay.json`、`replay-consumer-acks.json`；
记录明确区分修复前失败、修复后外部成功及确定性的回归测试。

## 3. 首版门禁覆盖追踪

“本地已覆盖”仅指表中列出的测试层次；未据此将全项标为真实交付完成。
按 §16.1 的明示条件组合已有证据，不把“更多部署组合”等无限扩展措辞当成新增首版门禁；
实际 Provider/Telegram 联合要求集中记录在 WV-34/40/41，其他行保留其准确测试层次。

| WV | 已有证据 | 仍需的联合/专项证据 |
| --- | --- | --- |
| 01–02 | Ledger 真实 PG：并发重投、Run/Event/Admission 冲突与稳定 Receipt；W3 真实 webhook 重放单 Admission | 已有 PG 并发/冲突与实际入口重放共同支持；没有追加跨入口全排列要求 |
| 03–04 | W5 接管提交前真实强杀、已提交 Run 的 ACK 通道故障与原 sequence 重启恢复；真实纵切 Run 先于 Manifest | ACK 故障为真实 broker 权限拒绝；Manifest 延迟为 PG/NATS/SDK 纵切，不冒称随机网络丢包 |
| 05 | W11真实PG/NATS/Reader/Processor的12种错误Manifest、零Claim/Resolve/SDK/候选/Final与两个有效SDK/Session对照 | 区分耐久错误发布拒绝后的缺失等待和Claim前终态；模型/Profile HTTP为fixture |
| 06 | W5空导出、分页固定upper、6项离线重叠；W11唯一Manifest增量源丢失后真实Owner重建、原Run接管与二次重启幂等 | 当前MaxAge=0，实际验证增量源缺口；没有时间淘汰或整个broker灾难恢复的验收声明 |
| 07–08 | 真实 PG 多连接争抢、锁后 Fence；W3 双进程同 Session 排队与 SIGKILL 到期接管；W7 Profile 行锁后真实 proof 复核 | PG 锁后 Fence 与实际强杀/锁后授权复核组合支持，不追加无界锁竞争排列 |
| 09–10 | W5 Revision 1→2→1/Generation；W6 同私聊三 Account/两 Tenant 六轮三份独立正式历史 | Thread 执行仅有 Worker 域证据；实际 Gateway topic ignore、零执行，未开放群聊 |
| 11–13 | 批次关闭JSON/16项Factory负例；W7实际响应丢失/超时与锁后200→403；W9四种批次变异整批拒绝、真实proof响应丢失→Control503新Attempt及真实owner403→稳定失败Final | 实进程批次身份代表项为AttemptID，其他身份为Factory证据；不混合层次，外部联合要求另列 |
| 14 | W4 实际 Control live rotate/clear/改目标拒绝；原 Attempt 不混批次、新 Attempt 新 key、clear 后新 Run 拒绝 | live 授权协议已有真实 Control/Worker 证据；外部联合验收见34/40/41 |
| 15–18 | SDK sticky error；W4 部分 SSE 强杀/Session 登录故障；W6 实际候选已提交丢响应、新连接固定身份核验且不重跑 | 真实SDK/PG与实际进程故障互补；不把每种中断再接外部Provider追加为独立首版条件 |
| 22–24 | 显式准备/无隐式 DDL；W6 候选已提交丢响应同 Attempt 核验；Outbox 故障使 Completion Tx 全回滚 | 实际Session constructor/SDK错误、唯一候选写与Completion事务边界已分别覆盖；不追加无限SQL断点排列 |
| 25–30 | W3 Gateway -9 与原 Reply 重放；W4 proof恢复/变异；W5拒绝落账失败不ACK；W7外部接收后响应丢失、UNKNOWN与重启不重发；W10合法proof Tenant/Manifest错配、已结束Attempt proof200与过期原Final ACK | W12 真实 Telegram/模型、Worker 重启与原事件重投已取证；保留旧准备阶段失败 |
| 31 | W7固定Run/执行窗与排队到期；W10独立Reply过期/原Final晚到；真实PG最大未来ReceivedAt偏差拒绝 | 以DB时刻判断资格；不声称实际移动过主机/NTP时钟，也不追加物理时钟全排列 |
| 32 | W4活跃/入队/扫描上限；W6全状态retained Run；W11真实NATS容量拒绝后原Outbox恢复、Relay轮询退避 | 辅助receipt/tombstone/Session总量与磁盘、GC按§13.2后续治理；不称所有数据已有总预算 |
| 33 | 真实 App Drain 跨 lease 续租；W3/W4 实际 OS SIGKILL，含部分 SSE flush 后接管 | 真实App Drain/证明/续租与实际强杀共同支持；外部联合验收集中在34/40/41 |
| 34 | W12 指定 Bot/会话/Binding，四轮真实文本、三条实际 Final；Worker 重启、重复 RunRequested | 第三轮旧 getMe 准备失败保留；修复后重部署新轮次成功，不将其算作历史失败补发 |
| 36–37 | W7真实双进程固定120s执行窗、lease同刻到期、RETRY_WAIT/排队到期、唯一NONE与后继正式历史；W3最后Attempt强杀 | 明示末次Attempt、失租同刻到期与RETRY_WAIT条件已有实际证据；不追加其他排列 |
| 39–40 | WorkerV1静态发布；W7区分execution/node上限；W8公开SDK回调保持已知模型显式参数、拒绝不降额重试；无Memory/累计Token初始化 | W12实际DeepSeek保持发布参数、四次成功usage与Run/Attempt逐项一致 |
| 41 | W3 跨进程 fixture；W12实际DeepSeek/Telegram从公开发布到Final的同次关联轨迹 | 观测代理透明转发真实模型；三个实际ACCEPTED与一个修复前NOT_SENT分别取证 |

## 4. 可重复命令

```bash
# 普通回归：某些外部依赖集成用例可以 Skip，不作为 PG/渠道验收。
go test -count=1 ./...

# 自建 Docker gate：命名 Go gate 要求零 Skip；跨进程 gate 要求全部断言成功。
python3 scripts/test-v1-database-isolation.py
python3 scripts/test-worker-v1.py --race
python3 scripts/test-worker-v1-joint.py --model-name gpt-4o --race --faults --contracts --recovery --durability --uncertainty --authorization --finality --manifest-rebuild
```

Worker gate 串行运行会重置 Worker Schema 的测试，在单独 broker 上验证实际 ACL，并最后运行
实际 bootstrap 的 mTLS/Drain 用例。生产数据库、卷、Bot 和凭据不作为测试脚本的隐式输入。

## 5. 下一步与后续计划

W12 已完成首版明示的真实 Provider/Telegram 联合纵切与重启/重投验证。本批交付将
Worker、Control/Gateway 接缝、同库隔离、测试与文档整理为 `worker` 分支的独立提交；
提交和远端状态以 Git 引用为准，不把分支发布等同于合入 main 或其他工作树完成同步。

用户手工 Telegram 验收使用独立后台进程，标准输入关闭或助手回复结束不会清理服务；
本次提交不重启服务、不改 Bot/Binding/Webhook，不把本机 `.env` 或测试凭据纳入源码。
后续持久部署仍需另行选择部署目标、常驻运行配置与数据迁移窗口。

后续运维改进可单独补充 Telegram 准备阶段失败原因观测及终态 NOT_SENT 的显式人工恢复
流程；不在此次验收中直接修改旧账本或引入静默重发。long polling 按 Channel/Gateway
独立分支发布后再对齐，不提前把未合并实现纳入当前 V1 基线。

Memory 整体、SDK 原生组合、Tools/Knowledge、累计 Token 预算、Session GC/历史迁移、
进阶调度、企微专项、Channel 页面与 Helm 继续按设计 §15.2 独立后置，不悄悄恢复为首版条件。

## 6. 历史阶段记录：IM Tracing 增量（M1，本地技术验证）

基于 `worker/fd1f790`，当前 task 独立实施 M1：共享 Carrier/Trace Runtime、两个服务配置与
生命周期、SDK Agent/LLM/Tool 接线和正文过滤、完整 Runner 流生命周期、Worker 日志关联。
实际 OTLP protobuf 和真实 SDK + 本地确定性模型/Tool 的测试通过；这不是外部模型/Telegram
Trace 联合验收。SDK Tool 测试没有改变 Manifest 能力；Memory 继续后置。

M1 没有 migrations 或 NATS/数据库 Carrier 接线，没有正式 Session 分层/Reply send Span，
没有切换当前手测服务或部署 Collector。M2–M5 仍按
[实施计划](../operations/im-runtime-tracing-v1-plan.md) 继续；旧 Worker V1 的历史验收不作
新 Tracing 的端到端证据。所有修改都在当前 Worker 工作树，不派发其他工作树修改。

## 7. 历史阶段记录：IM Tracing 持久入站（M2，2026-09-08）

M2 已实现 Gateway Outbox creation Carrier → NATS Header → Worker process context
同事务接纳 → ScheduledRun/activeCtx 恢复。两个服务只追加可空观测列，不改变事件 JSON、
摘要、幂等、ACK 或 Session/Run 决策。专项真实 PG/NATS 与新的恢复测试进程证明元数据
可以持久读取；完整部署重启/真实 IM 仍由 M5 验收，不复用旧 W12 的成功结果。

本轮没有切换手测服务。M3 正式 Session、M4 Reply/Delivery、M5 部署仍待继续，
参见 [计划 §17](../operations/im-runtime-tracing-v1-plan.md#17-m2-实施记录2026-09-08)。

## 8. 历史阶段记录：IM Tracing Session 分层（M3，2026-09-08）

M3 已增加实际 Manifest/Claim/prepare/credential/open/load/stage/verify/complete/commit/
terminalize 边界。正式 Session commit 以事务返回区分 accepted/failed/UNKNOWN，重放不
制造第二次 commit；SDK overlay 只记录真实操作，partial append 汇总到 Runner 数值属性。

专项实际 PG 与 SDK/OTLP 测试验证上述区别，未改执行次数、候选核验、租约或预算。完整
部署的网络不确定性/重启及真实 IM 仍留在 M5；下一阶段为 M4 Reply/Delivery。


## 9. 历史阶段记录：IM Tracing Reply / Delivery（M4，2026-09-08）

M4 已在当前 Worker 工作树实施：Reply Outbox 不可变 creation Carrier、真实 NATS Header、Gateway 两事务接纳、TracedClaim 恢复、真实 send certainty、内部 mTLS proof 传播。实际 PG/NATS/mTLS 和发送分支专项测试通过；Memory 继续后置。下一阶段 M5 为部署、真实进程/Telegram 与故障回滚联合验收。该阶段结束时运行部署未切换，Tracing 增量尚未提交。


## 10. 当前交付与审查修复

M5 完成状态、此前真实 Telegram / 外部模型 / 正式 Session 及故障恢复证据，以专项计划
§11、§28 和联合验收记录为准，不将旧阶段日志当成当前状态。本次仅修复审查发现，
不重启或替换运行中的 Worker/Gateway，不改数据库、接收模式或模型/预算契约。
加强后的父链门禁同时检查正常单根、无环、关键 ancestry 与消息 creation Link；
崩溃模式只容忍具体缺失 parent，不能把错父级或多根当成恢复缺段。


## 11. 合入 main 的兼容边界

Tracing 与 main 的授权、Pending Intake、配额和模型消费门禁并存：生产接纳仍使用
`NewIntake`，保留 main 的 current authorization / quota reconciler / BeforeModel，
Gateway 保留 policy projection、授权刷新和 Telegram receive-mode runtime。
本次合并不启用这些可选能力，不修改其业务契约，也不替换当前手测进程。

Tracing 的四个迁移文件已在独立 Worker 版本执行过；迁移账本以完整文件名和摘要为身份，
不是数字前缀。本次保留原文件名及字节，与 main 的同前缀授权/策略迁移共存，
新增不可变摘要回归测试，不重命名或改写已执行迁移。
