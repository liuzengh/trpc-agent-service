# Worker V1 真实跨进程联合 Gate

入口：

```bash
python3 scripts/test-worker-v1-joint.py --artifacts /absolute/evidence-directory
python3 scripts/test-worker-v1-joint.py --race --faults --contracts --recovery --durability --uncertainty --authorization --finality --manifest-rebuild --artifacts /absolute/evidence-directory
python3 scripts/test-worker-v1-joint.py --model-name gpt-4o --race --faults --contracts --recovery --durability --uncertainty --authorization --finality --manifest-rebuild --artifacts /absolute/known-model-evidence
PYTHONPATH=scripts/worker-v1-joint python3 -m unittest discover \
  -s scripts/worker-v1-joint -p 'test_*.py' -v
```

依赖 Python 3 标准库、Go、OpenSSL、Docker；使用仓库固定的 PostgreSQL 17.6 与
NATS 2.11.8 镜像。每次执行创建独立容器、随机数据库及 broker 密码、临时 CA/证书目录，
并在 `finally` 中停止进程、删除容器与私有文件。只继承编译/本地运行必需的环境变量；
不会从已有部署读取数据库、Bot、模型或平台密钥。Evidence 目录保留公开发布结果、
静态状态日志、PID/退出状态与测试断言，不保存私有配置和凭据。

## 真实路径和外部 fixture

以下均运行实际编译的二进制：

- **Control**：首次管理员引导 → HTTP 登录/改密 → 创建 Owner/租户 → AgentVersion →
  ProfileRevision → DeploymentRevision → 实际 Manifest Relay/Owner Export。
- **Gateway**：实际管理 API 创建 Account/Binding、Credential、Enable → mTLS Catalog →
  显式 `config.receive_mode=webhook` → getMe/getWebhookInfo/setWebhook 注册及复查 → 认证 webhook → Admission/Run Outbox → NATS。
  账户使用新 receiver 凭据消费授权；多 Bot identity 等待查询当前 receiver 的目录版本、
  managed URL/revision、owner lease 和实例资格，不再等待旧 registration attempt 账本。
- **Worker**：固定 Manifest → mTLS Profile 批次与 Control 在线 Grant 反查 → SDK →
  PostgreSQL 正式 Session → Completion/Final/Reply Outbox。
- **Gateway**：已提交 Final 证明 → Delivery 与 Reply transport receipt → SDK sendMessage。

只有 **OpenAI-compatible 模型服务**、**Telegram Bot API 服务**及 webhook 外部 HTTPS
入口使用显式本地 fixture。Control、Worker 证明或 Profile 接口不由 fixture 代替，
不手写/修改 Manifest，不通过 SQL 插入或修复业务状态。

四个 schema 和八个角色来自实际 `provision-schemas.sh`；各服务自行迁移。
Session preparation 是独立显式 CLI，不是 Worker 运行时 DDL。产品表仅读取证据；
故障场景仅在私有实例临时改变角色能力或安装匹配单条消息的故障 trigger，并在 finally 恢复。
管理面接受 DSN 时拆分固定 destination 与 password；在线 purpose=`dsn` 返回 password，
Worker 只使用已发布 destination 组装连接，不把凭据值作为可覆盖目的地的 URI。
NATS 使用实际 TLS/ACL 和 reconciler，服务运行身份不创建或重置拓扑。

## 常规断言

`--model-name` 显式选择经真实 Control Runtime Profile 发布的模型名，默认保持
`joint-fixture`。该参数不切换到外部 Provider，模型 HTTP 地址仍是本次私有 fixture；
fixture 保留请求并在普通/部分 SSE 中返回同名模型，不把已知名称替换成绕过 SDK 的别名。
两轮后从真实公开 DeploymentRevision 读取 `manifest_view`，按根 LLM 的 resource 和
节点/执行上限检查实际 SDK 的 `model`、`max_completion_tokens`，并拒绝额外 `max_tokens`。
当前完整公开发布使用平台既有默认上限4096，不为测试修改平台契约。超过 SDK 内置 cap 的
18000/300000等值由独立真实SDK/HTTP契约测试验证，不把两个证据层次混为同一次运行。

- 公开 HTTP 发布响应和实际 Control Outbox `PUBLISHED`；Worker Owner Export/增量追赶后 ready。
- 负向 mTLS 预检通过实际 Worker 与实际 Control 在线回调，伪 Grant 一律 403；
  不把网络连通或证书握手误认为已有执行资格。
- 两轮模型 SDK 请求；第二轮实际请求包含第一轮已接受的助手回复。
- 两轮 `SUCCEEDED`、正式 Completion 和已发布 Reply；实际 Gateway Delivery 为 ACCEPTED，
  外部 fixture 收到与耐久 Final 完全相同的文本和原始 reply context。
- 错 webhook secret 为 401，同消息重放仅一个 Admission。
- SIGKILL Gateway、停止 Worker proof、重投 Worker Outbox 原始 Reply bytes 到新 broker sequence，
  重启后复用既有耐久接纳事实，不再发送一条外部消息。

## `--faults`

`faults.py` 管理两个**同时存活的独立 OS Worker 进程**。每个进程拥有不同 WorkerID、
客户端证书与真实监听端口。测试 L4 relay 只透传 TLS 字节，模拟稳定服务地址；
它不持有证书，不解析或制造证明，也不重试已发送的应用字节。

故障案例包含同一 Session 串行资格、阻塞模型后 SIGKILL 当前持有者、lease 到期后由
另一个存活进程恢复，以及最后一个 Attempt 用尽后 SystemTerminalizer 结束失败、
推进后续 Run。失败 Attempt 不进入正式 accepted history；恢复后的结果仍需实际 Gateway
Delivery 通过。模型 fixture 现在支持首次调用发送部分 SSE 后阻塞；重试同输入不再次注入部分文本。
证据明确记录模型服务端 flush，不额外声称直接观测 SDK 内部消费该 delta。

## `--contracts`：剩余契约与故障专项

该选项在初始两轮后执行 Reply 矩阵，再执行 Session 中断、容量恢复与 live credentials；
凭据 clear 场景最后运行，不将已清除的 Profile 继续用于后续正常模型调用。

- **Reply proof 中断与恢复**：在全新 Final 尚未接纳时断开到真实 Worker proof 的 L4 后端，
  清除 Gateway 旧连接。实际 NATS consumer 已消费/重投，但原消息仍保留，零 terminal receipt、
  Delivery 或外部发送。恢复后同一 sequence 生成 ACCEPTED receipt、ACK 和唯一发送。
- **Reply 变异**：正文、Generation、Completion 变异得到实际 mTLS proof 409 与 durable
  CONFLICT；非法 Tenant/Manifest wire 字段得到 INVALID_WIRE；两种未知 Intent 得到实际
  proof 404，保持待证明且无重复 Final。保持 `deny_delete=true`，负例保留到私有 broker 整体清理。
  非法 wire 字段拒绝不等于已测试合法 proof 中的 Tenant/Manifest 不匹配；未知 Intent 的
  路径没有触达 SQL UNIQUE(run_id) 分支；拒绝 receipt 写失败由 `--recovery` 独立验收。
- **部分 SSE 后强杀**：独立等待 `partial_sse_flushed/bytes>0/done=false`，再 SIGKILL
  实际 Lease 持有者；另一进程接管，重试与后继 SDK 请求、正式候选和 Final 都不含旧部分输出。
- **Session Store 中断**：仅对自建 PG 的 `session_runtime` 临时关闭登录并终止该角色连接，
  真实密码/TCP 探针被拒绝。既有 Completion/head/Final 不变，原 Final 重放不重跑模型；
  新 Run 在首次 Prepare 依赖失败时零模型调用，恢复 LOGIN 后由新 Attempt 读取正式历史。
  `finally` 恢复 LOGIN；这不是业务表写，也不冒充 Put 提交后响应丢失测试。
- **容量与恢复**：显式把入队、活跃 Attempt、scan batch 设为 1，阻塞一个模型请求。
  下一条 Gateway 已发布输入被实际 Worker 报 capacity，且尚无 Run/模型调用；释放容量后，
  原请求无需第二次 webhook 或 Gateway 再发布就执行。它验证排队背压，不等于验证累计
  历史账本容量、broker 字节上限或实际磁盘耗尽。
- **live rotate/clear**：真实 Control 公开管理 API 轮换模型 key、拒绝 DSN 改目标，旧已
  初始化 Attempt 保持旧批次完成、新 Attempt 使用新认证代次；同时 clear 模型和 Session
  凭据后，已初始化 Attempt 仍完成候选与 Final。clear 后新 Run 以 CREDENTIAL_DENIED 失败，
  零模型/零候选、head 不变，并按既有契约发送固定失败 Final，不混同 SystemTerminalizer 的 NONE。

所有产品表均只读取证。模型认证事件只保存非 secret 的代次与 accepted 布尔值，不记录 key、
其摘要或 association token。单元测试验证这些 fixture 钩子本身，完整 `--race --faults --contracts`
才验证实际跨进程路径。真实外部 Provider 和真实 Telegram Bot 的验收仍独立保留。

## `--recovery`：接管窗口、Manifest 与 SessionScope

该选项可单独运行，也可与 `--faults --contracts` 同次组合。实际 Control 启动但尚未发布
Manifest 时先测试空集合，然后依次运行下列场景；凭据 clear 保持最后执行。

- **接管提交前强杀**：在匹配输入的真实 receipt INSERT 之后，以私有 PG trigger 阻塞事务，
  同时观测 PG 正在等待、Run/receipt 尚不可见、原 broker 消息未 ACK、零 SDK 调用。
  SIGKILL Worker 后事务回滚，撤除 trigger，另一实际进程从原 sequence 恢复唯一执行。
- **接管提交后的 ACK 故障**：对私有副本 NATS 配置临时移除 Worker 的 Run ACK publish 权限，
  实际 broker 明确拒绝该 sequence 的 ACK；Run/receipt/Final 已提交但消息仍保留。强杀后
  恢复原权限并重启，由原 receipt 重放 ACK，无第二次模型、候选、Completion 或发送。
  这是 ACK 通道未获 broker 接受的故障，不描述成拦截了某个网络包。拓扑与源码配置不变。
- **Manifest 空集合与离线重叠**：实际空 Export 后 Worker ready；离线期间公开发布 6 个
  Manifest，在分页中途再发布的项不进入旧固定 upper、新 Export 能读取。启动恢复后
  Owner Export 与保留增量各落同一份投影/receipt；新 Manifest 的离线 Run 执行，再次
  重启不重复模型/Final。生产 MaxAge=0，不假装已测时间淘汰或 broker 丢失重建。
- **Revision 与 RouteGeneration**：同一真实私聊通过公开 Binding API 执行 revision
  1→2→1，rev2 使用新 Session，切回 rev1 恢复原正式历史；仅 RouteGeneration 刷新仍复用
  原 Session。真实 supergroup topics 则按当前 Gateway V1 持久 ignore、零 Admission/Run/SDK，
  不伪装成已开放群聊或已验证 Thread 执行隔离。Account/Tenant 跨身份由 `--durability` 验收。
- **拒绝 receipt 存储故障**：真实 Final 的内容变异得到实际 Worker proof 409；私有 PG
  trigger 对精确 raw digest 的 REJECTED INSERT 返回 53100，至少两次真实错误证明消费
  与重试，此时零 receipt、原消息保留且零额外发送。撤故障后同 sequence 耐久 CONFLICT
  才 ACK，不手动 ACK、不重新发布、不修改业务行。

NATS 的可变测试配置来自源码的逐字节私有副本；每次 ACK 故障都恢复原内容，保持
`deny_delete/deny_purge` 与完整 Stream/Consumer 拓扑。所有临时角色/DDL/ACL 改动都局限
于本次 Harness 自建实例，进程退出时实例与私有配置一起清理。

## `--manifest-rebuild`：唯一 Manifest 增量源缺口与 Owner 重建

该选项在基础两轮之后、其他矩阵之前运行，并安装现有真实 mTLS relay 观察 Worker 的
Owner Export GET。它只重建私有 broker 的 `RUNTIME_MANIFESTS_V1`，不是全 broker 重建，
也不是降低 `MaxAge` 或放开 message Delete/Purge。已有 reconciler 身份执行闭集的单个
Stream 生命周期操作，再以原拓扑配置重建；另外三类 Stream/durable 身份必须保持不变。

- 原 Run 已实际接管并进入模型时强杀唯一 Worker，保留其数据库事实。
- Worker 离线时从真实 Control 公开 API 发布新 Manifest，记录 `PUBLISHED` 与原 broker
  sequence/bytes，确认 Worker 尚未投影它。
- 唯一 Manifest Stream 重建后，新 incarnation 消息数为0，旧 sequence 不再可读；
  Control 的原发布事实保持 PUBLISHED，不手动重发或改写业务 SQL。
- 新 Worker 经过真实 mTLS Owner Export 200，缺失 Manifest/receipt 唯一恢复；原 Run
  在原时间窗与lease/fence规则下接管，新 Manifest 的输入使用固定快照执行。
- 二次 Worker 重启重复导出保持幂等；恢复原 Binding，并确认现有正式历史和其他来源不变。

证据为 `manifest-gap-rebuild.json`。本项证明真实保留增量缺口的 Owner 恢复；当前
`MaxAge=0`，不把它描述成按时间淘汰、整 broker 灾难恢复或新建多运行组同步协议。

## `--durability`：提交响应丢失、跨身份与历史容量

该选项可单独运行，也可与其他矩阵组合；仍使用同一实际 Control/Gateway/Worker 和私有
PG/NATS。它在真实公开发布之前固定 Session 专用代理目标，并通过管理 API 创建同一
owner 的第二 Tenant；既有默认 fixture 和无此选项的路径保持原样。

- **Session 已提交但响应丢失**：透明 PostgreSQL 协议代理只针对实际 candidate INSERT
  的 Execute/Sync 屏蔽服务端成功响应。必须同时观察 `INSERT 0 1`、无 ErrorResponse 和
  `ReadyForQuery I`，然后独立查询已提交唯一候选、零 Completion/Final、未推进的正式 head，
  才断开客户端。真实 Adapter 在新连接读取固定候选，同 Attempt 一次模型调用完成，
  下一轮正式历史与 parent 引用精确匹配。代理不记录 SQL、参数或原始认证协议字节。
- **同私聊跨 Tenant/Account**：三个显式 fixture Bot，真实 API 创建 A1/A2/B1 的 Account、
  Binding 与对应发布，按 A1→A2→B1→A1→A2→B1 六轮验证。三个正式 Session 分离，
  每个各自第二轮精确续接自己的历史，Delivery 目标和实际 Bot API fixture 的 bot_id 匹配。
  没有 SQL 业务写入，也不为测试开放 Gateway 群聊能力。
- **保留 Run 历史容量**：已完成历史仍占 `max_retained_runs`，active=0 时新输入被实际
  intake capacity 拒绝，Session/Run/receipt/sequence 无副作用且原 broker 消息未 ACK。
  仅显式提高配置并重启，原 sequence 恢复一次模型与 Final，旧历史保持不变。该场景只
  证明 retained Run 上限，不代表辅助 receipt/冲突表、Session 累计候选或磁盘已具备总配额。

协议代理在服务进程停止后关闭；临时 bot/凭据与其他私有 fixture 资源一同清理。

## `--uncertainty`：凭据响应、固定期限与发送未知结果

该选项在 live credentials clear 之前运行，使用原实际三服务、SDK、PG/NATS；没有增加
产品运行模块、迁移或存储协议。外部模型与 Telegram Bot API 仍是明确 fixture。

- **真实 Resolve 响应丢失/超时**：两个私有 mTLS relay 分别连接真实 Control runtime
  和真实 Worker proof，验证客户端证书并以相同 fixture 身份转发一次原请求。完整收到
  真实 Control 200 后才抑制全部响应正文；分别断开连接或等待 Worker 显式 `800ms`
  请求超时。原 Attempt 不进入 SDK、不重新 Resolve；新 Attempt 恢复一次候选与 Final。
- **Profile 锁后的在线复核**：私有事务持有真实 Profile `FOR UPDATE` 行锁。第一次
  Worker proof 200 后强杀持有者，等待真实 durable lease 到期，再 `ROLLBACK` 解锁。
  同一授权组第二次真实 proof 为 403，Control 同样拒绝；幸存 Worker 的新 Attempt 完成。
  relay 不因下游进程死亡取消已发送的 owner 请求，不生成替代证明，不改业务行。
- **固定 execution deadline**：保留实际 Manifest `120s` 执行窗，显式 Run 窗设为150s。
  两个实际 Worker 在强杀/重试后仍沿用首次 Claim 的 execution_deadline；执行窗与末次
  lease 同刻到期，而 Run 尚未到期，唯一 SystemTerminalizer 得到 FAILED/NONE。
- **等待/排队到期**：Run 的8s期限先于20s重试退避；另一个6s新 Run 排在保留原30s
  策略的活跃队首之后。前者不再 Claim，后者零 Attempt/SDK 到期终结；accepted head
  均不变，新鲜后继输入继续正式 Session。数据库 deadline 是主要时间证据。
- **Telegram 发送结果未知**：fixture 精确接收一次真实 SDK `sendMessage` 后，在
  HTTP 响应开始前断开。Gateway Delivery 保留 UNKNOWN，part attempt=1 且无下一次
  自动重发；Reply transport 的 ACCEPTED/ACK 只代表耐久交接，不代表外部发送确认。
  实际 Gateway 强杀重启后，在 Worker proof 离线时重投原 Reply bytes，复用 receipt，
  不再发送。后继正常输入仍续接正式历史并成功 Delivery。

代理证据采用关闭的允许字段集合，仅记录公开 ID、epoch、状态、字节数量与本地整数关联号；
不记录凭据正文、能力 token 或其指纹。服务停止后关闭代理，重开 cleanup 文件验证无存活
连接。模型 fixture 的 hold 超时只是显式测试设置，不改变产品超时。
本组不替代 credential uses 负例与 proof 传输失败分类，二者由下面的 `--authorization`
独立验证；合法 Final proof 身份不符与独立 Reply 期限由 `--finality` 验证。
主机/DB 时钟偏移与真实外部 Provider/Telegram 验收继续单列，也不宣称外部 exactly-once。

## `--authorization`：整批拒绝与在线证明错误分类

该选项复用 `--uncertainty` 的两条 mTLS relay，两个选项同时启用也只安装一组代理；
在 live credentials clear 之前运行，不增加产品运行模块、迁移或存储协议。

- **四种完整批次负例**：真实 Control 完整200响应到达代理后，闭集模式分别删除一项、
  追加不同 CredentialID 的一项、保持长度但重复第一项、改变合法形状的 AttemptID。
  代理只变异这份真实响应并明确记录 mutation，不宣称 Control 自己返回了错误批次。
  实际 Worker 应整批拒绝，单 Attempt `FAILED/CREDENTIAL_DENIED`，零模型HTTP/候选；
  原 accepted head 保持，固定失败 Final 实际被 Gateway 接纳并交付外部 fixture。
  同 Session 的 seed→拒绝→follower 序号1→2→3，后继历史只包含接受过的seed及自身输入。
- **proof 响应丢失**：真实 Worker proof200到达代理后完全不交付下游，实际 Control
  应产生503，存活 Worker 当前 Attempt 以 `DEPENDENCY_UNAVAILABLE` 结束，Run退避后
  由新 Attempt 恢复。原 Attempt 不重发 Resolve，只有恢复后的 Attempt 进入 SDK。
- **proof 明确否决**：代理只翻转真实 proof 请求的合法 `manifest_digest` 一位，再
  发给真实 Worker。实际 owner 的403再经 Control403抵达仍存活的 Worker，持久化
  `CREDENTIAL_DENIED` 与固定失败 Final。代理不合成403，也不破坏 schema 冒充授权拒绝。
  这是显式请求错配负例，不声称正常有效 Grant 被 owner 否决，更不代表 Final proof
  的合法身份错配已覆盖。Gateway `finals:verify` 不受该故障匹配影响。

每种稳定拒绝的固定失败文本都是一个真实 Final/外部发送，零 SDK 不等于零回复。
代理只保留闭集状态、字节计数和进程内关联号；原凭据、token和能力指纹不写入证据。
Factory 单元测试另覆盖全部批次身份/用途/值的16项校验，并断言零 Session constructor、
返回 runtime=nil、同 Attempt 第二次 Prepare 不再请求 Resolve；不把仅有日志推断当构造器探针。

## `--finality`：合法证明身份与独立回复期限

该选项与其他矩阵共享已安装的 mTLS relay，单独启用时也会安装同一组 relay。
它必须在 `--contracts` 的未知 Intent 重投负例之前、live credential clear 之前运行，
避免已有待证明请求抢占本次单请求故障。没有新运行模块、存储协议或隐式模型额度。

- **合法 Final proof 身份错配**：真实 Worker 完整200 proof 与八个原请求字段一致后，
  私有 relay 只改一个格式合法的 TenantID 或 ManifestDigest。HTTP Client 仍完成 codec
  验收，由实际 Gateway Acceptor 与原 Admission target 比较并返回稳定 UNAUTHORIZED。
  原始 broker sequence 持久 `REJECTED/UNAUTHORIZED` 后才 ACK，零 Delivery/发送。
  测试不声称 Worker 自己返回错误证明，也不把未知 wire 字段拒绝当作此分支的证据。
- **执行成功时回复已过期**：保留公开 Manifest 的120s执行窗，显式 fixture 配置 Run150s、
  Reply4s。模型 hold 时，一条 DB 查询同时证明 Reply 已过、执行/Run未过且 Lease有效；
  释放后同 Attempt 成功接受 candidate/SessionCommit/head，Completion 是
  `SUCCEEDED`、`reply_disposition=NONE`、`reason=DEADLINE_EXPIRED`，零 Outbox/发送。
- **原 Final 在 broker 排队过期**：Reply20s，模型 hold 后正常停止 Gateway，Worker
  在期限内成功提交原 Final 并收到 PubAck。原始 bytes 留在真实 NATS，DB 期限自然经过后
  才重启 Gateway；原 sequence 记录 `REJECTED/EXPIRED` 后 ACK，零 Delivery/发送。

上述四个 victim 的 Worker 成功结果均保持不变，各自 fresh follower 继续同 Session 的
已接受历史并正常发送；没有回滚未送达结果、重跑前一 Run 或延长原 deadline。
所有业务表只读取证，测试不修改时间、不手写 Manifest/Reply，也不重新发布过期 Final。

## 证据文件

- `control-http.jsonl`：实际公开 API 方法、路径、状态；不记录请求正文。
- `control-publication.json`：真实公开 Deployment 发布响应。
- `fixture-resources.json`、`process-events.jsonl`：专用资源地址和进程 PID/退出状态。
- `dependency-preflight.json`：真实 mTLS 拒绝和 Session runtime 权限预检。
- `joint-two-rounds.json`：两个运行的 Completion/Reply 事实与实际模型请求。
- `joint-model-parameters.json`：真实公开发布视图、显式模型名、两轮实际SDK请求与有效参数逐项对照。
- `gateway-delivery-*.json`、`gateway-restart-reply-replay.json`：实际 Gateway 接纳、重启/重投。
- `worker-process-faults.json`：两个 Worker 的进程级故障断言（启用 `--faults`）。
- `reply-scenarios.json`：proof 中断/恢复、精确变异结果及保留的负例 sequence。
- `worker-session-scenarios.json`：部分 SSE flush、真实强杀与 Session 登录中断/恢复。
- `worker-capacity-scenarios.json`：显式容量、未接管时状态及原发布请求恢复。
- `worker-live-credentials.json`：公开 rotate/clear/目标拒绝、认证代次、Attempt/Final 关联。
- `intake-scenarios.json`：接管提交前后真实强杀、原 broker sequence 与恢复事实。
- `manifest-empty.json`、`manifest-recovery.json`：实际空导出、固定上界分页、离线重叠与重复启动。
- `manifest-gap-rebuild.json`：唯一Manifest增量源丢失、新incarnation、真实Owner导出与Run恢复。
- `worker-session-scope.json`：真实 Revision 切换历史、RouteGeneration 与群 topic ignore 边界。
- `receipt-scenarios.json`：精确拒绝写入失败、原消息保留与恢复后 ACK。
- `worker-session-commit-loss.json`：真实 PG 提交标记、响应前独立行证据、丢响应和同候选恢复。
- `worker-identity-scenarios.json`：同聊天跨 Tenant/Account 六轮 Session/SDK/Delivery 隔离。
- `worker-retained-scenarios.json`：全部状态 Run 满额、零新接管副作用、原 sequence 提高配置恢复。
- `worker-resolve-uncertainty.json`：真实 Control 完整响应丢失/超时、Profile 锁后 proof 复核与新 Attempt。
- `worker-credential-batch.json`：四种明确标记的完整响应变异、稳定失败 Final 与同 Session 后继。
- `worker-proof-authorization.json`：实际 proof 传输故障/403，经真实 Control503/403区分 Run 恢复与拒绝。
- `worker-final-identity.json`：完整真实 Final proof200 的两种合法身份错配、原sequence稳定拒绝和正常后继。
- `worker-reply-deadline.json`：回复先过期但执行成功、原Final排队过期，零发送与已接受历史保持。
- `resolve-proxies-cleanup.json`：真实 owner HTTP 状态、零凭据持久化、关闭及零存活连接。
- `worker-deadline-scenarios.json`：固定执行窗、重试等待/排队过期、唯一 NONE 与后继历史。
- `worker-delivery-uncertainty.json`：外部已接收但响应丢失、UNKNOWN、不自动重发及重启 receipt 重放。
- `control-api.log`、`worker-one.log`、`worker-two.log`、`gateway-*.log`：实际进程日志。

成功退出码为 0。任一断言失败、非预期子进程退出、race detector 报告或资源清理失败，
门禁均以非 0 退出。这里的跨进程联合门禁不等于真实线上模型或真实 Telegram 账号验收。
