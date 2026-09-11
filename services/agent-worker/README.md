# Agent Worker V1

Worker 将 Gateway 已固定的请求推进为可恢复的执行结果，并通过持久 ReplyIntent 将 Final
交还 Gateway。它不管理 Draft、不轮询最新 Profile，也不直接发送 Telegram 消息。

**当前代码范围**：显式 `llm / sequence / parallel / loop` 单父执行树、
`openai_compatible`、PostgreSQL/managed Redis 正式 Session 与同后端 Summary、
PostgreSQL/Redis Memory、Artifact、Knowledge，以及选定的 MCP Streamable HTTP 工具。
均由 AgentSpec → Profile → 固定 Manifest → 实际 tRPC-Agent-Go v1.11.2 装配，
不是默认给所有节点授权。Parallel 需要显式后继汇总，Loop 使用既有 body/迭代次数。
累计 Token 预算仍后置，数据服务保留各自接受/即时副作用边界，不新增跨库事务。
各后端与真实模型的实际验收范围见[编排最终矩阵](../../docs/architecture-next/agent-worker/orchestration-acceptance-v1.md)，
特别是 Loop 的额外真实模型指令遵循场景仍保留 FAIL，不将其等同于核心运行链路失败。
`max_output_tokens` 仍是既有单次输出契约；没有 `max_total_output_tokens` 或隐式 16384 限额。
节点 `generation.max_output_tokens` 保留现有 Schema 的 `262144` 上界；该上界不套用到
仅要求正整数的 `execution.max_output_tokens`。节点未配置时，SDK 使用已发布执行值；
节点有配置时，Worker 使用不超过执行值的节点值，不额外裁剪。固定 SDK v1.11.2 的
默认模型转换即使关闭 tailoring 也会按内置表裁剪已知模型；Worker 因此使用 SDK 公开的
`WithChatRequestCallback`，在转换之后将已验证值写入 typed `MaxCompletionTokens`。
实际请求保持发布的模型名称、历史与上限，不修改 SDK、不改写 HTTP 字节、不降额重试。
这保证请求装配遵守 Manifest，不保证 Provider 接受该参数或实际输出满额；Provider 拒绝
仍按既有模型错误路径处理。

实现和验收分开记录：[Worker 设计](../../docs/architecture-next/agent-worker/README.md)、
[实现状态](../../docs/architecture-next/agent-worker/implementation-status.md)。本地真实 PG/NATS/SDK
与测试模型的两轮 Session，以及真实 Control/Gateway/Worker 进程联合与强杀恢复均通过；
实际外部模型和真实 Telegram 的联合验收仍单独记录。

## 1. 目录与模块

```text
services/agent-worker/
├── cmd/agent-worker/           # 正常启动、check-config、probe、prepare-session
├── internal/
│   ├── bootstrap/              # 显式配置、双 DB 身份、mTLS、初始化、调度、健康、Drain
│   ├── infra/natsadapter/      # 绑定既有 durable、持久接管后 ACK、Final Relay
│   ├── manifest/
│   │   ├── domain/             # 不可变发布身份、缺失/冲突
│   │   ├── application/        # Projection 接口
│   │   └── adapter/            # wire、PostgreSQL、Control Owner Export
│   └── execution/
│       ├── domain/             # Run、Attempt、Grant、Head、Completion、运行计划
│       ├── application/        # 持久接管与一条纵向执行用例；定义消费方 Port
│       └── adapter/
│           ├── inbound/        # RunRequested wire、mTLS Attempt/Final 证明
│           └── outbound/       # PG 账本、固定 Manifest、Profile 批次、SDK、Session
├── migrations/                # Worker 账本/投影；独立迁移历史
├── sessionmigrations/         # runtime_session 不可变候选；仅显式准备
├── integration/               # 真实 PG/NATS/SDK 两轮纵切，模型/Profile 为 HTTP fixture
├── Dockerfile
└── README.md
```

两个业务 Module 是 **manifest** 和 **execution**。运行时由 bootstrap 装配；编译时 SQL、
NATS、HTTP、SDK Adapter 依赖内层接口。不会因为物理同库而访问 Control/Gateway 的私有表。

## 2. 执行路径与事实边界

1. Run consumer 严格解码已固定的 Route/Input；同事务提交 Receipt、Run 和 SessionSequence
   后 ACK。重复 Event/Run 重放原结果；冲突持久落账，不覆盖原输入。
2. Manifest 从完整发布事件或受认证的 Owner Export 获取，核对正文 Digest、固定身份、
   Worker V1 版本与发布 pin。缺失等待；身份冲突隔离所有相关 Manifest/Revision，保留 tombstone。
3. Execution 只 Claim 当前 Session 最早未完成 Run。锁序为 Session → Run → Attempt；
   获取锁后读取数据库时钟，续租、提交、凭据证明全部核对当前 Fence。
4. 每个 Attempt 只初始化一个完整的 Profile 凭据批次。Control 通过 mTLS 回调活跃 Grant；
   Worker 核对 Tenant/Profile/Revision/Run/Attempt/Worker/Epoch/Manifest/uses 后装配 SDK。
5. SDK 使用 accepted head 的完整历史与本次输入。AppendEvent 的 sticky error、失租或
   drain 异常都会阻止成功结果；未知结果不以无条件重跑掩盖。
6. Session Store 先写不可变候选。Worker 同一事务接受候选 ref/digest、推进 head、写唯一
   Completion、Final 与 Reply Outbox。候选先落库不等于正式历史；失败不推进 head。
7. Reply Relay 使用原正文/Digest/MsgID 获取 NATS PubAck 后标记。Gateway 查已提交 Final
   的不可变证明，Delivery 接纳和 transport receipt 均耐久后 ACK；重放不重新执行模型。

SessionScope 为 Tenant、Provider、Account、Conversation、Thread、Binding、DeploymentRevision 加社交身份 ID。
社交身份按租户／Provider／Bot／sender 稳定记录；不同用户在同一会话中分区，RouteGeneration 刷新不改变历史。
切换 Revision 隔离历史，切回同一 Revision 恢复该用户分区的历史。旧共享作用域不自动迁入新的用户分区。
当前接线和前向迁移见[最小身份与连续对话](../../docs/architecture-next/channel-gateway/minimal-identity-session.md)。

## 3. 同库隔离与启动

复用同一 PostgreSQL/database：

| Schema | 迁移身份 | 运行身份 | 用途 |
| --- | --- | --- | --- |
| `control` | `control_migrator` | `control_runtime` | 发布事实、私有凭据与 Control Outbox |
| `gateway` | `gateway_migrator` | `gateway_runtime` | Admission、Delivery 与 transport receipt |
| `worker` | `worker_migrator` | `worker_runtime` | 执行账本、Manifest 投影和接受引用 |
| `runtime_session` | `session_migrator` | `session_runtime` | 正文候选，仅 SELECT/INSERT |

Worker 的 `WORKER_DATABASE_URL` 与 `WORKER_MIGRATION_DATABASE_URL` 是部署连接。
Profile 管理 API 接受受限 PostgreSQL DSN，但 Control 仅加密其中的密码；在线授权批次中
`purpose=dsn` 的 `value` **是密码，不是完整 DSN**。Worker 用固定 Manifest destination
（host/port/database/username/sslmode）与该密码按 URI 规则转义后组装 Session DSN，
**不从 Worker 部署连接推导目标**。发布 target 必须采用 `session_runtime`，运行校验同名
身份、固定 Schema 与目的地。密码轮换不改变已发布 destination。
物理同库也不合并两条连接的事务，不授权跨 Schema 读写。

准备顺序：provision → Session 显式准备 → Control/Gateway/Worker 各自迁移与启动。
迁移池在启动后关闭；runtime 无 DDL、迁移历史写入或跨 Schema 权限。历史迁移 Digest 保持不变。
完整 TLS Compose 接线和证书清单见 [部署步骤](../../deploy/compose/WORKER_V1.md)。

```bash
# DSN 由部署环境注入；命令不把凭据写进参数或日志。
go run ./services/agent-worker/cmd/agent-worker prepare-session

# WORKER_CONFIG_FILE 是绝对路径。仅校验配置结构，不代替连接/证书/端点验证。
go run ./services/agent-worker/cmd/agent-worker --check-config

go run ./services/agent-worker/cmd/agent-worker
go run ./services/agent-worker/cmd/agent-worker probe http://127.0.0.1:8083/readyz
```

配置模板：[example.json](internal/bootstrap/example.json)、[nats.example.json](internal/bootstrap/nats.example.json)。
所有队列/并发/正文容量和超时都是显式运维配置；模板数值是可见配置，不改变 Manifest 的模型上限。
`limits.max_retained_runs` 是必填的全部状态 Run 保留上限，和仅统计未完成 Run 的
`max_queued_runs` 分开。满额拒绝新接管，不影响已有 Run、receipt 与 Final 恢复；示例值
`100000` 不作为隐藏默认值。它不等于所有辅助证据表或独立 Session Store 的累计配额。
Control 的 `CONTROL_DEPLOYMENT_EXPECTED_CONTRACT_DIGEST` 与 Worker 的
`platform_contract_digest` 必须来自相同发布版本/host allowlist；校验命令见
[Control Worker Runtime](../control-api/WORKER_RUNTIME.md)。旧 Manifest 不自动改写，需显式发布兼容 Revision。

## 4. 恢复、健康与退出

- NATS 拓扑由 reconciler 创建，Worker runtime 只绑定。Manifest durable 先于 Owner Export；
  全量导出结束后追赶保留增量，满足初始化条件才 ready。空集合同样有明确完成状态。
- 运行中检测 PG/NATS 状态与 Stream/durable 创建身份；被替换的拓扑触发退出重建，
  不以重建空 consumer 或继续旧游标掩盖丢失窗口。Manifest 采用无自动年龄淘汰、满额拒绝新增。
- `/healthz` 表示进程 HTTP 存活；`/readyz` 另要求初始化、存储和 transport 健康且非 Drain。
  这两个端点都不证明外部模型/Telegram 已验收。
- SIGTERM 首先停止新接管/扫描，给活跃 Attempt 显式 drain 时间；续租和证明接口仍保留。
  超时后取消活跃执行，再关闭 HTTP/NATS/DB。未完成资格到期后由后续 Worker 恢复。
- 没有活跃可信 Attempt 的过期 Run 由 SystemTerminalizer 原子结束为 FAILED/NONE；
  下一个 SessionSequence 可继续，正式历史不受失败候选污染。

运行阶段、积压与观测后端故障的明确语义见 [运行信号](OBSERVABILITY.md)。默认结构化日志，
OTLP Metrics 通过可选显式配置启用；不新增业务预算或改变 Run 成功条件。

## 5. 可重复验证

```bash
go test -count=1 ./...
go vet ./services/agent-worker/... ./api/runtime/...
python3 scripts/test-v1-database-isolation.py
python3 scripts/test-worker-v1.py --race
python3 scripts/test-worker-v1-joint.py --race --faults --contracts --recovery
```

这些 Python gate 自行创建并清理专用 Docker 实例，不使用当前业务数据库。Worker gate 串行运行
会重置 Schema 的集成用例，要求命名测试实际执行且零 Skip，覆盖 Session、执行账本、Manifest、
Control 发布/分发、NATS 权限及 Gateway Reply 交接。模型/Profile HTTP fixture 的日志显式标记，
不将其视作线上模型响应。联合 gate 另运行实际三个二进制，
通过真实 Control HTTP 发布、在线授权、Gateway 重投和双 Worker SIGKILL 恢复；另覆盖
live rotate/clear、部分 SSE 强杀、Session 中断、Reply proof 恢复及容量背压。仅模型/Telegram
为外部 fixture。`--recovery` 增加接管提交/ACK 窗口、Manifest 空集合/离线重叠、
Revision 切换和拒绝 receipt 存储故障的真实恢复证据；使用方法见 [联合 Gate](../../scripts/worker-v1-joint/README.md)。普通 `go test ./...` 的可选集成 Skip 与这些 gate 分开统计。

`--authorization` 复用真实 mTLS relay，检查完整凭据批次的缺项/多项/重复/Attempt身份错配
均在执行前稳定拒绝；实际 proof 网络错误与明确403经真实Control分别返回503/403。
固定失败 Final 与成功 Final 都需 Gateway Delivery，零 SDK 不表示零回复。
批次负例是明确标注的代理响应变异，proof否决是请求digest错配后的真实owner响应；
二者均不被描述成Control自然产生了错误授权，也不替代真实外部服务验收。

`--finality` 验证格式合法的 Final proof Tenant/Manifest 错配由 Gateway 稳定拒绝，
以及 Reply 期限先于执行完成或 Final 在 broker 排队过期时的独立结果：执行与正式
Session 保持成功，过期回复不发送，下一 Run 仍读取已经接受的成功历史。

真实验收仍需：明确 Bot/测试会话和 Binding → 兼容已发布 Manifest → 实际模型调用 → 两轮
正式 Session → Gateway Delivery/Telegram 回执，并补重启/重复消息的交叉证据。

`--manifest-rebuild` 进一步验证唯一 Manifest Stream 的真实增量丢失后，Worker 从
Control Owner 导出恢复固定快照；其他 Stream/durable 不重建，原 Run 与正式 Session 保留。
`TestWorkerManifestRejectionPGNATSSDK` 则将12种错误 Manifest 推进到真实 Consumer/PG/
Reader/Processor，观察零 Claim/Resolve/SDK，并用前后有效对照验证真实 SDK 与正式 Session。
`TestWorkerReplyCapacityPGNATSSDK` 验证实际 NATS 满额时原 Outbox 不丢失、显式容量恢复后
原 payload/Intent PubAck，不重新执行 Agent；App Relay 的 PollInterval 退避另有循环单测。

## 6. 后续计划

- SDK 原生 sequence/loop/parallel，先按其真实共享 Session/输出/错误语义增量开放。
- Tools/Knowledge 的精确资源授权与真实端点验收；Memory 的用途、作用域和读写协议另行设计。
- 需要产品配额时再定义累计 Token 预算与结算；不将单次输出上限挪用为总预算。
- Session 候选孤儿/保留与 GC、生产压测、扩展观测与 Trace、企微专项、Channel 页面及 Helm。

后续不恢复双份正式正文、Session projector、下一 Run 补投或自建分支事务引擎。
