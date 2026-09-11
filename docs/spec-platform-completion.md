# 平台补齐切片：收口全部未接线与未实现项

本切片把「README 与现状差距分析」列出的全部缺口（G1–G10）收敛为可独立发布的六个批次（S1–S6）。
每条缺口先给**事实**（引用代码），再给**设计决策**（选定方案与一句被否理由），再给**改动点 / 测试 / 验收**。
截至本切片，平台已有两批实测：legacy 链路（`scripts/e2e.sh` 44 条、`scripts/fault_drill.sh` D1–D7 143 条）与可靠链路
（`scripts/reliable_e2e.sh` 74 条、`scripts/reliable_fault_drill.sh` 29 条）；本切片只做增量，不动已验行为。

> **实施进度（滚动更新）**：
> - **S1 ✓** 已实施并验证：单测 + `reliable_e2e.sh` 74 条回归 + 真实 MySQL 集成测试全绿。
> - **S2 ✓** 已实施并验证：`reliable_e2e.sh` 扩至 **92 条**（真加密回调 / webchat 邮箱 / 企微投递），
>   `fault_drill.sh` D1–D7 **143 条**、legacy `e2e.sh` **44 条**全绿；门禁中发现并修复两处既有回归（见 S2 节末）。
> - **S3 ✓** 已实施并验证：skill/workspace 单测（含 symlink 逃逸、真实 bash 执行）+ execution 组装集成测试 + 全量单测。
> - **S4-G6 ✓** 已实施并验证：Router 单测 + 投影接入 jobs 角色，`reliable_e2e.sh` 扩至 **95 条**
>   （新增 Redis 投影段：投影版本追平 MySQL 事实源）。
> - **S4-G7 ✓** 已实施：`-migrate-sessions`（Redis SCAN 枚举 → 逐会话迁移至 MySQL + `-dry-run`）
>   与 `-reindex-kb`（重置 KB `ready` 文档直接重索引）；CLI 固件已编译就绪。
> - **S5、S6 待实施**（设计已定稿，可直接开工）。

## 0. 缺口清单与切片范围

| # | 缺口 | 类型 | 对应批次 |
| --- | --- | --- | --- |
| G1 | 可靠 worker 未接线 knowledge/memory/artifact 工具（`WithKnowledge` 等三 option 零调用点） | 未接线 | S1 |
| G2 | 可靠 gateway 角色不存在：KF 回调 HTTP 挂载/企微/webchat 接收端持久化未接 | 未实现 | S2 |
| G3 | Delivery 仅微信客服一个通道（webchat/企微被明确拒绝） | 未实现 | S2 |
| G4 | `trpcservice/skill` 空壳：SKILL.md 工作流零实现 | 未实现 | S3 |
| G5 | `trpcservice/workspace` 空壳：沙箱工作目录零实现 | 未实现 | S3 |
| G6 | 租户级后端选择未落地：`backend_profiles.session_backend` 只记账 | 部分实现 | S4 |
| G7 | 数据迁移工具不存在（Redis→SQL、知识库重嵌入） | 未实现 | S4 |
| G8 | 灰度发布未实现（revision 放量/白名单） | 未实现 | S5 |
| G9 | 审计文件无轮转 | 部分实现 | S1 |
| G10 | K8s 集群行为 / otel-collector / Linux UID / delivery unknown 容器级演练未验 | 验证受限 | S6 |
| G11 | 预算限额：租户级 token/调用次数/成本上限未实现 | 未实现 | S7 |
| G12 | 工具调用二次确认：危险工具需审批才能执行 | 未实现 | S7 |
| G13 | 每租户成本计量：token × 单价聚合未实现 | 未实现 | S7 |
| G14 | 日志脱敏：密钥/密码等敏感信息可能出现在日志/trace/审计_detail 中 | 未实现 | S7 |

**范围外**（维持既有决议，不随本切片改变）：

- **Telegram / 微信公众号通道**：`spec-im-channels.md` 事实 #5「接口预留，不实现」，题目最低要求为两类、现有三类达标。扩展时按现有 `Adapter` 契约实现即可，不在本切片。
- **跨区域多活 / 强一致投影读**：方案文档 §5 的取舍不变——投影是缓存，允许滞后，永不领先。
- **`workspace` 在 legacy 路径的装配**：legacy 网关没有 `session_pk`，工作目录以可靠链路的会话为主键，legacy 装配留到有真实需求时再议。

**依赖事实**（框架 v1.11.2，均已核实存在）：

- `skill.FSRepository` / `skill.RepositoryProvider` / `skill.SkillScope`；`llmagent.WithSkills`、`WithSkillRepositoryProvider`、`WithCodeExecutor`、`WithMaxLoadedSkills`。
- `codeexecutor/local.New(WithWorkDir, WithTimeout, ...)`；`llmagent.WithCodeExecutor` 存在时默认暴露 `workspace_exec` 类工具（`WithWorkspaceExecSurfaceEnabled` 默认 true）。
- 框架会话键：`execution.FrameworkKeys` 给出 `appName = encode(tenantID)`、`userID = encode(tenantID, actorKey)`（长度前缀编码，`trpcservice/execution/keys.go`）。

---

## S1 小步先行：worker 工具接线（G1）与审计轮转（G9）

### G1 可靠 worker 接线 knowledge / memory / artifact

**事实**：`trpcservice/execution/worker.go` 定义 `WithKnowledge` / `WithMemory` / `WithArtifacts` 三个 `RunnerOption`，
组装处（`DefaultRunnerFactory` 内）在 revision pin 到 `knowledge_search` / `memory_*` / `artifact_save` 且依赖为 nil 时
报错「this deployment has … disabled」。全仓库除定义外零调用；`cmd/trpc-service/roles.go` 的 `sweepWorker`
调用 `DefaultRunnerFactory(cdp, resolver)` 不传任何 option。后果：可靠模式下这三个能力族的工具**永远不可用**。
E2E 佐证：`scripts/reliable_e2e.sh` 知识段只验了 upload→index→ready 管道，注释写明「tool call 验证在集成测试中」。

**设计决策**：新增**延迟构建（deferred provider）**选项，worker 启动时**不触碰网络**；首次有 revision pin 到相关工具时才构建，
构建成功缓存复用，失败**不缓存**（下一条消息自然重试）。被否方案：
① worker 启动时无条件构建——MinIO/Qdrant 故障会让整个 claim loop 起不来（违反现有注释中的设计意图）；
② 保持现状（pin 即报错）——能力在可靠链路永远不可用，缺口不收敛。

**改动点**：

1. `trpcservice/execution/worker.go`：
   新增三个 option，与现有三个静态 option 并存：

   ```go
   // 与 WithKnowledge 并存；build 在第一次需要时调用，成功结果缓存，失败不缓存。
   func WithDeferredKnowledge(build func(context.Context) (*knowledge.Service, error)) RunnerOption
   func WithDeferredMemory(build func(context.Context) (*memory.Service, error)) RunnerOption
   func WithDeferredArtifacts(build func(context.Context) (*artifact.Service, error)) RunnerOption
   ```

   `runnerDeps` 内部由 `func(ctx) (svc, error)` 解析（首次组装调用时传入 factory 的 ctx）。

2. `cmd/trpc-service/roles.go`：
   - 把 `buildKnowledgeStack` 拆成可复用的三段（minio+qdrant+embedder 共享一次构建），新增
     `newDeferredKnowledgeStack(cfg, cdp, resolver)`：持有 `sync.Mutex` + 已构建结果，暴露三个满足上述签名的 builder。
   - `sweepWorker` 改为：

     ```go
     stack := newDeferredKnowledgeStack(cfg, cdp, resolver)
     worker := execution.NewWorker(svc, opts,
         execution.DefaultRunnerFactory(cdp, resolver,
             execution.WithDeferredKnowledge(stack.knowledge),
             execution.WithDeferredMemory(stack.memory),
             execution.WithDeferredArtifacts(stack.artifacts),
         ))
     ```

   - 行为：配置未启用 knowledge 时 builder 立即返回明确错误（沿用现有 fail-loudly 文案）；
     后端故障时该消息组装失败、留在队头等待重试（lease 过期后再次尝试），其他不 pin 工具的消息完全不受影响。

**测试**：`trpcservice/execution/tools_ledger_test.go` 增加用例——
① 带 deferred builder 的 factory 成功组装 pin `knowledge_search` 的 revision（假模型 + mock 向量库）；
② builder 失败时组装报错且**错误发生在该 claim 内**，下一次组装重试成功（builder 被调用两次）；
③ 既有「无 option 时报 disabled」用例保持不变（回归）。

**验收**（S1 批次门禁）：`go build ./... && go test ./trpcservice/execution/...`；
`scripts/reliable_e2e.sh` 全绿（回归）；新增可选断言段（P5）：假模型按脚本触发 `knowledge_search` tool_call，
账本出现 `succeeded`、最终回复包含 seed 文档中的关键词——并入 S6 前的端到端收口。

### G9 审计文件轮转

**事实**：`trpcservice/audit/audit.go` 追加式写入（`bufio` + 每行 Flush），无大小/日期轮转；
`spec-governance-observability.md` 已把「审计文件无轮转（追加式，运维侧 logrotate）」标为一期限制。

**设计决策**：**平台内实现按大小轮转**，配置驱动，不引入第三方；保留「运维 logrotate 仍可用」的兼容性
（轮转只是重命名 + 重新打开）。被否方案：坚持运维侧 logrotate——容器卷场景下运维无法触达文件句柄。

**改动点**：

1. `trpcservice/config/config.go`：`AuditConfig` 增加 `MaxSizeMB int`（0=不轮转，默认 0）、`MaxBackups int`（默认 3）。
2. `trpcservice/audit/audit.go`：
   - `Options{Path string; MaxSizeMB, MaxBackups int}`，`New` 保留旧签名（内部转 `NewWithOptions`）。
   - `Log` 写入前懒检查（每 256 次或每行 Flush 后计数一次 `Stat`）：当前文件 >= MaxSizeMB 时，
     `path.(N) → path.(N+1)` 滚动（`os.Rename`），删除超出 `MaxBackups` 的最旧备份，用 `O_APPEND|O_CREATE` 重开。
     全程持 `l.mu`，与既有单写者模型一致。
3. 配置样例与 `config.example.yaml` 注释同步。

**测试**：`trpcservice/audit/audit_test.go` 增加——① 小阈值（1MB 文件用测试参数下调）触发轮转且 `.1` 备份存在；
② 超过 MaxBackups 的最旧文件被删除；③ 并发 Log 无数据竞争（`-race`）；④ MaxSizeMB=0 时行为与现状逐字节一致。

**验收**：`go test ./trpcservice/audit/...`；e2e 与现网日志格式零变化（回归）。

---

## S2 可靠链路合拢：接收端（G2）与投递端（G3）

### G2 可靠 gateway 角色：KF 回调挂载 + 企微/webchat 接收端持久化

**事实**：`roles.go` 注释自述「gateway legacy HTTP surface; reliable-mode gateway lands with the WeCom/webchat receive split
(KF's durable callback hook already exists, its HTTP mount does not)」；角色仅有 all/worker/delivery/jobs。
`KfPuller.RecordNotification`（`wechatkf_puller.go`）已实现「先落 `channel_notifications` 再 ACK」的 durable 钩子并有单测，
但没有任何 HTTP 入口调用它；企微/webchat 的接收端没有「接收→inbox」的持久化路径。

**设计决策**：新增 `-role gateway`，与 legacy `/callback` 面**同路径、不同角色**（角色互斥部署，不存在双实现路由冲突）。
接收语义按通道分三档，全部**先持久化、后 ACK**：

| 通道 | 路径 | 持久化动作（ACK 前） | ACK |
| --- | --- | --- | --- |
| 微信客服 | `/callback/wechat_kf/{tenant}` | 验签 → `RecordNotification(tenant, binding, event_token, scope_key)` | 200 空串 |
| 企业微信 | `/callback/wecom/{tenant}` | 验签+解密 → `inbox.Accept`（事务内） | "success" |
| webchat | `/callback/webchat/{tenant}` | 直接 `inbox.Accept` | 202 |

**改动点**：

1. `trpcservice/channels/receiver.go`（新）：`Receiver` 类型，持有 `*controlplane.DB`、`*inbox.Service`、`*KfPuller`、
   以及三个通道的**协议层解析函数**（见下）。`Receiver` 对外暴露
   `Routes(tenantScoped bool) map[string]http.Handler` 与 `Accept(ctx, in *InboundMessage) error`。
2. 协议层复用（单一实现原则）：把现有 `WeChatKf.Callback` 的「验签 + 解析 XML/JSON 事件」与
   `WeCom.Callback` 的「验签 + 解密 + 解析」抽取为包内纯函数
   （`parseKfCallback(r) (events []kfEvent, err error)`、`parseWeComCallback(r) (msgs []*InboundMessage, err error)`），
   legacy 适配器与 `Receiver` 共同调用，避免两份验签逻辑漂移。
3. `Receiver` 对企微/webchat 的 `Accept`：解析 binding（`channel_bindings` 按 `tenant + channel_type = active`）→
   解析目标 revision（与 `KfPuller.resolveTarget` 同构，S5 起走灰度解析入口，见 G8）→
   `inbox.NewService(cdp).Accept`（复用现有预检 + 唯一键幂等）。
4. `cmd/trpc-service/roles.go`：
   - 角色白名单加入 `gateway`；`runRole` 分支调用 `runReceiver(cfg, cdp, workerID)`。
   - `runReceiver` 组装：`Receiver` + `web.NewServer` 的健康探针（`/readyz` 打 MySQL ping，
     与 worker 角色的存活语义一致）+ `/webchat/stream`（可靠版，见 G3）。
   - 该角色只挂接收面与探针，不挂聊天页与 admin（admin 在 all 角色/独立部署）。
5. `deploy/compose/reliable.override.yml` 与 `deploy/k8s/21-deployment-reliable.yaml`：
   增加第四个进程/Deployment `gateway`（同一个镜像，`-role gateway`，暴露 8080）。K8s 探针沿用 `/readyz`。

**测试**：`trpcservice/channels/receiver_test.go`（新）——① KF 回调落 `channel_notifications` 且返回 200 空串，
随后 puller 可拉走（不再需要 SQL 模拟）；② 企微回调解密后落 `inbox_messages`，「success」在落库之后返回；
③ webchat 重投（同 `msg_id`）只落一行；④ 验签失败 401、绑定缺失 404、revision 未发布 409（可重试错误不 ACK）。

**验收**（S2 批次门禁）：`bash scripts/reliable_e2e.sh` 增加三段断言后全绿：
① KF 段改为**真回调**：向 gateway POST 一条加密回调 → 断言 `channel_notifications` 新行 → jobs 拉取 → 回复投递；
② webchat 段：POST → 202 → `inbox_messages` 新行 → worker 执行 → `reply_outbox` → delivery → `sent`；
③ 企微段：签名回调 → inbox → worker → 回复经企微 sender（打假企微上游）→ `sent`。

### G3 Delivery 多通道：webchat 与企微

**事实**：`channels/delivery.go` 的 `DeliverySender.Send` 只有 `TypeWeChatKF` 分支，其余通道返回
`Rejected, "…is not wired into the reliable path yet"`。legacy 的 webchat 是进程内 SSE（`WebChat.streams` map），
企微是 `access_token` + 主动发消息 API。

**设计决策**：

- **企微（同 KF 模式，直投）**：delivery 进程从 control plane 读 `WeComBinding`，复用现有 `WeCom.sendText` 调用链。
- **webchat（邮箱模式，跨进程不共享内存）**：`reply_outbox` 增加 `pushed_at` 列；delivery 的 webchat 分支
  **只做状态确认**（Send 返回成功，行保持 `pushed_at IS NULL`）；可靠版 `/webchat/stream` 在 gateway 进程内
  按连接轮询「本会话已 `sent` 且未 `pushed_at`」的行，推给浏览器后 `UPDATE … SET pushed_at = NOW()`。
  断线重连天然补投（未推送的行仍在）。被否方案：Redis pub/sub——可靠栈刻意不依赖 Redis。

**改动点**：

1. `migrations/0005_completion.sql`（新）：
   ```sql
   ALTER TABLE reply_outbox ADD COLUMN pushed_at TIMESTAMP(6) NULL AFTER status;
   CREATE INDEX idx_reply_outbox_webchat ON reply_outbox (tenant_id, session_pk, status, pushed_at);
   ```
   （`pushed_at` 对 KF/企微恒为 NULL，语义限定 webchat。）
2. `trpcservice/channels/delivery.go`：`DeliverySender` 增加 `WeCom` 与 `WebChat` 字段；
   `Send` 分支——`TypeWeCom`：binding 查找（复用 `KfBindingLookup` 的同构实现 `WeComBindingLookup`）→ `sendText`；
   `TypeWebChat`：确认 `reply.Status == sent` 后返回 `outbox.Sent`（真正的推送在 gateway 轮询侧）。
3. `trpcservice/channels/wecom.go`：抽出「从 binding 构造可发送的 WeCom」构造子（现有 `WeCom` 依赖注入的
   binding 闭包可复用；需要补一个不依赖 legacy 注册表的纯构造入口）。
4. `trpcservice/channels/webchat_reliable.go`（新）：`WebChatMailbox`——
   `Push(ctx, tenant, actorKey)` 在 SSE 连接内启动 1s ticker：
   `SELECT r.outbox_id, r.payload, r.attempts FROM reply_outbox r JOIN sessions s ON (s.tenant_id, s.session_pk) = (r.tenant_id, r.session_pk)`，
   过滤 `s.actor_key = ? AND r.status='sent' AND r.pushed_at IS NULL`，按 `outbox_id` 升序推送（SSE `chunk`+`done` 事件），
   成功后逐行 `UPDATE reply_outbox SET pushed_at=NOW(6) WHERE outbox_id=? AND pushed_at IS NULL`（条件更新保证单次推送）。
   `/webchat/stream?tenant&user` 的契约与 legacy 一致（前端零改动）。
5. `cmd/trpc-service/roles.go`：`sweepDelivery` 的 `DeliverySender` 补 `WeCom`/`WebChat` 字段接线；
   `runReceiver` 的 `/webchat/stream` 使用 `WebChatMailbox`。

**测试**：`trpcservice/channels/delivery_test.go` 扩展——① webchat sender 不触网、行状态语义正确；
② 企微 sender 对 5xx/超时映射 `outbox.Unknown`/重试（复用既有投递语义测试）；③ `WebChatMailbox` 单测：
已推送行不重复推送、条件更新竞态只有一方成功。

**验收**：见 G2 的 ②③ 段；另在 `scripts/reliable_e2e.sh` 断言
「webchat 断线期间提交的回复，重连后经 stream 补投且 `pushed_at` 非空」。

**S2 门禁发现并修复的既有回归**（由 `d2f6e55`「Redis 跨副本协调」引入，非本切片新增；
两处均已补单测钉住）：

1. **协调层硬依赖**：该提交把 `claim`/`lockSession` 全量切到 coordinator 且失败即拒，
   Redis 宕机时新消息被静默拒收（适配器已回 202，失败响应写不进去）——从 IM 侧看与
   「挂死」无异，D4 的失败话术/审计断言随之全灭。修复：协调失败降级到进程内
   dedup/锁（单副本语义，警告日志），见 `trpcservice/channels/channels.go` 与
   `coord_fallback_test.go`。
2. **启动路径错报组件**：Redis 不可达时 `admin.RuntimeStore.Load` 先于 session probe
   退出，把「会话后端宕机」报成 `load runtime config`，D5 断言失败。修复：新增
   `ErrSnapshotUnavailable` 哨兵——快照存储不可达时降级用文件配置、由 probe 给出准确
   定位（点名 session 后端 + probe）；快照解析失败仍拒绝启动。见
   `trpcservice/admin/runtime_store.go`、`runtime_store_test.go` 与 `main.go`。

---

## S3 新模块：skill（G4）与 workspace（G5）

### G4 `trpcservice/skill`：租户级 SKILL.md 技能库

**事实**：`skill.go` 仅 3 行包注释；README 目录要求「可运行的 Skill」；框架 v1.11.2 自带
`skill.FSRepository`、`llmagent.WithSkills`、`tool/skill` 工具与 `SkillLoadMode*` 系列选项。

**设计决策**：平台层**不做 SKILL.md 解析器**（框架已有），只做四件事：
**租户路径隔离、配置、装配、校验**。技能根目录为 `<skills.root>/<tenant_id>/<skill-name>/SKILL.md`；
可靠模式经 revision 的 tool pin 名 `skill` 启用；legacy 模式配置平台根目录即启用。本切片**不接 codeexecutor**
（skill 只做加载/选择；执行沙箱是 G5 的独立开关）。被否方案：自研解析与注入——重复框架能力且必然漂移。

**改动点**：

1. `trpcservice/config/config.go`：新增
   ```go
   type SkillsConfig struct { Root string `yaml:"root"` } // 空 = 禁用
   ```
   平台级配置；`Validate` 检查目录存在性与可读性（启动时一次性，避免每条消息都探）。
2. `trpcservice/skill/skill.go`：
   ```go
   const Name = "skill" // revision tool pin 名
   // For 返回租户的技能仓库；结果缓存（含 Refresh）。
   func For(root, tenantID string) (*skill.FSRepository, bool, error)
   ```
   隔离规则：`filepath.Join(root, tenantID)`；`tenantID` 经白名单校验（`^[a-z0-9][a-z0-9_-]*$`，防路径注入）；
   目录不存在时返回 `ok=false`（该租户无技能，不报错）。symlink 越界检查：加载后校验
   `filepath.EvalSymlinks(path)` 仍在租户根内（实现时验证 `FSRepository` 是否已防；缺则平台层补）。
3. `trpcservice/agent/agent.go`：`NewRunner` 增加可变参数
   `extraOpts ...llmagent.Option`（或抽出 `RunnerConfig`），使可靠与 legacy 两条路径装配同一构造函数：
   `llmagent.WithSkills(repo)`、`WithMaxLoadedSkills(8)`、`WithMaxOverviewSkills(64)`、`WithSkillLoadMode(SkillLoadModeTurn)`。
4. `trpcservice/execution/worker.go` 的 `DefaultRunnerFactory`：`pinsName(pinned, platformskill.Name)` 为真时，
   经注入的 `WithSkillStore(root string)`（新的 RunnerOption）构造租户仓库并装配；`root` 为空时报
   「revision pins skill but this deployment has no skills root configured」（与 knowledge 的 fail-loudly 一致）。
5. `cmd/trpc-service/roles.go`：worker 的 factory 传 `WithSkillStore(cfg.Skills.Root)`；
   legacy `main.go` 的 `agent.NewRegistry` 按租户 root 装配。

**测试**：`trpcservice/skill/skill_test.go`（新）——① 租户 A/B 同名技能互不可见；
② tenantID 注入 `../` 被拒；③ 无目录租户返回 `ok=false` 而非错误；④ `FSRepository` 缓存与 Refresh 行为。
`agent_test.go` 增：装配 skill 后 runner 可构建（假仓库）。

**验收**：`go test ./trpcservice/skill/... ./trpcservice/agent/...`；可选 E2E（S6 门禁前合并）：
假模型触发 `skill_load`，回复包含 SKILL.md 中的独特关键词（沿用 P5 扩展）。

### G5 `trpcservice/workspace`：会话级沙箱工作目录

**事实**：`workspace.go` 仅 3 行包注释；`tool/tool.go` 注释「在配置具体 sandbox/approval provider 之前，
不注册任何 shell/文件系统/网络写工具」；框架提供 `codeexecutor/local`（`WithWorkDir`/`WithTimeout`）与
`llmagent.WithCodeExecutor`（默认暴露 `workspace_exec` 工具面）。

**设计决策**：v1 用**框架本地执行器 + 会话级目录**（跨平台可用），不做容器沙箱（Linux 原生 `codeexecutor/sandbox`
留作 `workspace.mode: strict` 的二期开关，本切片只落 `local`）。范围限定可靠链路（会话以 `session_pk` 为主键）；
默认**禁用**，必须显式配置 `workspace.root` 且 revision pin `code_exec` 才启用。安全红线写进配置注释与部署文档：
本机执行器以平台进程权限运行，生产建议容器级隔离（只读根 + 独立卷）。

**改动点**：

1. `trpcservice/config/config.go`：
   ```go
   type WorkspaceConfig struct {
     Root     string        `yaml:"root"`      // 空 = 禁用
     Mode     string        `yaml:"mode"`      // local（默认）
     Timeout  time.Duration `yaml:"timeout"`   // 单次执行上限，默认 30s
     MaxAge   time.Duration `yaml:"max_age"`   // 目录保留期，默认 24h（GC 用）
   }
   ```
2. `trpcservice/workspace/workspace.go`：
   ```go
   const Name = "code_exec" // revision tool pin 名
   type Manager struct{ root string; mode string; timeout time.Duration }
   func NewManager(cfg config.WorkspaceConfig) (*Manager, error)
   // Dir 返回（并按需创建，0700）<root>/<tenant>/<session_pk>，全部路径经 Clean + 前缀校验。
   func (m *Manager) Dir(tenantID string, sessionPK int64) (string, error)
   // Executor 为该会话构造框架执行器；同一会话复用同一实例。
   func (m *Manager) Executor(tenantID string, sessionPK int64) (codeexecutor.CodeExecutor, error)
   // GC 删除 mtime 超过 MaxAge 的会话目录。
   func (m *Manager) GC(now time.Time) (removed int, err error)
   ```
   路径安全：`Dir` 内拒绝 `..` 逃逸与 symlink 逃逸（`EvalSymlinks` 后前缀校验）；目录权限 0700；
   执行器 `local.New(local.WithWorkDir(dir), local.WithTimeout(m.timeout))`。
3. `trpcservice/execution/worker.go`：新增 `WithWorkspaceManager(*workspace.Manager)` RunnerOption；
   `pinsName(pinned, workspace.Name)` 时 `llmagent.WithCodeExecutor(exec)`；未配置时报 fail-loudly。
4. `trpcservice/jobs/jobs.go` + `roles.go`：jobs 角色注册 `workspace_gc` 任务（每小时触发 `GC`）。
5. `cmd/trpc-service/roles.go`：worker factory 传 `WithWorkspaceManager`（`cfg.Workspace.Root` 为空时不传）。

**测试**：`trpcservice/workspace/workspace_test.go`（新）——① 目录布局与 0700 权限；② 租户/session 隔离；
③ `..` 与 symlink 逃逸被拒；④ GC 只清过期目录；⑤ `Executor` 在目录内执行 `echo`/写文件成功，
访问目录外路径被拒。`execution` 增：pin `code_exec` 的组装用例。

**验收**：`go test ./trpcservice/workspace/... ./trpcservice/execution/...`；
E2E（可选，S6 合入）：pin `code_exec` 的会话让假模型触发一次 workspace 执行，断言目录内产物存在、目录外无写入。

---

## S4 后端与迁移：租户级后端选择（G6）与迁移工具（G7）

### G6 租户级后端选择落地

**事实**：`backend_profiles.session_backend / redis_key_prefix / session_ttl` 已入库、随 revision 固定
（`controlplane/profiles.go`），但运行时零消费：legacy 的 session 服务是平台级单例（`storage.NewSessionService`），
可靠模式会话事实源是 MySQL（`sessionstore` 快照）。`coordination.SessionProjection` 已实现投影读写，
但 jobs 的 `session_project` 任务在 roles.go 中仍是 no-op（注释「no projection store in this deployment」）。

**设计决策**：按模式分两条落地，语义在文档与代码注释中一次写明：

- **legacy（网关路径）**：`backend_profiles` 驱动**真实的 per-tenant session 服务选择**——
  `session_backend=memory|redis` 决定该租户 runner 使用的后端实例；`redis_key_prefix`/`session_ttl`
  作为该租户 Redis 后端的键前缀与 TTL。数据源：`control_plane.mode=mysql` 时读 control plane
  （每租户最新版本、active），file 模式退回平台级配置（现状）。被否方案：可靠模式也做后端混用——
  与「MySQL 事实源」冲突，且事务语义无法跨两个后端。
- **可靠模式**：`session_backend` 的语义限定为**投影开关**——`redis`（默认）→ jobs 的 `session_project`
  任务对活跃租户执行真投影（`coordination.SessionProjection.Project`，版本只增不减）；`memory` → 跳过
  （维持现状 no-op 语义）。投影的消费方是**最终一致读路径**：新增 `-session-state` CLI（诊断/运维）读投影展示；
  claim 快照读**不接投影**（一致性优先，见范围外）。

**改动点**：

1. `trpcservice/storage/router.go`（新）：
   ```go
   // Router 按租户解析 session.Service：租户级 profile 优先，缺省回落到平台默认。
   type Router struct{ defaultSvc session.Service; /* per-tenant 缓存与配置 */ }
   func NewRouter(defaultSvc session.Service) *Router
   func (r *Router) For(tenantID string) session.Service
   // ApplyProfiles 由 admin 热更新或启动时调用，整体替换租户表（旧实例延迟 Close）。
   func (r *Router) ApplyProfiles(profiles map[string]Profile)
   ```
   Profile 来自 `controlplane`（新增 `Scope.ListActiveBackendProfiles(ctx)`，读每租户最新版本）。
2. `trpcservice/agent/agent.go`：`NewRegistry` 接受 `sessionFor func(tenantID string) session.Service`
   （由 Router 提供），逐租户构建 Runner；`Apply` 同步刷新。
3. `cmd/trpc-service/main.go`：`all` 角色装配 `Router`（mysql 模式注入 control plane 数据源，
   admin 热更新后 `ApplyProfiles`）；`defer router.Close()` 统一关闭。
4. `cmd/trpc-service/roles.go`：jobs 的 `session_project` 处理器实装——
   读会话快照（`SELECT state, summary, session_version FROM sessions WHERE …`）→ `ProjectedSession{Version: session_version}`
   → `SessionProjection.Project`；`backend_profile.session_backend='memory'` 的租户跳过并记录 debug 日志。
   `session_project` 任务若在无 Redis 部署执行，保持现行为（完成并记录原因），不阻塞队列。
5. `cmd/trpc-service/main.go` 新增 `-session-state -resolve-tenant <t> -session-pk <n>` CLI：
   优先读投影，miss 回读 MySQL 快照，输出状态/摘要/版本（证明「投影允许滞后、MySQL 是事实」）。

**测试**：`trpcservice/storage/router_test.go`（新）——① 两租户分别 memory/redis 得到不同实例、互不串写；
② profile 热更新后新消息走新后端；③ 缺省租户回落默认。`trpcservice/coordination/projection_test.go` 已覆盖投影语义，
补 jobs 处理器测试（投影写入版本单调、memory 租户跳过）。

**验收**：`go test ./trpcservice/storage/... ./trpcservice/coordination/...`；
`scripts/e2e.sh` 回归全绿；`scripts/reliable_e2e.sh` 增加断言：两次提交后 `session_version` 与投影版本一致（允许滞后一次）。

**实现记录（S4-G6 落地）**：

- `storage.Router` 按 `backend_profiles` 最新一行（`MAX(profile_id)` 每租户）路由：无 profile → 平台默认；
  `memory` / `redis`（部署级 URL + 租户 prefix/TTL，空 prefix 默认 `tas:<tenant>:`）；非法 backend 降级到默认并告警。
- `agent.NewRegistryWith(cfg, sessionFor)` 是唯一接线点（`NewRegistry` 保留单服务形态供测试/内存模式），
  `Apply` 每次重建时重新询问 provider。
- **热更新点**：`admin.WithCommitHook`（settings 写入提交后重新加载 profiles）；启动时加载一次。
- 可靠模式的“投影开关”落地为 jobs 角色的 `session_project` 处理器：MySQL 快照 →
  `coordination.SessionProjection`（版本单调）；`-session-state` CLI 对照打印两边版本；
  `reliable_e2e.sh` D2 段用 python 严格比较两侧 `version`（不做“grep 到 version 就算数”的假绿断言）。

### G7 数据迁移 CLI

**事实**：方案文档 §5 有迁移策略（双写→搬运→切读→回滚），无实现；`-migrate` 只是 schema 迁移。
仓库中向量库只有 Qdrant（远端 HTTP），不存在「本地向量库→远端」实物，故迁移范围调整为：
**会话数据从 legacy Redis 后端入 MySQL 事实源** + **知识库重嵌入**（换 embedding 模型/向量库的实际场景）。

**设计决策**：一次性、单向、幂等、可重跑，全部 CLI 化并带 `-dry-run`。被否方案：在线双写——legacy 与可靠
是两套部署形态，不存在需要零停机的同一实例双写场景，工具化搬运 + 切换窗口是最小正确做法。

**改动点**：

1. `cmd/trpc-service/migrate_sessions.go`（新）：`-migrate-sessions`（需 `control_plane.mode=mysql` 与
   `storage.session.backend=redis`）：
   - **枚举**：经框架 `redisession` 服务列举会话（实现时验证 `session.Service` 是否提供枚举；不足则按
     既有键前缀 `SCAN` + `GetSession` 读取）。
   - **映射**：legacy 会话键 `tenant:channel:actor` → `channel_bindings`（tenant+channel_type）→ 目标
     `sessions` 行（`actor_key=actor`、`generation=0`），事件按时间顺序写入 `session_events`，
     `in_seq=head_seq=len(events)`、`session_version` 从 0 起自增。
   - **幂等**：按 `(tenant_id, app_id, binding_id, actor_key, generation)` 查已有行；存在则只补缺失事件
     （按事件 ID 去重）。`-dry-run` 打印将要迁移的会话数与事件数，不写库。
   - 无法解析的键（缺 channel/binding）跳过并输出明细，退出码 0（迁移不是全有全无）。
2. `cmd/trpc-service/reindex.go`（新）：`-reindex-kb -resolve-tenant <t> -ingest-kb <kb>`：
   重置该 KB 的 `documents.status='ready'→'pending'` 并对每个 ready 文档重跑 `doc_cleanup`+`doc_index`
   任务（复用 `knowledge.Service` 的 job 处理器），用于换 embedding 模型或向量库后重建。
3. `docs/README.md` 与根 README 的「快速开始」补一节「迁移」。

**测试**：`cmd/trpc-service/main_test.go` 扩展——① 幂等：同一 Redis 数据跑两次，第二次零写入；
② 事件顺序与去重；③ `-dry-run` 不产生写。`knowledge/pipeline_test.go` 补 reindex 用例（重嵌入后 chunk 数与向量数一致）。

**验收**：`go test ./cmd/... ./trpcservice/knowledge/...`；`scripts/reliable_e2e.sh` 追加迁移冒烟段
（造 legacy 会话 → 迁移 → 新链路上继续该会话对话）。

---

## S5 灰度发布（G8）

**事实**：`RollbackToRevision`（`controlplane/revision.go`）与 admin/v2 已实现回滚；
代码中不存在 canary/百分比/白名单相关实现（全仓库零命中）。revision 选择的三个现场：
`KfPuller.resolveTarget`、即将实现的 `Receiver.Accept`（G2）、legacy 网关（不涉及 revision，范围外）。

**设计决策**：**会话粘性比例放量**。以**会话（actorKey）**而非消息为分配单位（同一会话始终命中同一 revision，
杜绝中途切换）；哈希用框架无关的 FNV-1a 取模 10000，确定性可复算。白名单即 `basis_points=10000` 的定向
rollout（对单 actor 亦可扩展 `actors` JSON 列，本切片用全量/比例两档，白名单列预留）。被否方案：
随机 per-message 分流——会话中途换 revision 会破坏「修订固定」的核心承诺。

**改动点**：

1. `migrations/0005_completion.sql` 追加：
   ```sql
   CREATE TABLE rollouts (
     rollout_id   BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
     tenant_id    VARCHAR(64) NOT NULL,
     app_id       BIGINT UNSIGNED NOT NULL,
     revision_id  BIGINT UNSIGNED NOT NULL,   -- 候选修订
     basis_points INT UNSIGNED NOT NULL DEFAULT 0, -- 1..10000
     status       ENUM('active','stopped') NOT NULL DEFAULT 'active',
     created_by   VARCHAR(128) NOT NULL,
     created_at   TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
     stopped_at   TIMESTAMP(6) NULL,
     PRIMARY KEY (rollout_id),
     KEY idx_rollouts_active (tenant_id, app_id, status),
     CONSTRAINT fk_rollouts_tenant FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE CASCADE
   ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;
   ```
   约束：同一 `(tenant, app)` 至多一条 `status='active'`（应用层校验 + 事务内回收旧行）。
2. `trpcservice/controlplane/rollout.go`（新）：
   ```go
   func (s Scope) StartRollout(ctx, appPublicID string, revisionID int64, basisPoints int, by string) (*Rollout, error)
   func (s Scope) StopRollout(ctx, appPublicID string, by string) error
   func (s Scope) GetActiveRollout(ctx, appPublicID string) (*Rollout, error)
   // ActiveRevisionForActor: 无 rollout → current_revision_id；
   // 有 → fnv1a(actorKey)%10000 < basis_points ? candidate : current
   func (s Scope) ActiveRevisionForActor(ctx, appPublicID, actorKey string) (revisionID int64, rollout bool, err error)
   ```
   候选修订必须属于同一 app 且已发布（校验）。
3. 调用点替换：`wechatkf_puller.go` 的 `resolveTarget` 改为「先取 current + active rollout，再按 actorKey 解析」；
   `channels/receiver.go` 的 `Accept` 同构复用（抽一个 `resolveTargetForActor(ctx, scope, bindingID, actorKey)` 供两处调用）。
4. `trpcservice/admin/control_api.go`：新增
   `POST /admin/v2/apps/{id}/rollouts`（body：revision_id、basis_points、by）、`DELETE …/rollouts`（停）、
   `GET /admin/v2/apps/{id}/rollouts`；写 `audit.EventAdmin`（decision=allow/block）。
5. 回滚语义文档化：停 rollout → 下一条消息全量回 current；再需要时 `RollbackToRevision` 移指针。

**测试**：`trpcservice/controlplane/rollout_test.go`（新）——① 分配确定性：同一 actorKey 永远同一结果；
② 比例边界（0/10000 两端与 5000 的统计分布 ±3σ，固定种子集合断言）；③ 并发创建仅一条 active；
④ 停 rollout 后全量回 current；⑤ 候选 revision 校验（跨 app/未发布被拒）。

**验收**：`go test ./trpcservice/controlplane/... ./trpcservice/admin/...`；
`scripts/reliable_e2e.sh` 追加灰度段：启动 5000 bp rollout → 用两组固定 actorKey 断言一组走新修订（指令差异可观察）、
一组走旧修订，停用后全部回旧修订。

---

## S6 验证补强（G10）

G10 不是「未实现」，是「本机环境验不了」。本切片把它变成**可执行的补验方案**，如实标注前提：

1. **K8s 集群行为**：`scripts/verify_k8s.sh`（新，需 kind + docker）——创建临时集群 → `kubectl kustomize | kubectl apply`
   → 等就绪 → 断言 `/readyz`、探针摘流量（停 MySQL pod 观察 503 摘除）、HPA 骨架存在、四角色（gateway/worker/delivery/jobs）Deployment 各自 Running
   → 拆集群。CI 无 kind 时该脚本 SKIP 并计入 `--strict` 失败（沿用门禁惯例）。
2. **otel-collector 实收 span**：`deploy/compose` 增加 `observability` profile（collector + 导出到 stdout/file），
   `scripts/e2e.sh` 追加可选段：以 `traces.exporter=otlp` 起栈，断言 collector 输出出现
   `im.callback → gateway.dispatch → model.call` 金字塔非零 span。镜像不可得时 SKIP（如实标注）。
3. **Linux UID 写权**：`scripts/check_deploy.sh` 增加一条（需 docker daemon）：`docker run --rm -u 65534:65534 -v <tmp>:/data`
   冒烟写文件；macOS 上自动 SKIP（Docker Desktop 行为与 Linux 不同）。
4. **delivery unknown 容器级演练**：`scripts/reliable_fault_drill.sh` 增加 R4——
   令假 KF 上游对发送接口返回 500/挂起 → 断言 `reply_outbox` 进入 `unknown`/重试序列 → 恢复后状态收敛且无重复发送
   （靠 delivery 的条件更新保证幂等）。

**验收**：四个方案各自「有环境则跑、无环境则 SKIP 且 `--strict` 显式失败」，输出写回
`deploy/README.md` 的验证清单（同时纠正其「D1–D7 尚未执行」的过时标注——已由 `.smoke/drill6-strict.log`
143 PASS 证伪）。

---

## 汇总 A：数据模型变更（`migrations/0005_completion.sql`）

| 变更 | 归属 | 说明 |
| --- | --- | --- |
| `reply_outbox.pushed_at` + `idx_reply_outbox_webchat` | G3 | webchat 邮箱模式的推送标记 |
| `rollouts` 表 | G8 | 会话粘性比例放量 |

迁移门禁：`trpcservice/storage/mysql/migrate_test.go` 增补；`-migrate` 幂等重跑已有断言覆盖。

## 汇总 B：实施顺序与批次门禁

| 批次 | 内容 | 依赖 | 门禁 |
| --- | --- | --- | --- |
| S1 | G1 + G9 | 无 | `go test ./...`；reliable_e2e 回归 |
| S2 | G2 + G3 | 0005 迁移 | reliable_e2e 扩段全绿；D1–D7 回归 |
| S3 | G4 + G5 | S1 的 option 基建 | skill/workspace 单测；execution 组装用例 |
| S4 | G6 + G7 | S2（投影消费场景） | e2e 回归；迁移冒烟段 |
| S5 | G8 | S2（接收端统一解析入口） | 灰度段断言 |
| S6 | G10 | 全量 | 有环境全跑，无环境 SKIP + strict 失败 |

每批收口的固定动作：`./format.sh && ./lint.sh && ./coverage.sh && go test ./...`、
`scripts/check_deps.sh`（本切片**不新增第三方依赖**，框架已有能力全部覆盖）、
`scripts/check_deploy.sh`、README 与 `docs/README.md` 同步、`.smoke/` 证据归档。

## 汇总 C：风险与回滚

| # | 风险 | 缓解 |
| --- | --- | --- |
| 1 | webchat 邮箱轮询对 `reply_outbox` 读放大 | 专用索引（已建）；ticker 仅在有连接时启动；行数按会话过滤 |
| 2 | deferred provider 掩盖配置错误（一旦成功缓存，后端再坏不重试构建） | 缓存的是「服务句柄」，每次调用仍走真网络；后端故障由工具调用路径自然失败入账本 |
| 3 | workspace 本机执行的权限风险 | 默认禁用；文档红线；`mode=strict`（容器/Linux sandbox）留二期 |
| 4 | 迁移工具解析歧义（legacy 键字段含 `:`） | 已知限制：迁移只支持标准 `tenant:channel:actor` 三键；歧义键跳过并列出，不改写 |
| 5 | 灰度哈希稳定性依赖 actorKey 不变 | actorKey 已是会话主键组成部分；文档写明「改 actor 即换会话即重新分配」 |
| 6 | G2 与 legacy 同路径双实现漂移 | 协议解析抽成单函数由两侧共调；签名逻辑只此一份 |

---

## 附：与既有文档的关系

- 本文档替代「README 未验清单」中除环境受限项外的所有未实现标注；落地后同步更新：
  根 `README.md`（快速开始增加迁移/灰度/接收端说明）、`docs/README.md`（本 spec 入表）、
 `deploy/README.md`（验证清单刷新 + 过时标注纠正）、各 spec 尾部的「未验/未接」清单（逐条划销）。
- 不修改 `docs/spec-im-channels.md` 的范围结论；Telegram/公众号维持预留。

---

## S7 治理补齐：预算限额、成本计量、工具二次确认与日志脱敏（G11–G14）

### G14 日志脱敏（敏感信息泄漏防护）

**事实**：`trpcservice/log/log.go` 已有 `Redact(secret)` 函数（短值全掩、长值留首三末四），
但未在日志、trace attribute、审计 `Detail` 中系统调用；API key 和通道凭据可能在错误传播路径中
暴露（`channels.go` 中 `failModel` 的 `f.detail` 会包含框架 session 的原始错误，含 `dial tcp` 等
内部地址信息——目前由文案隔离而非脱敏）。

**设计决策**：两级防护。第一级（轻量）：在关键错误点上调用 `log.Redact(secret)`——
但 `log.Redact` 只能处理已知密钥，对框架内部路径暴露的 DNS/地址信息无效。
第二级（扎实）：替换 `tool/build.go` 与 `channel/wechatkf_puller.go` 等处的
`err.Error()` 传播为脱敏版本；引入 `sensitive.Replacer`（构建于启动时加载的敏感串
白名单：`env:MODEL_API_KEY`、`env:KF_SECRET`、`env:WECOM_SECRET` 等「env:」引用值）。

**改动点**（两阶段）：

1. `trpcservice/log/log.go`：
   新增 `RedactAttrs(h slog.Handler, patterns []string) slog.Handler`——一个 slog
   Handler 包装，在输出 `msg`/`err`/`detail` 等字段时用 patterns 列表做子串替换
   （`strings.ReplaceAll(v, secret, "<redacted>")`）。Patterns 在启动时从
   `secrets.Resolver` 的已解析值收集（resolver 知道哪些 `env:NAME` 被使用了）。
2. `trpcservice/secrets/resolver.go`：新增 `Secrets() []string` 方法返回当前已解析的
   所有 secret 明文，用于初始化 `RedactAttrs` 的模式列表。
3. `cmd/trpc-service/main.go`：log.Init 后安装 `RedactAttrs` 包装。

**测试**：`log_test.go` 追加 `RedactAttrs` 用例（已知 secret 不出日志、非 secret 字段不变）；
`secrets/secrets_test.go` 追加 `Secrets()` 单测。

**验收**：`go test ./trpcservice/log/... ./trpcservice/secrets/...`；e2e 日志检查无暴露。

### G13 每租户成本计量

**事实**：`metrics.Recorder.Tokens` 按 tenant+type（prompt/completion）计数，
无成本聚合；`model_profiles` 无单价字段；审计 `Record.Cost` 字段（`PromptTokens` +
`CompletionTokens`）有 token 计数但无成本。

**设计决策**：在模型 profile 上加单价（厘/token 级别），gateway dispatch 时计算
成本并写入审计的 `cost` 字段（微元，避免浮点累积误差）。成本不独立落新表——
审计行已有的 token 字段加上 profile 单价即可事后计算；但为「实时预算」G11 的
基础，仍需要在运行时记录近似成本。被否方案：单独的成本表——与审计冗余且查询
路径复杂。

**改动点**：

1. `trpcservice/controlplane/profiles.go`：`ModelProfile` 加
   `PromptCostPer1K uint32` 和 `CompletionCostPer1K uint32`（单位：毫厘/
   1000 token，即 4200=4.2 ¥/1K）；`CreateModelProfile` 构造参数不动（字段零值=
   未知=事后不可算），新增 `WithCost` 方法或扩展签名。
2. `trpcservice/metrics/metrics.go`：`Recorder` 新增
   `Cost(tenant string, costCents int64)` 计数器。
3. `trpcservice/audit/audit.go`：`Record` 加 `CostMicroCents int64`（微元，
   1 ¥=1_000_000 μ¢）。
4. dispatch 路径（`channels.go failModel` / `dispatch`）：模型调用返回 token
   计数后，查模型 profile 的单价 → 算成本 → 写审计行 + 指标。

**测试**：`metrics_test.go` 追加 Cost 用例；`controlplane` 的 profile round-trip 测试；
`channels` dispatch 审计行成本字段断言（resilience_test 或 governance_test）。

**验收**：`go test ./trpcservice/metrics/... ./trpcservice/controlplane/...`；
通过 metrics stdout 断言审计行含 `cost_microcents` 字段。

### G11 租户级预算限额

**事实**：READIME §3.5 要求「预算限制」、§8 风险 #10「token 成本失控」；
当前 Guardrail 只有关键词/长度规则，无 token/调用/成本上限。`agent_revisions`
有 `max_llm_calls`（每次消息的模型调用次数），但无「每租户每时间段」的总额
限制。

**设计决策**：在 `Guardrails` 加预算字段，在 dispatch 前做准入检查（类似
`max_input_bytes` 与 `max_concurrency_per_tenant`）。预算状态储存在 Redis
（或进程内表，降级场景退化为进程内——与 dedup/coord 的 fallback 一致）。
被否方案：预算单独落 MySQL 表——预算检查是热路径（每消息），Redis 的原子
加减符合需求。

**改动点**：

1. `trpcservice/tenant/tenant.go`：`Guardrails` 加 `MaxPromptTokens
t uint`、
   `MaxCompletionTokens uint`、`MaxCostCents uint`、`MaxCalls uint`、
   `BudgetResetInterval time.Duration`。
2. `trpcservice/channels/budget.go`（新）：`BudgetTracker` 类型，接口
   `Spend(ctx, tenantID, promptTokens, completionTokens, costCents) (ok bool, err error)`。
   实现用 Redis（协调层）或进程内 `map[string]*budget`。
3. `trpcservice/channels/channels.go`：`dispatch` 中在 `quota.acquire` 之后、
   `runner.Run` 之前加预算检查（`budget.Spend`）。失败时记录审计
   `throttled`（stage=admission, rule=budget）并回复 ThrottleText。
4. 配置示例：`config.example.yaml` 更新注释。

**测试**：`channels/budget_test.go`（新）—— Redis 实现（真 Redis/假 Redis bridge）
与进程内实现的 Spend/Reset 语义。

**验收**：`go test ./trpcservice/channels/...`；e2e 扩展 D7 断言预算拒绝留痕
（审计 + 指标 `result=throttled`）。

### G12 工具调用二次确认（危险工具审批）

**事实**：`tenant.Tools.ApprovalRequired` 字段已有但未被消费——legacy 路径
（`platformtool.Select`）无视它；可靠路径（`platformtool.Governor` 的
`Wrap`）检查 risk_level 但无审批阻塞。READIME 要求「危险工具二次确认」。
框架的 `guardrail` 包（`Guardrail`）可注入 Plugin 但未对接审批。

**设计决策**：二次确认走现有的 `execution.BlockForReview` 机制——当
governor 被调用的工具在绑定中被标记为 `approval_required=true` 且
risk_level=high 时，**不作为工具执行**，而是阻塞会话（blocked_reason=
`tool_approval_required`）。审批走已有的人工处置路径：`-list-blocked` +
`-resolve-session`。
被否方案：独立审批队列——BlockForReview 已有 list/resolve 工具且与未知
结果的处置路径一致，复用降低实现成本。

**改动点**：

1. `trpcservice/governor.go`（新建或在 `tool/governor.go` 加）：`Wrap` 内
   检查 binding 的 `ApprovalRequired`（来自 `tool_bindings.status+approval`）。
   若为 true，返回 `GovAction{Block: true, reason:"tool_approval_required"}`。
   框架 governor 的 `Wrap` 返回 `tool.Tool` 而不是 `error`，因此阻塞信号
   需要放在 `Call(ctx, args)` 的运行时：检查 ctx 中的 `Governor` → 调用
   governor 的 `BlockExecution(ctx, reason)` → execution 感知后阻
   塞会话。这在现有架构下应该已经在 `tools_ledger_test.go` 的
   `charge_upstream` 测试中做了（unknown 阻断）。同理，approval_required
   就是「还没执行就先 unknown」。
   
   具体：在 `tool/governor.go` 的 `CallWithResult` 中（如果存在；若不存在则
   在 `Wrap` 返回的 `WrapCtx` 里），检查 `meta.ApprovalRequired &&
   meta.RiskLevel == "high"` → 直接返回 `Unknown` 结果 + 写入账本
   `result=blocked`。`ClaimedAttempt` 的阻塞话术复用已有文案。

**简化决策（v1）**：不新建审批 UI，`-list-blocked` + `-resolve-session` 即
可完成审批——human 看到 blocked reason `tool_approval_required`→ 调
`-resolve-session <PK> -resolution confirmed`（放行）或 `cancelled`（拒绝→
消息重跑会再触发审批）。

**测试**：`tools_ledger_test.go` 加——绑定 `approval_required=true` 的工具→
执行→账本 `blocked`→list-blocked 可见→人工 `confirmed`→重跑成功。

**验收**：`go test ./trpcservice/tool/... ./trpcservice/execution/...`；
e2e 可选扩展（工具段加 approval_required 绑定）。
