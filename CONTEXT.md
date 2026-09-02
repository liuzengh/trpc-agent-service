# CONTEXT.md — 共享语言与术语表

> 本文件记录本项目（trpc-agent-service）在需求/设计对齐中确立的领域术语。
> 只含领域术语，不含实现细节。每个术语一行定义：是什么，不是做什么。
> 由 grill-with-docs 在每次对齐会话中即时维护。

## 消息与编排

- **Inbound 消息**：IM 用户发往平台的原始消息，归一化后进入 `stream:inbound`。
- **Outbound 消息**：平台发给 IM 用户的回复，经 MySQL Outbox → `stream:outbound` → IM 通道。
- **IM 桥接（Gateway）**：把 IM 适配器接入总线的双向桥——**入向**：消费每个 adapter 的 `Inbound()`、按绑定/默认解析 agent、`PublishInbound`（`Message.ID = channel:platformMsgID` 保幂等），并记住 `session → (adapter, chatID)` 路由；**出向**：`Run()` 无消费组跟随 `stream:outbound`，按 session 路由分发到发起会话的 adapter 的 `Send`。真实 WSS Conn 已实现（企业微信 aibot SDK / 飞书 lark SDK），连接由 **binding 驱动**的 `channels.Manager` 管理（`Reload` 按 binding 集动态启停 adapter、凭据经 secret store 解析），前端通道页保存即连接；桥接层用 mock 可全测。
- **会话（Session）**：`{tenant}:{channel}:{user|group}:{id}` 维度；串行处理由会话锁保证。
- **Agent 发布**：把不可变 RuntimeProfile 冻结为版本号并切换 current 指针；可原子回滚。
- **RuntimeProfile**：Agent 一个版本的运行时配置快照（system_prompt / endpoint / tools / kbs / skills / 需审批工具）。
- **挂载**：发布 Agent 时把资产（工具、知识库、Skill、审批工具）勾选进 RuntimeProfile。
- **Admin 对话（/chat）**：管理台经 `channel=admin` 与 Agent 会话，走**与 IM 相同的 worker 链路**（幂等/锁/审批/技能/工具/RBAC）；回复经 outbox→outbound 流，前端用 **SSE**（按 session 过滤、无消费组 XREAD）转发，不影响真实 IM 的 group 消费。非完整账本——chat_messages 表仍为预留（会话历史/成本页启用）。
- **IM 通道绑定（ChannelBinding）**：`channel_bindings` 记录把外部 IM 账号（channel+account）绑到租户+Agent；`credential_ref`（bot/app Secret）与 `verification_token_ref`（飞书验签 token）都存**密钥引用**（非明文，指向 secret store），前端通道页填明文自动加密入 store。真实 SDK 收发已接线（conn 实现），需本地账号手测联调。
- **会话账本（Conversation Ledger）**：`chat` 域写 `chat_sessions`/`chat_messages` 业务账本——每轮 **USER + ASSISTANT** 两行共享 `turn_id`/`turn_timestamp`（turn 分页游标），工具调用细节留在框架 session_events/审计不重复；worker 收尾**同步 best-effort** 写入（失败不阻断对话），重复投递靠 `message_id` 唯一键幂等。区别于框架 session 滑动窗口：账本不受 prompt 截断影响，服务会话列表/历史/成本页。

## 审批治理（阶段 13 确立）

- **审批（Approval）**：对一次工具调用的放行决策。人工审批经 IM 会话回复完成；不是 RBAC（RBAC 决定"能否挂载/调用"，审批决定"高风险调用是否放行"）。
- **审批策略（Approval Policy）**：决定某工具调用是否需要审批。平台为**双轨**：Agent 发布勾选的 `approval_tool_ids` ∪ 工具元数据 `risk_level=high` 自动触发。
- **待审批（Pending）**：已发起、等待人工决策的审批请求；**会话级单 pending**——同一会话同时至多一个待批，用户直接回复批准/拒绝词即可，无需编号。
- **审批决策（Decision）**：批准（approve）或拒绝（deny）或超时；同步模型下由 reviewer 轮询获得。
- **同步挂起（Blocking Review）**：工具执行前 agent 轮次真实暂停；reviewer 阻塞轮询 Redis 决策键；批准后**原工具调用原参数继续执行**。选择它而非"异步重放"（后者需模型多跑一轮）。
- **审批通知（Approval Notice）**：待审批时发给用户的外发消息，说明待批工具与"回复 批准/拒绝"。
- **锁外审批回复**：批准/拒绝回复在取会话锁**之前**被识别处理（无锁分支），否则会与会话锁互等形成死锁。

## 基础设施（Redis key 维度，租户/会话级隔离）

- **会话锁（Session Lock）**：`lock:session:{tenant}:{session}`；审批等待期间**自动续期**（避免超时被他人抢锁）。
- **审批请求键**：`approval:req:{tenant}:{session}` 存待审批载荷。
- **审批结果键**：`approval:res:{tenant}:{session}` 存人工决策。
- **有界并发消费**：worker 消息消费为每消息 goroutine + 信号量上限；保证审批回复不被阻塞中的 agent 轮次挡住；同会话正确性仍由会话锁串行 + 幂等双保险兜底。

## 凭据管理（阶段 21 确立）

- **Secret（凭据密文）**：统一凭据存储里的加密值，经 key 引用；模型 api_key 与 IM 通道凭据只存此处，域表（endpoints/channel_bindings）不存明文。
- **Secret Key（凭据引用）**：opaque key；模型约定 `endpoint:{id}`，通道用 binding.credential_ref 指向。管理 API 只返回 key 与 updated_at，**永不返回明文**。
- **主密钥（Master Key）**：AES-256-GCM 加密密钥来源（env `TRPC_SECRET_MASTER_KEY` 优先，否则 config `secret.master_key`）；经 sha256 派生 32 字节 AES 密钥、只在内存；**不落库**；MySQL 下无主密钥则凭据存储禁用（拒绝明文落盘）。
- **KeySource**：llm 侧的凭据解析接口（`Get(ctx,key)`），由 secret.Store 满足——避免 llm 反向依赖 secret 包；`Endpoint.APIKeyRef` 在 Resolve 时经它取用，取代明文 `APIKey`。
- **AES-256-GCM**：密文 `base64(nonce‖ciphertext)`，随机 nonce；不同主密钥无法解密（GCM tag 校验失败）。

## 用量计量（阶段 22 确立）

- **Usage 计量（Metering）**：按租户/Agent/维度记录平台消耗量，用于成本归属；**只计量、不折价**（无单价价目，金额口径未定）。
- **维度（Dimension）**：`token | tool | sandbox | artifact | skill`；当前仅 token 维度由 worker 每回合自动写入。
- **幂等计量**：record_id = 入站消息 id + ":" + 维度；`INSERT IGNORE` 保证重放不重复计。
- **UsageRow / UsageSummary**：明细行与按维度聚合（SUM(amount), COUNT）两类读视图；`GET /usage` 返回二者。

## 制品与对象存储（阶段 14 确立）

- **Artifact（制品）**：Agent 执行中产生的命名、带版本二进制文件（代码产物/报告/图片等）。不是日志；日志走 audit/usage。
- **Artifact Service**：框架 `artifact.Service` 接口（Save/Load/List/Delete/ListVersions），经 `runner.WithArtifactService` 注入——**代码执行工具自动保存产物**，平台无需在工具内手动上传。
- **版本（Revision）**：同一文件名的第 N 次保存；首个保存为 revision 0，逐次 +1；旧版本保留可回溯。
- **对象键布局**：`{tenant}/{user}/{session}/{filename}/{revision}`；`user:` 前缀文件名走 `{tenant}/{user}/user/...`（跨会话持久）。tenant 段实现对象级隔离。
- **MinIO**：S3 兼容对象存储，承载 artifact 字节；部署资产已含独立 `artifact-minio` 服务（compose）。
- **metadata 表 vs 对象**：artifact 版本信息编码在对象键内（无额外索引）；MySQL `artifacts` 表（009）预留为审计/管理读视图，待消费侧（代码执行沙箱）落地后回填。

## 数据访问与存储域（阶段 18 对齐）

- **数据域（Data Domain）**：平台的存储划分单位 = **session / memory / summary / artifact / knowledge（向量库）/ audit**。各自承载一类状态或产物，各有其一致性与生命周期特征。
- **DataBackend**：租户级 `data_backend` map（domain → backend 值）；operator 改租户数据即可换某域后端。**storage.Router** 按租户解析并缓存实例（现注册 session/memory 两域）。
- **统一数据访问抽象**：一个聚合入口，让上层（web / worker / main）以一致方式取得各数据域的实现并统一初始化——目标是**说清楚"每域如何存储"并收敛初始化点**，不是强制每个域都有多个后端实现（只有实际出现第二实现才纳入逐租户可选）。

## 代码执行（阶段 15 确立）
- **code-exec 工具**：平台内置工具（目录 id `code-exec`，工具名 `execute_code`），让 Agent 在隔离容器里运行 Python/Bash 并取回输出。
- **DockerExecutor**：平台自实现 `codeexecutor.CodeExecutor`（框架窄接口 ExecuteCode+Delimiter），经 **docker CLI** 执行 `docker run --rm -i --network none <python|alpine 镜像>`，代码走 stdin；120s 超时；**网络隔离**内建。无 Go docker SDK 依赖。
- **运行时错误语义**：非零退出（语法错误/异常）作为**输出**回给模型（可自修复）；传输错误（docker 缺失/镜像拉取失败/超时）才作为工具 error。
- **高风险自动审批**：code-exec 定义 `risk_level=high` → 走审批 rail2，每次执行前自动人工审批（与阶段 13 打通）。
- **容器隔离层 vs sandbox（易混术语）**：平台承担"沙箱"职责的实际后端是 **Docker 容器**（镜像沙箱 + `--network none`）；框架自带 `codeexecutor/sandbox` 是**进程级 OS 沙箱（seccomp）**，非容器、仅 Linux/macOS（Windows 为 stub），平台未接入。同名"沙箱"指两种不同隔离层，务必区分。
- **K8s Pod 执行后端（已决策：不实现）**：把 `CodeExecutor` 后端实现为「在 Kubernetes 集群创建 Pod 执行代码」。与 Docker 后端的差别在**编排层**（调度/配额/网络策略/多节点），而非执行语义本身；平台若跑在 K8s 集群内，节点通常没有 docker socket/docker CLI（K8s 用 containerd，不经 docker），Docker 后端不可用 → Pod 后端是集群环境的执行通道。**阶段 17 grill 决策**：生产部署形态为 Docker Compose/单机，Docker 后端已覆盖 → 不实现；若未来交付到 K8s 环境，靠窄 CodeExecutor 接口后端替换补充（AGENTS.md §7 决策区）。
