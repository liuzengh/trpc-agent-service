# CONTEXT.md — 共享语言与术语表

> 本文件记录本项目（trpc-agent-service）在需求/设计对齐中确立的领域术语。
> 只含领域术语，不含实现细节。每个术语一行定义：是什么，不是做什么。
> 由 grill-with-docs 在每次对齐会话中即时维护。

## 消息与编排

- **Inbound 消息**：IM 用户发往平台的原始消息，归一化后进入 `stream:inbound`。
- **Outbound 消息**：平台发给 IM 用户的回复，经 MySQL Outbox → `stream:outbound` → IM 通道。
- **会话（Session）**：`{tenant}:{channel}:{user}`（单聊）或 `{tenant}:{channel}:{group}`（群聊）维度；串行处理由会话锁保证。
- **Agent 发布**：把不可变 RuntimeProfile 冻结为版本号并切换 current 指针；可原子回滚。
- **RuntimeProfile**：Agent 一个版本的运行时配置快照（system_prompt / endpoint / tools / kbs / skills / 需审批工具）。
- **挂载**：发布 Agent 时把资产（工具、知识库、Skill、审批工具）勾选进 RuntimeProfile。

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

## 制品与对象存储（阶段 14 确立）

- **Artifact（制品）**：Agent 执行中产生的命名、带版本二进制文件（代码产物/报告/图片等）。不是日志；日志走 audit/usage。
- **Artifact Service**：框架 `artifact.Service` 接口（Save/Load/List/Delete/ListVersions），经 `runner.WithArtifactService` 注入——**代码执行工具自动保存产物**，平台无需在工具内手动上传。
- **版本（Revision）**：同一文件名的第 N 次保存；首个保存为 revision 0，逐次 +1；旧版本保留可回溯。
- **对象键布局**：`{tenant}/{user}/{session}/{filename}/{revision}`；`user:` 前缀文件名走 `{tenant}/{user}/user/...`（跨会话持久）。tenant 段实现对象级隔离。
- **MinIO**：S3 兼容对象存储，承载 artifact 字节；部署资产已含独立 `artifact-minio` 服务（compose/K8s）。
- **metadata 表 vs 对象**：artifact 版本信息编码在对象键内（无额外索引）；MySQL `artifacts` 表（009）预留为审计/管理读视图，待消费侧（代码执行沙箱）落地后回填。

## 代码执行（阶段 15 确立）

- **code-exec 工具**：平台内置工具（目录 id `code-exec`，工具名 `execute_code`），让 Agent 在隔离容器里运行 Python/Bash 并取回输出。
- **DockerExecutor**：平台自实现 `codeexecutor.CodeExecutor`（框架窄接口 ExecuteCode+Delimiter），经 **docker CLI** 执行 `docker run --rm -i --network none <python|alpine 镜像>`，代码走 stdin；120s 超时；**网络隔离**内建。无 Go docker SDK 依赖。
- **运行时错误语义**：非零退出（语法错误/异常）作为**输出**回给模型（可自修复）；传输错误（docker 缺失/镜像拉取失败/超时）才作为工具 error。
- **高风险自动审批**：code-exec 定义 `risk_level=high` → 走审批 rail2，每次执行前自动人工审批（与阶段 13 打通）。
- **执行隔离边界**：容器级隔离（镜像沙箱 + 无网络）；框架 sandbox（seccomp）仅 Linux/macOS，本机（Windows）由 Docker 后端承载；K8s Pod exec 后端仍未实现。
