# WX.md 可复用性评估报告

> 阶段：0（准备与审阅）　｜　状态：草案（待用户确认后转正至 `docs/`）
> 评估对象：资料库 `WX.md`（《多租户分布式 AI Agent 平台设计》）

## 一、结论摘要

`WX.md` 是一份**通用方法论**性质的多租户分布式 AI Agent 平台设计文档，其价值在于**架构理念、对象建模、安全与可观测性范式**，但它**不绑定任何具体框架**（甚至不含 tRPC-Agent-Go 或任何 Go API）。因此：

- **可复用**：核心设计原则、分层思想、核心业务对象（Tenant/Workspace/Task/Run）、Tool 受控入口、Sandbox 隔离、审批/事件/产物/用量、分层存储、队列/Worker 分工、安全底线、trace 贯穿。
- **不可照搬**：所有具体实现细节（数据表、存储选型、SDK 调用、代码结构）都必须映射到 **tRPC-Agent-Go 官方 API** 与本平台的租户/IM 需求，严禁原样复制。

一句话：**借鉴「想什么」，不抄「怎么写」。**

---

## 二、可借鉴的模式（概念层，可直接采纳）

### 1. 核心设计原则（强烈推荐，直接采用）
> 「模型只负责决策，平台负责权限、安全、执行、记录和交付」；「模型可以提出请求，但不能完成授权」。

这条原则是整个平台治理模型的地基，将直接映射为：所有工具调用必须经 `FunctionTool` 封装 + `Plugin/Guardrail` 校验 + 租户上下文授权，模型参数只表示「意图」而非「授权」。

### 2. 八层分层架构（采纳思想，映射到真实包）
WX.md 的 8 层（用户入口/接入/业务域/队列调度/执行/Runtime/平台服务/基础设施）是清晰的分层范式。映射到本平台：

| WX.md 分层 | 本平台落点（tRPC-Agent-Go） |
| --- | --- |
| 用户入口层 | Vue3 前端（`front/`） |
| 接入层 | Admin API + IM WSS Adapter（Gateway 角色） |
| 业务域层 | 租户/Agent/Session/Message 领域模型（平台层新增） |
| 队列调度层 | Redis Streams + Consumer Group |
| 执行层 | Worker 角色，调用 `runner.Runner` |
| Runtime 层 | `agent/llmagent`、`agent/graph`、`tool`、`session`、`memory`、`knowledge`、`plugin` |
| 平台服务层 | 知识库、审计、用量、密钥、审批（平台层新增） |
| 基础设施层 | MySQL、Redis（+ 待确认的向量库/对象存储）、K8s |

### 3. Tenant 作为「第一隔离边界」
- 所有核心业务对象带租户上下文；所有 DB 查询带租户过滤；对象存储路径含租户维度；Sandbox Pod 打租户标签；Usage/Audit 带租户信息。
- **直接采纳**，是本平台多租户隔离（会话/记忆/知识库/工具权限/审计）的实现纲领。

### 4. Run 状态机
`queued → running → waiting_approval → completed / failed / cancelled`，用于表达「一次 Agent 执行」的生命周期。
- **采纳**，作为平台层「Agent 运行时管理」的核心状态抽象（在 `runner` 之上叠加平台级 Run 状态）。

### 5. Tool Layer 作为受控网关
工具注册、参数校验、Tool Context 注入、权限判断、Guardrail/Policy 校验、审批触发、事件/审计/用量记录。
- **采纳**，映射到 tRPC-Agent-Go 的 `tool.FunctionTool` + `plugin`（BeforeTool/AfterTool 回调）+ 租户工具白名单。

### 6. 分层存储原则（采纳）
- 结构化状态（业务/事件/审批/用量/审计）→ MySQL。
- 队列/锁/缓存/短期状态 → Redis。
- 大文件/日志/附件/产物 → 对象存储（**待确认是否引入**）。
- 沙箱运行临时目录 → 容器 Volume。

### 7. 队列与 Worker 分工 + 幂等（采纳思想）
- Agent Run 队列、异步化长任务；幂等要求（Run 重试不重复执行危险命令、审批恢复只执行一次、产物去重）。
- 映射为：Redis Streams（XAUTOCLAIM 消费/认领 + 消息 ID 幂等去重）。

### 8. 安全底线（直接采纳）
默认不信任模型输出：Tenant 强隔离、Tool Permission、Guardrails、Sandbox 隔离、Secret Masking、Path Boundary、Network Policy、Audit Log、Rate Limit、Quota。

### 9. Observability（直接采纳）
每个 Run 一个 `trace_id`，贯穿 Channel → Dispatch → Worker → Runner → Governance → Tool → Session 写入 → Reply 全链路，与 tRPC-Agent-Go 的 OpenTelemetry 能力对齐。

### 10. 事件流与产物（参考采纳）
Run Event 类型（`run.started` / `message.delta` / `tool.started` / `tool.completed` / `run.completed` / `run.failed` 等）用于前端实时展示与审计回放；Artifact 走对象存储 + 短期签名下载 + 权限校验。

---

## 三、不适合直接照搬的部分及理由

| 条目 | 理由 | 本平台做法 |
| --- | --- | --- |
| **无任何框架绑定** | WX.md 是概念文档，不含 tRPC-Agent-Go API | 全部落到官方包：`agent/llmagent`、`agent/graph`、`runner`、`session`、`memory`、`knowledge`、`tool`、`plugin`、`server/*`、`openclaw` |
| **基础设施用 PostgreSQL** | 任务明确要求 MySQL + Redis | 后端抽象层以 MySQL 为主（参考 PR 用 PG 仅作思路借鉴） |
| **强调 Tenant→Workspace→Task 三级** | 本平台核心是「租户 + Agent」；Workspace 层级是否必要需用户确认 | 默认 Tenant→Agent 两级，Workspace 作为可选扩展待定 |
| **Sandbox 基于自定义 K8s exec + Sandbox Service** | tRPC-Agent-Go 自带 `codeexecutor` 抽象，需评估「复用框架沙箱」vs「自建 K8s 沙箱服务」 | 沙箱范围与实现方式待用户确认（阶段 7/10） |
| **无 IM 通道细节** | WX.md 未涉及企业微信/飞书协议 | 独立设计 `IMAdapter` 接口，企业微信 `wss://openws.work.weixin.qq.com` + 飞书 `ws.Client`/`EventDispatcher`（参考 PR 的 WSS 客户端模式） |
| **Skill 模型（SKILL.md 工作流）** | 需与 tRPC-Agent-Go `skill` 包对齐，且需判断是否纳入租户级 Skill 管理 | 待确认是否纳入范围 |
| **Approval 人审机制** | WX.md 重点强调，但任务 Prompt 未明确要求 | 待确认是否纳入（可作为治理模块的可选能力） |

---

## 四、与参考实现 PR 的关系（附：仅作思路借鉴）

参考 PR（`liuzengh/trpc-agent-service#4`）是一份**完整实现**（8586 行新增），其可借鉴的架构思路（**不可照搬，需适配 tRPC-Agent-Go 实际 API 与平台需求**）：

1. **单一二进制 + 角色标志**（Gateway / Worker / Admin），简化部署 —— 符合 ponytail 简洁原则。
2. **Inbox/Outbox 可靠性投递** + **Redis Stream XAUTOCLAIM** 实现队列认领、幂等、故障转移。
3. **WSS 客户端模式**接入企业微信/飞书，无 webhook/公网回调/隧道。
4. **不可变 RuntimeProfile / AgentVersion 发布与回滚**。
5. **租户级向量库（Qdrant）与对象存储（MinIO）隔离**。
6. **端到端 OTel 埋点**（Channel/Dispatch/Worker/Runner/Governance/Tool/Session/Reply spans）。

> 注意：参考 PR 使用 PostgreSQL + Qdrant + MinIO；本任务强制 MySQL + Redis，其余后端需经用户确认后再引入。这构成阶段 0 必须澄清的关键分歧点。

---

## 五、复用边界总表

| 能力 | WX.md 贡献 | 复用方式 |
| --- | --- | --- |
| 租户隔离 | 设计原则 | 概念采纳 |
| 分层架构 | 8 层范式 | 映射到 tRPC-Agent-Go 包 |
| Run 状态机 | 状态定义 | 平台层新增状态抽象 |
| Tool 受控入口 | 网关思想 | `FunctionTool` + `plugin` |
| Sandbox | 隔离思想 | `codeexecutor` 或自建（待定） |
| 审批/事件/产物/用量 | 机制设计 | 按需纳入平台层 |
| 存储分层 | 原则 | MySQL/Redis 抽象层 |
| 队列/Worker/幂等 | 模式 | Redis Streams |
| 安全底线 | 原则 | 全部采纳 |
| Observability | trace 贯穿 | OTel + Jaeger |

---

## 六、结论

- **高度复用** WX.md 的架构理念、对象建模、安全与可观测性范式。
- **零复用** 其具体实现（无框架绑定、无 IM 协议、PostgreSQL 等）。
- 所有落地代码以 tRPC-Agent-Go 官方 API 为准，通过 `import` 官方包复用，严禁照抄源码重构。
