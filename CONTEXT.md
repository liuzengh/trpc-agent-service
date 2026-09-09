# Agent Service Context

本项目提供基于 tRPC-Agent-Go 的多租户、节点化 Agent 平台。本文统一平台领域中的核心名词，避免平台对象与上游 Agent 运行时能力混用。

## Tenant And Application

**Tenant**:
平台中配置、权限、运行数据和审计边界的租户。
_Avoid_: Account, Customer

**Agent App**:
租户注册、配置、发布并可被请求路由到的 Agent 应用。
_Avoid_: Agent, Bot, Worker

**Deployment**:
Agent App 的一条独立发布记录，关联选定的 Deployment Version 及其生命周期状态。
_Avoid_: Instance, Node

**Active Deployment**:
同一 Tenant 和 Agent App 范围内唯一可接收新路由请求的 Deployment。
_Avoid_: Primary Deployment, Default Deployment

## Runtime And Routing

**Gateway**:
接收外部请求，并负责租户、Agent App、Channel Binding 和 Session 路由的平台入口。
_Avoid_: Runner

**Worker**:
执行 Agent App 的运行进程或实例。
_Avoid_: Agent App, Node

**Node**:
承载 Gateway、Worker 或 Channel Adapter 等运行单元的可调度平台运行单元。
_Avoid_: Worker, Host

**Control Plane Store**:
保存 Tenant、Agent App、Deployment、Deployment Version、Backend Selection、Channel 路由和 Governance Policy 的权威共享状态；多节点部署使用 PostgreSQL，SQLite 仅用于单节点开发，InMemory 仅用于测试。
_Avoid_: MemoryPlatform, Local Control File, Session Store

## Conversation And Data

**Channel Binding**:
租户和 Agent App 与某个外部 IM 账号、回调入口或通道配置之间的绑定关系。
_Avoid_: Channel, IM Account

**Session**:
某个租户用户在特定会话范围内形成的连续交互上下文。
_Avoid_: Conversation, Chat

**Session Event**:
按会话顺序记录的消息、状态变化或 Agent 执行事件。
_Avoid_: Log, Message

**Projection Checkpoint**:
记录 Session State 或 Summary 已连续投影到的最高 Session Event sequence；它不能跳过尚未处理的事件。
_Avoid_: Migration Checkpoint, Session Sequence, Snapshot

**Session Execution Lease**:
允许一个 Gateway 在限定时间内独占执行某个 Tenant Session 的可续租资格；每次授予都产生新的 fencing token，租约丢失必须取消对应执行。
_Avoid_: Sticky Session, Process Lock, Session Ownership

**Fencing Token**:
Session Execution Lease 每次授予时递增的序号，Storage Adapter 用它拒绝失去租约的旧 Gateway 继续写入 Session Event。
_Avoid_: Sequence, Lock ID, Request ID

**Memory**:
可跨 Session 检索的长期信息。
_Avoid_: Session History, Context

**Summary**:
对 Session 历史进行压缩后的上下文表示。
_Avoid_: Memory, Snapshot

**Artifact**:
Agent 执行产生或消费的文件型内容及其租户归属、版本和存储引用；大对象内容与控制面元数据分离保存。
_Avoid_: Knowledge, Session Event, Attachment Metadata

**Knowledge**:
Agent App 可检索的租户隔离知识集合，包括文档来源、切片及其向量索引引用。
_Avoid_: Memory, Artifact, Prompt

**Failure Event**:
描述 Agent 执行失败的 Session Event，例如 `run.failed`，必须可被管理界面检查。
_Avoid_: Application Error, Audit Event

**Migration Job**:
在源 Storage Adapter 与目标 Storage Adapter 之间复制 Session Event 和 Memory，并记录进度与校验结果的受控操作。
_Avoid_: Data Copy, Offline Script

**Migration Checkpoint**:
标记 Migration Job 已完成位置、用于中断后恢复的持久化进度。
_Avoid_: In-Memory Progress, Cursor

**Audit Event**:
用于安全、合规和运营追踪的审计记录。
_Avoid_: Application Log, Trace

**Tool Outcome**:
Tool 副作用执行结果的治理状态；当 Tool 已开始但结果无法持久化时标记为 outcome_unknown，禁止把它等同于未执行并自动重放。
_Avoid_: Runner Error, Delivery Status

**Execution Record**:
控制面中一条 Agent 执行的权威状态记录，绑定 request、trace、Session Lease、Deployment Version、Governance Policy revision、预算预留和最终结果。
_Avoid_: Session Event, Platform Trace, Runner Invocation

## Delivery And Integration

**Phase**:
一个可独立开发、测试并移交的纵向能力切片，具有明确范围、验收门槛和下一阶段输入。
_Avoid_: Milestone, Sprint

**Tenant Context**:
由受信任入口解析并注入请求的租户身份上下文，业务服务据此执行资源隔离。
_Avoid_: tenant_id request field, User Context

**Runner Adapter**:
平台 Worker 调用 Agent runtime 的稳定端口及其具体实现之间的适配边界。
_Avoid_: Worker, Agent App

**Execution Manifest**:
Gateway 为一次执行签发的不可变、可验证声明，绑定 Tenant、Agent App、Deployment Version、Governance Policy revision、请求身份和 Worker 必须执行的治理规则。
_Avoid_: Runner Request, Deployment Configuration, Client Policy

**Framework Runtime**:
由 `trpc-agent-go` 提供的 Agent 执行能力，包括 Agent、Runner、模型消息和运行事件；平台通过 Runner Adapter 使用它，不直接把其内部类型暴露给外部接口。
_Avoid_: Worker, Platform Runtime

**AgentFactory**:
根据已发布 Deployment Version 构建租户 Agent 运行实例的稳定平台能力；它决定 Agent 配置如何进入 Framework Runtime。
_Avoid_: Runner, Agent Registry

**Model Provider Profile**:
服务端拥有的模型提供方配置，保存 OpenAI-compatible endpoint、密钥引用和允许的模型范围；Deployment Version 只能引用它，不能保存或提交模型凭据。
_Avoid_: Model Configuration, Deployment Secret, Client Provider

**Channel Adapter**:
将外部 IM 入站/出站消息转换为平台消息和 Runner Event 的通道集成边界。
_Avoid_: Channel Binding, Webhook Handler

**Storage Adapter**:
平台对后端存储实现的统称；Session、Memory、Artifact、Knowledge 和 Audit Event 使用各自独立的存储端口，并由 Tenant 的 Backend Selection 组合路由。
_Avoid_: Database Driver, Repository (as a backend choice)

**Backend Selection**:
Tenant 选择的、由服务端配置并授权的 Storage Adapter，用于该 Tenant 的数据访问。
_Avoid_: Client-Selected Backend, Database Preference

**Public Error Contract**:
面向 API 消费者的稳定错误码与脱敏消息，不包含后端地址、路径、凭据或驱动细节。
_Avoid_: Raw Driver Error, Internal Diagnostic

**Public API Contract**:
Management Console 和外部调用方依赖的 HTTP 路径、请求响应语义、SSE envelope 与稳定错误码；内部 Go 接口、数据库结构和 Gateway/Worker 协议不属于该兼容边界。
_Avoid_: Internal Worker Protocol, Database Schema

**Deployment Version**:
Agent App 一次可发布、可路由并可回滚的配置版本。
_Avoid_: Node, Worker Instance

**Mock IM**:
实现标准 Channel Adapter 契约并可注入重复、乱序、超时、限流和重试故障的测试通道。
_Avoid_: Fake Channel (when it omits failure semantics)

**Management Console**:
平台管理员、租户管理员和运维人员管理平台资源、策略与运行状态的渐进式 Web 应用。
_Avoid_: Admin API, Chat Client

**Chat Workspace**:
Management Console 中通过 Web UI 创建或恢复 Session、发送消息并查看流式 Agent 回复的交互区域，也是 Mock IM 的本地可视化验证入口。
_Avoid_: Real IM, Channel Adapter

**Development Identity**:
仅用于开发和自动化验收、由服务端校验并建立 Tenant Context 的非生产身份。
_Avoid_: tenant_id request field, Production Identity

**Real IM Provider**:
通过真实平台协议实现 Channel Adapter 的外部消息提供方；本项目要求的两个实现为 Enterprise WeChat 和 Telegram。
_Avoid_: Mock IM, Chat Workspace

**IM Simulator**:
Management Console 中用于模拟外部 IM 入站消息、Session 路由和回复投递的本地验证工具；它验证 Channel Adapter 流程，但不属于 Real IM Provider。
_Avoid_: Real IM, Chat Workspace

**Bot Tenant Allowlist**:
将一个真实 IM Bot 的外部用户或群组标识绑定到一个或多个 Tenant 的受控映射，用于多租户 Bot 路由。
_Avoid_: Tenant Context, User Identity

**Provider Account**:
平台级外部 IM 账号，例如一个可服务多个 Tenant 的 Telegram Bot 或 WeCom Smart Bot；它拥有 provider 身份和连接状态，但不归属于任一 Tenant。
_Avoid_: Channel Binding, Agent App

**External IM Subject**:
Provider Account 中可被路由的外部会话或用户标识；Telegram 群聊使用 `chat_id`，私聊优先使用 `chat_id` 并允许 `user_id` 兜底。
_Avoid_: Tenant User, Session

**Delivery Status**:
Channel Adapter 对一次入站、路由或出站处理的稳定结果，例如 delivered、unmapped、duplicate 或 failed。
_Avoid_: Session Event, Audit Event

**WeCom Smart Bot**:
企业微信 API 模式的智能机器人，使用 BotID 和长连接 Secret 通过 WebSocket 接收 `aibot_msg_callback` 并发送 `aibot_respond_msg`；它不同于传统企业微信应用回调。
_Avoid_: WeCom App, WeCom Webhook App

**Provider Transport**:
Real IM Provider 与平台之间的消息传输方式；本项目的 Telegram 传输为 long polling，WeCom Smart Bot 传输为 WebSocket 长连接。两者都不是传统企微自建应用 webhook。
_Avoid_: Channel Adapter, Delivery Status

**WeCom App**:
传统企业微信自建应用及其 CorpID、AgentID、应用 Secret 和 Access Token 体系；本项目 Stage 4 不使用该接入类型。
_Avoid_: WeCom Smart Bot, Provider Account

**Bot Credential**:
平台级 Provider Account 的进程启动凭据；本阶段包括 `TRPC_TELEGRAM_BOT_USERNAME`、`TRPC_TELEGRAM_BOT_TOKEN`、`TRPC_WECOM_BOT_ID` 和 `TRPC_WECOM_BOT_SECRET`，只从服务端环境读取，不进入租户绑定或浏览器状态。
_Avoid_: Tenant Secret, Browser Credential

## Governance And Observability

**Production Identity**:
生产模式中由 Identity Provider 验证、并与服务端 Tenant/Role 分配绑定的用户身份；原始身份令牌不作为浏览器持久状态。
_Avoid_: Development Identity, Tenant Context

**Governance Policy**:
按 Tenant 和 Agent App 隔离、带 revision 的服务端执行策略，定义 Tool/MCP、Guardrail、IM 权限、预算、限流和脱敏规则。
_Avoid_: Deployment Configuration, Client Policy

**Tool Confirmation**:
危险 Tool 在产生副作用前创建的 Tenant 隔离审批记录；决定与执行通过 request ID 保持幂等。
_Avoid_: Approval Workflow, In-Memory Waiter

**Platform Trace**:
由平台定义的、Tenant 隔离且可按 request ID 或 trace ID 检索的有序执行路径；不暴露上游 runtime telemetry 类型。
_Avoid_: Audit Event, Application Log

**Governance Center**:
承载 Governance Policy、Audit Event、Tool Confirmation、预算/限流计数和 Platform Trace 的服务端边界。
_Avoid_: Framework Runtime, Management Console

**Governance API**:
供 Worker 在 Tool 执行边界持久化治理决定、Tool Confirmation 和执行结果的内部服务接口；它不属于 Public API Contract。
_Avoid_: Admin API, Governance Policy, Tool Callback

**Control Plane Outbox**:
与控制面状态变更在同一 PostgreSQL 事务内写入的待发布记录，用于可靠导出 Audit Event、指标或 Trace。
_Avoid_: Application Log, Best-Effort Callback
