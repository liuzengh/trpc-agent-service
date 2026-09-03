# 实施路线和代码组织

## 当前实施状态

截至当前代码版本，已经完成：

- 常驻 HTTP 服务、真实模型切换、优雅关闭和 readiness；
- InMemory / Redis Session；
- Local / Redis Session Coordinator、续租和 fencing token 传播；
- Local / Redis `message_id` 幂等；
- PostgreSQL 16 migration；
- tenant、agent app、revision、channel/backend binding 控制面模型；
- InMemory / PostgreSQL Control Plane Repository；
- Channel Binding → tenant/app/revision 可信路由；
- tenant-scoped Session、Coordinator 和 Idempotency key；
- Agent Revision Compiler、严格配置解析和不可变 Agent cache；
- PostgreSQL conversation/inbound/agent_run/queue_outbox 原子事务；
- `/inbound` 持久化 ACK 和重复消息唯一约束；
- PostgreSQL Outbox Relay 和 `FOR UPDATE SKIP LOCKED` claim；
- Memory / Redis Streams Queue、Consumer Group 和 pending reclaim；
- 异步 Agent Worker、稳定 request ID、run/outbound 持久化；
- Channel Adapter Registry、HTTP Test Adapter 和 Reply Sender；
- outbound claim、长度切分、Retry-After 和 provider receipt；
- `all/gateway/relay/worker/sender` 独立进程角色；
- 企业微信验签、AES callback、Token cache 和应用消息发送；
- Telegram webhook、topic Session、sendMessage 和 Retry-After；
- binding-scoped 外部身份与 Session ID 规范化；
- Redis 和 PostgreSQL Docker Compose 开发依赖。

接下来的最近里程碑是 Admin API、控制面写服务和 Revision 发布/回滚。

## 1. 建议目录

现有目录可以继续使用，但需要补齐控制面、运行面和数据面的边界。建议逐步演进为：

```text
cmd/trpc-service/
    main.go

trpcservice/
    app/                    # 进程装配、生命周期、命令行
    config/                 # 静态启动配置、环境变量
    controlplane/
        tenant/             # tenant、RBAC、quota
        agentapp/           # app、revision、rollout
        channelbinding/     # IM 账号绑定
        backendbinding/     # 后端配置和迁移状态
        repository/         # PostgreSQL repository
    gateway/
        http/               # callback、OpenAI-compatible 等入口
        routing/            # binding、identity、session 路由
        inbox/              # inbound message、outbox
    channels/
        channel.go          # 统一 Adapter 接口
        wecom/
        wechat_official/
        telegram/
    runtime/
        worker/             # 队列消费和 Runner 调用
        runnerpool/         # 共享 Runner、Agent revision cache
        coordinator/        # session lease、fencing token
        events/             # Event 聚合和 drain
        approval/           # HITL 审批
    agent/
        compiler/           # revision → Agent
        registry/           # LLMAgent/GraphAgent 等构造器
    storage/
        router/             # Session/Memory/Artifact router
        backend/            # Redis/SQL/S3/Vector builders
        scopedvector/       # 强制租户过滤
        migration/          # 双写、回填、校验
    jobs/
        summary/
        memory/
        outbox/
        cleanup/
    governance/
        plugin/             # budget、redaction、audit
        permission/         # Tool PermissionPolicy
        guardrail/
    audit/
    telemetry/
    log/
    web/

deploy/
    compose/
    kubernetes/

docs/
```

原有 `tool`、`skill` 和 `workspace` 包可以保留，但需要放到治理和租户上下文之下，不能成为全局无边界注册表。

## 2. 依赖基线

第一步建立依赖 BOM：

- tRPC-Agent-Go 主模块 `v1.11.2`；
- Session Redis/PostgreSQL/MySQL 使用同一发布系列；
- Memory Redis/PostgreSQL/MySQL 使用同一发布系列；
- S3、Qdrant、Milvus、AG-UI 按需要引入；
- OpenClaw 主要复用其接口和 Channel 设计，是否直接依赖 `openclaw/app` 需要单独兼容测试。

如果纳入当前 AG-UI、OpenClaw、Qdrant、Milvus，项目 Go 版本提升到至少 1.24.6。CI 固定 toolchain，禁止开发环境自动把依赖升级到未经验证的版本。

## 3. 里程碑 M0：服务骨架

目标是让服务成为真正的常驻进程。

交付：

- 配置文件和环境变量加载；
- HTTP Server、`/healthz`、`/readyz`；
- SIGTERM 优雅关闭；
- 结构化日志和 Secret 脱敏；
- OpenTelemetry trace/metric 初始化；
- PostgreSQL、Redis 连接和 readiness；
- 修正 `start.sh`，确认进程启动失败时不会写入误导 PID；
- 基础单测、race test 和 CI。

验收：服务持续运行，启动失败返回非零，SIGTERM 后所有 goroutine 和连接正常退出。

## 4. 里程碑 M1：单租户闭环

目标是先打通一条真实消息链路，不急于开放所有后端。

推荐组合：

```text
PostgreSQL: control plane、inbound、agent_run、outbox、audit
Redis: Session、Memory、lease、Redis Streams
MinIO: Artifact
Mock/OpenAI-compatible model
Telegram 或 HTTP 测试 Channel
```

交付：

- tenant、agent app、revision、channel binding 表；
- 一个 LLMAgent compiler；
- 共享 Runner；
- Redis Session 和 Memory；
- inbound 唯一索引和稳定 request_id；
- session lease 和 fencing token；
- Event 聚合、最终回复 outbox；
- 一个通道 Adapter；
- callback → Runner → reply 的 trace。

验收：同一消息并发投递多次只执行一次；两个 Worker 可以交替处理同一会话；杀死 Worker 后消息可以恢复。

## 5. 里程碑 M2：多租户和企业微信

交付：

- Storage Scope 编解码和 context 交叉校验；
- `TenantSessionRouter`、`TenantMemoryRouter`、`TenantArtifactRouter`；
- Agent revision cache 和按 conversation 固定 revision；
- 企业微信验签、解密、账号绑定、身份映射和异步回复；
- 租户级 ToolFilter、PermissionPolicy、预算、审计；
- Secret Manager 接入；
- tenant/user/session 限流。

验收：构造跨租户 appName、知识过滤和 Artifact ID 的攻击请求全部被拒绝；企业微信重复回调不产生重复 run。

## 6. 里程碑 M3：持久化异步任务

交付：

- summary job、memory job 和 reply job 的 durable queue；
- summary high watermark 条件更新；
- 使用 `memory/extractor.MemoryExtractor` 的独立 Job Worker；
- outbox relay、重试、死信和管理查询；
- tool execution journal；
- approval record 和 IM 确认卡片；
- Artifact SQL 版本分配和病毒扫描。

验收：Job Worker 在任意步骤退出后可重试；summary/memory 水位不回退；危险工具未经审批无法执行。

## 7. 里程碑 M4：多后端和数据迁移

交付：

- PostgreSQL/MySQL Session、Memory builders；
- Qdrant/Milvus/pgvector Knowledge builders；
- S3/MinIO Artifact；
- ScopedVectorStore；
- backend binding 状态机；
- Redis → SQL 双写、回填、校验、切换和回滚；
- vector collection 迁移和影子查询。

验收：迁移期间读写不中断，repair backlog 可观察，切读后可以一键回滚。

## 8. 里程碑 M5：生产部署

交付：

- Admin、Gateway、Channel、Worker、Jobs、Reply Sender 独立 Deployment；
- HPA、PDB、NetworkPolicy、资源限制；
- sandbox Worker pool；
- 租户灰度、回滚和 emergency policy；
- SLO dashboard 和告警；
- 备份恢复、故障注入和容量压测报告。

验收：达到目标 SLO，通过风险文档中的上线门禁和故障演练。

## 9. 测试策略

### 单元测试

- storage scope 解析和越权拒绝；
- request/session ID 生成稳定性；
- Channel 签名、解密、格式转换；
- revision 选择和 pin；
- Tool PermissionPolicy；
- Event 聚合和长度切分；
- Secret redaction。

### 接口契约测试

对所有 Session backend 运行同一套测试：

```text
Create/Get/Delete Session
Append Event + StateDelta
并发 Append
Summary watermark
TTL
分页
Close
```

Memory、Artifact 和 VectorStore 也采用相同思路，防止后端切换后语义变化。

### 集成测试

使用容器启动 Redis、PostgreSQL、MySQL、MinIO 和 Qdrant。测试 callback、队列、Runner、Tool、Session、reply 的完整链路。外部模型和 IM API 使用可控 mock server。

### 并发和故障测试

- `go test -race ./...`；
- 同一 session 高并发消息；
- lease 续期失败和 token 过期；
- Worker 在各持久化边界退出；
- Redis/SQL 超时和断连；
- Tool 未知结果状态；
- Event channel 消费者断开；
- outbox 重复发布。

### 安全测试

- 跨租户 ID、filter、object key 注入；
- SSRF、路径穿越、压缩炸弹和超大文件；
- prompt injection 请求危险工具；
- approval token 重放；
- 日志和 trace 密钥扫描；
- 管理 API RBAC。

## 10. README 验收项映射

| README 要求 | 对应文档 / 里程碑 |
| --- | --- |
| 多租户与节点部署 | `architecture.md`，M1/M2 |
| 跨节点 Session 路由 | `architecture.md`、`data-consistency.md` |
| 多后端和迁移 | `backend-adapters.md`、M4 |
| 至少两类 IM | `im-channels.md`、M1/M2 |
| 完整消息时序 | `sequence.md` |
| 数据模型 | `data-model.md` |
| 治理、监控和安全 | `governance-operations.md`、M2/M5 |
| 故障恢复和 Go 生命周期 | `sequence.md`、`governance-operations.md` |
| 最小与生产部署 | `governance-operations.md`、M0/M5 |
| 至少 8 项风险 | `risks.md` |
| GitHub 实现代码 | M0 至 M5 的代码交付 |

## 11. 建议的第一个实现 PR

第一个 PR 不引入模型和 IM，先完成：

1. Go 版本和依赖 BOM；
2. 常驻 HTTP 服务与优雅关闭；
3. PostgreSQL/Redis 配置和健康检查；
4. tenant、agent app、revision、channel binding 最小模型；
5. 日志脱敏和 OTel 初始化；
6. CI、单测和 race test。

第二个 PR 再加入共享 Runner、Redis Session、稳定 request ID 和 HTTP 测试 Channel。这样每个变更都能独立验证，不会把协议、分布式一致性和 Agent 行为一次性揉在一起。
