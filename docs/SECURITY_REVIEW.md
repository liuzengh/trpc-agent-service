# 安全架构与审计设计

范围：Go 源码、migrations、Compose/Kubernetes 模板和发布校验。本文定义安全控制与信任边界，验证记录见[验收证据](ACCEPTANCE_EVIDENCE.md)。

## 1. 威胁模型与信任边界

不可信输入包括 IM webhook、正文/附件 URL、Tool 参数、模型输出、租户管理员配置和第三方 Provider 响应。信任边界分为公网 Gateway、内部 Consumer/Worker、Admin、PostgreSQL/Redis、Qdrant/S3 和 Telemetry 出口。主要威胁是跨租户读取、伪造请求、重复副作用、提权、秘密泄露、SSRF、队列/预算耗尽和供应链替换。

## 2. 安全控制

| 领域 | 控制 | 证据入口 |
|---|---|---|
| 回调认证 | Telegram secret header；WeCom SHA1、AES-CBC、CorpID+AppID/AgentID 绑定和 URL 验证 | `pkg/channel`、Gateway 测试 |
| 内部认证 | Consumer→Worker HMAC-SHA256 绑定 method/path/body/traceparent；Redis nonce 防重放 | `pkg/auth/service.go`、`pkg/auth/service_test.go` |
| 管理授权 | bearer→Principal；role/action/tenant 白名单；不信任 `X-Admin-Actor` | `pkg/adminauth`、`cmd/admin` |
| 租户作用域 | SQL 复合键/查询带 tenant；scoped reader；能力缺失时拒绝访问 | `pkg/tenant`、`pkg/reliable`、`pkg/controlplane` |
| 知识隔离 | Qdrant 物理 ID=`SHA256(tenant\0app\0logicalID)`；保留 metadata 不可覆盖 | `pkg/knowledgeplane` |
| 文件隔离与完整性 | tenant-scoped object key；不可变版本；最大 16 MiB；MIME/标识校验；加载时验证 SHA-256 | `pkg/artifactplane` |
| 密钥处理 | Knowledge/Artifact/MCP profile 只存 env 名，凭据仅 Worker 解析；拒绝 JSON 未知字段/原始秘密；秘密字段私有、不可序列化 | `pkg/runtimeplane`、`pkg/releaseverify` |
| 工具授权 | Runner BeforeTool 统一授权；tenant+version 双白名单；危险工具持久审批一次性消费 | `pkg/governance/plugin.go`、`pkg/governance/approval_postgres.go` |
| MCP 边界 | 运维管理 HTTPS profile；精确远端 Tool 白名单；仅 Worker 接收 Header SecretRef；禁用 stdio/危险 Header | `pkg/platformtool/mcp.go`、MCP 纵向集成测试 |
| 成本控制 | Redis Lua 原子预留、dispatch 和结算；未知 usage 保守计费 | `pkg/governance/budget.go`、`pkg/summaryruntime/budget_model.go` |
| 幂等与 fence | Inbox/Outbox unique+hash；owner/lease_version/expiry；结果未知进入 reconciliation | `pkg/reliable`、`pkg/controlplane/session_fence.go`、`pkg/resultcache/postgres.go` |
| 输出安全 | 工具结果和最终模型输出递归脱敏；错误稳定分类；用户 ID 使用租户 HMAC 假名 | `pkg/governance`、`pkg/telemetry` |
| 部署防护 | non-root、read-only rootfs、drop ALL、no-new-privileges、seccomp、default-deny、digest 门禁 | `deploy`、`pkg/releaseverify` |

## 3. 密钥和配置隔离

租户模型/API/IM 密钥使用 AES-GCM envelope 或运维管理的 `env://TRPC_SECRET_*`。读 API 返回遮盖值；更新时，`***REDACTED***` 只有匹配既有 identity 才表示保持不变。Session/Memory 只引用运维 profile，禁止租户写 DSN/URL。Admin 仅获取 Knowledge/Artifact/MCP 的公开 profile；数据面/MCP Header Secret 仅给 Worker，模型 Key 仅给 Worker/Summary Worker，Channel Secret 仅给 Gateway/Delivery；Consumer/Admin 不接收这些秘密。

Kubernetes `runtime-data-plane-profiles` 中的 `profiles.json`、`mcp-profiles.json` 只存无秘密声明；`runtime-data-plane-credentials` 的四个 key 仅可在 Worker 主容器各引用一次。releaseverify 拒绝非 Worker、sidecar 或重复暴露，并回归检查 MCP、模型和 Channel Secret 的进程范围。内置 resolver 读取受限环境变量；生产通过 workload identity 接入 KMS/Vault，按[密钥系统验收](EXTERNAL_ACCEPTANCE_RUNBOOK.md#7-kmsvault-生产验收)配置与验证。

配置更新显式选择凭据来源：提交新的 SecretRef 时清除该字段的 inline 值，提交 inline 值时清除旧引用；同一请求同时提交两种来源则拒绝。遮罩回传只保留同一 provider/model 或 Channel account 的既有凭据。企业微信 AES Key 的两个字段别名采用一致规则，避免来源切换时回填旧密钥。

## 4. 数据面安全

Qdrant profile 校验 endpoint、TLS、dimension、collection、embedding endpoint 和 tenant allowlist；HTTP embedding 须显式启用本地 `allowInsecure` 且 host 为 loopback。S3 profile 校验 endpoint、bucket、region、object 大小和 credential refs，生产要求 TLS。Artifact 文件名拒绝路径分隔符、控制字符和格式字符，object key 各段使用 base64url；advisory lock 使用固定 digest，避免用户文本进入锁/活动查询。

Knowledge/Artifact 投影在副作用前后检查 lease fence，过期执行者不得写完成标记，见[同步策略](DATA_SYNC_IDEMPOTENCY.md#6-各后端的同步策略)。Artifact 同版本异内容拒绝，同内容重放复用已保存 key 并修复正文；新写入使用独立对象标识。删除先提交 tombstone，清理失败可重试；提交未知时保留正文，清理前在作用域锁内核对元数据引用。迁移恢复顺序见[持久化写入协议](ONLINE_MIGRATION.md#持久化写入协议)。

## 5. 执行与网络安全

Gateway 在 durable commit 后确认回调。Consumer→Worker 除 HMAC 外，生产还须使用 HTTPS，或显式启用已验收严格 peer authentication 的 mesh 模式。模型/Tool 调用设置 deadline；Consumer 与 IM Adapter 仅将 DNS/连接建立阶段的传输失败视为可重试。`GotConn` 后按可能已发送处理；`WroteRequest` 缺失或晚到不能证明未执行，未知结果进入 reconciliation，判定见[执行恢复协议](DATA_SYNC_IDEMPOTENCY.md#8-执行结果与恢复判定)。危险 Tool 批准绑定 tenant、actor、owner、session、tool、canonical args 和 invocation；raw approval token 不进入 Inbox、Session 或 HTTP 响应。

Worker 只向已安装模型 Provider 传递经验证的附件引用，不下载正文；生产拒绝 userinfo、fragment、localhost 和字面量私网/链路本地地址。运维管理出网地址与凭据，以域名/IP allowlist、DNS 和重定向策略约束实际连接；各 MCP profile 的目标系统认证、权限和配额独立验收。

## 6. 可观测与审计

公网回调创建可信 root span，不直接采用外部 trace header 为父；内部 traceparent 纳入 HMAC。日志/trace 禁止记录 webhook token、模型 key、数据库密码、Authorization、完整 payload 或原始用户标识。执行审计包含 tenant/channel/pseudonymous user/session/agent version/tool/decision/latency/error/cost/trace，与控制面变更、审批、DLQ replay 分离保存。

`auditLevel=basic` 保留执行结果、工具和安全决定审计；`detailed` 额外在 Runner 调用前持久化 `execution_admitted`，两种级别均不记录提示词正文。数据库审计与 stderr 镜像独立写入，数据库写失败不得确认执行成功；执行前失败可重试，执行后失败进入结果核对。故障指标与处置见[审计写入故障](SLO.md#audit-sink-failure)。

生产 `/metrics` 需要 bearer，仅 loopback Compose 可启用 unauthenticated。tenant/model/agent 标签按[SLO 标签限制](SLO.md#服务目标)有界聚合。OTLP 必须启用 TLS、服务身份和 attribute processor 二次清洗。

## 7. 供应链和运行时

生产 Dockerfile 固定 Go 1.26.7 基础镜像 digest，最终镜像无 shell 依赖并按上述最小权限运行。Kubernetes releaseverify 拒绝 tag-only image、缺失 workload、错误 Worker transport、profile/secret 泄露和未受控 egress。生产发布配置构建 provenance、SBOM、镜像签名、准入验证、依赖漏洞扫描、分支保护和双人批准。

## 8. 安全运行与验收

真实 IM、密钥系统、传输身份、云数据面、HA/DR 和业务 Tool/MCP 按[目标环境验收手册](EXTERNAL_ACCEPTANCE_RUNBOOK.md)配置、验证并留存证据；失效信号、恢复措施和责任见[风险登记表](RISK_REGISTER.md)。
