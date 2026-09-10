# 系统架构

本文只描述当前代码和部署文件中存在的组件。`Storage Adapter`、`Governance`、`Telemetry` 等是进程内模块或外部依赖，不额外虚构独立服务。

## 架构图

```mermaid
flowchart LR
  classDef ext fill:#fff7ed,stroke:#f59e0b,color:#7c2d12
  classDef channel fill:#eff6ff,stroke:#60a5fa,color:#1e3a8a
  classDef control fill:#ecfdf5,stroke:#34d399,color:#065f46
  classDef queue fill:#f5f3ff,stroke:#a78bfa,color:#5b21b6
  classDef worker fill:#f0fdfa,stroke:#2dd4bf,color:#115e59
  classDef data fill:#f8fafc,stroke:#94a3b8,color:#334155
  classDef gov fill:#fff1f2,stroke:#fb7185,color:#9f1239

  subgraph EXT["External Systems"]
    IMU["IM 用户"]
    IMP["WeCom / Feishu 平台"]
    CLIENT["OpenAI-compatible Client"]
    OP["Operator / Admin UI"]
  end

  subgraph CH["Channel Adapter Deployment (replicas=1; Recreate)"]
    CI["WeCom + Feishu Adapters / Normalize + Identity"]
  end

  subgraph CP["Gateway Deployment (replicas>=2; HPA)"]
    API["Gateway HTTP Ingress"]
    ADMIT["gateway.Gateway<br/>Admission"]
    ADMIN["Admin API"]
    PG[("PostgreSQL Authority")]
  end

  subgraph QC["Queue / Coordination"]
    DS["Dispatch Outbox + Redis Stream"]
    EXLEASE["Execution Lease<br/>PostgreSQL owner / run token"]
    SLEASE["Session Lease<br/>Redis TTL / token"]
    SLOCK["Session Lock<br/>per-session serialization"]
    RLIMIT["Reply Rate Limiter<br/>Redis per binding"]
  end

  subgraph WR["Worker / Runtime"]
    RT["Worker Consumer + tRPC-Agent-Go Runtime"]
  end

  subgraph CHR["Channel Reply Runtime (single owner)"]
    RES["Reply Sender + Channel Outbound Resolver"]
  end

  subgraph DATA["Storage / External Provider Backends"]
    SES[("Session / Event / Summary<br/>PostgreSQL / Redis / InMemory")]
    MEM[("Memory scope / entries<br/>TencentDB Memory")]
    KNO[("Qdrant Knowledge")]
    ART[("COS Artifact")]
    MODEL["OpenAI-compatible Model"]
  end

  subgraph GOV["Governance / Observability"]
    TOOL["ToolCatalog + Approval + Audit"]
    OBS["OTel / Collector / Jaeger / Prometheus / Grafana"]
  end

  IMU --> IMP
  IMP --> CI
  CI -->|ChannelInput| ADMIT
  CLIENT --> API --> ADMIT
  OP --> ADMIN --> PG
  ADMIT --> PG --> DS --> RT
  PG -. reply_outbox .-> RES --> IMP
  PG -. owner / run token .-> EXLEASE
  EXLEASE -. claim / fence .-> RT
  SLEASE -. TTL / renewal .-> SLOCK
  SLOCK -. serialize same session .-> RT
  RES -. acquire before SendOnce .-> RLIMIT
  RT -. pinned ConfigVersion .-> SES
  RT -. scoped access .-> MEM
  RT -. scoped access .-> KNO
  RT -. scoped access .-> ART
  RT -. model call .-> MODEL
  RT -. authorization .-> TOOL
  API -. trace / metrics .-> OBS
  ADMIN -. audit / metrics .-> OBS
  RT -. trace / metrics .-> OBS
  RES -. trace / metrics .-> OBS
  class IMU,IMP,CLIENT,OP ext
  class CI channel
  class API,ADMIN,PG control
  class DS,EXLEASE,SLEASE,SLOCK,RLIMIT queue
  class RT worker
  class RES channel
  class SES,MEM,KNO,ART,MODEL data
  class TOOL,OBS gov
```

源文件：[system-architecture.mmd](diagrams/system-architecture.mmd)，查看版：[system-architecture.svg](diagrams/system-architecture.svg)。

## 部署边界和组件关系

| 层 | 真实组件 | 主要职责 | 是否持有业务状态 | 权威数据 | 水平扩展与故障行为 |
| --- | --- | --- | --- | --- | --- |
| 外部系统 | WeCom Bot、Feishu/Lark、OpenAI-compatible model、TencentDB Memory、Qdrant、COS | IM 输入/输出、模型、记忆、向量、对象 | 各自持有 Provider 数据 | 对应 Provider；但租户授权仍由平台 SQL 控制 | Provider 连接/限流/错误由 adapter/resolver 处理；真实生产行为需结合部署环境验证 |
| Channel | `channel` 角色中的 `wecom.Adapter`、`feishu.Adapter`、attachments ingestor、Reply Sender、outbound resolver | 官方 WebSocket/long connection、规范化、binding revision、身份映射、媒体 staging、异步回复；通过同一 `gateway.Gateway` Admission 入队 | 进程内连接/客户端句柄；持久映射在 SQL | `channel_binding`、identity/conversation/inbox；发送状态在 `reply_outbox` | 单副本 `Deployment` + `Recreate`；不挂 Service/HPA，避免多个进程枚举同一 active binding；Reply Sender 与 outbound resolver 也只由该 owner 运行 |
| Control Plane | `admin` HTTP、`auth`、`config`、`postgres.Store` | Admin API、租户/app/config/credential/binding 查询和变更、迁移命令 | 少量缓存无权威意义 | PostgreSQL `platform.*` | API 可多副本；PostgreSQL 不可用时 readiness/admission 失败；Admin API 通过 `Admin UI → Admin API → PostgreSQL` 访问控制面 |
| Gateway | OpenAI Ingress、`gateway.Gateway`、Queued Runner、Admission、dispatch relay | HTTP 认证、配置 pin、原子入队、durable event projection、outbox relay；不启动 IM 长连接 | 不持有 Runner/Session 内容 | PostgreSQL execution/event | HTTP 可横向扩展；请求可以到任意 Gateway，不依赖 sticky |
| Queue / Coordination | Redis Stream Consumer Group、dispatch relay、Execution Lease、Session Lease、Session Lock、Reply Rate Limiter | durable outbox 后的传输、execution owner/fence、Session 串行和 IM pacing | Execution Lease 的 owner/run token 在 PostgreSQL；Session Lease/Lock 与限流 token 在 Redis；都不是 Session 内容 authority | execution 状态在 PostgreSQL；Session 内容在配置的 Session Backend；reply 状态在 PostgreSQL outbox | Redis 故障时 relay/consumer/backoff、Session lease 和 limiter 暂停；pending 可 XAUTOCLAIM；PostgreSQL execution lease 仍是最终 claim/fence 依据 |
| Worker / Runtime | Consumer、Runtime、framework Runner/LLMAgent、ToolCatalog、Migration/Cleanup | claim/fence、模型/工具/数据访问、持久事件、迁移清理 | 运行中 goroutine；无业务权威状态 | PostgreSQL execution/event/outbox + 外部领域后端 | Worker 可加副本；claim/session lease/heartbeat 支持故障接管 |
| Storage | PostgreSQL、framework Session Redis/PostgreSQL/InMemory、Qdrant、COS、TencentDB resolver | 控制面、Session、Knowledge、Artifact、Memory | PostgreSQL 与 framework/Provider 各持领域状态 | 见[后端适配](backend-adaptation.md)；运行时由 resolver 按 ConfigVersion 选择 provider | framework/provider 故障按各自适配语义处理；最终 authority 与迁移边界见[后端适配](backend-adaptation.md) |
| Governance | ToolPolicy、Budget callback、Approval、audit | 工具可见/可执行、预算、二次确认、元数据审计 | Approval/audit 在 PostgreSQL | `tool_approval`、`audit_event` | 未知工具 fail closed；audit 是 best-effort，失败有指标，不写 raw payload |
| Observability | OTel SDK/Collector、Jaeger、Prometheus、Grafana、Operations API | trace、低基数 metrics、dashboard、运行摘要 | exporter/时序数据在外部 | 不是业务状态 authority | exporter 不可用回退 noop；不能阻断核心执行 |

## 关键边界

1. Gateway 的 Admission 是系统入口的原子边界；`execution` 插入成功前没有可消费的业务请求。控制面变更走 `Admin UI → Admin API → PostgreSQL`，不绕过 Admin API 直接写库。
2. Redis Stream 只承载 `Dispatch{tenant_id, app_id, request_id, traceparent/tracestate}`；Worker 必须回 PostgreSQL 读取和 claim 精确 execution。
3. Runner 事件先写 `execution_event`，Queued Runner 从 durable event source 读取；HTTP stream 断开不会删除执行。
4. `reply_outbox` 把模型执行成功与 IM Provider 发送解耦；Provider receipt 不确定时保存 `UNCERTAIN`，不猜测发送结果。
5. Execution Lease 是 PostgreSQL execution row 的 owner/run token/fence；Session Lease 是 Redis TTL/token；Session Lock 是基于该 Session Lease 的串行化句柄；Reply Rate Limiter 只负责 binding 级发送节奏，四者不共享同一语义。
6. Admin API 使用独立 bearer token 和 role/tenant allowlist；它不接受 data-plane API key 作为控制面权限。

## 运行角色

- `gateway`：HTTP ingress、Admin API、`gateway.Gateway` Admission、dispatch relay；可多副本 + HPA，不拥有 IM 长连接。
- `channel`：WeCom/Feishu 官方 WebSocket adapters、channel ingress、Reply Sender 和 outbound resolver；固定单副本 owner，健康检查只服务 Pod，不承接外部 HTTP 流量。
- `worker`：HTTP health/readiness、consumer、Runner、migration、artifact cleanup、heartbeat；不创建 IM outbound client。
- `all`：本地或单进程组合角色；只能作为单一 Channel owner 使用，生产不应多副本运行。

Kubernetes 当前部署 Gateway、Channel、Worker 三类 Deployment；只有 Gateway/Worker 有 Service/HPA，Channel 用单副本 `Recreate` 保证 ownership。Admin UI 只在 Compose 中作为 Nginx 容器提供。所有角色启动时连接并校验 PostgreSQL/Redis，进程运行 migrations；Worker 另依赖 Qdrant（当配置对应 Backend 时还需 COS/TencentDB endpoint）。
