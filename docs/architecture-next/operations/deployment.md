# 部署目录结构

- **设计状态**：已接受
- **实现状态**：Control API / Gateway / PostgreSQL / NATS 的 Compose 基线、local 与
  fixture overlay 已实现；Control 多副本 Platform Contract 固定 Digest 与只读 CLI 已实现。
  Gateway 当前默认 Control 来源已接账户/凭据、Telegram 注册/入站、Runner 与 observations，
  共有 10 个迁移（0001–0010）；真实 Telegram 入站已验收。ReplyIntent Consumer、Worker、
  完整回复与可观测性仍待交付；Helm 始终属于 FINAL-INTEGRATION。
- **确认日期**：2026-08-31；当前实现与两侧集成口径于 2026-09-06 复核
- **适用范围**：本地 Compose、NATS 基础设施、可观测性配置和 Kubernetes Helm 部署

本文定义仓库级部署资产的唯一组织方式。它只描述部署与运维资产的所有权，不改变
`ARC-001` 中每个生产 Workload 独立二进制、独立镜像的约束。

本文的部署环境标签与 Compose overlay 表示平台运维资产，不是 Deployment V1 的
Environment 业务对象、用户发布输入或运行配置深层合并；业务设计见
[`Deployment V1`](../control-api/deployment.md)。

## 1. 已接受的目录

```text
deploy/
├── compose/
│   ├── compose.yaml
│   ├── compose.local.yaml
│   ├── compose.gateway-fixture.yaml
│   └── compose.observability.yaml      # 后续可观测性 overlay
├── nats/
│   ├── streams.yaml
│   └── permissions.yaml
├── observability/
│   └── ...
└── helm/                              # FINAL-INTEGRATION；当前不落地 Chart
    └── agent-platform/
```

该结构是后续新增部署文件的规范性入口。禁止同时建立第二套平级 `docker/`、`k8s/`、
`charts/` 或按个人习惯分散的部署目录。

## 2. 目录职责

### 2.1 `deploy/compose/`

Compose 文件采用“稳定基线 + 显式 overlay”的组合方式：

- `compose.yaml` 定义单机自托管环境的完整基线拓扑，包含 PostgreSQL、NATS 和已经
  纳入发行的生产 Workload。它只引用由 CI 固定的 `image@sha256:<digest>` 或不可变
  Release Tag，不声明 `build:`，并统一定义内部网络、命名 Volume、健康检查和 Secret
  挂载。它不挂载源码，也不向宿主机暴露 PostgreSQL、NATS 等内部服务端口。
- `compose.local.yaml` 是本地开发 overlay，只增加 `build:`、回环端口、调试变量和
  本地配置覆盖。当前 Control API 直接使用环境变量，不要求 Secret 文件；它不能改变
  Workload 职责、事件语义或运行拓扑。
- `compose.gateway-fixture.yaml` 是显式开发账户来源 overlay，替代 Control 来源，
  不与生产 Control 账户/凭据输入混用。
- `compose.observability.yaml` 是后续可选的可观测性 overlay，增加 Collector、Prometheus、
  Tempo、Loki、Grafana 等组件；未启用时不影响业务服务运行。

固定组合顺序为：单机自托管只加载基线；本地开发加载基线后再加载 local overlay；
启用可观测性时始终把 observability overlay 放在最后。根 `justfile` 封装这些组合命令，
标准组合由根入口维护；fixture 开发验证显式追加对应 overlay，具体命令见 Compose README。

上述不可变镜像引用是**发布要求**，不代表当前已经实施发布门禁。当前 `compose.yaml` 仍默认使用 `:local`
的 Control/Gateway 镜像（reconcile 复用 Gateway 镜像）；本地第一切片可由 overlay 构建使用。
自托管发布须显式提供由 CI 固定的 digest/不可变 Release Tag，并核验实际镜像；
不可变发布引用校验仍待交付。本地镜像启动证据与生产发布验收分开记录。

Compose 用于本地开发、集成测试和单机验收；它不承担跨节点调度和高可用承诺。

### 2.2 `deploy/nats/`

- `streams.yaml` 声明 JetStream Stream、Subject、Retention、Storage 和复制要求。
- `permissions.yaml` 声明各 Workload 的 Publish、Subscribe 与管理权限边界。
- 文件只保存声明式配置，不保存 NATS Credential、Token 或运行时状态。
- 不兼容的 Stream 变更必须由显式迁移处理，禁止启动时静默删除并重建。

NATS Server 不直接读取这两份项目 YAML；**拓扑与身份权限采用两条不同的应用路径**：

```text
streams.yaml     → topology reconcile → JetStream 管理 API → Stream / Durable Consumer
permissions.yaml → auth config render → NATS Server 配置校验 → 部署加载 / 受控 reload
```

`streams.yaml` 由唯一版本化 reconciliation 命令校验并幂等应用；不兼容的 Stream/Consumer
变更应明确失败并要求迁移。`permissions.yaml` 在首版文件式认证方案下转换成 NATS Server
原生授权配置，真实凭据从部署环境注入，不进入项目 YAML 或渲染审计输出。授权由 Server
执行，不是创建 Stream 就会自动生效的属性。[NATS 官方授权说明](https://docs.nats.io/learn/security/authorization)

部署流程负责配置校验、挂载及启动/受控 reload；若未来采用 JWT/operator，需另设签发与
分发路径，不伪装成 JetStream reconciliation。Compose 与最终 Helm 复用相同的两条路径和
版本化工具，工具可共用构建产物，不因此新增常驻 Workload。

Runtime 使用最小权限，只消费/发布本方事件及必要的查询、ACK、reply inbox subject；
拓扑管理凭据只交给一次性作业，Server 授权配置的更新权限只交给部署流程。
验收要分别检查拓扑一致性和真实连接的允许/拒绝行为：包括合法 PubAck/ACK、错凭据、越权
发布和拓扑修改被拒。reconcile 返回成功并不证明 ACL 已加载或 runtime 权限正确。

Durable Consumer 的业务语义仍由消费方拥有；部署文件只提供基础设施声明，不能成为
新的全局事件业务包。

### 2.3 `deploy/observability/`

该目录保存 OpenTelemetry Collector、Prometheus、Tempo、Loki、Grafana Alloy、
Grafana、Alertmanager、NATS Surveyor 和 postgres_exporter 的版本化配置、Dashboard
与告警规则。完整约束见 [可观测性与 Telemetry 设计](observability.md)。

### 2.4 `deploy/helm/agent-platform/`（FINAL-INTEGRATION）

该目录最终作为 Kubernetes 生产部署入口，负责把已经存在并已验收的独立镜像组装为
Deployment、Service、Job、Ingress、Secret 引用、NetworkPolicy、HPA 和其他平台资源。
它只在 Control、Gateway、Worker、Local IM、前端等全部生产 Workload 完成，且镜像、端口、
Probe、权限、Migration 与 Secret 契约稳定后进入 FINAL-INTEGRATION；当前阶段不创建 Chart
或 Gateway 专属 Helm 模板。

Helm 不重新定义业务配置、事件协议或数据库 Schema。Compose 与 Helm 必须使用相同
镜像、端口语义、健康检查、环境变量和 Secret 契约。

数据库迁移按 Workload 的逻辑表所有权分开：Control 只执行 Control Migration；Gateway、
Worker 等只执行各自拥有的 Migration。当前 Control SQL 嵌入 Control 二进制，在监听端口前
通过 PostgreSQL advisory lock 串行执行。任一同一表集合在 Compose 与最终 Helm 中都只能
有一个执行者；未来改用独立 Job 时，通过架构决策一次性移除该集合的进程启动迁移。

## 3. 服务本地资产

以下文件继续跟随所属 Workload，不迁入 `deploy/`：

```text
services/<workload>/Dockerfile
services/<workload>/migrations/
services/<workload>/cmd/<workload>/
services/<workload>/internal/bootstrap/
```

`deploy/` 负责组装已经定义好的服务，不拥有服务内部的编译入口、Migration SQL、
Domain、Application、Repository 或业务 Adapter。

每个生产 Workload 仍然只构建自己的一个二进制和镜像。禁止建立一个全局 Dockerfile
一次构建全部二进制，再通过 Compose `entrypoint` 或 Helm `command` 选择业务角色。

## 4. 运行时文件与 Secret

以下内容禁止提交到 `deploy/`：

- `.env` 实际值和任何生产 Secret。
- PostgreSQL、NATS、Prometheus、Tempo、Loki 或 Grafana 的运行数据。
- Helm 渲染后的临时 Manifest。
- 本地生成的证书、Credential、Bootstrap Password 和 Token。
- Compose 容器日志、备份包和调试 Dump。

仓库只保存无真实值的配置示例、环境变量名称和配置 Schema。运行时数据与 Secret
必须位于 Git 忽略目录、Docker Volume、Kubernetes Secret 或外部配置管理中。
这里的部署配置不引入业务 Environment 实体或独立 Secret 管理子领域。

### 4.1 Profile 凭据加密 Key

Control API 进程和当前 Compose 都必需 `CONTROL_PROFILE_CREDENTIAL_KEY`，其值是
外部生成的随机 32 字节经标准 base64 编码后的字符串。进程严格解码并校验长度；
缺失或非法即启动失败，Compose 缺值也会在插值校验阶段失败。代码不生成默认 Key，
即使当前还没有 Profile 记录也必须配置。

- Key 属于进程部署配置，不属于 ProfileWrite、Canonical Spec 或数据库明文记录。
- Profile 使用它派生 AES-256-GCM 加密 Key 与用途分域 MAC Key；当前只保存加密当前值，
  不建设 KMS 或自动 Key 轮换。
- 同一数据库的所有 Control API 副本必须使用同一个 Key；重启、重新构建和升级时
  复用它。随意更换 Key 会使既有密文及关联/幂等 MAC 不再可用。
- 数据库备份与 Key 分开保管，但恢复凭据需要两者；只恢复数据库不足以恢复取值能力。
- Key 不写入仓库、镜像、日志或文档实际值，也不通过 `docker compose config` 完整输出
  传播；现有 `just compose-config` 使用 `--quiet`。

[Compose 启动说明](../../../deploy/compose/README.md) 给出当前命令与外部注入方式。
这项必需配置只启用控制面的加密存储，不会自动启用内部 Worker 取值路由；该路由
仍需真实执行授权 verifier 与可信工作负载认证成对接线。

### 4.2 Platform Contract 固定 Digest

Compiler 不在发布前审批资源出站 Host：编译产物会记录该 Deployment 实际会访问的
精确小写 Host 集合，供审计使用。同一平台发布的所有副本还必须显式配置相同的
`CONTROL_DEPLOYMENT_EXPECTED_CONTRACT_DIGEST`。其格式为 `sha256:` 加 64 位小写
十六进制，代表冻结实现契约、版本、执行范围和资源上限的整体身份，不是凭据或业务对象。

当前门禁由两个入口共同保证：`LoadConfig` 校验 expected 的存在与格式；Bootstrap
`New` 在打开 PostgreSQL、执行 Migration 和构造 HTTP Server 之前计算实际 Contract
Digest 并比较。不匹配即启动失败，不监听端口；`/healthz` 保留成功启动后返回 204 的
现有行为。Compose 对 expected 使用必填插值，不会按当前副本自动推导默认预期值。

发布准备阶段，使用待发布二进制与最终 Host 配置预计算一次：

```sh
go run ./services/control-api/cmd/control-api -print-deployment-contract-digest
# 已构建的同一发布二进制：control-api -print-deployment-contract-digest
```

CLI 只读冻结契约，不要求 DB、Profile Key 或 expected，也不启动服务。
把经过核对的输出保存进本次发布配置，再向所有副本注入同一个值。
对应 `.env.example` 中固定且有回归测试保护的配置：

```sh
export CONTROL_DEPLOYMENT_EXPECTED_CONTRACT_DIGEST='sha256:43d9ac6291cf7ccacf544cb122fa4d92b398317fa6020e4bba43e8666e8747b0'
```

默认内建 Host 集合与此 Compose 示例不同，预计算必须使用最终配置。Host 或二进制
契约变化时重新准备并统一更新发布值；禁止在每个副本的启动脚本里动态把自身计算结果
赋给 expected，那会绕过一致性门禁。该门禁不创建数据库协调表、策略服务或 Environment。

### 4.3 Profile 消费与 ChannelAccount 凭据边界

Profile 已实现直接 Write/公开 Read 分离、加密表、COW/live/CAS、`CheckUsable`、
`ResolveForAttempt` 与可选 runtime HTTP Adapter。后续 Worker 经 Profile Owner 在现有
Control API 内的认证批量接口解析本次 Attempt 所需凭据，不直连 Profile 数据表；
真实 Run/Attempt 授权拥有方、可信 Workload 认证、默认解析路由与 Worker 接线仍待完成。
该入口故障影响需凭据的新 Attempt，已完整解析的 Attempt 复用固定集合。

ChannelAccount 的 Telegram Token/企微 Secret 已由独立账户 Owner 管理，经 mTLS
快照/凭据协议供应 Gateway；不复用 Worker Profile Resolver。该能力通过
`CONTROL_CHANNEL_CONFIG_FILE` 显式启用，私有账户 Key、scope 与 Gateway 身份按
[Channel Runtime](../../../services/control-api/CHANNEL_RUNTIME.md)配置，不向租户暴露独立 Secret 产品。

## 5. 操作入口与一致性

根 `justfile` 是开发者操作入口，当前负责调用 Compose、测试和验证命令；进入
FINAL-INTEGRATION 后再封装 Helm。禁止为了每个部署动作继续增加仓库根目录 Shell 脚本。

当前测试入口与覆盖边界如下，均以根 `justfile` 为入口：

- `just openapi` 运行 `./api/...` 与 Agent / Runtime Profile / Deployment Domain；
  覆盖 OpenAPI、Schema、Event、Fixture 和 Compiler，不包含 Deployment Application / HTTP。
- `just test-race` 运行 Identity / Tenant / Admin / Agent / Runtime Profile / Deployment
  模块子树和 Bootstrap 的 race 测试；包含 Deployment Application / HTTP / PostgreSQL
  Adapter，但不包含 API 包、CLI 或独立 integration 包。
- `just test-integration` 调用 [scripts/test-control-integration.sh](../../../scripts/test-control-integration.sh)，
  必须显式注入测试专用 `CONTROL_TEST_DATABASE_URL`。脚本使用 `-count=1 -json` 执行
  integration、Bootstrap、Deployment PostgreSQL 与 Runtime Profile PostgreSQL 四个包，
  拒绝任何 Skip，并要求指定的真实生命周期、事务、并发、回滚、不可变性及凭据消费测试
  全部通过，成功输出 `CONTROL_INTEGRATION_GATE=PASS zero skipped tests`。

普通 `go test ./...` 缺少 DSN 时可跳过 PostgreSQL 测试，不替代真实零跳过集成门禁；
CLI 测试包含在该全仓库命令中。脚本放在 `scripts/` 复用，不增加平级根目录操作入口。

Compose 与 Helm 必须满足：

1. 发布镜像使用 `image@sha256:<digest>`，或由 CI 解析为 digest 的不可变 Release Tag；
   不使用不可追踪的 `latest` 作为发布依据。
2. 服务名、端口、Probe、环境变量和 Secret 名称保持一致。
3. PostgreSQL、NATS 和观测后端默认不直接暴露公网。
4. 本地覆盖不能改变生产业务语义。
5. 部署文件存在不代表服务已实现或生产验收已经完成。

## 6. 当前代码与部署状态

Control 与 Gateway 的 Compose 已组合 PostgreSQL/NATS 和两个独立 Go Workload；
Gateway 当前默认 Control 来源已接账户目录、凭据、Telegram 注册、WeCom Supervisor、
Runner/Maintenance 与 observations。显式 fixture overlay 才使用本地账户文件/环境引用。
分阶段配置、镜像、容器与运行证据见[实施状态](../channel-gateway/implementation-status.md)，
不因文件存在就推断当前实例在线或最新镜像已验收：

- `deploy/compose/compose.yaml` 保留 Control API/PostgreSQL，并加入 Gateway、NATS、
  `gateway-database` provisioning 与 `nats-reconcile` 一次性作业。
- `deploy/compose/compose.local.yaml` 包含 Control/Gateway 本地构建与回环 HTTP 端口。
- 本地 Compose 默认使用 `auto` 完成一次性 Platform Operator bootstrap，并直接通过
  `CONTROL_BOOTSTRAP_USERNAME` 和 `CONTROL_BOOTSTRAP_PASSWORD` 提供简单的本地凭证；
  已有 Operator 时 bootstrap 保持幂等。
- `CONTROL_PROFILE_CREDENTIAL_KEY` 由外部环境变量注入，没有内置值或启动时自动生成。
- 资源出站 Host 由 Compiler 从 Profile 中推导并写入 Manifest，不是发布前的审批清单；
  平台不会做在线探测或 Worker 节点枚举。
- `CONTROL_DEPLOYMENT_EXPECTED_CONTRACT_DIGEST` 由平台发布配置固定；只读 CLI 支持
  预计算，Compose 拒绝缺值，Bootstrap 在 DB / HTTP 前拒绝实际 Digest 不匹配的副本。
- 当前 Compose 已加入 NATS/Gateway，但不加入 Worker、Local IM 或可观测性组件，也不启用
  尚未接入真实 Run/Attempt 授权的内部凭据解析路由。

NATS 声明与 Server 授权配置生成物已有实现；`compose.observability.yaml` 仍待后续补充。
Helm Chart 保持 FINAL-INTEGRATION 目标，不随单个 Gateway 或其他 Workload 提前创建。
当前切片必须通过 Compose 配置校验、镜像构建、容器启动、PostgreSQL Migration，
再按各 Workload 的真实探针分别验收：

- Control API：`/healthz`，当前 HTTP 端口 `8080`。
- Gateway：管理端口 `8091` 的 `/livez`、`/readyz`；正常返回 `204`。
  公开端口 `8090` 不暴露管理探针，对该路径返回 `404`；Compose healthcheck 使用 Gateway
  自带 `probe` 命令检查 `8091/readyz`，而不是 Control 的 `/healthz`。

这里列出源码与既有验收记录中的契约。Connection/0005、Final/0006、Runtime/0007 的
历史镜像与探针验收保留于实施状态 §9–11；当前 Control 接线和真实 Telegram 入站见
§13–14。旧镜像结果不替代新增代码验收；真实企微账号验收仍待完成。

<a id="7-channel-gateway-部署扩展草案"></a>

## 7. Channel Gateway 部署扩展（当前实现与剩余目标）

本节与 [Gateway 设计](../channel-gateway/README.md)、[四个业务 Module](../channel-gateway/module-boundaries.md)、[公开 Go Connector](../channel-gateway/public-go-connector.md)
配套。当前采用纯 Go / 进程内企微库方向，替代此前的独立 Node Connector 提案。
完整目标继续区分已实现部分与剩余项；当前代码与部署状态见第 6 节、
[实施状态](../channel-gateway/implementation-status.md)与 [Compose README](../../../deploy/compose/README.md)。

### 7.1 目标部署单元

| 单元 | 目标职责 / 依赖 | 当前状态 |
| --- | --- | --- |
| `postgres` | 本地可复用实例；Control/Gateway/Worker 数据权限分别设计 | 当前 provisioning 新增 Gateway database/role；仍为同一物理实例 |
| `control-api` | 管理/发布；路由专用 Relay 在同进程 | Deployment 原子 Publication、ChannelAccount/Binding 和路由 Relay 已实现；Manifest 发布事件仍为 PENDING |
| `nats` | JetStream、持久 Volume、固定 subject 权限 | 当前已有 Compose/配置；运行证据见实施状态 |
| `channel-gateway` | 一个 Go 二进制/镜像；直接导入 Telegram SDK 与公开企微库 | 四 Module、Control 账户/凭据、动态入站、Runner 与 observations 已实现；真实 Telegram 入站已验收 |
| `agent-worker` | Run/Attempt、固定 Manifest 执行、ReplyIntent | 独立后续交付；完整执行 E2E 必需 |
| NATS reconciliation job | 幂等应用 Stream/Consumer 拓扑；权限由 Server 授权配置路径加载 | 当前已有一次性作业；不承担 Server ACL 加载 |

**不新增 `wecom-connector` 部署单元。** 不使用 Node 镜像/子进程/sidecar，也不增加 Go↔Node
内部 RPC、Connector Dockerfile 或独立监听端口。`platform/im/wecom` 是公开库，不是服务。
企微启用是 Gateway 的渠道/账户配置，不是另一个 Compose profile；Gateway 镜像只发布一份。

当前 Control Bootstrap 统一启动 Gateway Relay、投影消费者、账户 refresh、Telegram 注册、
observations、Delivery Runner 与 Connection Supervisor；各 Module 自己编排业务状态机，
Bootstrap 不拼接 Acquire/Renew/Release。Runner 独占 Maintenance 生命周期，fixture 来源
才由 App 独立运行维护；Maintenance 不依赖本地 owner/Sender，无账户也处理旧账本。
加入 ReplyIntent 消费时仍须同步交付 topology、ACL 和持久交接协议；当前 Runtime 的
无人 owner 过期处理、分页/worker/停止边界见[Runtime V1](../channel-gateway/delivery-runtime-v1.md)。详见[Module §7.6–7.7](../channel-gateway/module-boundaries.md#76-后台调度与无连接时的恢复目标补充尚未实现)。
这些都是同一个 Gateway 的能力，不是新增 Sender/Connector 服务，也不提前创建 Helm。
以后若因容量/隔离拆分部署，需要新的明确设计，不能由 local overlay 悄悄改变职责。

### 7.2 部署资产与后续增量

| 路径 | 当前已有 | 后续增量与验收 |
| --- | --- | --- |
| `services/channel-gateway/Dockerfile` | 单 Gateway 非 root Go 镜像；构建包含所导入公开包 | 后续能力继续编译进同一镜像，新增配置时复核启动/探针/退出；不增加 Connector 镜像 |
| `deploy/compose/compose.yaml` | Gateway、NATS、独立 Gateway database/role provisioning、网络/Volume/Probe | 随真实 Worker、Reply 通路及其他 Workload 到位扩展，保持数据库和事件权限分离 |
| `deploy/compose/compose.local.yaml` | Gateway 本地 build、回环端口；生产账户来自 Control | 显式 fixture overlay 才挂载账户文件，不增加 Node build 或企微 sidecar |
| `deploy/nats/streams.yaml` | Route/Run 的存储、保留、Consumer 与幂等 reconciliation | Reply topology 需随其生产消费者交付，不能只声明 subject 就算接线完成 |
| `deploy/nats/permissions.yaml` / `server.conf` | 独立 runtime/管理身份及渲染后的 Server 原生配置 | Reply 与 Worker 的窄 ACL、重新加载/启动及正反授权验收；CGR-28 密码预检仍开放 |
| 根 `justfile` | 已有 gateway build/test/integration、wecom-test 及 NATS 配置/调和入口 | 新用例必须对应真实命令和可重复测试，不增加只打印成功的占位入口 |
| `.env.example` / 配置说明 | Gateway PG/NATS、listener、Control mTLS/scope/来源身份及公网 origin；Bot 凭据经 Control 解析 | 后续 Workload 配置继续做跨服务集成；文件/环境账户只用于 fixture |

这里按源码资产区分“已有”和“后续”，既有运行证据仍以[实施状态](../channel-gateway/implementation-status.md)
的日期和范围为准；本次文档复审不把已有 Compose/镜像验收当作重新执行。

保留 base + local + optional observability 的既定组织，不增加平行 docker/ 或单独企微拓扑。
公开包不拥有 Dockerfile/migrations，不为库建立部署目录。Go 构建使用根 go.mod，
Docker build context 必须覆盖 platform/ 源码；没有另一个 Go module 或 npm lockfile。

`deploy/helm/agent-platform/` 不属于上述 Gateway 资产增量。它在全部生产 Workload 完成后的
FINAL-INTEGRATION 一次性组装平台；最终只有一个 Channel Gateway Deployment，不会增加
Connector Deployment。

### 7.3 数据、迁移与启动

- Control 的现有迁移仍由 Control 进程启动执行；每个 Workload 只迁移自己拥有的逻辑表，
  Gateway 自有表不进入 Control SQL。
- 第一切片采用 `channel_gateway` database、`gateway` role 与 Gateway 启动迁移；
  provisioning job 只建库/角色，不迁移业务表。当前共 10 个迁移（0001–0010）与 SHA ledger；
  0008 增账户目录，0009 增发送用途绑定，0010 增 Telegram 注册账本。Control 来源已装配
  Runner 并由它独占 Maintenance；0007 仍只加 Runtime 索引、不释放容量。全平台权限
  与最终升级验收另列，不增加第二个迁移执行路径。
- 复用 PG 实例不等于复用 Control DSN；新增 database/schema/role 要有幂等 provisioning，
  修改 POSTGRES_DB 不会为已有 Volume 自动补出新库和权限。
- D0-06/10 要使 Stream 保留、Consumer 离线窗口与 PG 事实/Outbox 恢复窗口一致；PubAck
  不等于 Worker 已接手，永久错误隔离、超窗告警和受控重驱动属于交付项。
- NATS 服务健康、拓扑已应用、身份权限已加载分别验收。应用不兼容配置时明确失败，禁止自动删建。
- Gateway 先初始化持久存储/投影，再按 account 取得 owner grant、经 Control 解析用途
  凭据并启动本地企微 Client；显式 fixture 才解析环境引用，无账户时不连接企微。
  库构造不自动联网；每个实例不因启动就连接全部 Bot。
- 缺少 Worker 时可验证接纳和积压，但不标记 Agent E2E 通过。容器 ready 不等于已有可接纳绑定。

### 7.3.1 显式 fixture 的企微配置与轮换

生产默认来源使用 Control 完整账户快照与 mTLS 凭据解析，目录失效或连接版本变化会
取消旧用途/Client；不依赖宿主文件更新。下列文件机制仅用于显式 fixture overlay。

Gateway 进程通过 `GATEWAY_WECOM_ACCOUNTS_FILE` 读取完整五字段账户列表：account_id、
bot_id、revision、enabled、secret_env。Compose 用 `GATEWAY_WECOM_ACCOUNTS_DIR` 挂载
父目录到 `/etc/gateway/wecom`，进程文件固定为该目录的 accounts.json；默认文件为 []。
目录挂载使宿主机在同目录 atomic rename 后，下次轮询可打开新文件；不使用单文件 bind
mount 来承诺可替换 inode 的配置热更新。关闭账户要用更高 revision 的 enabled=false；
删除本副本文件条目仅停止本地连接，不是跨副本的持久禁用命令。

配置 revision 必须跨副本单调，Bot 身份不得更换或删后复用。值只在取得 lease 后从环境
解析，已运行进程的环境不会跟随宿主 shell/export 更新。轮换要么切向预先注入的新
secret_env，并递增 revision；要么在递增 revision 的同时重建/重启进程并注入新环境。
环境变量引用不是秘密值，也不是 Control 凭据 Owner。完整步骤见[Compose 说明](../../../deploy/compose/README.md#企业微信账户配置显式-fixture-来源)。

### 7.4 多副本、网络与健康

- Telegram Webhook 可以进入任意可接纳的 Gateway 副本，持久去重在 Gateway 自有数据中完成。
- 企微一个 Bot 只由当前 Gateway owner 维护有效连接；其他实例不建立同 Bot Client。
  通过执行授权的 ReplyIntent 可被任意实例持久接纳；实际企微 Delivery 还须匹配首次 Admission
  保存的原 ReplyOrigin，不因重连或接管而替换为新 Client。当前 owner 资格和原连接身份是
  两道不同检查，见[Module §7.5](../channel-gateway/module-boundaries.md#75-企微原回复关联不等于当前发送资格)。
- Delivery 领取使用 claim token CAS；`MarkCalling` 与结果记账使用相应状态/attempt CAS。
  只有企微等 stateful provider 额外执行同事务 owner epoch 校验，且包含 `MarkCalling`，
  不仅在领取时检查。过期 CLAIMED 与 stale CALLING 分别恢复，见
  [Module §7.3](../channel-gateway/module-boundaries.md#73-外部副作用事务边界)。Telegram
  不创建伪 owner epoch。通知只唤醒持久调度，
  无需为同进程 SDK 再建 Gateway-to-Gateway Connector RPC 或靠普通负载均衡碰运气找到连接。
- Gateway 区分公网 webhook 入口和管理/探针入口，具体 listener/端口在配置中冻结。
  对外只暴露需要的 webhook；企微是 Gateway 主动发起的 WSS 出网，没有额外公开回调端口。
- PostgreSQL、NATS 和管理/metrics 不暴露公网，本地调试仅显式回环映射。
- liveness 检查进程；启动 readiness 要求 Migration、PG、投影初始化以及 JetStream topology
  已校验/应用。运行期 NATS 短时故障可在 Outbox 容量/年龄阈值内保持 ready、继续持久接纳并
  报告 degraded。达到 D0 阈值时，Admission Commit 原子拒绝新接纳，而不只摘 readiness；
  已持久重复事件仍返回原 Receipt。容量预算/并发预留与关闭门禁要有运行测试。
- 企微首次入站也要在 Admission 同一事务检查 owner/epoch/连接配置代次；context 取消
  不能替代提交 guard。旧 Receipt 重放保留首次结果；Telegram 不引入伪 owner。
- 账户恢复区分临时 Admission 失败、认证/协议终态与 confirmed replaced；前者采用有界
  重试/退避，后者的 replacement 隔离须跨副本持久保存，且 Close 失败仍应尝试记录。
  恢复条件、预算与隔离失败诊断在 [Module §6.4–6.5](../channel-gateway/module-boundaries.md#64-入站失败与连接恢复的组合契约)
  细化。当前 Connection 切片已实现有界恢复、入站 owner guard 和持久 replacement 隔离，
  既有验收见[实施状态 §9](../channel-gateway/implementation-status.md#9-connection-与企微持久入站已应用并验收)；
  Delivery/Sender 与 Runner 已在 Control 来源装配；ReplyIntent Consumer、跨重启全局预算、
  真实企微账号和 Final 回复验收仍待交付，不增加独立 Connector 单元。
- 企微 socket READY 不证明仍有 Final 身份容量；累计容量耗尽不同于瞬时 pending 背压，
  账户级发送可用性/告警/准入影响及恢复策略须在生产 Final 启用前冻结（CGR-37）。恢复不
  把旧 ReplyOrigin 迁往新 socket，详见[公开库组合门禁](../channel-gateway/public-go-connector.md#41-累计身份容量不等于瞬时背压gateway-组合门禁)。
- 单 Bot disconnected / credential-invalid / lease-lost 是账户级状态，不使整个 Gateway
  因一租户问题立即失活；副本尚未获分配 Bot 也不意味着进程不健康。
- 投影 initialized 依据持久完整性水位，允许完整空快照；不能以“存在任意一条 route”代替。
  source-observation age 与持续 apply lag 必须分开；源 heartbeat 成功不证明增量已应用。
  当前来源观察门限为 5 分钟，连续已知积压满 60 秒则另行拒绝新 Run；起点持久化且只在
  完整追平后清除，Ready 与事务 guard 一致。此时间从本地已知积压起算，不承诺 Control
  停用发布的端到端 SLA。见 [Module §4.4](../channel-gateway/module-boundaries.md#44-初始化不是收到任意一个事件)。
- 正常关闭先停止新接纳/领取/新发送和重连，在有界 drain、结果记账及 Client 关闭期间保持续租，
  Client 关闭后再停止 renew/release。失租、续租失败或 drain 超时立即取消，未确定发送保留
  UNKNOWN；cleanup 使用独立 deadline context，不因主 context 已取消而跳过收束。
- 企微远端不验证平台 fence；旧 owner 在途发送仍可能成功。本地 CAS 不提供外部 exactly-once。
- 镜像 Probe 使用真实存在的程序；distroless 不假设安装了 curl/wget/sh。Compose 先固定
  健康语义与可验证探针，FINAL-INTEGRATION 的 Helm 复用同一契约。

### 7.5 实施与验收前置

公开库 P0/状态 API、Gateway 自有 database/role、0001–0010 启动迁移、账户/owner/route
guard、Route/Run 拓扑及本地阈值已有明确选择；当前 Control 账户/凭据和 Runner 已接线。
逐项状态见[实施状态](../channel-gateway/implementation-status.md)。真正剩余的生产门禁包括
Execution 完成证明、Reply transport/Consumer、CGR-37、有效发送截止、保留/GC 与
全平台权限及恢复验收。Bot Token/Secret 不套用 Worker Attempt 的 Profile Resolver；
库接收调用方注入值，真实值不进入事件或部署 YAML，静态文件来源只保留为 fixture。

公开 Go 库通过本地 WS 协议/故障/race 测试并不等于企微真机通过。新增 Gateway 部署单元
必须具备真实入口、配置、持久纵切、健康检查和关闭测试后才加入 Compose。验收顺序为
配置校验 → 镜像构建 → PG/NATS 初始化 → 接纳/重投 → Worker/Final E2E → 企微真实连接/
抢占/断线窗口。全部生产 Workload 完成后再进入 FINAL-INTEGRATION，由 Helm 复用已经稳定的
配置、健康与初始化契约。

## 7. Database V1 更新（2026-09-07）

本节更新上文历史 Compose 部署布局：当前默认基线已改为同 PostgreSQL、同 database，
control/gateway/worker/runtime_session 四 Schema 与各自 migrator/runtime 角色。
`database-schemas` 取代旧 gateway-database 初始化；Control/Gateway 强制独立迁移 DSN，
先核对目标与角色，再执行自有迁移并关闭迁移 pool，业务不使用初始化管理员。
Worker 和 Session 只预置隔离空间，尚无业务表。现有旧卷不自动迁移，guard 在写入前终止。
准确配置、权限、迁移前提和真实 PG 验收见 [Database V1](database-v1.md) 与
[Compose 当前说明](../../../deploy/compose/README.md)。前文旧实例/历史验收不代表已迁库。
