# Worker V1 Compose 部署

本文对应 `compose.yaml + compose.local.yaml + compose.worker-v1.yaml` 的本地构建部署。
Worker V1 的业务终点是 Telegram 文本 → 单 LLM → 正式 Session → Final 回复。
Compose 配置展开、镜像构建、SQL 迁移、健康探测与协议 fixture 都是独立门禁，不能代替
真实模型与真实 Telegram 账号的两轮回复验收。

## 1. 部署单元与启动依赖

| 单元 | 本次职责 |
| --- | --- |
| postgres | 一台 PostgreSQL、一个 database；保留持久卷 |
| database-schemas | 管理身份显式建立四个 Schema/八个角色；不是运行服务 |
| nats | generated ACL + TLS wrapper；四条 Stream 的 broker |
| nats-reconcile | 唯一拓扑管理身份，创建四条 Stream/四个 durable |
| session-prepare | `session_migrator` 显式准备 runtime Session 表；执行完退出 |
| control-api | 管理面、Channel mTLS、Worker runtime mTLS、Route/Manifest relay |
| agent-worker | Worker migrations、Run/Manifest 接管、模型执行、Session/Completion、Reply relay |
| channel-gateway | Telegram 入站、Reply 接管、已有 Delivery Runner/Sender |

Worker 等待 schema provision、Session preparation 和 NATS reconcile 成功，再启动。
Control 在 reconcile 后启动；Worker 对 Control 依赖 `service_started`，实际配置与快照
依赖由启动/就绪校验继续证明，不把容器已创建当作依赖可用。

使用四个 Schema 和独立角色：`control`、`gateway`、`worker`、`runtime_session`。
Worker 进程只持有 `worker_runtime`/`worker_migrator` 的自有数据库连接。Session 的
Profile 在线授权返回 Session 密码；Worker 将其与固定 Manifest destination 组合为
`session_runtime` DSN，不把该运行密码/连接写进 Worker 环境变量或 JSON。
只有短生命周期 `session-prepare` 读取 `SESSION_MIGRATION_DATABASE_URL`。

新库 provision 不会自动迁移旧 `public` 表或旧 `channel_gateway` database；旧布局先走
明确的数据迁移。不要用删卷替代迁移，`docker compose down -v` 不是升级步骤。

## 2. 文件目录与容器路径

将私有部署文件放在仓库以外，并将下列环境变量设为宿主机绝对目录：

```text
/srv/trpc-agent-v1/
├── control-channel/       # CONTROL_CHANNEL_CONFIG_DIR
│   ├── config.json
│   ├── nats.json
│   ├── credential-keys.json
│   ├── server.pem
│   ├── server-key.pem
│   └── client-ca.pem
├── control-runtime/       # CONTROL_RUNTIME_CONFIG_DIR
│   ├── config.json
│   ├── nats.json
│   ├── server.pem
│   ├── server-key.pem
│   ├── worker-client-ca.pem
│   ├── worker-server-ca.pem
│   ├── execution-client.pem
│   └── execution-client-key.pem
├── worker-runtime/        # WORKER_RUNTIME_CONFIG_DIR
│   ├── config.json
│   ├── nats.json
│   ├── worker-client.crt
│   ├── worker-client.key
│   ├── control-ca.crt
│   ├── worker-server.crt
│   ├── worker-server.key
│   └── proof-client-ca.crt
├── gateway-control/       # GATEWAY_CONTROL_CERTS_DIR
│   ├── ca.pem
│   ├── client.pem
│   └── client-key.pem
├── gateway-worker/        # GATEWAY_WORKER_CERTS_DIR
│   ├── ca.pem
│   ├── client.pem
│   └── client-key.pem
└── nats-tls/              # NATS_TLS_DIR
    ├── ca.pem
    ├── server.pem
    └── server-key.pem
```

目录分别只读挂载到 `/run/control-channel`、`/run/control-runtime`、
`/run/worker-runtime`、`/run/gateway-control`、`/run/gateway-worker` 和 `/run/nats-tls`。
JSON 内使用**容器绝对路径**，不是宿主路径。

三个 Go 镜像基于 distroless `nonroot`；部署私钥及 private NATS/keyring JSON 要满足
进程实际 UID 可读，私钥和 private JSON 保持 owner-only（如 `0600`），父目录具备该 UID
的遍历权限。使用 Linux bind mount 时按镜像实际 UID（当前基础镜像为 `65532`）准备 owner；
单纯把宿主用户的 `0600` 文件挂进去不证明容器能读取。不要以扩大到 group/world-readable
绕过应用的权限校验。NATS 服务端私钥另按 NATS 镜像 UID 配置。

## 3. TLS 主机名、证书用途与 URI 身份

| 连接 | 服务端 DNS SAN | 客户端 URI SAN / 映射 |
| --- | --- | --- |
| Gateway → Control Channel `https://control-api:8081` | `control-api` | `spiffe://agent-platform/channel-gateway` → Channel workload `instance_id=gateway-1` |
| Worker → Control runtime `https://control-api:8082` | `control-api` | 如 `spiffe://agent-platform/agent-worker/worker-one` → Runtime mapping `worker_id=worker-one` |
| Control → Worker Attempt proof `https://agent-worker:8082` | `agent-worker` | `spiffe://agent-platform/control-api`，列入 Worker `control_principals` |
| Gateway → Worker Final proof `https://agent-worker:8082` | `agent-worker` | `spiffe://agent-platform/channel-gateway`，列入 Worker `gateway_principals` |
| Control/Worker/Gateway/reconciler → NATS `tls://nats:4222` | `nats` | NATS 独立用户/密码与 generated ACL；不是 HTTP caller mapping |

每张 HTTP 客户端证书使用单一 URI SAN，具备 clientAuth 用途；服务端证书具备 serverAuth。
Worker proof 两类 caller allowlist 不重叠；已结束 Attempt 的 Final 查询仍读取 committed
Completion，不把 live lease 当作完成证明。所有 CA 文件实际信任对应服务/客户端链。

NATS wrapper `deploy/nats/server-tls.conf` include 原 generated `server.conf`，保留原
按角色划分的 ACL 并加 TLS。没有关闭证书验证的配置。Gateway 与 reconciler 的 CA 路径
由 overlay 注入；Control 和 Worker 的 CA 路径还要写进各自私有 `nats.json`。

## 4. Control 的两个独立内部 listener

只配置 Worker runtime listener 不会同时开启 Channel account/catalog 或 Route relay。
Overlay 明确传入两个配置文件：

```text
CONTROL_CHANNEL_CONFIG_FILE=/run/control-channel/config.json
CONTROL_RUNTIME_CONFIG_FILE=/run/control-runtime/config.json
```

### 4.1 Channel 配置

`control-channel/config.json` 示例字段如下；scope/source epoch 和身份映射需在各侧一致，
source epoch 是持久部署身份，不在重启时重新生成：

```json
{
  "route_nats_file": "/run/control-channel/nats.json",
  "scope_id": "gateway-pool",
  "source_epoch": "00000000-0000-4000-8000-000000000001",
  "internal_address": ":8081",
  "tls_cert_file": "/run/control-channel/server.pem",
  "tls_key_file": "/run/control-channel/server-key.pem",
  "client_ca_file": "/run/control-channel/client-ca.pem",
  "credential_keys_file": "/run/control-channel/credential-keys.json",
  "max_tenant_accounts": 100,
  "workloads": [{
    "principal_id": "spiffe://agent-platform/channel-gateway",
    "instance_id": "gateway-1",
    "scope_id": "gateway-pool",
    "audience": "control-channel-v1",
    "consumers": ["telegram_receiver", "telegram_webhook", "telegram_delivery", "telegram_registration"]
  }]
}
```

对应 Gateway 环境为 `GATEWAY_INSTANCE_ID=gateway-1`、
`GATEWAY_CONTROL_SCOPE_ID=gateway-pool`、相同 `GATEWAY_CONTROL_SOURCE_EPOCH`。
`credential-keys.json` 使用已有 Channel keyring 格式：`active_key_id` 与 `keys`，每个 key
包含独立的 base64 `encryption_key` / `mac_key`。它用于托管 Channel 凭据，不放 Telegram
Bot Token 明文。该 keyring 与 `CONTROL_PROFILE_CREDENTIAL_KEY` 是不同契约，都需要跨重启保存。

### 4.2 Worker runtime 配置

`control-runtime/config.json`：

```json
{
  "internal_address": ":8082",
  "tls_cert_file": "/run/control-runtime/server.pem",
  "tls_key_file": "/run/control-runtime/server-key.pem",
  "client_ca_file": "/run/control-runtime/worker-client-ca.pem",
  "execution_url": "https://agent-worker:8082",
  "execution_ca_file": "/run/control-runtime/worker-server-ca.pem",
  "execution_cert_file": "/run/control-runtime/execution-client.pem",
  "execution_key_file": "/run/control-runtime/execution-client-key.pem",
  "manifest_nats_file": "/run/control-runtime/nats.json",
  "workers": [{
    "principal_uri": "spiffe://agent-platform/agent-worker/worker-one",
    "worker_id": "worker-one"
  }]
}
```

两份 Control `nats.json` 都使用同一 Control 发布身份；Broker 密码与
`NATS_CONTROL_PASSWORD` 对应。JSON 中填写实际值，不使用 shell `${...}` 表达式，读取器不做
环境替换：

```json
{
  "url": "tls://nats:4222",
  "user": "control",
  "password": "REPLACE_WITH_CONTROL_NATS_PASSWORD",
  "ca_file": "/run/nats-tls/ca.pem"
}
```

## 5. Worker 显式配置

以源码的 `services/agent-worker/internal/bootstrap/example.json` 为完整字段模板，复制到
`WORKER_RUNTIME_CONFIG_DIR/config.json`。该模板是操作示例，不是隐藏默认值；按部署选择并
保留所有 policy、capacity、timeout 字段。Worker 严格拒绝未知字段、重复字段和缺失必需字段。

必须对齐：

- `worker_id` 与 Control runtime `workers[].worker_id` 相同。
- `control_url=https://control-api:8082`。
- `internal_address=:8082`，`health_address=:8083` 与 overlay expose/probe 相同。
- client/server cert 路径与第 2 节的 mount 相同，URI allowlist 与第 3 节相同。
- `platform_contract_digest` 与 Control 本次 release pin 相同，不使用旧版本固定值。
- `nats_file=/run/worker-runtime/nats.json`，使用 `worker` 用户及对应
  `NATS_WORKER_PASSWORD`，URL 为 `tls://nats:4222`，CA 为 `/run/nats-tls/ca.pem`。
- `WORKER_STOP_GRACE_PERIOD` 大于 JSON 的 `shutdown_drain_timeout + shutdown_cancel_timeout
  + http_shutdown_timeout` 之和；示例三个值共 45 秒时，可显式配置 `60s`。不要给 Docker
  比应用 drain 更短的强制终止窗口。

`limits.max_retained_runs` 必须显式配置为正整数，示例为 `100000`，不是缺省回填。
它统计 Worker 账本中全部状态的 Run；`max_queued_runs` 只统计待完成 Run。累计已满时
拒绝接管全新 Run，但已有 Run 执行、receipt 回放和 Final 恢复继续进行。提高部署配置并
重启后可处理 broker 中原未 ACK 的消息；降低值不删除既有历史。多个 Worker 应使用一致
的容量配置，避免滚动切换期间不同接管上限。没有新增表或迁移，也没有新增数据库实例。
该字段不是辅助 receipt/冲突/tombstone 或独立 Session 候选的总量配额；这些保留计划仍后置。

运维 policy 的 Run/Reply 年龄、租约、并发/队列/保留 Run/快照 byte 容量，不是累计模型 Token Policy。
V1 不新增累计输出上限或 Token 预占结算；现有 Manifest `max_output_tokens` 仍是单次输出上限。

## 6. 发布契约、Session 目标与预备

出站 Host 不再需要发布前审批：Compiler 会把该 Deployment 实际访问的精确 Host 集合
写入 Manifest。修改发布契约或托管目录后重新计算 release pin，并同时更新 Control env
与 Worker JSON：

```bash
# 使用本次构建的 Control binary。
go run ./services/control-api/cmd/control-api -print-deployment-contract-digest
```

Profile storage resource 的目标示例：

```json
{
  "kind": "postgres_state",
  "dsn_credential_id": "pcr_session",
  "destination": {
    "host": "postgres",
    "port": 5432,
    "database": "agent_platform",
    "username": "session_runtime",
    "sslmode": "disable"
  }
}
```

Profile **管理 API 的输入**是与 destination 完全相同的受限 DSN，密码需要 URL 编码，
例如 `postgres://session_runtime:ENCODED_PASSWORD@postgres:5432/agent_platform?sslmode=disable`。
Control 解析该输入后仅加密 password，非秘密 destination 随 Profile/Manifest 固定发布；
**runtime `purpose=dsn` 的授权值仍仅是密码**，不是上述完整 DSN。Worker 用固定 destination
和授权密码按 URI 规则转义组装连接，不把密码当 URI 解析，不从自有数据库 URL 推导目标，
也不接受管理员或 `worker_runtime` 代替 Session 角色。密码中的 `@`、`:`、`/`、`?`、`#` 等
字符仍作为密码数据，不能改写目标或 query。
本地 PostgreSQL 示例为 `sslmode=disable`；改为 TLS 时管理 DSN 和发布 destination 一同对齐，
需要重新发布的目的地变化不通过旧 CredentialID 的密码轮换隐式生效。

`session-prepare` 只用 `session_migrator`；显式准备 `runtime_session.session_candidates`
与 migration ledger，赋予 `session_runtime` SELECT/INSERT。它不修改正式 accepted head，
执行期间的 Session adapter 不运行 DDL。相同迁移可重跑，成功输出 `SESSION_PREPARATION=PASS`。
Memory 整体后置；候选内容耐久化与 Completion 接受引用的协议不扩展为 Memory 系统。

## 7. 构建、校验与启动

在仓库根目录准备实际 `.env`（源于 `.env.example`），填入所有必需的独立角色密码、数据库
URL、目录、scope/epoch、release pin 和公网 Gateway origin。默认目录变量为空会 fail fast；
示例密码和证书目录不构成可直接上线的配置。

以下 shell 函数固定同一份 Compose 组合，避免后续命令漏 overlay：

```bash
compose_worker() {
  docker compose --env-file .env \
    -f deploy/compose/compose.yaml \
    -f deploy/compose/compose.local.yaml \
    -f deploy/compose/compose.worker-v1.yaml "$@"
}

compose_worker config --quiet
compose_worker build control-api channel-gateway agent-worker session-prepare
# 配置结构校验不打开 DB/NATS，也不等于证书与模型已经连通。
compose_worker run --rm --no-deps agent-worker --check-config

compose_worker up -d postgres nats
compose_worker run --rm database-schemas
compose_worker run --rm nats-reconcile
compose_worker run --rm session-prepare
compose_worker up -d control-api agent-worker channel-gateway
compose_worker ps
compose_worker logs --tail=100 control-api agent-worker channel-gateway
compose_worker exec agent-worker /agent-worker probe http://127.0.0.1:8083/readyz
```

`up` 仍会按 dependencies 启动/确认一次性任务；schema/Session migration/reconcile 都保持
可重入。`run --rm` 的成功输出是显式预检证据，不是对后续容器状态的代替。

默认只有 local overlay 将管理面与 Gateway 的 8090/8091 映射到宿主 loopback。Control
内部 8081/8082、Worker 8082/8083、PostgreSQL 和 NATS 不直接发布到公网。真实 Telegram
webhook 需要独立公网 HTTPS 入口转发至 Gateway；设置的 `GATEWAY_PUBLIC_ORIGIN` 不是
Compose 自动创建的域名或 TLS 入口。

## 8. 本地 fixture 与真实验收

分层记录实际证据：

1. **Compose config / Docker build**：结构、路径、依赖、镜像与命令可用。
2. **真实 PG/NATS + 本地模型/HTTP fixture**：事务、重投、失租、Session 接受和 Reply 交接。
3. **真实模型/Storage**：发布固定 Manifest，模型返回文本；第二个 Run 读取正式历史。
4. **真实 Telegram**：真实 Bot 的文本输入到原会话 Final；两轮会话、Execution Completion/
   SessionCommit/Reply Outbox 与 Gateway Delivery 事实交叉匹配。

真实 Telegram 前还需：Control 建立 ChannelAccount、托管 Bot Token/webhook secret、启用
ChannelBinding 指向已发布的单 LLM/Session Manifest、Gateway 远端注册 READY、模型和
Session credential 正常解析。单纯 readyz 204、空账户启动、NATS 中有 RunRequested，或
本地 Telegram HTTP fixture 成功，不宣布第 4 层已经完成。

服务级故障分类、恢复矩阵及首版/后续范围以
[Worker V1 设计](../../docs/architecture-next/agent-worker/README.md) 为准。
