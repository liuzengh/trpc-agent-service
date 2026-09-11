# 默认本地全栈入口

使用 `just managed-up` 一次启动 Web、Control、Worker、Gateway、PG/NATS
和 Redis/Qdrant/MinIO。配置只需初始化一次，后续复用私密 state 与命名卷。
完整说明见 [MANAGED_LOCAL.md](MANAGED_LOCAL.md)。下文保留基础 Compose 的底层配置参考。

# Control API + Channel Gateway + Worker V1 Compose

> **当前源码状态（Worker V1）**：Run/Manifest 消费、单 LLM、正式 Session、Completion/
> Reply relay、Gateway Reply consumer 和真实 committed-Final mTLS verifier 已接入。
> 完整部署请使用 [Worker V1 Compose 部署](WORKER_V1.md) 的三个文件组合；只使用 base/local
> 不会自动配置 Worker、Session preparation 或 Control 内部双 listener。本文保留按阶段记录的
> fixture/历史验收；这些记录不替代本轮真实模型和真实 Telegram 两轮回复验收。

本目录编排 Control API、Channel Gateway 和它们当前需要的 PostgreSQL / NATS。
`compose.yaml` 是服务基线，`compose.local.yaml` 增加本地构建、回环端口与本地
HTTP Cookie 配置。这里的启动命令说明如何运行当前代码，不替代实际部署验证记录。

当前默认 **Control mTLS 账户来源**已接账户目录、托管凭据/轮换、Telegram 注册与动态入站、
WeCom Supervisor、Reply consumer、Delivery Runner 和 observations；Gateway 执行 0001–0011 共 11 个迁移。
Runner 独占 Maintenance 生命周期；显式 fixture 来源才保留 App 独立维护，不叠加两次 Run。
真实 **Control 发布 → Telegram 用户入站 → PG Admission/Outbox → NATS RunRequested**
已验收，运行目标固定为 Binding 对应的 DeploymentRevision/Manifest，见
[真实入站报告](../../docs/architecture-next/channel-gateway/telegram-real-inbound-20260906.md)。

Worker V1 源码已接 Run → 单 LLM → Session/Completion → ReplyIntent → Gateway Delivery。
真实企微账号和真实模型/Telegram 完整回复验收仍需独立证据；容器 healthy 或 Runner 已启动
不表示真实外部账号与模型已验收。
Connection/0005、Final/0006 与 Runtime/0007 的历史镜像验收分别保留于实施状态 §9–11，
不以旧镜像数字代替当前接线验证。Helm 保持 `FINAL-INTEGRATION`，待全部生产 Workload 完成。

## 1. 部署单元与启动依赖

| Compose 服务 | 镜像/模式 | 职责 |
| --- | --- | --- |
| `postgres` | PostgreSQL 17.6 | 一个实例、一个共享 database；四个 Schema 分别授权 |
| `control-api` | Control Go 镜像 | Control API 与其自身 migration |
| `database-schemas` | PostgreSQL 镜像，一次性 provision job | 创建/校验 control、gateway、worker、runtime_session Schema 与八个独立角色 |
| `nats` | NATS 2.11.8 | 四角色 ACL 的 JetStream；Worker overlay 要求 TLS |
| `nats-reconcile` | Gateway 镜像，`reconcile` 模式，一次性 job | 根据声明创建并严格校验四条 Stream / 四个 durable |
| `channel-gateway` | Gateway 镜像，默认 Control 来源 | 历史及双 0011 migration、账户/凭据接入、Routing、Telegram Webhook/长轮询、Run relay、Reply 接管与 proof、Runner/Sender/Maintenance |

base/local 依赖顺序（Worker overlay 的八个单元与 Session preparation 见新部署指南）：

```text
postgres healthy → database-schemas completed → control-api
                            └─────────────────→ channel-gateway
nats healthy → nats-reconcile completed ────────→ channel-gateway
```

`database-schemas` 以独立管理身份执行 `provision-schemas.sh` / `provision-schemas.sql`。
Control/Gateway 分别使用自己的 migrator/runtime DSN；启动时先验证两个连接的实际目标，
然后只用迁移连接执行自有 migration，关闭它后用普通运行连接服务请求。

**当前 V1 是同一实例、同一 database、不同 Schema/role**，不增加 Worker PostgreSQL 容器。
`worker` 由 Worker 自有 migrator 建表；`runtime_session` 由一次性 session-prepare 显式准备。
base 的 schema provision 只建隔离空间/角色，业务表不由管理员 job 代建。
Gateway 历史 0001–0010 SQL 保持不变；`0011_reply_transport_receipts.sql` 增加 Reply
transport receipts，`0011_telegram_receive_modes.sql` 增加持久 receiver/cursor。两文件
均保留原始名称和哈希，账本按完整文件名区分版本。迁移账本进入各自 Schema，Control
账本仍在 `control`。
Schema 隔离不改变事件交接或领域所有权；不同服务不能借同库直接访问对方表。

旧 `provision-gateway.sh` 仅返回明确迁移提示，不再创建 `channel_gateway` 库。存在旧 public
对象或旧 channel_gateway 数据库时，新 provision 在写入前终止；不自动移动数据、改 owner、
覆盖密码或清理卷。旧卷升级流程与隔离规则见 [Database V1](../../docs/architecture-next/operations/database-v1.md)。
外部数据库由部署侧运行同一 provision；Compose job 只连接它配置的目标实例。

## 2. 必需配置与身份

从仓库根目录运行，通过外部配置或本地未提交的环境文件注入。`.env.example` 不含可用密码。

| 变量 | 用途 |
| --- | --- |
| PLATFORM_POSTGRES_DB / PLATFORM_POSTGRES_USER | 共享库名、独立初始化管理员；默认 agent_platform / platform_admin |
| PLATFORM_POSTGRES_PASSWORD | 仅 postgres 初始化和 database-schemas job 使用；不作为业务运行 DSN |
| CONTROL_MIGRATOR_PASSWORD / CONTROL_RUNTIME_PASSWORD | Control 建表身份与 DML 身份的独立密码 |
| GATEWAY_MIGRATOR_PASSWORD / GATEWAY_RUNTIME_PASSWORD | Gateway 建表身份与 DML 身份的独立密码 |
| WORKER_MIGRATOR_PASSWORD / WORKER_RUNTIME_PASSWORD | Worker migrator/runtime 的独立密码 |
| SESSION_MIGRATOR_PASSWORD / SESSION_RUNTIME_PASSWORD | 预置 Session Store 角色；runtime 默认仅 SELECT/INSERT |
| CONTROL_DATABASE_URL / CONTROL_MIGRATION_DATABASE_URL | 必需；control_runtime / control_migrator，两者同库同 Schema |
| GATEWAY_DATABASE_URL / GATEWAY_MIGRATION_DATABASE_URL | 必需；gateway_runtime / gateway_migrator，两者同库同 Schema |
| CONTROL_PROFILE_CREDENTIAL_KEY | 固定的 32 字节 base64 Profile 加密 Key |
| NATS_GATEWAY_PASSWORD / NATS_CONTROL_PASSWORD / NATS_WORKER_PASSWORD / NATS_RECONCILER_PASSWORD | 各自独立的 NATS 身份 |

Compose 内 DSN 形状（密码需要 URL 编码）：

```text
postgres://control_runtime:<encoded-password>@postgres:5432/agent_platform?sslmode=disable
postgres://control_migrator:<encoded-password>@postgres:5432/agent_platform?sslmode=disable
postgres://gateway_runtime:<encoded-password>@postgres:5432/agent_platform?sslmode=disable
postgres://gateway_migrator:<encoded-password>@postgres:5432/agent_platform?sslmode=disable
```

Schema 由 provision 固定各 role 在该 database 的 search_path，业务不依赖 public。运行 DSN
不回退到管理员或迁移 DSN；两身份相同、目标/Workload不符、public/系统 Schema、管理员
迁移连接或高权限 runtime 均在迁移 DDL 前拒绝，两个角色必须直接登录而非 SET ROLE 伪装。宿主机直跑使用实际发布地址，不能把 127.0.0.1 原样当容器中的数据库地址。
首次创建角色读取密码，provision 重跑不重设已有密码；密码轮换需独立管理员变更并同步部署。
如果 Profile Session 也指向该库，沿用 Profile 现有 host/database/username/sslmode 字段，
不添加任意 search_path/options；固定 Session role 或受信 Adapter 负责 Schema。将 postgres
作为 Profile destination 时需显式加入发布 endpoint hosts 并重新固定 Contract Digest。
Profile 管理 API 录入受限 DSN 后仅保存加密密码；runtime `purpose=dsn` 返回密码值，
Worker 用固定 Manifest destination 与授权密码转义组装 Session 连接，不复用 Worker 自有
数据库目标，也不把密码值当整条 DSN。完整输入/消费示例见 [Worker 部署 §6](WORKER_V1.md#6-发布契约session-目标与预备)。

真实隔离回归：`python3 scripts/test-v1-database-isolation.py`（需 Docker、Go、Python 3）。
脚本创建并清理独立 PG17/NATS 容器，验证 ACL、首启/重跑、迁移账本、跨 Schema 拒绝、
legacy guard 和 Control/Gateway 的真实数据库/启动回归，
不会连接当前业务实例；普通 go test 中集成 Skip 不算该门禁成功。

### Control Profile Key

首次初始化全新数据库时，可用 `openssl rand -base64 32` 生成一次，然后存入持久
部署配置。后续构建、重启和多副本复用同一值。默认 zsh 的非回显输入方式：

```zsh
read -r -s 'CONTROL_PROFILE_CREDENTIAL_KEY?Profile encryption key: '
printf '\n'
export CONTROL_PROFILE_CREDENTIAL_KEY
```

bootstrap 用户密码与 Profile Key 是不同配置。Key 与数据库备份分开保管并维持
恢复关系；当前没有主 Key 轮换流程。`compose-config`、`compose-up` 和
`compose-down` 都会解析必需变量，因此这些操作期间保留同一配置。
配置检查使用 `config --quiet`，不把展开了密码的完整 Compose 配置作为共享输出。

### 同一发布固定 Platform Contract Digest


多副本的 `CONTROL_DEPLOYMENT_EXPECTED_CONTRACT_DIGEST` 必须由同一份部署发布配置
显式注入，格式为 `sha256:` 加 64 位小写十六进制。Compose 拒绝缺值；Control API
在打开数据库、执行迁移和启动 HTTP 之前计算实际 Platform Contract Digest，
只有与预期值一致才继续启动。冻结契约、托管目录或资源上限不同的副本因此不会进入服务。
现有 `/healthz` 仍返回 204；不匹配的进程在监听前退出，没有可用健康端点。

在发布准备阶段，使用待发布二进制预计算一次；CLI 不需要数据库、
Profile 加密 Key 或预期 Digest，也不会启动服务：

```sh
go run ./services/control-api/cmd/control-api -print-deployment-contract-digest
# 或：control-api -print-deployment-contract-digest
```

将本次二进制实际输出保存为本次发布配置，并向所有 Control 副本注入同一
`CONTROL_DEPLOYMENT_EXPECTED_CONTRACT_DIGEST`；Worker JSON 的 `platform_contract_digest`
也必须相同。不要从历史报告复制旧 pin。`.env.example` 与 Worker bootstrap example 保存了
当前冻结契约对应的 pin；修改契约或托管目录后需重新预计算并统一更新。
不要在各副本的启动脚本中把自身计算结果自动赋给 expected 值，那会绕过多副本一致性门禁。
Digest 是平台配置身份，不是凭据。

### Telegram 账户配置（显式 fixture 来源）

本节只适用于显式 `compose.gateway-fixture.yaml` overlay；生产 Control 来源的账户与凭据
由 Control 提供，不能混入静态文件，具体配置见本文 GCI2 节。
fixture 默认 `gateway-accounts.empty.json` 内容为 `[]`。它允许 Gateway 以零账户启动；
这只说明基础设施和路由历史可初始化，不表示已创建 Bot、注册 webhook 或发布 Binding。
空账户时公开 HTTP listener 没有任何 Telegram 账户路由。

启用单个示例账户，可指定仓库中的示例文件：

```sh
export GATEWAY_TELEGRAM_ACCOUNTS_FILE="$PWD/deploy/compose/gateway-accounts.example.json"
# 通过外部配置注入 GATEWAY_TELEGRAM_WEBHOOK_SECRET 后启动。
```

文件只保存引用，不保存 Bot Token 或 secret 值：

```json
[
  {
    "account_id": "telegram-primary",
    "webhook_secret_env": "GATEWAY_TELEGRAM_WEBHOOK_SECRET"
  }
]
```

Gateway 从对应环境变量读取 16–256 个 URL-safe 字符组成的 webhook secret。
它是 Telegram `setWebhook.secret_token` 对应的请求鉴权值，不是 Bot API Token。
fixture 来源不自动执行 BotFather 创建或 `setWebhook` 注册，也不从这个文件构造出站
Bot Token 客户端。生产 Control 来源已接账户 Owner、凭据解析/轮换与 `setWebhook` 注册；
两种来源都不把 webhook secret 当作出站 Bot Token。

文件可以声明最多 100 个稳定账户身份，secret 引用可不同；当前 Compose 只显式
透传了示例变量。增加其他引用时，需在自己的 overlay 中同时把那些环境变量传入
`channel-gateway`。建议使用宿主机绝对文件路径，避免 Compose 相对路径基准歧义。

### 企业微信账户配置（显式 fixture 来源）

企微是 Gateway 内的可选账户功能，不是独立 service/profile/端口。本节文件/环境配置
仅用于显式 fixture overlay；生产来源由 Control 账户目录和凭据解析提供。fixture 默认目录
`deploy/compose/wecom/` 内 accounts.json 为 []，零账户可以启动且不拨号。

| 配置 | 含义 |
| --- | --- |
| `GATEWAY_WECOM_ACCOUNTS_FILE` | **进程**读取的文件路径；直接运行二进制时设置。Compose 固定为 `/etc/gateway/wecom/accounts.json` |
| `GATEWAY_WECOM_ACCOUNTS_DIR` | **Compose 宿主机**目录挂载来源；建议绝对路径，容器中只读挂载整个目录 |
| `GATEWAY_WECOM_BOT_SECRET` | 示例唯一透传的凭据环境变量；只在获 lease 后解析。新增 secret_env 引用须在自己的 overlay 显式透传 |
| `GATEWAY_INSTANCE_ID` | 可选显式实例标识；省略时每进程生成。多个实例应使用各自身份，不共同固定成相同值 |

首次配置可在仓库外准备非秘密账户文件：

```sh
export GATEWAY_WECOM_ACCOUNTS_DIR="$HOME/.config/channel-gateway/wecom"
mkdir -p "$GATEWAY_WECOM_ACCOUNTS_DIR"
chmod 700 "$GATEWAY_WECOM_ACCOUNTS_DIR"
cp deploy/compose/wecom/accounts.example.json "$GATEWAY_WECOM_ACCOUNTS_DIR/accounts.next.json"
# 编辑 accounts.next.json：填写已核验的 Bot ID/稳定账户 ID，保持以下五字段，勿填 Secret 值。
# 编辑并复核完毕后，在同一目录发布：
mv "$GATEWAY_WECOM_ACCOUNTS_DIR/accounts.next.json" "$GATEWAY_WECOM_ACCOUNTS_DIR/accounts.json"
# 再从外部秘密配置注入 GATEWAY_WECOM_BOT_SECRET，执行 just compose-config / just compose-up。
```

文件格式（全部字段必需）：

```json
[
  {
    "account_id": "wecom-account",
    "bot_id": "REPLACE_WITH_VERIFIED_BOT_ID",
    "revision": 1,
    "enabled": true,
    "secret_env": "GATEWAY_WECOM_BOT_SECRET"
  }
]
```

- 最多 100 个账户；拒绝未知/重复字段、缺字段、非整数 revision、重复账户/Bot 与无效引用。
  revision 是连接配置代次，独立于 Routing 的 generation；所有副本更新必须单调。
  同 revision 修改 enabled/secret_env 属于冲突，旧 revision 拒绝；Bot 身份绑定后不更换。
- 需要热更新文件时，在**同一宿主目录**写完整临时文件，再 atomic rename 到 accounts.json。
  Compose 挂载目录而非单文件，下一次轮询重新打开路径；默认轮询 1 秒，不是全副本发布 SLA。
- **停用**使用更高 revision + enabled=false，并分发到所有副本；数据库更新使旧 grant 的
  Renew/Check/新接纳失效。仅删去文件条目不是全局禁用，其他副本可能仍持有旧配置。
- **凭据轮换**：先把新环境引用/值注入全部目标进程，再将文件 revision 递增、secret_env
  指向新引用。若值没有预先注入，须重建/重启进程并注入新的环境；同名引用更换值也应
  协调递增配置 revision。修改宿主 shell 的 export 不会修改已经运行容器/进程的环境。
  引用名变化不代表 Bot 身份变化，Secret 不写入 accounts.json 或普通日志。
- **replaced** 的当前 revision 持久隔离，普通重启/lease 过期不解除；只允许可信更高配置
  revision 恢复。若隔离写失败，不宣称跨副本封禁成立，也不主动 Release 加速接管。
- 默认 Adapter 临时接纳最多 6 次/2s，100ms 起步封顶 400ms，重试保持同一归一化输入；
  耗尽后由明确 Retryable 交给 Supervisor。Supervisor 最多 3 次 1/2/4s 快速重建，之后
  每 60s 一次半开；短暂 Ready 不清预算。预算在本进程/账户/revision 内，不跨重启持久化。
- 单 Bot 不 ready、认证错误、被替换或被其他副本持有，不导致共享 readiness 立即失败；
  完整配置源、Supervisor 主循环、PG、Routing 与预算门禁仍需正常。当前 Control 来源
  已上报账户 observations，但共享 /readyz 仍不表示某个 Bot 一定可用，也不等于完整可观测性。

获得连接只说明本地租约/协议链路建立。新消息仍需有效 Routing 投影；忽略/交互决策也
通过 owner guard。可回复的首次 Admission 另存原 owner/epoch/revision/socket generation；
相同事件跨 owner 重放仍保留首次 Origin。显式 fixture 来源和其测试发布者、订阅者、
committed-Final fixture 不是真实 Control/Worker。完整 Worker 链路使用生产 Control 来源。

## 3. NATS 声明、生成物与四个角色

两份声明各自拥有不同的事实：

```text
streams.yaml ─────→ channel-gateway reconcile ──→ JetStream Streams / Consumer
permissions.yaml → channel-gateway nats-config → server.conf → nats-server ACL
```

- `deploy/nats/streams.yaml` 声明四条 Stream 的 subject、retention、容量和副本数。
- `deploy/nats/permissions.yaml` 声明角色与 secret 环境变量引用。
- `deploy/nats/server.conf` 是生成物，只保存 `$NATS_*_PASSWORD` 引用；NATS Server
  启动时解析真实值，生成命令不会展开 secret。Worker overlay 由 `server-tls.conf`
  include 原 ACL 并启用 TLS；Control/Worker/Gateway/reconciler 显式信任 NATS CA。
- Gateway/Worker 运行角色只读取、校验现有 topology；创建动作由 `reconciler` 执行。
  已存在但不兼容的 topology 会报错，reconcile 不静默改写 retention 或破坏历史。

| 角色 | 当前发布/读取权限 |
| --- | --- |
| `control` | 发布 Route/Manifest，读取对应 Stream Info，接收 `_INBOX.control.>` PubAck |
| `gateway` | 发布 Run；读取 Route/Run/Reply Info；拉取/ACK Route 和 Reply durable；接收 `_INBOX.gateway.>` |
| `worker` | 只发布 Reply；读取 Run/Manifest/Reply Info；拉取/ACK Run 和 Manifest durable；接收 `_INBOX.worker.>` |
| `reconciler` | 管理 `$JS.API.>`，接收 `_INBOX.reconciler.>` |

运行角色没有 Stream 创建/删除权限，也没有跨 owner 发布权限。Reply consumer 在 Control
来源下启动，复用已有 Delivery Acceptor/Runner/Sender，不新增第二个投递循环。

| Stream | Subject | 当前保留语义 / durable |
| --- | --- | --- |
| `CHANNEL_ROUTES_V1` | `control.channel-route.v1` | Limits、64 MiB、单条 16 KiB；`channel-gateway-routes-v1` |
| `RUN_REQUESTS_V1` | `execution.run-requested.v1` | WorkQueue、256 MiB、单条 1 MiB；`agent-worker-runs-v1` |
| `RUNTIME_MANIFESTS_V1` | `control.runtime-manifest.published.v1` | Limits、256 MiB、单条 1 MiB；`worker-manifests-v1` |
| `REPLY_INTENTS_V1` | `execution.reply-intent.v1` | WorkQueue、256 MiB、单条 1 MiB；`channel-gateway-replies-v1` |

四条 Stream 当前为 FileStorage、单副本、无 age 自动淘汰，容量耗尽时拒绝新消息，
而不是淘汰旧消息。删除/清空禁用；Route/Manifest 的 ACK 不删除历史；尚未引入来源 GC。
Run 的 WorkQueue ACK 发生在 Worker durable 接管/拒绝后，不是执行完成，也不是 Telegram
webhook 的 HTTP ACK。Reply 的 ACK 发生在 Gateway durable 结果/拒绝落库后，不是远端已读。
对 PG/proof 临时错误保持重投；Delivery 已有 receipt 先重放，再补 transport receipt。

权限声明变更后生成配置：

```sh
just nats-config
```

这只重新生成 `server.conf`；变更权限后还需重启/重载相应 NATS 部署。
`streams.yaml` 变更则需要重新运行 reconciliation，并处理严格兼容校验结果。

## 4. 启动、检查与停止

下列 `just compose-*` 为 base/local 历史入口，只启动 Control、Gateway 与 PG/NATS；
完整本地平台请改用 `just managed-up`。完整 Worker V1 使用 [新指南 §7](WORKER_V1.md#7-构建校验与启动)
固定三个 Compose 文件组合，并先注入显式配置目录、证书、Session DSN 与 release pin。
已准备相应 base/local 运行配置时，从仓库根目录执行：

```sh
just compose-config
just compose-up

curl --fail http://127.0.0.1:8080/healthz
curl --fail --output /dev/null http://127.0.0.1:8091/livez
curl --fail --output /dev/null http://127.0.0.1:8091/readyz

docker compose -f deploy/compose/compose.yaml -f deploy/compose/compose.local.yaml ps --all
```

- `compose-config` 使用 quiet 配置校验；`compose-up` 本地构建并等待服务。
- 默认回环映射：Control `8080`、Gateway public `8090`、Gateway admin `8091`。
  覆盖变量分别为 `CONTROL_API_HTTP_PORT`、`GATEWAY_HTTP_PORT`、`GATEWAY_ADMIN_PORT`。
- `8090` 只承载 `POST /v1/telegram/{account_id}`；`/livez` 与 `/readyz` 仅在 `8091`。
  对外反向代理只应转发 public listener，admin listener 留在管理网络。
- 两个一次性 job 成功时状态为 exited 0，其他服务运行；reconcile 成功输出
  `NATS_RECONCILE=PASS`。
- Gateway 的 `/readyz` 返回 204 表示 route replay 已初始化且无已检测的缺口/来源陈旧/
  超限连续积压/quarantine，数据库可查询且预算未饱和。已有 route row 数量不是 readiness 条件。
  0004 迁移新增持久积压起点；来源观察年龄上限 5 分钟，连续已知积压满 60 秒另行拒绝
  新 Run。部分推进和重启不续期，完整追平才恢复；旧 Receipt 重放保持原结果。
- NATS 短时断连、尚未越过 freshness/budget 限制时，readiness 可保持 204 并带
  `X-Gateway-State: degraded`。超限后返回 503。
- `CONTROL_BOOTSTRAP_MODE` 默认 `auto`；已有 Operator 时不重复创建。
- 资源出站 Host 由 Deployment Compiler 从 Profile 推导并写入 Manifest；同一发布固定 expected contract digest。
- Control 在监听前执行自己的迁移；`/healthz` 返回204，与 Gateway 双 listener 的探针各自独立。

也可直接使用镜像中的同一个二进制检查内部管理端口：

```sh
docker compose -f deploy/compose/compose.yaml -f deploy/compose/compose.local.yaml \
  exec channel-gateway /channel-gateway probe http://127.0.0.1:8091/readyz
```

停止容器并保留命名数据卷：

```sh
just compose-down
```

Shutdown 先关闭新 Admission gate，再有界 drain 已进入处理的请求；Connection 正常 drain
期间续租，Client 关闭后释放 lease，失租/期限耗尽则取消。新增 Sender reservation 的 Drain
覆盖在途发送与有界结果落账。默认 Maintenance 随 App 取消并在关闭 PG/NATS 前完成退出；
Control 来源的生产 Runner 已启用并独占 Maintenance；Worker overlay 显式配置足够的
stop grace period。PostgreSQL Outbox 保留未完成
投递，重启后继续处理。命名卷保存 PostgreSQL 与 JetStream 数据；停止容器不删除账户
identity/epoch 或 replaced 隔离。

## 5. 验证入口与完成边界

```sh
just gateway-build
just gateway-test
```

真实数据库与 broker 验证需指向专门的测试实例：

```sh
# 外部注入 GATEWAY_TEST_DATABASE_URL 和 GATEWAY_TEST_NATS_URL。
# 该 broker 必须为独立测试实例；此开关允许测试重置其 Streams。
export GATEWAY_TEST_ALLOW_NATS_RESET=1
just gateway-integration
```

单纯 `go test` 成功不证明 PostgreSQL/NATS 已实测：未提供测试变量时相应测试会 Skip。
集成测试可能重建固定命名的测试 Streams；使用与运行服务分离的测试 broker。
具体已执行结果以本轮 implementation-status 和审计输出为准。

历史企微 Adapter、Connection owner/lease 与直接 SDK 装配的实际树/镜像结果保留于
[实施状态](../../docs/architecture-next/channel-gateway/implementation-status.md) §9；Final/0006
记录保留于 §10。2026-09-05 Runtime 历史验收见 §11，当前 Control 运行接线和真实
Telegram 入站验收见 §13–14，不以旧镜像或旧成功数字替代。

当前源码已有 Delivery Final barrier、Acceptor、PG Ledger、Dispatcher、Telegram Sender、
Connection 受限 reservation 与 WeCom Sender；本地纵切已用真实 PG + httptest HTTP/WS 验证
原回复目标、A2 后发送、ACK/UNKNOWN、Observation/Finish 和旧 Origin 拒绝。执行授权使用
显式 committed-Final fixture，不是真实 Worker/IM 账号验证。只复现这两条纵切时无需 NATS：

```sh
# 外部注入专用 GATEWAY_TEST_DATABASE_URL；未配置会 Skip。
go test -race -count=1 ./services/channel-gateway/internal/bootstrap \
  -run '^(TestTelegramDeliveryRealPGHTTPOrderedFinal|TestWeComDeliveryRealPGWSFinalAndOriginalOwnerFence)$'
```

当前 Control `bootstrap.App` 已接托管凭据解析/轮换与 Delivery Runner，并由 Runner 独占
Maintenance；Worker V1 已接 ReplyIntent NATS Consumer 和真实 Execution committed-Final
mTLS verifier，读取 Worker durable Completion，不依赖活跃 Attempt lease。
**Delivery/0006 历史切片**的实际工作树、镜像及运行验收记录于实施状态 §10.4；没有新增
Connector 部署单元。**2026-09-05 Runtime 切片**的七迁移镜像与空账户维护记录见 §11.4；
该数字不是当前十一迁移镜像验收声明。健康检查、样本 RunRequested、迁移已执行或空账户
启动都不等于 Agent E2E。精确规则见[Runtime V1](../../docs/architecture-next/channel-gateway/delivery-runtime-v1.md)。

Control Runtime Profile 的 11 个管理操作仍独立存在；显式 `CONTROL_RUNTIME_CONFIG_FILE`
启用 runtime mTLS listener、Worker 身份映射、真实 Attempt authorizer、Profile 凭据解析
及 Manifest relay/export。未注入该配置不自动启用 listener；完整 overlay 负责显式挂载。

相关说明：

- [Channel Gateway 服务](../../services/channel-gateway/README.md)
- [Control API](../../services/control-api/README.md)
- [部署所有权](../../docs/architecture-next/operations/deployment.md)
- [Runtime Profile 凭据](../../docs/architecture-next/control-api/runtime-profile-credentials.md)


### NATS 四角色密码的当前输入边界

当前 bundled NATS 2.11.8 使用不加引号的 `$NATS_*_PASSWORD` 配置引用。Server 会再次
按配置语法解析环境变量内容，不保证任意随机字符串都被当作 string；数字、布尔字面量
或特定数字/单位前缀可能导致启动失败。不要把引用包在双引号里，也不要给同一密码变量
人为嵌入引号后同时提供给 Server 和客户端。

为 bundled 部署分别生成四个独立密码时，可使用下面的字母前缀 + 随机十六进制格式：

```bash
printf 'nats_%s\n' "$(openssl rand -hex 24)"
```

每个角色独立运行一次并存入秘密配置。建议的 bundled 输入契约为
`^[A-Za-z][A-Za-z0-9_-]{31,127}$`；**当前 Compose 仍仅检查非空，没有实现该正则预检**。
预检与明确诊断属于 [CGR-28](../../docs/architecture-next/channel-gateway/design-review.md#113-cgr-28bundled-compose-的-nats-密码解析边界)
的后续工作；它不应改变通用 Gateway Auth 对外部自管 NATS 的兼容性。


## GCI2: Control 账户目录成为生产默认源

最新代码的 `LoadConfig` 默认 `GATEWAY_ACCOUNT_SOURCE=control`，生产 Compose 已切换到
该模式。上述静态账户说明仅用于显式 fixture overlay；历史仅维护的验收另按日期保留。当前 Control 模式
启动 account catalog refresh、WeCom Supervisor、Telegram registration reconciler、
Delivery Runner（复用原 Dispatcher，并独占 Maintenance 生命周期）、ReplyIntent NATS 消费、
真实 Worker committed-Final verifier 和 observations 上报。Runner 启动仍不等同真实 Agent E2E。

没有新增 Connector 容器或 Node 镜像；Telegram Go SDK 与公开 `platform/im/wecom` 在
同一个 Gateway Go workload 中使用。Helm 留到所有 workload 完成后。

生产配置：

- `GATEWAY_CONTROL_URL`: 实际 Control 内部 HTTPS origin；CA 必须覆盖其主机名。
- `GATEWAY_CONTROL_SCOPE_ID` / `GATEWAY_CONTROL_SOURCE_EPOCH`: Control 发放的固定来源身份。
- `GATEWAY_INSTANCE_ID`: mTLS principal 对应的实例 ID；每个同时运行的副本必须不同。
- `GATEWAY_CONTROL_CERTS_DIR`: 本机目录，只读挂载 `ca.pem`、`client.pem`、`client-key.pem`。
- `GATEWAY_PUBLIC_ORIGIN`: Webhook 账户按需使用的平台 HTTPS origin；纯长轮询可留空。路径由账户目录决定，不接受租户任意 URL。
- `GATEWAY_HTTPS_PROXY` / `GATEWAY_HTTP_PROXY` / `GATEWAY_NO_PROXY`: 可选出站代理；默认 bypass 本地及内部服务，不在文档或日志记录代理凭据。
- Gateway PG 身份不变；NATS 当前为四角色，Worker overlay 要求 TLS/CA。SDK Token/Secret 不由 Compose 注入。

Control 不可达时公共 listener 保持运行，但新工作拒绝且 readyz 返回 503；snapshot 恢复
后重新建立本实例资格。注册 READY 仅是某版本有界时间的观测。源 epoch 不自动信任更新。

开发静态账户只能显式附加 overlay：

```sh
docker compose -f deploy/compose/compose.yaml \
  -f deploy/compose/compose.local.yaml \
  -f deploy/compose/compose.gateway-fixture.yaml config --quiet
```

生产启动命令与真实联调前提见 `services/channel-gateway/CONTROL_RUNTIME.md`。


## Telegram 双接收模式（源码实现，发布前须联合验收）

仍使用上面的单个 `channel-gateway`，不新增 polling 或 connector 服务/镜像。Control
管理 `ChannelAccount.config.receive_mode`；新账户默认 `long_polling`，旧账户迁移为显式
`webhook`。静态 fixture overlay 仍是原 Webhook 测试入口，不冒充 Control 管理面配置。

纯长轮询的 Compose 可不配置 `GATEWAY_PUBLIC_ORIGIN`，不需要公网入站端口映射；需可用的
Telegram 出站网络、Control mTLS、Gateway PostgreSQL 和 NATS。混合模式部署的启用
Webhook 账户若缺 origin，会报告 CONFIG_INVALID 并影响 readiness，不被长轮询成功掩盖。

模式修改需先停用，再保存配置，再显式启用。保存不触 Telegram；Gateway 在新的接收
实例启动前处理旧 owner 和在途请求。只协调空 Webhook 或本平台有持久管理证据的端点；
遇到外部未知 Webhook 会报告 WEBHOOK_CONFLICT，不盲目接管。两方向都不主动丢弃积压，
持久 cursor/Receipt 不随模式清空。积压仍受 Telegram 保留期约束。

本版本使用协调升级窗口：先停止旧 Gateway 接收并排空旧预检，再部署双读 Control、
新 Gateway/Web 和迁移，最后开放新模式写入。旧程序的严格快照校验不支持任意混部；
不要先对旧 Gateway 发布含 receive_mode 的新快照。源码/镜像回退不等于远端 Webhook
恢复，已存在 LP 账户时不直接降级到 Webhook-only 构建。Helm 继续延后。

实现与验证入口见 [双模式开发记录](../../docs/architecture-next/channel-gateway/telegram-receive-modes-implementation.md)。

## Worker V1 完整部署 overlay

同库多 Schema 基础上新增 Worker、显式 Session preparation、NATS TLS、Control Channel/
Runtime 双内部 listener 与 Reply proof 接线。当前步骤、配置目录和真实验收分层以
[Worker V1 Compose 部署](WORKER_V1.md) 为准。历史真实 Telegram 入站证据仍不等于
Worker 回复验收；真实回执请与本轮实现状态及审计记录交叉核对。


## 可选 IM Tracing 栈

独立 Collector/Tempo/Grafana profile、TLS overlay、查询与可重复验证见 [TRACING_V1.md](TRACING_V1.md)。它不启动或替换现有业务服务。
