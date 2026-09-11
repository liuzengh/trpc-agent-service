# Channel Gateway

Channel Gateway 是一个独立 Go Workload：**一个二进制、一个镜像、一个服务部署单元**。
Routing、Admission、Connection、Delivery 是内部业务 Module，不是四个微服务。
公开 Go Connector 保持独立于 Gateway 业务的顶层 Go 包；企业微信已在 Gateway 进程内
直接导入 `platform/im/wecom`，不增加独立 Node Connector 镜像或部署单元。

## 1. 当前已实现的纵向切片

```text
Control route event（Control 已发布绑定的真实 Relay）
  → NATS route consumer
  → 严格 schema 校验 + 完整历史 replay/checkpoint/quarantine
  → PostgreSQL Routing Projection

Telegram webhook
  → 账户绑定路由 + secret 鉴权 + 规范化
  → Admission Receipt / Inbox
  → 固定 RouteSnapshot + Admission + RunRequested Outbox（同事务）
  → NATS execution.run-requested.v1

可选 WeCom account
  → Connection Supervisor / PG owner lease
  → 公开 Go SDK connect/subscribe/callback
  → 规范化 + 同事务 owner guard
  → 共用 Admission / RunRequested Outbox
  → 首次 ReplyOrigin 仅另存于 Gateway Admission
```

- Routing 只消费已发布事实，不访问 Draft 或编译 RuntimeManifest。
- Telegram 采用 `github.com/go-telegram/bot/models` v1.25.0 DTO；自有 webhook
  Handler 在持久 Admission 成功后才返回 HTTP 200，不使用 SDK 的提前 ACK 队列。
- Telegram 入站首期只启用真实非 Bot 用户的私聊非空文本。Callback 记录为 interaction，
  group/edit/bot/service/media/未知能力记录为 ignore，均不变成 prompt。
- 同一外部事件永久对应首次 Receipt；摘要改变返回冲突，路由切换不会重新创建 Run。
- 执行事件通过版本化 JSON Schema 和 generated DTO 验证后写入 Outbox。
- Outbox 到 JetStream 是 at-least-once；稳定 EventID 在重试中保持不变。

**默认服务模式已接线**：Control mTLS 账户目录/凭据、Routing、动态 Telegram 注册/Admission、Outbox relay 与可选 Connection/企微
入站；Delivery Maintenance 在空账户时也维护旧账本；Control 模式由 Runner 独占其生命周期，fixture 模式由 App 独立维护。
新企微文本首次接纳时把 owner instance/epoch/config revision/socket generation
保存到 `reply_origin`；它不进入 SourceDigest、持久 input JSON 或 Execution wire。重放保留
首次 Receipt 和 Origin，旧库 NULL 不推测成当前 socket。

**Delivery 本轮已有源码及本地纵切**：Acceptor、PG Ledger、Dispatcher、严格 ReplyIntent
Adapter、Telegram SDK Sender、Connection 原始会话 reservation 与 WeCom Sender。
发送按 A1 claim → A2 CALLING → Provider → Observation/Finish 处理；UNKNOWN Final 不普通
自动重发，已失效 Origin 不迁移到新 socket，reservation 覆盖调用和有界结果落账窗口。

**Worker V1 Reply 接线已实现**：Control 模式的 `bootstrap.App` 绑定 ReplyIntent NATS
Consumer、mTLS committed-Final verifier，并复用已有 Runner/Sender；见本文末节。历史 HTTP/WS
Delivery 纵切中的 `committedFinalFixture` 仍是测试替身，不代表真实模型或真实 Telegram 账号验收。
完整 Worker/IM E2E 以 Worker 的独立验收记录为准；Telegram Callback 业务命令与
`answerCallbackQuery` 仍未接线。

历史 Delivery/0006 的六迁移镜像与本地验收保留于实施状态 §10.4。当前 Runtime 新增
Maintainer、Runner、LocalOwner、0007 与 CGR-36 分类，**最终验收已通过**，见
[实施状态 §11](../../docs/architecture-next/channel-gateway/implementation-status.md#11-delivery-runtime-v1实现与本轮验收)；不复用旧镜像数字。

## 2. 代码所有权

```text
services/channel-gateway/
├── cmd/channel-gateway/           # 单二进制入口与工具模式
├── internal/
│   ├── bootstrap/                # 配置、composition、双 listener、shutdown
│   ├── routing/                  # Projection / replay / generation guard
│   ├── admission/                # Receipt / 同事务 Inbox+Admission+Outbox
│   │   ├── domain/
│   │   ├── application/
│   │   └── adapter/
│   │       ├── inbound/{telegramadapter,wecomadapter}/
│   │       └── outbound/postgres/ # 首次 ReplyOrigin、只读回复快照
│   ├── connection/               # owner/epoch、Supervisor、受限 Sender reservation
│   │   ├── domain/
│   │   ├── application/
│   │   └── adapter/outbound/{postgres,wecomclient}/
│   ├── delivery/                 # Control 模式启用 Maintenance/Runner；ReplyIntent Consumer + mTLS 完成证明
│   │   ├── domain/               # Final barrier、分片、certainty、retry policy
│   │   ├── application/          # Acceptor / Dispatcher / Maintainer / Runner / ports
│   │   └── adapter/
│   │       ├── inbound/eventadapter/
│   │       └── outbound/{postgres,telegram,wecomadapter}/
│   └── infra/nats/               # 声明加载、ACL生成、topology校验/协调、传输
├── migrations/                   # Gateway 自己的 schema 与执行器
└── Dockerfile                    # distroless nonroot，单 Go 二进制
```

Connection / Delivery 是同一 Workload 内的真实 Module；目录存在不代表默认部署已启动
所有用例。公开协议库位于仓库顶层 `platform/im/wecom`，不持有 PG/NATS/租约/业务账本。
跨 Workload wire 位于 `api/events/control/v1` 和 `api/events/execution/v1`；
传输 DTO 位于 `gen/events/...`，内部 Domain 不依赖 Provider SDK 或传输 DTO。

## 3. 构建与二进制模式

从仓库根目录：

```sh
just gateway-build
```

生成 `bin/channel-gateway`。同一二进制支持：

| 调用 | 作用 |
| --- | --- |
| `channel-gateway` | 运行 Gateway 服务 |
| `channel-gateway reconcile` | 使用独立 reconciler 凭据，按 topology 声明创建/校验 NATS 资源 |
| `channel-gateway nats-config permissions.yaml` | 输出 NATS `server.conf`，只包含 secret 环境引用 |
| `channel-gateway probe URL` | 两秒超时的 HTTP probe；仅 204 算成功 |

对应 `just` 命令还包括 `gateway-reconcile` 和 `nats-config`。`reconcile` 成功打印
`NATS_RECONCILE=PASS`。服务模式只验证已有 topology，不使用管理员身份静默修复。

## 4. 服务配置

| 环境变量 | 说明 / 默认值 |
| --- | --- |
| `GATEWAY_DATABASE_URL` | 必需；常驻连接使用 `gateway_runtime` role，其默认 `search_path` 指向 `gateway` schema |
| `GATEWAY_MIGRATION_DATABASE_URL` | 必需；启动迁移使用独立 `gateway_migrator` role，指向同一 database 与 schema；不回退到运行连接 |
| `GATEWAY_NATS_URL` | 必需；NATS 地址 |
| `GATEWAY_NATS_USER` / `GATEWAY_NATS_PASSWORD` | 成对配置；Compose 服务使用 `gateway` 用户 |
| `GATEWAY_NATS_TOPOLOGY_FILE` | 默认 `deploy/nats/streams.yaml`；容器挂载到 `/etc/gateway/streams.yaml` |
| `GATEWAY_NATS_CA_FILE` | 可选 broker 信任 CA 文件；配置后 `GATEWAY_NATS_URL` 的每个地址必须为 `tls://`，保留证书链和主机名验证；reconcile 命令同样读取 |
| `GATEWAY_HTTP_ADDRESS` | public listener，默认 `:8090` |
| `GATEWAY_ADMIN_ADDRESS` | 独立 admin listener，默认 `:8091` |
| `GATEWAY_ACCOUNT_SOURCE` | 默认 `control`；`fixture` 显式启用开发账户文件，生产拒绝静态账户输入 |
| `GATEWAY_TELEGRAM_ACCOUNTS_FILE` | 仅 fixture 模式必需；最多 64 KiB 的 JSON 数组文件，允许 `[]`，最多 100 个账户 |
| 每个账户的 `webhook_secret_env` 所指变量 | 16–256 个 `[A-Za-z0-9_-]` 字符；只在运行时读取 |
| `GATEWAY_WECOM_ACCOUNTS_FILE` | 仅 fixture 模式的可选企微账户投影文件；空数组不拨号；字段为 account_id/bot_id/revision/enabled/secret_env |
| `GATEWAY_INSTANCE_ID` | Control 模式必需且匹配 mTLS 身份；fixture 模式省略时生成进程实例 ID，多副本各自独立 |
| 每个企微账户的 `secret_env` 所指变量 | 取得 owner lease 后由 Connection 解析；文件仅保存引用 |
| `GATEWAY_WECOM_URL` | 可选 SDK 连接地址覆盖；未指定时采用公开 SDK 默认地址，本地 WS 测试显式注入 |

生产所需 mTLS 文件、scope/source epoch、公网 origin 见 [Control Runtime](CONTROL_RUNTIME.md)。以下为显式 fixture 账户示例：

```json
[
  {
    "account_id": "telegram-primary",
    "webhook_secret_env": "GATEWAY_TELEGRAM_WEBHOOK_SECRET"
  }
]
```

配置的稳定 `account_id` 绑定 `POST /v1/telegram/telegram-primary`，不从 Provider
请求中的 account/tenant/binding 字段选择内部身份。Webhook secret 对应 Telegram
注册时的 `secret_token`，不是 Bot Token。上述静态 fixture 不做远端注册；Control 模式通过独立用途读取托管凭据并协调 webhook。

空账户数组用于先启动基础设施。它不代表 Bot 已创建或已配置，不代表有可接纳新 Run
的 Binding；也不会注册任何账户 webhook 路由。`readyz` 表达基础设施/投影状态，
不是“至少配置了一个 Bot”检查。

V1 部署使用 PostgreSQL 17.6，共用同一 database，Control、Gateway、Worker 与 runtime Session 分别使用
`control`、`gateway`、`worker`、`runtime_session` schema 和独立角色；Gateway 不通过跨模块
SQL 访问其他 schema。部署阶段创建 schema/role 并固定角色默认 `search_path`，业务代码不动态
切换 schema。启动先检查迁移与运行连接的实际 `current_database()`、`current_schema()` 与
`current_user` / `session_user`：两个身份必须一致（直接以部署 role 登录），连接配置的 host/port/database 必须相同，实际目标 database/schema 必须一致且角色不同，schema 不是 `public` 或系统 schema，
迁移角色必须是 schema owner 并持有 CREATE，且不持有 superuser、createdb、createrole、replication
或 bypassrls；运行角色既不持有上述角色特权，也不拥有 schema 或 CREATE 权限。生产启动固定
绑定 `gateway` / `gateway_migrator` / `gateway_runtime`，两个 URL 同时误配为另一 Workload
也在执行迁移前拒绝。测试私有入口可显式指定随机 fixture identity，配置文件和环境变量没有此覆盖项。

检查通过后，仅迁移连接依次执行 0001–0011，并在同一迁移事务提交前移除运行角色对
`gateway_schema_migrations` 的写入、TRUNCATE、REFERENCES 与 TRIGGER 权限。业务表默认 DML
授权不代表运行身份有权修改迁移历史。迁移连接关闭后，App 只保留运行连接；启动错误不输出 DSN。
历史迁移 SQL 和摘要保持不变：0004 持久化路由积压 episode，0005
拥有 Connection 账户/epoch/lease/replaced 隔离，0006 新增 Admission 的 nullable
`reply_origin` 和 Delivery intents/parts/attempts/observations 账本。旧 Admission 的 Origin
保持 NULL；0007 仅新增 Runtime 查询五索引，不改旧 SQL 或业务事实、不释放容量。
0008–0010 分别增加 Control 账户目录、发送资格绑定与 Telegram 注册账本。
0011 新增 Reply transport terminal receipts。独立维护与发送循环分开，Control 模式启动
Reply Consumer 和已有 Runner；既有迁移记录保留，
Gateway 数据与 Control schema、Profile 凭据解析职责分离。
详见 [本地 Compose](../../deploy/compose/README.md)。

独立 PostgreSQL 角色契约测试使用已显式 provision 的一次性 V1 数据库，无需 NATS：

```sh
# 两个连接分别由测试环境注入；使用角色默认 search_path，不附加临时 SET。
GATEWAY_V1_TEST_DATABASE_URL="$GATEWAY_DATABASE_URL" \
GATEWAY_V1_TEST_MIGRATION_DATABASE_URL="$GATEWAY_MIGRATION_DATABASE_URL" \
go test ./services/channel-gateway/internal/bootstrap -run TestGatewayV1 -count=1
```

另注入 `GATEWAY_V1_TEST_ADMIN_DATABASE_URL` 可运行隔离 schema 的迁移身份负例，验证管理员
迁移、startup role 伪装、非 owner 迁移与其他 Workload 配置在 DDL 前拒绝，账本仍不存在。
该测试实际迁移并验证重复启动、运行角色的业务 DML/禁止 DDL、迁移账本只读与同角色配置拒绝。
缺少上述两个环境变量时测试跳过，不代表真实 PostgreSQL 已验收。历史 App 集成 fixture 仍使用
专用测试 schema，但现在由管理员仅 provision，显式创建普通 owner/migrator 与独立运行角色，并移交已播种表的
ownership；App 的迁移和业务执行都使用普通部署身份。

## 5. HTTP、健康与关闭

| Listener | 路由 | 行为 |
| --- | --- | --- |
| public `8090` | `POST /v1/telegram/{configured_account_id}` | 鉴权、规范化、等待持久 Receipt 后返回 200 |
| admin `8091` | `GET /livez` | 进程存活返回 204 |
| admin `8091` | `GET /readyz` | route replay/来源年龄/连续积压/quarantine 与 PG 预算通过；若启用 Connection，还要求共享配置源及 Supervisor 主循环正常；通过返回 204，否则 503 |

Public listener 不注册 `/livez`、`/readyz`。Admin listener 应留在管理网络。单个 Bot 未认证、
standby 或失败不自动撤销整个 Gateway readiness；204 不代表发送 Runner 已运行或 Final 可发送。
Maintenance 已独立启动，但当前 probe 未映射其 Snapshot 失败状态。

Webhook 拒绝非法方法、错误鉴权、超过 1 MiB 的 body、重复 JSON key 和多余 JSON
对象。账户身份来自注册配置；收到的原始 Provider JSON 只在请求期间存在。
HTTP 状态：401 鉴权失败、400 输入错误、413 超限、409 事件摘要冲突、503 暂时不可用。

默认新接受预算：未发布 Outbox 10,000 条、最老 10 分钟、新 Inbox 每固定分钟窗
10,000 条；后者也约束 ignore/interaction。默认 receipt lookup 与新接受各 128 并发，
单次接受总预算 10 秒。这些是配置初值，不是压测容量承诺。真正的容量门禁位于
PostgreSQL 接受事务内，readiness 只观察结果。

路由健康区分 5 分钟来源观察年龄与 60 秒连续已知积压：PG 保存积压起点，部分进展、
观察和重启不续期，完整追平才恢复。Resolve 与接纳事务 guard 同时执行门禁；旧 Receipt
仍可重放。它不是 Control 发布到停用的端到端 SLA。
NATS 短时断连且尚未越过 freshness/budget 边界时可报告 degraded 并继续使用 Outbox。
路由历史缺口、同序号不同内容、投影损坏会进入显式阻断状态，不通过忽略错误继续 ready。
Shutdown 同时关闭新 Admission gate，并 drain 已进入 Commit 的请求；首次 Receipt
在停止期间仍可 replay。Connection 正常关闭先 quiesce、保持续租并有界 drain，再关闭
Client/释放 owner；失租立即取消。已实现的 Sender reservation 使调用与结果落账都计入
该 drain。默认 Maintenance 随 App 取消并参与后台退出等待，PG/NATS 在其退出后关闭；
Control 模式 Runner 同样参与有界关闭。未发布 Outbox 在重启后重试。

## 6. NATS 持久化与权限

`deploy/nats/streams.yaml` 拥有 Stream 声明；`deploy/nats/permissions.yaml` 经
`nats-config` 生成 `server.conf`，其中仍是 `$NATS_*_PASSWORD` 引用。

- `control` 只发布 Control route subject；这项 ACL 不代表生产发布者已经实现。
- `gateway` 发布 RunRequested，消费/ACK route durable，并验证 Stream/Consumer Info。
- `reconciler` 持有 topology 管理权限；与常驻 Gateway 角色分离。
- 当前尚无 Worker 角色和 Worker consumer，也没有 ReplyIntent Stream/consumer 的生产接线。
  `execution.reply-intent.v1` schema 与 eventadapter 已存在，不代表 NATS 已订阅该 subject。

Route Stream 保留完整历史，route consumer ACK 不删除历史。Run Stream 使用
WorkQueue retention，未来 Worker ACK 会释放已消费消息。两个 Stream 容量满时
拒绝新消息，不丢弃旧历史；不兼容 topology 需要显式迁移。

## 7. 验证

```sh
just gateway-test
go vet ./services/channel-gateway/...
```

真实集成验证需要专门的 PostgreSQL 与 NATS 测试实例：

```sh
# 外部注入 GATEWAY_TEST_DATABASE_URL、GATEWAY_TEST_NATS_URL。
export GATEWAY_TEST_ALLOW_NATS_RESET=1
just gateway-integration
```

未设置相应变量时测试会 Skip；带 reset 开关的 suite 会重建测试 broker 中固定命名
的 Streams，因此使用独立测试 broker。PostgreSQL Adapter 测试建立隔离 schema，
使用真实 workload migrations 和真实 routing generation guard，结束后清理。App 启动集成测试的
`GATEWAY_TEST_DATABASE_URL` 还需要创建临时 LOGIN role 和授权的测试管理权限；常驻服务仍使用
单独创建的 DML-only role。生产运行连接不承担测试环境准备。

单独验证 Admission 的真实 PostgreSQL 行为：

```sh
# 外部注入 GATEWAY_TEST_DATABASE_URL。
go test -race -count=1 ./services/channel-gateway/internal/admission/...
```

覆盖 concurrent single-winner、SourceDigest 冲突、route-switch replay、SQL 与 COMMIT
失败回滚、预算并发门禁、停止、claim CAS、过期与 crash recovery。测试 broker 的
PubAck、样本 RunRequested、服务 ready 都不等于 Agent E2E；后续需联通真实 Control
publisher、Worker 和生产 Delivery 接线，再验证外部 IM 回复闭环。

历史切片已记录真实 61 秒 PG apply-lag 红绿、旧库升级、生产 Consumer.Run 周期观察
驱动的双副本 HTTP 故障测试与 Gateway/API/gen race；SDK/Connection 的实际树和镜像历史
结果见[实施状态](../../docs/architecture-next/channel-gateway/implementation-status.md) §8.3/§9。
这些结果作为历史证据保留，本次 README 更新没有重新执行它们。

本 Delivery 修改副本已有两条本地纵切：真实 PG + Telegram SDK/httptest HTTP 的有序 Final，
以及真实 PG + Supervisor/公开 SDK/httptest WS 的企微原 callback → A2 → ACK → Observation/
Finish 与跨 owner Origin 拒绝。执行授权来自显式 committed-Final fixture，不是真实 Worker；
没有使用真实 IM 账户。单独复现这两条测试只需专用 PG，不使用/重置 NATS：

```sh
# 外部注入专用 GATEWAY_TEST_DATABASE_URL；缺失时测试 Skip。
go test -race -count=1 ./services/channel-gateway/internal/bootstrap \
  -run '^(TestTelegramDeliveryRealPGHTTPOrderedFinal|TestWeComDeliveryRealPGWSFinalAndOriginalOwnerFence)$'
```

历史切片工作树、镜像及运行结果见实施状态 §10.4，Runtime 切片见 §11。
GCI2 已在 Control 模式启动 Runner；GCI3 验证真实 Telegram 入站。ReplyIntent Consumer
和真实 Worker 完成证明仍待接线，见实施状态 §13–14。

## 8. 设计入口

- [Gateway 设计总览](../../docs/architecture-next/channel-gateway/README.md)
- [四个业务 Module](../../docs/architecture-next/channel-gateway/module-boundaries.md)
- [Delivery Runtime V1](../../docs/architecture-next/channel-gateway/delivery-runtime-v1.md)
- [设计复审](../../docs/architecture-next/channel-gateway/design-review.md)
- [Admission 实现边界](internal/admission/README.md)
- [Telegram Adapter 能力](internal/admission/adapter/inbound/telegramadapter/README.md)
- [Control route wire](../../api/events/control/v1/README.md)
- [RunRequested / ReplyIntent wire](../../api/events/execution/v1/README.md)
- [公开 WeCom Go SDK](../../platform/im/wecom/README.md)
- [Telegram 出站 Sender 与凭据边界](internal/delivery/adapter/outbound/telegram/README.md)

Helm 继续排在平台 `FINAL-INTEGRATION`，待 Control、Gateway、Worker、Local IM、
前端等所有生产 Workload 完成后统一处理。

## Worker V1 Reply 接管

Control account mode 现在同时运行 `channel-gateway-replies-v1` durable Consumer。
初始化仍由独立 reconciler 创建 Stream/Consumer，Gateway runtime 只验证和消费。
新增必填配置：

```text
GATEWAY_WORKER_URL=https://agent-worker:8093
GATEWAY_WORKER_CA_FILE=/run/gateway-worker/ca.pem
GATEWAY_WORKER_CERT_FILE=/run/gateway-worker/client.pem
GATEWAY_WORKER_KEY_FILE=/run/gateway-worker/client-key.pem
```

Worker 端需要将该客户端证书身份列入 Gateway 完成查询调用方 allowlist；
证书只证明调用方身份，正文仍必须匹配 Worker 已提交的 Completion/Final。
查询是 `POST /internal/v1/execution/finals:verify`，与 Profile 的 live Attempt 授权分离。
404、401/403、5xx、超时或不可解码的响应保持重试；只有已认证服务返回的明确 409
内容不匹配，或合法响应与请求/Admission 的字段不匹配，才形成永久业务拒绝。

Reply 使用已有严格 codec 和 Delivery Acceptor，不新增 Provider 发送循环。
`0011_reply_transport_receipts.sql` 持久记录 broker stream incarnation/sequence、
原始字节摘要、ACCEPTED/REJECTED 和稳定原因。顺序固定为：

1. 根据可信 broker identity 读取 transport receipt，重复消息直接返回原终态。
2. Delivery 业务 Receipt-first 接管；原会话目标仍由 Admission 提供。
3. 单独提交 transport terminal receipt，然后 ACK；这不是一个跨模块原子事务。
4. Delivery Runner 继续负责已有 Sender、重试、UNKNOWN 和连接恢复。

如果第 2 步提交后第 3 步失败，重放已有 Delivery receipt 后补第 3 步，即使证明服务
此时离线或原 deadline 已过也不重判。数据库/容量/证明故障只 NAK；坏 wire、确定冲突、
可信永久否决、EXPIRED、UNSUPPORTED 必须先持久拒绝再 ACK。

验证命令：

```bash
go test -count=1 ./services/channel-gateway/internal/delivery/adapter/inbound/nats ./services/channel-gateway/internal/delivery/adapter/outbound/workerhttp
# 使用专属空 NATS broker；测试只创建/清理自己的 Stream 与随机 PostgreSQL Schema。
GATEWAY_REPLY_TEST_DATABASE_URL="$DISPOSABLE_ADMIN_URL" \
GATEWAY_REPLY_TEST_NATS_URL="$DISPOSABLE_NATS_URL" \
go test -count=1 -v ./services/channel-gateway/internal/delivery/adapter/inbound/nats -run TestReplyHandoffPostgresNATSIntegration
```


### NATS TLS broker 信任

跨容器 V1 部署使用 TLS broker 地址，例如 `GATEWAY_NATS_URL=tls://nats:4222`，
并设置 `GATEWAY_NATS_CA_FILE=/run/nats/ca.pem`。Broker 服务证书 SAN 必须包含实际连接
主机名 `nats`。该 CA 验证 broker，不是 Worker/Control HTTP mTLS 的调用方证书。
NATS 用户名/密码继续按原角色 ACL 授权，通过 TLS 传输；没有跳过证书验证选项。
配置 CA 后任一 failover 地址为明文 `nats://` 均在连接前拒绝，避免重连降级。
未配置 CA 的历史本地明文 fixture 继续可用。私有 CA 轮转使用文件挂载与连接重建/重连，
配置不会将原始证书错误、URL 或密码写入启动错误。

```bash
GATEWAY_TEST_TLS_NATS_URL="$DISPOSABLE_TLS_NATS_URL" \
GATEWAY_TEST_TLS_NATS_CA_FILE="$DISPOSABLE_NATS_CA_FILE" \
GATEWAY_TEST_TLS_NATS_USER=gateway \
GATEWAY_TEST_TLS_NATS_PASSWORD="$DISPOSABLE_GATEWAY_PASSWORD" \
go test -count=1 -race -v ./services/channel-gateway/internal/infra/nats -run 'Test(ExplicitCA|NATSTrustedTLS|Declarations)'
```

该门禁实际验证可信 TLS 握手、错误 CA 与缺失 CA 拒绝，以及声明与生成 ACL 不漂移。

## 可选 OTLP Tracing（M1）

新源码增加 `GATEWAY_TRACING_CONFIG_FILE`。缺省关闭；设置时指向绝对路径的普通 JSON
文件（最多 64 KiB），内容直接是六字段对象，不包在 `tracing` 内：

```json
{
  "traces_endpoint": "https://collector.example.com/v1/traces",
  "sampling_ratio": 1.0,
  "export_timeout": "2s",
  "batch_timeout": "1s",
  "max_queue_size": 2048,
  "max_export_batch_size": 512
}
```

启用时要求 `GATEWAY_INSTANCE_ID`，Resource 环境来自 `DEPLOYMENT_ENVIRONMENT`（缺省
unspecified），版本来自构建 vcs.revision（缺省 development）。共享模块严格校验字段、
端点与容量；远端使用系统根校验的 HTTPS/TLS 1.3，本地 HTTP 只接受字面回环 IP。
不从 `OTEL_*` 继承目标/认证头，不跟随重定向；导出失败不影响业务 readiness。

> 当前状态：Tracing M1–M5 已实现，此前版本已部署到本机手测实例。以下 M1/M2/M4
> 内容为阶段历史记录；本次合并不触发部署，提交状态以 Git 引用为准。当前状态与证据见
> [Worker 实现状态](../../docs/architecture-next/agent-worker/implementation-status.md)。

bootstrap 拥有一个独立 Provider，并在业务结束后用独立 5s context 关闭。M1 当时只为公开
`/v1/telegram/` 回调建立本地 SERVER 根，不接收外部 Trace parent，不记录 URL/正文；
health/admin 不生成回调 Span。此阶段尚未将 Carrier 写入 Admission/Outbox/NATS 或
Delivery，不能据此宣称 IM 端到端 Trace；后续见
[IM Tracing V1 计划](../../docs/architecture-next/operations/im-runtime-tracing-v1-plan.md)。

### 历史阶段记录：M2 持久入站增量

Admission 在原事务新增保存 creation Carrier，Migration 为 `0012_admission_trace.sql`；
Outbox Claim 返回 Carrier，Relay 每次 publish 创建独立 Span，实际 NATS Header 保持
原 creation context 与 `Nats-Msg-Id`。重复 Receipt 链接原 creation，不改第一次保存的值。
新部署须先应用迁移；该阶段结束时手测部署尚未切换。Delivery/Reply 的持久关联仍待 M4。


## 历史阶段记录：M4 持久 Delivery Tracing

0013_delivery_trace.sql 保存首次 Reply process context；ClaimTraced 从原 Claim 查询恢复 Carrier，Dispatcher 按 part 继续 Trace。gateway.im.send 只记录真实发送，UNKNOWN 仍不自动重发。TransportReceipt replay 在原查询 Link 已持久上下文；业务摘要、两事务与 ACK 不变。专用 Worker proof mTLS 客户端显式传 W3C，不向第三方注入。上述源码已专项验证，该阶段结束时手测部署尚未切换。
