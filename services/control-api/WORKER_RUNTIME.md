# Control 的 Worker V1 运行接口

本文件描述已实现的 Control 接缝，不把 Worker 执行或真实 Telegram 验收记作 Control 的交付事实。

## 发布契约

生产 bootstrap 以 `worker-v1` 平台契约发布新 Manifest；历史 `platform-v1` 构造器与读取规则保留，
已发布正文、Digest 和 Revision 不改写。Agent/Profile 的 Draft Schema 保持原范围。

新发布要求：恰好一个 `llm` 根节点、一个 `openai_compatible` 模型、必需且唯一的
`postgres_state/session` 存储，以及空 Tool/Knowledge/callable closure。Memory 与组合后置。
Validate 和 Publish 共用 Compiler 门禁；公共 `api/schemas/deployment/v1.ValidateWorkerV1`
让 Worker 独立核对版本、固定平台 Digest、Adapter、资源闭包、端点与凭据 audience。
没有新增累计 Token 预算或隐式输出上限。

平台版本变化参与 Contract Digest。发行时重新运行以下现有命令并保存结果给所有副本，
不在进程启动时自动制造 expected 值：

```bash
go run ./services/control-api/cmd/control-api --print-deployment-contract-digest
```

上线前逐一检查已有 Binding 指向的固定 Manifest：旧版本保持不可变但不是 Worker V1 的运行契约，
应显式发布兼容 Revision，再更新 Binding；不把旧配置静默降级为单 LLM。

## 显式配置与认证

`CONTROL_RUNTIME_CONFIG_FILE` 指向绝对路径 JSON 文件；未设置时以下接口、Manifest Relay
和额外监听器都不注册。启用时配置必须完整：

```json
{
  "internal_address": ":8082",
  "tls_cert_file": "/run/runtime/control-server.crt",
  "tls_key_file": "/run/runtime/control-server.key",
  "client_ca_file": "/run/runtime/worker-ca.crt",
  "execution_url": "https://agent-worker:8082",
  "execution_ca_file": "/run/runtime/execution-ca.crt",
  "execution_cert_file": "/run/runtime/control-client.crt",
  "execution_key_file": "/run/runtime/control-client.key",
  "manifest_nats_file": "/run/runtime/control-nats.json",
  "workers": [
    {"principal_uri": "spiffe://agent-platform/worker/one", "worker_id": "worker-one"}
  ]
}
```

NATS 私有文件使用既有 closed `{url,user,password,ca_file?}` 格式，私钥/私有文件要求仅 owner
可读。远程 NATS 使用 TLS，明文连接只接受回环地址。部署方先创建
`RUNTIME_MANIFESTS_V1` / `control.runtime-manifest.published.v1` Stream；Control 仅发布，
没有运行时创建 Stream 的权限。完整 Event 限 1 MiB，Content 限 512 KiB。

内部监听器独立于 public HTTP 与 Channel 内部监听器，要求 TLS 1.3/mTLS。
认证从已验证证书的唯一 URI SAN 得到身份，逐项映射到固定 WorkerID；请求头不提供身份。

## Credential Resolve 与在线 Attempt 验证

Worker 调用既有 `POST /internal/v1/runtime-profiles/credentials/resolve`。
Control 用 mTLS 回调 Execution 的 `POST /internal/v1/execution/attempts:verify`，
共享协议位于 `api/runtime/execution/v1/attempt.go`。在线授权必须来自当前 Run/Attempt/Fence，
不是未过期签名本身；Control 在获得 Profile 锁后再次查询并核对相同 Grant。

- Execution 明确 403：`EXECUTION_UNAUTHORIZED` / HTTP 403。
- 网络、PG、未认证回调、未知状态、无效依赖响应：`EXECUTION_DEPENDENCY_UNAVAILABLE` / HTTP 503。
- 不回传原始错误、Token、DSN 或部分凭据；HTTP 客户端不重定向，不做透明重试。
- 成功批次新增 `profile_revision_number`，与 Tenant/Profile/Run/Attempt/Worker/Epoch/Manifest
  及完整 uses 集合一起校验。初始化失败后是否新建 Attempt 由 Worker 的固定策略决定。

Storage 的 `purpose=dsn` 是既有用途名，不意味着内部 `value` 包含完整 URI。管理 API
接受受限 PostgreSQL DSN 并解析固定 destination，但仅加密 password；runtime Resolve
返回授权密码。Worker 用固定 Manifest destination 与密码按 URI 规则转义组装 Session
连接，不从返回值或 Worker 自有数据库连接选择新的 host/database/schema。密码轮换不
改写目标，目的地变化仍按 Profile/Deployment 发布协议处理。

## Manifest Relay

Control 从自身 `control_outbox` 精确选取 `deployment/RuntimeManifestPublished.v1`，
以 `FOR UPDATE SKIP LOCKED` 和随机 claim token 接管，15 秒租期、5 秒发布超时。
先严格验证存储正文、所有重复身份和 canonical payload digest，再使用稳定 EventID 作为
`Nats-Msg-Id` 发布；只接受目标 Stream 的耐久 ACK。失败用显式退避重试；确定存储冲突标记
FAILED。发送成功但确认写库前崩溃可重投，由稳定身份去重；旧 claim 不可更新新 claim。

## 最小 Owner Export

`GET /internal/v1/runtime-manifests/export?limit=4&cursor=<opaque>` 采用同一 Worker mTLS 认证。
limit 范围 1–4。公共 DTO 位于 `api/runtime/control/v1/manifest_export.go`。

响应包含 `schema_version`、`snapshot_upper`、完整 `events`、`next_cursor`、`complete`。
第一页固定已提交 Outbox 的最大 `(created_at,tenant_id,event_id)`；后续按同一上界进行
C 排序 keyset 分页。Cursor 包含固定上界与当前位置，使用已有平台 Profile 密钥作
带固定 domain separator 的 HMAC；未知字段、重复参数、越界和篡改返回 400。
空集合明确返回 `snapshot_upper:null, events:[], next_cursor:"", complete:true`。
PENDING、PUBLISHED、FAILED 的不可变发布正文都属于导出来源，不依赖 Relay 今日状态。

恢复顺序必须是：先创建保留增量的 durable → 完整分页导出 → 消费 durable 的保留增量 →
检查 pending/保留窗口。并发事务可能在旧 cursor 以下提交，预先创建的 durable 负责覆盖该窗口，
因此分页上界不是跨 HTTP 的数据库快照事务。超过 Broker 保留窗口时重新执行 Owner 导出；
不使用公开 `manifest_view`、Draft 或直接跨 Schema 读表补正文。V1 不清理历史 Manifest/Outbox。

## 验证入口

```bash
go test ./api/events/control/v1 ./api/schemas/deployment/v1 ./api/runtime/...
go test ./services/control-api/...
CONTROL_TEST_DATABASE_URL='<独立测试管理员 DSN>' \
  go test ./services/control-api/internal/deployment/adapter/outbound/postgres \
  -run TestManifestDistributionAgainstPostgreSQL -count=1 -v
CONTROL_MANIFEST_TEST_NATS_URL='<独立测试 broker>' \
  go test ./services/control-api/internal/deployment/adapter/outbound/nats \
  -run TestManifestPublisherAgainstNATS -count=1 -v
```

PG 测试只创建并清理随机测试 Schema。NATS 测试要求指定 Broker 尚无 Manifest Stream，
只创建并清理该测试自有 Stream；不替换已有 Stream。
