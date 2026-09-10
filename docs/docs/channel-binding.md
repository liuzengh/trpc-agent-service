# Channel Binding 与可信入站路由

> 本页是 Issue #26 的实现契约，固定控制面模型、候选路由和可信边界，并由
> `trpcservice/channels` 的领域模型、InMemory Repository 和 fake verifier 实现。Telegram
> long polling 的适配器契约见 [Telegram 长轮询 Adapter](telegram.md)；本页仍只定义控制面和
> trusted routing，不把协议运行时细节混入 Binding 领域模型。

## 目标与边界

Channel Binding 把一个外部 IM 账号绑定到同一租户的 Agent App，负责公开回调候选发现、
验签后的可信租户上下文和 runtime identity；协议适配器在此边界上完成收发、幂等和可靠投递。

本 Issue 交付：

- 稳定的 `tenant_id + binding_id` 身份、租户内唯一 `binding_key`、状态、版本和配置摘要；
- `wecom` 与 `telegram` 两种显式 Channel 类型，未知类型拒绝；
- 只保存不可逆的公开 route key 摘要、Secret Manager 引用和经过 schema 校验的非敏感协议配置；
- 显式租户边界的 InMemory Repository、乐观锁生命周期和防御性复制；
- 只以 `channel + public_route_key_digest` 查找候选的受限索引；
- 不含 Tenant/App/Secret 的一次性 `CandidateBindingContext`，以及用途绑定、过期和防重放的
  candidate-scoped Resolver/Verifier 契约；
- 验签成功后固定 `tenant + binding + app + version` 的 `VerifiedBinding`、可信 Routing Target
  和无拼接碰撞的单聊/群聊/线程 Runner identity；
- 使用 fake resolver/verifier 的离线集成测试。

控制面 migration 已落地本页的 `channel_binding` 表；企业微信 AES 解密、Telegram webhook、
HTTP Gateway、Secret Resolver、消息去重、回复 Outbox、队列和审计持久化由对应 Adapter、
runtime 和 bootstrap 共同接入，并以 deterministic/live E2E 验收。
Telegram long
polling 运行时契约见 [Telegram 长轮询 Adapter](telegram.md)，不属于本 Binding 领域模型。

## 运行时 Adapter 契约

Issue #60 在控制面模型之上定义了窄的运行时边界：`channels.Adapter` 只拥有
协议类型与关闭生命周期；`channels.PollingAdapter` 用于 Telegram 这类由通道拥有
阻塞轮询循环的实现；`channels.WebhookAdapter` 用于 WeCom 这类由通道拥有 HTTP
验签和入站处理的实现。两种实现都将已验证的协议消息规范化为同一个
`gateway.InboundMessage`，再交给 Gateway 的可信 Principal 路由。

该边界不把 Telegram long polling 和 WeCom HTTP callback 伪装成相同的传输协议。
验签、解密、供应商 SDK、poll loop 和 HTTP 生命周期继续由具体 Adapter 负责；
Gateway 的 `InboundMessage` 是共享入站契约，`outbox.Provider` 是由
Channel Provider 适配到的协议中立出站回复契约。Binding 根包只定义候选、验证和
可信路由；Provider 与回复渲染分别由 `channels/provider` 和 `gateway/replies`
拥有。Telegram 与 WeCom Provider 都以稳定的 reply/segment identity 实现出站交付。

## 控制面模型

### 领域字段

| 字段 | 约束与用途 |
| --- | --- |
| `tenant_id`、`binding_id` | 不可变稳定身份；所有 Admin 操作显式携带二者 |
| `binding_key` | 租户内唯一、规范化的小写机器键；不可变，不参与跨租户路由 |
| `channel` | 只接受 `wecom`、`wecom_aibot`、`telegram`；协议类型不能由入站 payload 覆盖 |
| `provider_account_id` | 外部 corp/bot/account 的稳定规范身份；不使用昵称 |
| `public_route_key_digest` | route key 的 SHA-256 摘要；只用于候选发现，不保存明文 |
| `app_id` | 同租户 Agent App 引用；可信路由固定它，payload/header 不能覆盖 |
| `secret_ref` | Secret Manager 的不透明引用；不保存 token、AES key 或 bot token |
| `protocol_config` | 按 Channel 的显式 schema 校验的非敏感配置；未知字段拒绝 |
| `status`、`version` | `draft → active ↔ suspended → disabled`；更新和迁移使用 expected version |
| `config_digest` | 规范化非密钥配置的 SHA-256；用于缓存失效和审计，不是授权凭据 |
| `created_at`、`updated_at` | UTC 生命周期时间，更新时单调不回退 |

`secret_ref` 可以参与 `config_digest`，但 Secret 值永远不能进入 Binding、快照、事件、日志、
trace 或缓存键。Secret Resolver 在可信 Tenant 已确定后，必须以 `(tenant_id, secret_ref)`
作为查找边界。

### PostgreSQL 形状

下面是已经由 Issue #37 migration 落地的持久化形状摘要；`protocol_config` 仍只有在应用层按
Channel schema 解码、拒绝未知字段和敏感字段后才能写入。

```sql
CREATE TABLE channel_binding (
    tenant_id                 TEXT NOT NULL REFERENCES tenant(tenant_id),
    binding_id                TEXT NOT NULL,
    binding_key               TEXT NOT NULL,
    channel                   TEXT NOT NULL
                              CHECK (channel IN ('wecom', 'wecom_aibot', 'telegram')),
    provider_account_id       TEXT NOT NULL
                              CHECK (length(btrim(provider_account_id)) BETWEEN 1 AND 256),
    public_route_key_digest   TEXT NOT NULL
                              CHECK (public_route_key_digest ~ '^[0-9a-f]{64}$'),
    app_id                    TEXT NOT NULL,
    secret_ref                TEXT NOT NULL
                              CHECK (length(btrim(secret_ref)) BETWEEN 1 AND 256),
    protocol_config           JSONB NOT NULL DEFAULT '{}'::jsonb,
    status                    TEXT NOT NULL DEFAULT 'draft'
                              CHECK (status IN ('draft', 'active', 'suspended', 'disabled')),
    version                   BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    config_digest             TEXT NOT NULL CHECK (config_digest ~ '^[0-9a-f]{64}$'),
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (tenant_id, binding_id),
    UNIQUE (tenant_id, binding_key),
    FOREIGN KEY (tenant_id, app_id)
        REFERENCES agent_app(tenant_id, app_id),
    CHECK (binding_key ~ '^[a-z][a-z0-9-]{1,63}$')
);

-- Route lookup never scans by tenant and never returns a full Binding.
CREATE INDEX channel_binding_candidate_idx
    ON channel_binding (channel, public_route_key_digest)
    WHERE status = 'active';

-- A provider account has one active owner. Shared accounts require a new,
-- explicit sharing model; they must not weaken this invariant.
CREATE UNIQUE INDEX channel_binding_active_account_idx
    ON channel_binding (channel, provider_account_id)
    WHERE status = 'active';
```

Admin Repository 的 `Create/Get/Update/Activate/Suspend/Resume/Disable` 都接收显式
`tenant_id + binding_id`。Binding key 只在 `(tenant_id, binding_key)` 内唯一，因此不同租户
可以使用相同的 key。当前 InMemory 生命周期边界校验 Binding 自身的状态、版本和 active 账号
唯一性；它不偷偷加载跨聚合的 Tenant/App。可信快照边界 `NewRoutingTarget` 必须再校验 Tenant
active、Binding active、App 属于同一 Tenant 且 App 可执行；不能因为两个对象的字符串 ID 看起来
相似就跳过复合归属检查。

## 生命周期与版本

```text
                 Activate                 Resume
       draft ----------------> active <------------ suspended
        |                       |  ^                    |
        |                       |  |                    |
        v                       v  |                    v
       disabled              suspended              disabled
              Suspend                         Disable (terminal)
```

- `draft`：可编辑，不进入候选索引；新建对象的默认状态。
- `active`：可以由候选索引发现并接受新消息；配置更新仍需 expected version。
- `suspended`：拒绝新路由，但保留配置，可以在重新校验后恢复。
- `disabled`：终态，不得恢复或重新进入候选索引。

每次写入都在锁内校验 `expected_version`，成功后递增一次版本并返回变更事件。事件只包含
租户、Binding、actor、reason、correlation ID、状态/摘要和前后版本，不包含 route key 明文、
Secret ref 的值或协议 payload。

## 候选发现与可信边界

公开 route key 是发现线索，不是授权凭据。适配器先规范化 Channel 并计算
`SHA-256(长度前缀(channel) + 长度前缀(route_key))`，Repository 只接收摘要。候选查询只接收
`channel + digest`，并返回一个或多个不透明候选；不存在、非 active 或不可用候选使用同一类
不可枚举错误，不返回 Tenant、App、Secret 或完整 Binding。

```go
type CandidateBindingContext struct {
    Channel                Channel
    PublicRouteKeyDigest   string
    BindingVersion         int64
    ConfigDigest           string
    Purpose                VerificationPurpose
    CandidateToken         string // opaque, short-lived, single-use
    IssuedAt               time.Time
    ExpiresAt              time.Time
}
```

`Purpose` 固定候选只能用于 `webhook-verification` 这一候选用途；Resolver 不得把同一个候选
转换成别的用途。`CandidateToken` 是候选索引签发的 opaque bearer capability；它不是 `binding_id`、`tenant_id`、
`app_id`、`secret_ref` 或 Secret 值，也不能由 URL、header 或 payload 自行拼接。候选 Context
及 token 都只在短 TTL 内有效，调用方修改返回副本不能改变索引内部状态。Resolver 必须把
token 绑定到唯一候选和明确用途，例如 `webhook-verification`，消费成功后返回一次性
`ScopedVerifierHandle`。Verifier 只接受供应商
的 signature/token、timestamp、nonce、密文摘要和 receive ID 等协议输入；它不能接受调用方
提供的 `tenant_id`，也不能让 payload/header 选择 App。

```mermaid
sequenceDiagram
    participant IM as IM callback
    participant A as Channel Adapter
    participant I as Candidate Index
    participant R as Scoped Resolver
    participant V as Candidate Verifier
    participant G as Gateway / trusted target

    IM->>A: public route + untrusted payload/header
    A->>I: channel + route_key_digest
    I-->>A: opaque CandidateBindingContext(s)
    A->>R: context + purpose=webhook-verification
    R-->>A: one-time ScopedVerifierHandle
    A->>V: signature/token + timestamp + nonce + ciphertext digest
    alt verification fails, expires, replays, or purpose mismatches
        V-->>A: generic verification failure
        A-->>IM: provider-safe failure/ack; no tenant identity
    else verification succeeds
        V-->>A: VerifiedBinding(tenant, binding, version, app, channel)
        A->>G: normalized message + VerifiedBinding
        G->>G: validate active tenant/binding/app and fixed versions
        G->>G: ignore payload/header tenant/app/binding fields
        G-->>A: trusted RoutingTarget + idempotency input
    end
```

`VerifiedBinding` 是验签器固定的结果，至少包含 `tenant_id + binding_id + binding_version +
app_id + channel + provider_account_id`。建立 Routing Target 时再次读取/校验 Binding、Tenant
和 App 的可信快照，并要求版本与验签结果一致。只有这个边界之后，系统才能创建 Tenant Context、
Execution Plan 或 `tenant + binding + channel + message_id` 的幂等键。

## Binding-aware identity

Runner 的 identity 不能用易碰撞的字符串拼接，也不能只依赖 Tenant 前缀。平台先用长度前缀
编码构造 Binding 作用域：

```text
Encode(parts...) = len(bytes(part_1)) + ":" + part_1 + ...

user    = Encode(channel, binding_id, external_user_id)
direct  = Encode(channel, binding_id, "direct", external_peer_id, thread_id)
group   = Encode(channel, binding_id, "group", external_chat_id, thread_id)
```

再把 `user` 与 `direct/group` session 值传给现有
`tenant.NewRunnerIdentity(tenant_id, scoped_user, scoped_session)`。因此同一个外部用户在
不同 Binding、同一个用户在不同群聊、以及不同线程都不会复用 identity；持久化层仍然必须显式
携带 `tenant_id` 和 `binding_id`，identity 只是第二层碰撞保护。

## 与现有运行时的映射

可信 Routing Target 只固定路由身份和非敏感摘要，不直接创建 HTTP 响应或真实 IM client：

1. 用 Target 的 Tenant ID 读取 active Tenant Configuration Snapshot；
2. 用 Target 的 App ID 读取同租户 active App 和当前 published Revision；
3. 由现有 `runtime.NewExecutionPlan` 固定 Agent、Model、Backend 的版本和摘要；
4. 由 Binding-scoped identity 创建 Runner `userID/sessionID`；
5. Gateway/Worker 按 Tenant-scoped Secret Resolver 获取出站或模型凭据。

Tenant、App、Binding 任一状态不是 active 时，Target 构造失败并拒绝新消息。已创建的执行
继续使用自己的固定 plan；暂停或停用不篡改进行中的快照。

## 离线验证矩阵

fake resolver/verifier 不连接外部 IM，也不把 fake secret 写进 Binding。测试必须证明：

- route digest 能发现候选，但 Candidate Context 没有 Tenant/App/Secret 字段；
- 正确 proof 生成固定版本的 VerifiedBinding 和 Routing Target；
- 错误 proof、过期、重放和用途错配统一失败关闭；
- payload/header 伪造 Tenant/App/Binding 不影响 VerifiedBinding；
- 同 Channel/账号的 active Binding 不可跨租户重复；相同 binding key 在不同租户可以存在；
- suspended/disabled Binding、非 active Tenant 或不可执行 App 不能生成 Target；
- 更新和候选消费响应 Context 取消，读写返回防御性副本，竞争更新只有一个 expected version 获胜；
- direct/group/thread identity 在不同 Binding、会话和线程之间无碰撞且同一输入稳定。

测试覆盖平台安全边界和领域闭环；WeCom、Telegram 与 WeCom AI Bot 的真实协议验签、解密、
消息幂等、出站重试和持久化一致性分别由对应 Adapter E2E 与 runtime conformance 共同验证。
