# Model Profile、Secret Resolver 与最小 Runner 链路

> 本文是 Issue #22 的设计与实现契约：
> [runtime: implement tenant-scoped Model Profile and minimal Runner execution spine](https://github.com/XnLemon/trpc-agent-service/issues/22)。
> 本文同步记录代码、迁移、测试和部署验收，代码遵守本文的租户边界、快照和生命周期语义。

## 目标与边界

Tenant、Agent App/Revision 和 Backend Profile 生成不可变、无密钥的运行时输入，
`Revision.ModelProfileID` 通过同租户 Resolver 进入 tRPC-Agent-Go 的 `runner.Runner`。
当前交付的最小纵向链路为：

```text
trusted Tenant snapshot
        │
        ├── Agent App + published Revision ──┐
        ├── active Model Profile             ├── Execution Plan
        └── active Backend Profile ───────────┘
                         │
                         ▼
          model.Factory + Storage Factory
                         │
                         ▼
            LLMAgent → Runner → Session
                         │
                         ▼
             complete Event stream / reply
```

本 Issue 的实现范围：

- 租户级 Model Profile 根实体、配置 schema、生命周期、乐观锁版本、摘要和 InMemory/PostgreSQL Repository。
- 不包含 Secret 值的 `ModelExecutionSnapshot`、可比较 `FactoryCacheKey` 和 `ModelFactoryInput`。
- 显式携带 `tenant_id + secret_ref` 的 Secret Resolver 契约，以及脱敏的错误边界。
- 将 Tenant、当前 published Agent Revision、active Model Profile 和 active Backend Profile
  组合成一次执行固定的 `ExecutionPlan`。
- 固定兼容的 tRPC-Agent-Go 版本，使用其 `LLMAgent`、`Runner`、`Session` 和 `Event` 能力。
- deterministic fake model、OpenAI provider 配置路径、Session 集成测试，以及取消和 Event channel 收尾测试。

Channel Binding、HTTP Gateway/Admin API、PostgreSQL/Redis/S3 runtime capability、OTel exporter、
计费和跨节点缓存均通过相应模块接入。Issue #37 的
`migrations/0001_control_plane.up.sql` 与 `0002_control_plane_repository_functions.up.sql`
落地本页领域契约对应的无密钥表形状和受控写入口；Go 实现在
`trpcservice/model/postgres`，总体验证见
[PostgreSQL 控制面与启动装配](postgresql-control-plane.md)。

## 交付 ledger

以下可观察契约均有代码或测试证据：

| Ledger 项 | 已交付契约 | 验证证据 |
| --- | --- | --- |
| Model Profile 身份 | `(tenant_id, model_profile_id)` 稳定；`profile_key` 仅在租户内唯一且不可变 | ID/key/时间/version/摘要边界测试 |
| 配置 schema | provider、model、endpoint、generation、allowlisted options、opaque `secret_ref`；未知 provider/model/option 拒绝 | Catalog 与配置归一化测试 |
| 生命周期 | `active`、`suspended`、`disabled`；disabled 是终态；新执行只允许 active | 状态转换和执行门禁测试 |
| 并发与存储 | 完整配置替换、expected version、Context-aware lock、线程安全和防御性副本 | InMemory Repository 测试与 race |
| Secret 边界 | Resolver 只接收可信 tenant scope；解析结果只传给 Model Factory | tenant scope、错误脱敏和无泄露测试 |
| Execution Plan | 固定所有参与对象的 ID、version、revision、content digest；拒绝跨租户/过期/非 active 输入 | Plan 构造失败场景测试 |
| Runner spine | 复用上游协议；普通消息进入 Runner，完整 Event stream 写入 InMemory Session | 离线纵向集成测试 |
| 取消与收尾 | 取消 context 后 Runner 和消费者都退出；消费者继续 drain 直到 channel 关闭 | bounded cancellation test |
| 文档与验证 | README 进度、MkDocs 导航和数据模型链接同步 | format/lint/build/test/MkDocs strict/diff check |

## 1. Model Profile 控制面

### 1.1 身份、版本和时间

Model Profile 与 Backend Profile 采用相同的租户边界约定：

- `tenant_id` 来自认证后的控制面上下文，不能由模型配置或用户消息选择。
- `model_profile_id` 由服务生成，使用 `mp_` 前缀的 Crockford/ULID 风格稳定 ID。
- `profile_key` 规范化为 `[a-z][a-z0-9-]{1,63}`，创建后不可修改；同一个 key 可以在不同租户重复。
- `version` 从 1 开始，完整配置更新和状态迁移各递增一次。
- `created_at`、`updated_at` 使用 UTC；更新时不得早于现有 `updated_at`。
- `content_digest` 是规范化配置的 SHA-256 小写十六进制摘要，不包含时间、actor 或 Secret 值。

建议的领域结构如下。字段是控制面数据，不代表运行时客户端：

```go
type Profile struct {
    TenantID      string
    ProfileID     string
    ProfileKey    string
    DisplayName   string
    Description   string
    Status        Status
    SchemaVersion int
    Configuration Configuration
    ContentDigest string
    Version       int64
    CreatedAt     time.Time
    UpdatedAt     time.Time
}

type Configuration struct {
    Provider   string
    Model      string
    Endpoint   string
    Options    map[string]string
    SecretRef  string
    Generation GenerationConfig
}
```

领域对象、Repository 返回值和快照访问器都必须返回深拷贝。特别是 `Options` 不能把调用方
持有的 map 直接留在 Repository 或 Snapshot 内部。

### 1.2 Provider schema 与 generation 参数

配置不是任意 JSON。Provider Catalog 是由受信代码注册的不可变 schema，每个 provider 明确声明：

| 字段 | 约束 |
| --- | --- |
| `provider` | 规范化的小写 provider 名称；未注册 provider 拒绝 |
| `model` | 该 provider 的 allowlist 模型名；未知模型拒绝 |
| `endpoint` | provider schema 明确允许时才接受；`EndpointSchemes` 与 `EndpointHosts` 都必须命中精确 allowlist（IPv6 host 在 schema 中不带方括号）；拒绝 userinfo、query、fragment、控制字符和超长值；Endpoint 不是 DSN |
| `options` | key 必须在 provider schema 中；值按 string/boolean/integer/enum 规则规范化；未知 key 拒绝 |
| `secret_ref` | opaque 引用；provider 可声明 required/optional/forbidden；绝不保存 Secret 值 |
| `generation` | 只允许受控的 `temperature`、`top_p`、`max_output_tokens` 等字段，并在 schema 版本中固定语义 |

`options` 的 map 只是控制面表达方式，不是未知字段的逃生通道。schema 版本不支持时必须 fail
closed；JSON 解码使用 `DisallowUnknownFields` 或等价的显式解码逻辑，不能静默吞掉新字段。
敏感 option key（如 `api_key`、`password`、`token`、`dsn`、`credential`）即使被错误注册也拒绝。

配置被接受前依次执行：

1. 校验 schema version、身份引用和字段长度。
2. 校验 provider/model/endpoint/secret_ref 和 option allowlist。
3. 物化 schema 默认值，排序稳定字段，复制所有 map/pointer。
4. 对规范化结构做确定性序列化并计算 `content_digest`。

摘要可以包含 `secret_ref`，因为切换引用必须令 Factory cache key 变化；摘要不能包含该引用
解析出的值，也不能包含 client、连接池、函数指针或其他运行时对象。

### 1.3 生命周期与乐观锁

```text
                         ┌──────────────┐
                         │   disabled   │  terminal
                         └──────▲───────┘
                                │ disable
             suspend            │
active ─────────────────────────┘
  ▲  │
  │  └── suspended ── resume ──┘
  │
  └── active 允许更新完整配置，但每次更新都产生新 version/digest
```

实际允许的迁移为：

- `active -> suspended`：阻断新执行，但保留配置并允许修复后恢复。
- `suspended -> active`：重新校验完整配置后恢复；不能恢复一个缺少必需字段的 Profile。
- `active -> disabled`、`suspended -> disabled`：终态；之后拒绝更新、恢复和新执行。
- 新建只能是 `active` 或 `suspended`，不能直接是 `disabled`。

配置更新和状态迁移都接收 `expected_version`。版本不匹配返回可识别的 conflict 错误，不能
覆盖并发修改。`InMemory Repository` 的每次操作在开始、等待锁后和提交前检查 `Context`；读锁
和写锁都必须能在取消时返回，而不是永远等待。

### 1.4 PostgreSQL 持久化形状

`model_profile` 的 SQL 根表只保存已经由受信 Provider Catalog 规范化的无密钥配置。字段、
状态、复合租户边界、唯一性、版本和摘要约束见 [PostgreSQL 控制面与启动装配](postgresql-control-plane.md)。
`options` 和 `generation` 使用 JSONB 只是为了保存有界的结构化配置，不是允许未知字段或
Secret 的逃生通道；Repository 必须严格解码字符串 option、已支持的 generation 字段和
`secret_ref` 引用，并在配置更新与 Outbox 写入的同一事务中重新计算 `content_digest`。

## 2. Secret Resolver 边界

Secret Resolver 是平台与 KMS/Vault/Secret Manager 之间的窄契约，接口、fake 测试和 Vault
适配器共同遵守以下边界：

```go
type SecretScope struct {
    TenantID  string
    SecretRef string
}

type CandidateBindingContext struct {
    Channel              string
    PublicRouteKeyDigest string
    BindingVersion       int64
    ConfigDigest         string
    Purpose              string
    CandidateToken       string // opaque, short-lived, single-use capability
    IssuedAt             time.Time
    ExpiresAt            time.Time
}

type CandidateSecretRequest struct {
    Candidate CandidateBindingContext
    Purpose   string // e.g. webhook verification/decryption
}

type SecretValue struct { /* value is private and redacted when formatted */ }
type ScopedVerifierHandle struct { /* one-time capability; not serializable */ }

type SecretResolver interface {
    ResolveCandidate(context.Context, CandidateSecretRequest) (ScopedVerifierHandle, error)
    Resolve(context.Context, SecretScope) (SecretValue, error)
}

type ModelFactory interface {
    New(context.Context, ModelFactoryInput, SecretValue) (trpcmodel.Model, error)
}
```

边界规则：

- `SecretScope` 必须同时包含可信 `tenant_id` 和 `secret_ref`；没有全局 `Resolve(ref)` 入口。
- `tenant_id` 必须来自已认证的 Tenant snapshot/Execution Plan，而非请求体、header 或模型输出。
- 公开 route 的验签发生在建立可信 `tenant_id` 之前，不能调用上面的租户级 `Resolve`。Channel
  Adapter 先用公开 route/channel 查候选 Binding，得到只含 Channel、route digest、固定版本/摘要
  和短时 opaque `candidate_token` 的 `CandidateBindingContext`；其中不带 `tenant_id`、`app_id`、
  `secret_ref` 或 Secret 值，也不接受请求体自带的 `secret_ref`。`ResolveCandidate` 在全局候选
  索引内校验 token 对应的唯一候选、Binding 状态、用途和过期时间，返回只供一次验签/解密使用的
  `ScopedVerifierHandle`。
- `CandidateToken` 由 Registry/Resolver 绑定**一个**候选 Binding、用途、签发/过期时间和单次
  消费边界后签发；不得由 URL、header、消息正文或候选 `tenant_id` 自行拼接。验签失败不创建
  租户事件，也不进入租户级存储；token 消费后不能重放。
- 验签成功并加载可信 Tenant snapshot 后，才允许按现有 `SecretScope{TenantID, SecretRef}`
  调用租户级 `Resolve`；候选 handle 和其中的 Secret 值都不得传给 Runner 或领域对象。
- Resolver 失败返回固定、可分类但不带底层凭据的错误；原始 KMS 错误不能直接进入日志、trace 或响应。
- `SecretValue` 只能作为一次 Model Factory 调用的临时参数；不得写回 Profile、Snapshot、Plan、
  `ModelFactoryInput`、cache key、audit event 或 context value。
- Model Factory 自身的错误也必须在平台边界脱敏，避免回显 token、完整 credential 或可还原 DSN。
- `SecretRef` 可以留在无密钥配置和摘要中，但 Secret 值不能出现在任何可序列化结构中。

Resolver 调用顺序分为两条路径：入站路径先做候选发现 → `ResolveCandidate` → 验签/解密 →
建立可信 Tenant；执行路径再验证 Execution Plan → 读取无密钥 `ModelFactoryInput` → 按 schema
分支处理 `secret_ref`：required 必须非空并解析一次，optional 仅在引用存在时解析，forbidden
拒绝任何引用；没有引用时不调用租户级 Resolver，并把显式的空 `SecretValue` 直接传给 Model
Factory → 丢弃临时值。候选 handle 也必须在 Channel Adapter 返回前销毁。
Factory 缓存只缓存不含 Secret 的模型配置或由 Factory 自行管理的安全 client 句柄；空 Secret
不是全局或空引用查询。

## 3. Execution Plan

`ExecutionPlan` 是一次 Worker 执行的 sealed input，不是 Repository 的聚合写模型。构造函数接收
同一个已认证的 `tenant.ConfigurationSnapshot` 和四个已校验的控制面对象：

```text
Tenant snapshot
  ├── Agent App + current published Revision
  │     └── Revision.ModelProfileID
  ├── Model Profile
  └── Tenant.DefaultBackendProfileID → Backend Profile
```

构造时连续验证：

1. Tenant snapshot 已由受保护构造器生成且为 `active`。
2. App、Revision、Model Profile、Backend Profile 的 `TenantID` 全部相同。
3. App 为 `active`，Revision 为 `published`、属于同一个 App（`Revision.AppID == App.AppID`），
   且等于 App 的 `current_revision`。
4. Revision 的 `ModelProfileID` 与传入 Model Profile ID 相同；同租户不同 App 的 Revision 也必须拒绝。
5. Model Profile 和 Backend Profile 均为 `active`；Backend Profile 仍满足其 Session binding 门禁。
6. 所有摘要、版本、时间和 schema 不变量都能重新验证。

任意一项失败都拒绝创建计划，不回退到平台默认模型、其他租户 Profile 或旧缓存。计划创建后，
控制面更新只影响新计划；本次执行继续使用计划中已经复制的版本。

计划至少固定以下可比较身份：

```text
tenant_id + tenant_version
agent_app_id + app_version + published_revision + agent_content_digest
model_profile_id + model_profile_version + model_content_digest
backend_profile_id + backend_profile_version + backend_content_digest
```

这些字段组成计划级 cache identity。它不能包含 Secret 值、HTTP client、Session service、
模型实例、channel、请求消息或可变 map。对外访问器返回防御性副本，不能让调用方反向修改计划。

计划向下游暴露三份无密钥输入：

| 计划部分 | 下游 |
| --- | --- |
| `AgentExecutionSnapshot.FactoryInput()` | `llmagent.New` 的 name、description、instruction、generation 和 runtime policy |
| `ModelExecutionSnapshot.FactoryInput()` | Model Factory 的 provider、model、endpoint、options、secret_ref 和版本/摘要 |
| `BackendExecutionSnapshot.FactoryInput()` | Storage Factory 的 Session/Memory/Knowledge/Artifact binding |

平台层只负责组合、授权和固定版本，不复制 tRPC-Agent-Go 的 Agent、Runner、Session 或 Event 协议。

## 4. 最小 Runner execution spine

当前固定 `trpc.group/trpc-go/trpc-agent-go` 的兼容版本 `v1.11.2`，版本写入根 `go.mod` 和
`go.sum`，避免上游接口漂移影响控制面契约。

Runner 装配逻辑为：

1. 从 Execution Plan 读取 `AgentFactoryInput`、`ModelFactoryInput` 和 `StorageFactoryInput`。
2. 通过显式 Tenant scope 解析 Secret，并把结果直接交给 Model Factory 得到 `model.Model`。
3. 将 Agent 输入的受控 generation 参数映射到上游 `model.GenerationConfig`，构造
   `llmagent.New`；`RuntimePolicy` 同步映射为上游的 LLM/tool call limits、
   `WithEnableParallelTools`、`ToolConcurrencyConfig.MaxConcurrency`，并在 Runner 边界
   固定 `WithMaxRunDuration`；不把 Revision 的未知字段或平台配置 map 直接传给上游。
4. 将 Backend 的 Session binding 映射到已选择的 Session service；集成测试覆盖上游
   `session/inmemory.NewSessionService()` 与 PostgreSQL/Redis runtime adapter，并在其外层
   保持固定 Tenant 的 `TenantSessionService` 边界。
5. 由 `trpcservice/agent` 使用上游
   `runner.NewRunner(appID, llmAgent, runner.WithSessionService(sessionService))`，
   并由同一适配边界负责调用、错误脱敏、事件归一化和取消时的 bounded drain。
   `runtime/execution` 不直接依赖上游 Runner 或 Event 类型。
6. 用 `tenant.NewRunnerIdentity` 生成无歧义的 `userID`/`sessionID`，再由
   `TenantSessionService` 把所有 app/user/session/state 操作固定到 Plan 的 Tenant。Runner 的
   字符串命名空间只是第二层防碰撞，不能替代 adapter 的授权检查；双租户 conformance test
   必须证明相同外部 user/session ID 不能跨租户读取或追加事件。

一次普通消息的测试链路如下：

```text
model.NewUserMessage("hello")
        │
        ▼
agent.Invoke(ctx, runner, invocation, drainTimeout)
        │
        ├── LLMAgent 调用 deterministic fake model
        ├── `agent` 将上游 Event 转为服务内 RunnerEvent
        ├── runtime/execution 消费 RunnerEvent，直至 channel close
        └── 上游 Session service 写入有效 user/assistant event
        │
        ▼
GetSession(session.Key{AppName, UserID, SessionID})
        └── 断言最终 assistant reply 和 session.Events
```

消费者必须完整消费服务内 RunnerEvent channel。取消时，`agent` 适配边界负责在
有界时间内排空上游 Event channel；runtime 只需要消费并关闭自己的中立事件流：

```go
events, err := agent.Invoke(ctx, runner, agent.Invocation{
    UserID: userID, SessionID: sessionID, Message: message, RequestID: requestID,
}, drainTimeout)
if err != nil { return err }
for event := range events {
    // 处理或丢弃事件，但继续 drain 到 channel 关闭。
}
```

不能只 `break` 然后放弃 channel，因为 Runner 可能仍在向 channel 写事件。取消由传入的
`context.Context` 传播到模型、工具、Session 和 Runner；测试使用有界等待确认上游 source
和服务内事件 channel 最终关闭。
Runner、外部 Session service 和任何 fake model 的资源都由创建方明确 `Close` 或等待收尾。

## 5. Deterministic fake model

fake model 只用于测试，不模拟真实 provider 认证，也不读取环境变量。它实现上游 `model.Model`：

- `Info().Name` 与 Model Profile 的固定 model name 对齐。
- `GenerateContent` 不访问网络，返回一个固定的 assistant `Response`，并将 response channel 正常关闭。
- 如果收到取消的 context，在发送响应前结束，不创建无法退出的 goroutine。
- 测试通过 fake factory 记录收到的 `TenantID`、`SecretRef` 和 secret 是否只出现在 Factory 调用中；
  随后检查计划、snapshot、factory input、错误和 Session state 都没有 Secret 值。

该 fake 保持在 deterministic 测试路径；生产 provider 通过同一 Model Profile、Secret Resolver
和 Factory 输入边界装配，真实模型配置由部署文档的显式环境变量驱动。

## 6. 验证矩阵

验收矩阵覆盖：

### Model Profile 与 Repository

- ID/key、展示字段、状态、schema、generation、endpoint、option、secret_ref、时间、version 和 digest 边界。
- 相同 key 跨租户允许，同租户重复拒绝；跨租户读取不能成功。
- 完整配置更新、状态迁移、disabled 终态和 expected version conflict。
- 读写返回深拷贝；并发更新只有一个匹配版本的调用成功。
- Context 已取消和等待锁时都及时返回。

### Secret 与 Snapshot

- required/optional/forbidden secret_ref 分支分别要求解析、条件解析和拒绝引用；无引用不调用 Resolver；跨租户 scope 被拒绝。
- resolver/factory 的底层错误被替换为脱敏错误；测试 Secret 值不会出现在 `Error()`、序列化结果或日志输入中。
- Snapshot、Factory input 和 cache key 固定版本/摘要且不含 Secret 或 live client。

### Plan 与 Runner

- 拒绝跨租户对象、非 active Tenant/App/Model/Backend、不同 App 的 Revision、非当前 published Revision、Revision 模型引用不匹配和不完整 snapshot。
- Tenant-scoped Session adapter 的双租户 conformance test 拒绝跨租户 `GetSession`、`AppendEvent` 和 state 操作。
- 离线测试完整消费 Event stream，最终回复与 InMemory Session 状态一致。
- 取消 context 后模型、Runner、消费者和 Event channel 都在 bounded timeout 内退出。
- 控制面对象在 Plan 创建后变更，不改变本次执行的 Factory input/cache identity。

### 工程检查

```text
go test ./...
go test -race ./trpcservice/model/... ./trpcservice/runtime/... ./trpcservice/backend/...
./scripts/format.sh --check
./scripts/lint.sh
./scripts/build.sh
mkdocs build --strict -f docs/mkdocs.yml
git diff --check
```

文档与 lint 工具由 CI 固定版本提供，PR 使用统一命令记录检查结果。

## 7. 交付关系

```text
Model Profile + Secret Resolver + minimal Runner spine  (#22)
  └── Channel Binding
        └── HTTP Gateway / streaming API
              └── WeCom adapter + idempotency
                    └── persistent repositories and production adapters
```

当前实现和扩展实现共同遵守：

- `tenant_id` 是每个 Repository、Resolver、Factory 和持久化查询的显式边界。
- 真实 Secret 只能在已授权的 Factory 短路径中出现。
- 新执行必须从固定快照开始，不能在执行中重新读取“当前配置”。
- Runner 的事件和取消语义由 tRPC-Agent-Go 负责，平台只管理装配、租户隔离、生命周期和资源所有权。
