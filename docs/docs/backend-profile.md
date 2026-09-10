# Backend Profile 控制面与运行时边界

> 本设计承接 Tenant 与 Agent App/Revision 闭环，定义租户级数据后端选择、生命周期、
> 并发控制和一次执行快照。实现跟踪见
> [Issue #20](https://github.com/XnLemon/trpc-agent-service/issues/20)。

## 目标与范围

Backend Profile 是租户级控制面根实体。它把一次 Agent 执行所需的数据能力绑定到明确的
provider 配置，使 Gateway/Worker 可以固定 Session、Memory、Knowledge、Artifact 和 Audit
后端，而不把 Secret 值或进程内客户端写入配置。

当前交付包含以下边界：

- 稳定 Profile 身份、租户内唯一 key、生命周期和乐观锁版本。
- 六种能力绑定以及严格的 provider/config 规范化契约。
- 显式以 `tenant_id + profile_id` 为作用域的 InMemory Repository。
- 创建、完整配置替换、暂停、恢复和停用的结构化变更事件。
- 同时固定 Tenant/Profile 版本和配置摘要的不可变执行快照。
- 只含 Secret 引用的 Storage Factory 输入。
- PostgreSQL 目标 DDL、同租户默认引用和 tRPC-Agent-Go 装配映射。

本页的 Profile、Catalog、Secret Resolver、Storage Factory、migration 和 runtime adapter
共同形成无密钥的后端装配链路。Issue #37 的 `migrations/0001_control_plane.up.sql` 复用本页
DDL；SQL Repository、Gateway、Worker 和跨节点缓存通过对应模块接入。

## 核心模型

### Profile 根实体

Profile 至少包含：

| 字段 | 约束 |
| --- | --- |
| `tenant_id` | 可信租户隔离键；所有 Repository 调用都显式传入 |
| `profile_id` | 服务端生成，格式为 `bp_` 加 26 位 Crockford ULID；创建后不可变 |
| `profile_key` | trim、小写化后的 `[a-z][a-z0-9-]{1,63}`；租户内唯一且不可变 |
| `display_name` | trim 后 1–200 字符 |
| `description` | 最多 2000 字符 |
| `status` | `active`、`suspended` 或 `disabled` |
| `schema_version` | 当前只接受 `1`；未知版本必须拒绝 |
| `bindings` | 按 capability 排序的规范化完整配置；每种 capability 最多一个 |
| `content_digest` | 规范化配置的 SHA-256 小写十六进制摘要 |
| `version` | 从 1 开始，每次成功配置更新或状态变更递增一次 |
| `created_at` / `updated_at` | UTC；更新时保持创建时间不变 |

`profile_id` 和 `profile_key` 都不是全局授权边界。即使 ID 冲突概率可忽略，所有读取和修改仍
必须同时携带 Tenant ID；按 key 查询同样显式限定 Tenant。

### Capability Binding

第一版能力集合是封闭枚举：

| Capability | 语义 |
| --- | --- |
| `session` | 对话事件、状态、摘要和会话生命周期 |
| `memory` | 跨会话长期记忆 |
| `knowledge` | Knowledge/RAG 的索引与检索存储 |
| `artifact` | 会话产物及其版本 |
| `audit` | 强制审计事件的持久化出口；不等同于采样 trace |

每个绑定只保存以下控制面字段：

```go
type CapabilityBinding struct {
    Capability Capability
    Provider   string
    Endpoint   string
    Options    map[string]string
    SecretRef  string
}
```

切片而不是 `map[Capability]Binding` 是对外序列化边界；规范化后按 capability 固定排序。
重复 capability、未知 capability 或空 provider 均直接失败，不能使用“最后一个覆盖前一个”的
隐式语义。

### Provider Schema Catalog

租户提交的 provider 名称不能自动变成可执行插件。Provider Schema Catalog 由进程启动代码
构造并保持只读；Catalog 属于受信任平台代码，不属于租户配置。每条 schema 明确：

- provider 规范名以及允许绑定的 capability。
- endpoint 是禁止、可选还是必需。
- `secret_ref` 是禁止、可选还是必需。
- 允许的 option key、类型/取值规则、默认值和规范化函数。

Repository 在创建和完整配置更新时必须通过同一个 Catalog 规范化绑定。未注册的
capability/provider 组合、未知 option、非法值或缺少必需字段都返回可识别的 invalid error。
Factory 只消费已经规范化的快照，不能再次接受任意 JSON 或忽略未知字段。

Catalog 注册运行时允许使用的 tRPC-Agent-Go provider。单元测试使用受控 schema 覆盖 endpoint、
option 和 Secret 规则；每个 adapter 同时提供对应 schema、Factory 映射与集成测试。

### 无密钥配置规则

`endpoint` 只表达网络位置，不是 DSN：

- trim 后的原始 UTF-8 序列最长 2048 字节；URI 解析、主机规范化和百分号转义后的最终序列化结果也最长
  2048 字节。百分号转义可能扩展非 ASCII path，因此两个边界都会独立校验。原始文本和解析后的 path
  都不允许控制字符。
- URI 不允许 userinfo、query 或 fragment，因而不能携带用户名、密码或签名参数。
- 当前 binding 接受单一 hostname authority；多节点/故障转移列表由 provider schema 以
  结构化字段建模，不能塞入逗号分隔的 endpoint 字符串。
- provider schema 决定 endpoint 是否必需以及允许的 scheme。

`options` 使用 `map[string]string`，避免任意 JSON 类型穿过运行时边界。Catalog 只接受显式
allowlist；`password`、`passwd`、`pwd`、`passphrase`、`token`、`api_key`、`secret`、`credential`、`dsn`、
`connection_string` 等敏感 key 即使被 schema 错误声明也必须由公共校验拒绝。错误消息不能
回显 endpoint、option value 或 Secret 值。

`secret_ref` 是不透明引用，trim 后为 1–256 个安全字符。它可以参与摘要，以便 Secret 引用
切换产生新缓存键，但引用解析结果永远不能回写 Profile、事件、快照或 Factory 输入。真正的
Secret Resolver 以可信 Tenant 作用域解析引用，并直接把结果交给具体 adapter。

### 规范化与内容摘要

完整配置写入前执行以下确定性步骤：

1. 校验 `schema_version = 1`、字段长度和 capability 唯一性。
2. 规范化 provider、endpoint、`secret_ref` 和所有 option。
3. 由 Catalog 物化参与语义的默认 option。
4. 按 capability 固定排序绑定，按 key 排序每个 options map。
5. 使用带长度前缀的确定性编码计算 SHA-256。

摘要包含 schema version、所有 capability/provider、endpoint、规范化 options 和 Secret 引用；
不包含 Profile ID/key、展示字段、状态、version、时间、actor 或 reason。展示字段更新仍递增
Profile version，但只有后端语义改变才改变 digest。状态转换不改变 digest。

## 生命周期

```text
                创建完整 Session 配置
                         │
                         ▼
                      active
                         │  ▲
                  暂停   │  │ 恢复（重新校验 Session）
                         ▼  │
                     suspended
                         │
                停用     │
                         ▼
                     disabled
                       终态
```

- 创建时状态默认为 `active`；也可以显式创建 `suspended`，但不能直接创建 `disabled`。
- 任意非 disabled Profile 至少有一个有效 binding；active 必须包含 `session`。
- suspended 拒绝新快照，但允许完整配置替换；因此可以先补齐或切换 Session 后再恢复。
- active 的完整配置替换不能移除 Session。
- resume 必须重新校验完整配置以及 Session binding。
- disabled 是终态，拒绝配置更新、恢复和新执行；读取仍可用于管理和审计。
- 暂停或停用只影响后续快照，不篡改已经创建的执行快照。

Tenant 是第一道运行门禁，Backend Profile 是第二道门禁；只有二者都 active 才能创建新快照。
当前默认 Profile 选择源是 `tenant.default_backend_profile_id`。它为 `NULL`、指向非 active
Profile 或与传入 Profile 不一致时，快照构造失败；选择始终经过可信来源、同租户校验和固定
优先级，不能把任意 Profile 参数当作授权。

## Repository 与审计事件

目录沿用当前 `trpcservice` 责任域，不改变现有 Tenant/Agent 结构：

```text
trpcservice/backend/
├── backend.go       # 包说明、Profile、binding、Catalog 和领域校验
├── repository.go    # 租户作用域 Repository、写入输入和事件
├── execution.go      # BackendExecutionSnapshot 与 Factory 输入
└── inmemory/
    ├── inmemory.go  # 单进程 Repository
    └── rwmutex.go   # 可响应 Context 取消的锁边界
```

目标接口为：

```go
type Repository interface {
    Create(context.Context, CreateInput) (*Profile, ChangeEvent, error)
    Get(context.Context, string, string) (*Profile, error)
    UpdateConfiguration(
        context.Context,
        UpdateConfigurationInput,
    ) (*Profile, ChangeEvent, error)
    TransitionStatus(
        context.Context,
        TransitionStatusInput,
    ) (*Profile, ChangeEvent, error)
}
```

`Get` 的两个字符串按顺序是 Tenant ID 和 Profile ID；写入输入结构体必须同时包含这两个字段
（Create 只由调用方提供 Tenant ID，Profile ID 由服务端生成）。Create、完整配置替换和状态
迁移都要求可信 `ChangeMetadata`，并与新状态一起返回事件：

| 字段 | 说明 |
| --- | --- |
| `event_type` | `created`、`configuration_updated`、`suspended`、`resumed`、`disabled` |
| `tenant_id` / `profile_id` | 明确的租户和 Profile 作用域 |
| `previous_status` / `current_status` | 创建事件的 previous status 为空 |
| `previous_digest` / `current_digest` | 配置变更前后摘要；状态事件两者相同 |
| `actor_type` / `actor_id` | 非空可信主体 |
| `reason` | trim 后 1–1000 字符 |
| `correlation_id` | 非空管理请求关联 ID |
| `previous_version` / `next_version` | 创建为 `0 -> 1`，其余必须为 `n -> n+1` |
| `occurred_at` | UTC 发生时间 |

事件和 Profile 一起构造后才能提交内存状态。生产 Repository 必须在同一事务内写 Profile 和
Outbox；不能先提交配置再尽力记录事件。事件同样返回防御性副本。

### 乐观锁、隔离和 Context

- 创建唯一索引按 `(tenant_id, profile_key)` 分区；跨租户同 key 合法。
- Get、Update、Transition 都以 `(tenant_id, profile_id)` 定位，Tenant 不匹配表现为 not found，
  不能泄露对象是否存在于其他租户。
- Update 和 Transition 必须匹配 `expected_version`；并发操作只有一个成功。
- 每次调用在入口、获得锁后、提交修改前检查 `ctx.Err()`。
- 可取消锁保证等待写锁时 Context 取消会及时返回；不能在锁内执行外部 I/O。
- Repository 保存和返回时深拷贝 binding slice、options map 以及所有可选指针。

InMemory 用于单进程开发和确定性测试；持久化、跨进程一致性和 Secret 解析由对应 adapter
与 bootstrap 组合提供。

## 不可变执行快照

`BackendExecutionSnapshot` 封闭保存 Tenant 和 Profile 的防御性副本。构造器接收既有的
`tenant.ConfigurationSnapshot`、Profile，以及接受该 Profile 时使用的同一个不可变 Catalog，
并校验：

1. Tenant snapshot 有效且 Tenant active。
2. Profile 自身全部不变量和 digest 有效。
3. Tenant 的 `default_backend_profile_id` 非空且等于 Profile ID。
4. Profile 与 Tenant ID 相同且 Profile active。
5. Profile 包含有效 Session binding。

快照访问器每次返回副本；零值、手工拼装或 Context 中损坏的快照不能通过校验。Context 注入
与读取都再次克隆，避免调用方在一次执行中观察到控制面热更新。

缓存键至少包含：

```text
tenant_id + tenant_version + profile_id + profile_version + content_digest
```

`StorageFactoryInput` 是快照到运行时 adapter 的唯一输出，包含固定身份/version/digest 和
规范化 bindings。它不包含 Secret 值、数据库连接、HTTP client、tRPC-Agent-Go Service、
Factory 函数或任意 `any` 字段。其 `SecretRef` 仍只是引用；Resolver 和 adapter 在
可信 Tenant 作用域内完成解析与客户端构造。

## PostgreSQL 目标模型

以下 DDL 描述完整性和事务边界，已由 Issue #37 的
`migrations/0001_control_plane.up.sql` 落地；本页继续作为领域约束的详细说明。

```sql
-- Match Go strings.TrimSpace/unicode.IsSpace for every boundary field that
-- crosses between the SQL adapter and the Go Repository/runtime.
CREATE OR REPLACE FUNCTION public.trim_backend_profile_text(value TEXT)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT pg_catalog.btrim(
        value,
        U&'\0009\000A\000B\000C\000D\0020\0085\00A0\1680\2000\2001\2002\2003\2004\2005\2006\2007\2008\2009\200A\2028\2029\202F\205F\3000'
    )
$$;

CREATE TABLE backend_profile (
    tenant_id      TEXT NOT NULL REFERENCES tenant(tenant_id),
    profile_id     TEXT NOT NULL
                   CHECK (profile_id ~ '^bp_[0-7][0-9A-HJKMNP-TV-Z]{25}$'),
    profile_key    TEXT NOT NULL
                   CHECK (profile_key ~ '^[a-z][a-z0-9-]{1,63}$'),
    display_name   TEXT NOT NULL
                   CHECK (display_name = public.trim_backend_profile_text(display_name)
                          AND length(display_name) BETWEEN 1 AND 200),
    description    TEXT NOT NULL DEFAULT ''
                   CHECK (description = public.trim_backend_profile_text(description)
                          AND length(description) <= 2000),
    status         TEXT NOT NULL DEFAULT 'active'
                   CHECK (status IN ('active', 'suspended', 'disabled')),
    schema_version INT NOT NULL DEFAULT 1 CHECK (schema_version = 1),
    content_digest TEXT NOT NULL CHECK (content_digest ~ '^[0-9a-f]{64}$'),
    version        BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (tenant_id, profile_id),
    UNIQUE (tenant_id, profile_key)
);

CREATE TABLE backend_profile_binding (
    tenant_id  TEXT NOT NULL,
    profile_id TEXT NOT NULL,
    capability TEXT NOT NULL
               CHECK (capability IN ('session', 'memory', 'knowledge', 'artifact', 'audit')),
    provider   TEXT NOT NULL CHECK (provider ~ '^[a-z][a-z0-9_-]{0,63}$'),
    endpoint   TEXT NOT NULL DEFAULT '' CHECK (length(endpoint) <= 2048),
    options    JSONB NOT NULL DEFAULT '{}'::jsonb
               CHECK (jsonb_typeof(options) = 'object'),
    secret_ref TEXT NOT NULL DEFAULT '' CHECK (length(secret_ref) <= 256),

    PRIMARY KEY (tenant_id, profile_id, capability),
    FOREIGN KEY (tenant_id, profile_id)
        REFERENCES backend_profile(tenant_id, profile_id)
        ON DELETE RESTRICT
);

ALTER TABLE tenant
    ADD CONSTRAINT fk_tenant_default_backend
    FOREIGN KEY (tenant_id, default_backend_profile_id)
    REFERENCES backend_profile(tenant_id, profile_id);
```

JSONB 只用于持久化已经由 Go Catalog 校验并规范化的字符串 map，不是开放扩展口。SQL
Repository 必须严格解码，拒绝非字符串 value、未知 option 和未知 schema version。数据库
约束不能独自表达 provider-specific schema，因此写入权限只授予受控函数。

active 必须存在 Session binding 是跨表约束，应由延迟 constraint trigger 或同一事务中的受控
写函数在事务结束时验证；不能分别提交根表和 bindings。Profile identity 使用 trigger 禁止修改。
配置替换应锁定根行、验证 expected version、替换完整 binding 集合、重新计算 digest、递增一次
version，并在同一事务写 Outbox。

```sql
CREATE TABLE backend_profile_change_outbox (
    event_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_type        TEXT NOT NULL CHECK (event_type IN (
                          'created', 'configuration_updated',
                          'suspended', 'resumed', 'disabled'
                      )),
    tenant_id         TEXT NOT NULL,
    profile_id        TEXT NOT NULL,
    previous_status   TEXT CHECK (
                          previous_status IS NULL
                          OR previous_status IN ('active', 'suspended')
                      ),
    current_status    TEXT NOT NULL
                      CHECK (current_status IN ('active', 'suspended', 'disabled')),
    previous_digest   TEXT,
    current_digest    TEXT NOT NULL CHECK (current_digest ~ '^[0-9a-f]{64}$'),
    actor_type        TEXT NOT NULL
                      CHECK (actor_type = public.trim_backend_profile_text(actor_type)
                             AND length(actor_type) > 0),
    actor_id          TEXT NOT NULL
                      CHECK (actor_id = public.trim_backend_profile_text(actor_id)
                             AND length(actor_id) > 0),
    reason            TEXT NOT NULL
                      CHECK (reason = public.trim_backend_profile_text(reason)
                             AND length(reason) BETWEEN 1 AND 1000),
    correlation_id    TEXT NOT NULL
                      CHECK (correlation_id = public.trim_backend_profile_text(correlation_id)
                             AND length(correlation_id) > 0),
    previous_version  BIGINT NOT NULL CHECK (previous_version >= 0),
    next_version      BIGINT NOT NULL CHECK (next_version = previous_version + 1),
    occurred_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    FOREIGN KEY (tenant_id, profile_id)
        REFERENCES backend_profile(tenant_id, profile_id),
    CHECK (
        (event_type = 'created'
         AND previous_status IS NULL
         AND current_status IN ('active', 'suspended')
         AND previous_digest IS NULL
         AND previous_version = 0
         AND next_version = 1)
        OR
        (event_type <> 'created'
         AND previous_status IS NOT NULL
         AND previous_digest IS NOT NULL
         AND previous_digest ~ '^[0-9a-f]{64}$'
         AND previous_version >= 1)
    )
);
```

以下触发器封住不能只靠根表 `CHECK` 表达的跨表不变量。两条 constraint trigger 都是
deferred，使受控事务可以先替换 bindings 再提交根状态，但事务结束时所有非 disabled Profile
必须至少有一个 binding，且 active Profile 必须有 Session binding。

```sql
CREATE OR REPLACE FUNCTION backend_profile_reject_disabled_insert()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.status = 'disabled' THEN
        RAISE EXCEPTION 'backend profile cannot be created disabled';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER backend_profile_no_disabled_insert
BEFORE INSERT ON backend_profile
FOR EACH ROW EXECUTE FUNCTION backend_profile_reject_disabled_insert();

CREATE OR REPLACE FUNCTION backend_profile_reject_identity_change()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
       OR NEW.profile_id IS DISTINCT FROM OLD.profile_id
       OR NEW.profile_key IS DISTINCT FROM OLD.profile_key THEN
        RAISE EXCEPTION 'backend profile identity is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER backend_profile_identity_immutable
BEFORE UPDATE ON backend_profile
FOR EACH ROW EXECUTE FUNCTION backend_profile_reject_identity_change();

CREATE OR REPLACE FUNCTION backend_profile_binding_reject_identity_change()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
       OR NEW.profile_id IS DISTINCT FROM OLD.profile_id
       OR NEW.capability IS DISTINCT FROM OLD.capability THEN
        RAISE EXCEPTION 'backend profile binding identity is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER backend_profile_binding_identity_immutable
BEFORE UPDATE ON backend_profile_binding
FOR EACH ROW EXECUTE FUNCTION backend_profile_binding_reject_identity_change();

CREATE OR REPLACE FUNCTION backend_profile_require_valid_bindings()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_tenant_id  TEXT;
    v_profile_id TEXT;
    v_status     TEXT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        v_tenant_id := OLD.tenant_id;
        v_profile_id := OLD.profile_id;
    ELSE
        v_tenant_id := NEW.tenant_id;
        v_profile_id := NEW.profile_id;
    END IF;

    SELECT status INTO v_status
    FROM backend_profile
    WHERE tenant_id = v_tenant_id AND profile_id = v_profile_id;

    -- A deleted/absent root needs no capability check. Profile deletion is not
    -- exposed by this lifecycle, but this keeps the trigger total.
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    IF v_status <> 'disabled' AND NOT EXISTS (
        SELECT 1
        FROM backend_profile_binding
        WHERE tenant_id = v_tenant_id
          AND profile_id = v_profile_id
    ) THEN
        RAISE EXCEPTION 'non-disabled backend profile requires at least one binding';
    END IF;

    IF v_status = 'active' AND NOT EXISTS (
        SELECT 1
        FROM backend_profile_binding
        WHERE tenant_id = v_tenant_id
          AND profile_id = v_profile_id
          AND capability = 'session'
    ) THEN
        RAISE EXCEPTION 'active backend profile requires a session binding';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER backend_profile_root_bindings_guard
AFTER INSERT OR UPDATE OF status ON backend_profile
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION backend_profile_require_valid_bindings();

CREATE CONSTRAINT TRIGGER backend_profile_binding_rows_guard
AFTER INSERT OR UPDATE OR DELETE ON backend_profile_binding
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION backend_profile_require_valid_bindings();
```

生命周期入口必须锁定 Profile、校验 expected version 和默认引用，并原子写 Outbox。以下函数
展示 suspend/resume/disable 的完整数据库语义；Catalog 校验过的完整配置替换采用相同锁定和
Outbox 模式，但 binding payload 仍由严格解码的 SQL Repository 传入，不能在 SQL 中猜测
provider schema。disabled 是终态门禁：锁定并确认对象存在后，所有 mutation 都先返回 disabled，
再判断 expected version；这样当前或陈旧调用在 InMemory 与 SQL adapter 中具有相同错误分类。

```sql
CREATE OR REPLACE FUNCTION transition_backend_profile_status(
    p_tenant_id TEXT,
    p_profile_id TEXT,
    p_expected_version BIGINT,
    p_next_status TEXT,
    p_actor_type TEXT,
    p_actor_id TEXT,
    p_reason TEXT,
    p_correlation_id TEXT
) RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    v_default_profile_id TEXT;
    v_previous_status TEXT;
    v_previous_version BIGINT;
    v_previous_updated_at TIMESTAMPTZ;
    v_digest TEXT;
    v_next_version BIGINT;
    v_occurred_at TIMESTAMPTZ;
    v_actor_type TEXT;
    v_actor_id TEXT;
    v_reason TEXT;
    v_correlation_id TEXT;
BEGIN
    v_actor_type := public.trim_backend_profile_text(p_actor_type);
    v_actor_id := public.trim_backend_profile_text(p_actor_id);
    v_reason := public.trim_backend_profile_text(p_reason);
    v_correlation_id := public.trim_backend_profile_text(p_correlation_id);

    -- All operations that change Tenant defaults or Profile status use the
    -- same Tenant -> Profile lock order.
    SELECT default_backend_profile_id INTO v_default_profile_id
    FROM public.tenant
    WHERE tenant_id = p_tenant_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'tenant does not exist';
    END IF;

    SELECT status, version, updated_at, content_digest
    INTO v_previous_status, v_previous_version, v_previous_updated_at, v_digest
    FROM public.backend_profile
    WHERE tenant_id = p_tenant_id AND profile_id = p_profile_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'backend profile does not exist';
    END IF;

    IF v_previous_status = 'disabled' THEN
        RAISE EXCEPTION 'backend profile is disabled';
    END IF;

    IF v_previous_version <> p_expected_version THEN
        RAISE EXCEPTION 'backend profile version conflict';
    END IF;

    IF v_actor_type IS NULL OR length(v_actor_type) = 0
       OR v_actor_id IS NULL OR length(v_actor_id) = 0
       OR v_correlation_id IS NULL OR length(v_correlation_id) = 0
       OR v_reason IS NULL OR length(v_reason) NOT BETWEEN 1 AND 1000 THEN
        RAISE EXCEPTION 'backend profile transition requires valid audit metadata';
    END IF;

    IF (v_previous_status, p_next_status) NOT IN (
        ('active', 'suspended'),
        ('active', 'disabled'),
        ('suspended', 'active'),
        ('suspended', 'disabled')
    ) THEN
        RAISE EXCEPTION 'invalid backend profile status transition';
    END IF;

    IF p_next_status = 'active' AND NOT EXISTS (
        SELECT 1 FROM public.backend_profile_binding
        WHERE tenant_id = p_tenant_id
          AND profile_id = p_profile_id
          AND capability = 'session'
    ) THEN
        RAISE EXCEPTION 'active backend profile requires a session binding';
    END IF;

    IF p_next_status = 'disabled'
       AND v_default_profile_id = p_profile_id THEN
        RAISE EXCEPTION 'tenant default backend profile must be switched first';
    END IF;

    -- now() is fixed at transaction start and may predate a lock wait. Capture
    -- wall-clock time once after validation and never move updated_at backwards.
    v_occurred_at := GREATEST(clock_timestamp(), v_previous_updated_at);

    UPDATE public.backend_profile
    SET status = p_next_status,
        version = version + 1,
        updated_at = v_occurred_at
    WHERE tenant_id = p_tenant_id
      AND profile_id = p_profile_id
      AND version = p_expected_version
    RETURNING version INTO v_next_version;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'backend profile version conflict';
    END IF;

    INSERT INTO public.backend_profile_change_outbox (
        event_type, tenant_id, profile_id,
        previous_status, current_status,
        previous_digest, current_digest,
        actor_type, actor_id, reason, correlation_id,
        previous_version, next_version, occurred_at
    ) VALUES (
        CASE p_next_status
            WHEN 'suspended' THEN 'suspended'
            WHEN 'active' THEN 'resumed'
            ELSE 'disabled'
        END,
        p_tenant_id, p_profile_id,
        v_previous_status, p_next_status,
        v_digest, v_digest,
        v_actor_type, v_actor_id, v_reason, v_correlation_id,
        v_previous_version, v_next_version, v_occurred_at
    );
    RETURN v_next_version;
END;
$$;

REVOKE ALL ON FUNCTION transition_backend_profile_status(
    TEXT, TEXT, BIGINT, TEXT, TEXT, TEXT, TEXT, TEXT
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION transition_backend_profile_status(
    TEXT, TEXT, BIGINT, TEXT, TEXT, TEXT, TEXT, TEXT
) TO tenant_admin_writer;
```

SQL adapter 集成测试必须让一个早启动事务等待 Tenant/Profile 锁，证明提交后的 `updated_at` 不早于
锁内读取的前一版本时间，且根记录与 Outbox `occurred_at` 使用同一个时间值。

停用 Tenant 默认 Profile 时，Admin 编排事务必须先切换或清空
`tenant.default_backend_profile_id`，否则拒绝停用。所有相关 SQL 路径统一使用“先锁 Tenant，
再锁目标 Profile”的顺序；设置默认引用时也必须在持有 Tenant 行锁后锁定并验证目标 Profile
属于同一 Tenant 且 active。复合外键只证明对象存在，不能代替状态校验。Backend Repository
本身不能凭过期的调用方布尔值推断 Tenant 默认引用；SQL 实现在同一事务校验，InMemory 阶段
由上层 Tenant 编排负责。无论哪种实现，都不得自动选择另一个 Profile。

运行时角色不能直接修改 Profile、binding 或 Outbox 表。受控 SQL 函数使用固定
`search_path`、显式撤销 `PUBLIC EXECUTE`，并由不属于运行时连接池的 migration owner 持有。
Worker 只接收快照，不枚举控制面表。

## 与 tRPC-Agent-Go 的映射

映射以 tRPC-Agent-Go 固定兼容版本
[`0e352fd`](https://github.com/trpc-group/trpc-agent-go/commit/0e352fdd1428d30a8d978d39877f5a7b2591ccc1)
为依据；adapter 通过编译和集成测试检测上游变化。

tRPC-Agent-Go 的 Session、Memory、Knowledge VectorStore 和 Artifact 接口没有独立
`tenant_id` 参数。平台 adapter 必须在构造时捕获 `StorageFactoryInput.TenantID`，并在每次
底层表查询、keyspace、collection、bucket prefix 或等价 provider 分区中强制使用该可信
Tenant 边界；框架的 AppName/UserID/SessionID 命名空间只是第二层防碰撞，不是授权条件。
无法提供可验证租户分区的 provider 不得共享给多个租户。每个真实 adapter 落地时必须增加
双租户 conformance test，证明相同框架 ID 不会跨租户读写。

| Capability | tRPC-Agent-Go 边界 | 平台 adapter 责任 |
| --- | --- | --- |
| Session | [`session.Service`](https://github.com/trpc-group/trpc-agent-go/blob/0e352fdd1428d30a8d978d39877f5a7b2591ccc1/session/session.go)；通过 [`runner.WithSessionService`](https://github.com/trpc-group/trpc-agent-go/blob/0e352fdd1428d30a8d978d39877f5a7b2591ccc1/runner/runner.go) 注入 | adapter 固定 Tenant 分区并构造受支持实现；Runner identity 继续使用 Tenant 命名空间作为第二层隔离 |
| Memory | [`memory.Service`](https://github.com/trpc-group/trpc-agent-go/blob/0e352fdd1428d30a8d978d39877f5a7b2591ccc1/memory/memory.go)；通过 `runner.WithMemoryService` 注入 | adapter 固定 Tenant 分区，再映射 AppName/UserID |
| Knowledge | [`knowledge.Knowledge`](https://github.com/trpc-group/trpc-agent-go/blob/0e352fdd1428d30a8d978d39877f5a7b2591ccc1/knowledge/knowledge.go) 和 [`vectorstore.VectorStore`](https://github.com/trpc-group/trpc-agent-go/blob/0e352fdd1428d30a8d978d39877f5a7b2591ccc1/knowledge/vectorstore/vectorstore.go) | adapter 先固定 Tenant 分区并构造 VectorStore；Agent Factory 再以 [`knowledge.New`](https://github.com/trpc-group/trpc-agent-go/blob/0e352fdd1428d30a8d978d39877f5a7b2591ccc1/knowledge/default.go) 和 `knowledge.WithVectorStore` 构造 Knowledge，最后通过 [`llmagent.WithKnowledge`](https://github.com/trpc-group/trpc-agent-go/blob/0e352fdd1428d30a8d978d39877f5a7b2591ccc1/agent/llmagent/option.go) 注入 LLMAgent |
| Artifact | [`artifact.Service`](https://github.com/trpc-group/trpc-agent-go/blob/0e352fdd1428d30a8d978d39877f5a7b2591ccc1/artifact/service.go)；通过 `runner.WithArtifactService` 注入 | adapter 固定 Tenant/bucket prefix，再映射 App/User/Session 作用域 |
| Audit | tRPC-Agent-Go 复用 OpenTelemetry，没有与上述 Service 同构的强制审计存储接口 | 平台 Audit adapter 独立持久化强制事件；OTel exporter 只负责 telemetry，采样不能代替审计 |

tRPC-Agent-Go 仓库提供 Session、Memory、VectorStore 和 Artifact 接口；平台通过对应 adapter、
Catalog schema 和 conformance test 控制每个 provider 的租户启用边界。

Knowledge 的 embedder/model 引用不属于 Backend Profile。它来自已发布且不可变的 Agent/Knowledge
配置与同租户 Model Profile，并与 Backend snapshot 一起固定后再调用 `knowledge.New`。运行时
Factory 对 Knowledge binding 使用同租户已发布依赖快照，不能借用聊天模型、进程环境变量或全局
默认 embedder。

Storage Factory 的构造顺序是：校验快照 → 读取已注册 adapter → 按 Tenant 授权解析
`secret_ref` → 构造客户端/Service → 注入 Agent/Runner。Secret Resolver 失败必须令本次构造
失败，不能使用空密码、环境变量全局默认值或另一个租户的缓存项降级。

## 验证证据

实现测试至少覆盖：

- ID/key/status/version/time/schema 和 capability/provider/config 边界。
- 同 key 跨租户允许、同租户重复拒绝以及跨租户读取表现为 not found。
- 创建、读取、完整配置更新、暂停、恢复、终态停用。
- active Session 必需规则和恢复时重新校验。
- slice/map 的入参、存储、返回值、事件、快照和 Factory 输入深拷贝。
- expected version 并发单一获胜，以及等待锁时 Context 取消。
- 所有写入事件的 actor、reason、correlation、状态、digest 和前后版本。
- Tenant/Profile 非 active、作用域不一致、损坏摘要和 Context 零值快照拒绝。
- Tenant 默认 Profile 为空、不匹配或非 active 时拒绝快照，且不回退到其他 Profile。
- 快照固定 Tenant/Profile version/digest 且不可能携带 Secret 值或 live client。
- 每个真实 adapter 以相同框架 ID 做双租户 conformance test，证明底层查询/分区显式隔离。

验收使用 `trpcservice/backend` 领域/Catalog/Repository 测试、执行快照与 Storage Factory
边界测试，以及全量 `go test`、race、format、lint、build、MkDocs strict 和 diff check。

## 交付依赖顺序

```text
Backend Profile
  └── Model Profile / Secret Resolver
        └── Channel Binding
              └── Gateway / Worker 最小执行链路
```

运行时 adapter、Secret 解析和数据迁移均遵循本控制面契约；每个运行时输入保持可审计、
可冻结且不会泄露凭据。
