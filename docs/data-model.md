# 数据模型设计

## 1. 模型分层

平台数据分为三类：

- 控制面数据：租户、Agent App、revision、通道、后端绑定和策略；
- 运行面数据：入站消息、Agent run、会话、事件、摘要、工具执行和回复 outbox；
- Agent 数据：Memory、Knowledge 元数据、Artifact 元数据和审计日志。

控制面和运行面推荐使用 PostgreSQL。Session、Memory、Knowledge 和 Artifact 的实际内容可以按租户路由到 Redis、SQL、向量库或对象存储。即使租户把 Session 放在 Redis，Control DB 中仍保留 inbound、agent run 和 delivery journal，用于幂等、审计和故障恢复。

以下 DDL 是最小逻辑模型，省略了组织成员、RBAC、计费明细和知识文档分片等扩展表。

当前仓库的可执行 schema 由 [001–024 migrations](../trpcservice/database/migrations) 管理，核心包括：

024 增加控制台存储：`admin_session`（登录摘要与到期时间）、`agent_draft`（带版本的草稿）、`debug_snapshot`（不可变执行配置）、`debug_session`（发起者与独立运行身份）、`debug_run`（消息、租约、审批关联和结果）、`debug_event`、`debug_tool_execution`、`debug_tool_approval`、`debug_approval_decision` 和 `console_worker`。这些表使用 tenant_id + record_id 主键、owner_id、app_id、status、version、JSONB data 和时间字段；具体数据形状由 Go 类型约束。

认证表与调试表分开授予数据库权限，Worker 不获得 admin_session 读取权限。调试审批/Journal 不复用正式 IM 外键，避免把临时快照伪装成发布版本。调试快照不能从正常发布与通道路由中读取；草稿发布在 PostgreSQL 中对草稿/App 加锁，并原子创建版本、切换稳定指针和更新草稿发布标记。

发布、回滚和灰度操作的审计写入参与同一 PostgreSQL 事务，重复草稿发布不追加重复历史。`background_job.payload.source_request_id` 保存新任务的因果关联，查询接口只返回元数据；旧任务不回填推测的关联。`console_worker.data.checks` 保存带时间、租户、后端绑定和配置指纹的只读观测，心跳记录一分钟到期，过期数据由 Worker 清理。

```text
tenant / agent_app / agent_revision
backend_binding / backend_migration
channel_binding / external_identity / conversation
inbound_message / agent_run / outbound_message / queue_outbox
tool_approval / tool_execution / background_job / audit_log
```

Session/State/Event/Summary、Memory、向量和对象内容由租户选择的 tRPC-Agent-Go backend 管理；Control DB 保存消息、运行、审批、迁移、任务和审计真相。

## 2. 租户和 Agent App

```sql
CREATE TABLE tenant (
    tenant_id          VARCHAR(64) PRIMARY KEY,
    display_name       VARCHAR(255) NOT NULL,
    status             VARCHAR(32) NOT NULL,
    region             VARCHAR(64) NOT NULL,
    quota_config       JSONB NOT NULL DEFAULT '{}'::jsonb,
    audit_policy       JSONB NOT NULL DEFAULT '{}'::jsonb,
    secret_namespace   VARCHAR(255) NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE agent_app (
    app_id              VARCHAR(64) PRIMARY KEY,
    tenant_id           VARCHAR(64) NOT NULL REFERENCES tenant(tenant_id),
    name                VARCHAR(255) NOT NULL,
    description         TEXT NOT NULL DEFAULT '',
    status              VARCHAR(32) NOT NULL,
    stable_revision_id  VARCHAR(64),
    rollout_policy      JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name)
);

CREATE TABLE agent_revision (
    revision_id       VARCHAR(64) PRIMARY KEY,
    tenant_id         VARCHAR(64) NOT NULL REFERENCES tenant(tenant_id),
    app_id            VARCHAR(64) NOT NULL REFERENCES agent_app(app_id),
    revision_no       BIGINT NOT NULL,
    agent_type        VARCHAR(64) NOT NULL,
    agent_config      JSONB NOT NULL,
    model_config      JSONB NOT NULL,
    tool_policy       JSONB NOT NULL,
    knowledge_config  JSONB NOT NULL DEFAULT '{}'::jsonb,
    memory_config     JSONB NOT NULL DEFAULT '{}'::jsonb,
    guardrail_config  JSONB NOT NULL DEFAULT '{}'::jsonb,
    checksum          VARCHAR(128) NOT NULL,
    created_by        VARCHAR(128) NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (app_id, revision_no)
);
```

`agent_revision` 发布后不可修改。修正提示词、模型或工具策略时创建新 revision。`agent_app.stable_revision_id` 只指向已经验证过的 revision。

## 3. 后端和通道绑定

```sql
CREATE TABLE backend_binding (
    binding_id        VARCHAR(64) PRIMARY KEY,
    tenant_id         VARCHAR(64) NOT NULL REFERENCES tenant(tenant_id),
    app_id            VARCHAR(64) REFERENCES agent_app(app_id),
    resource_type     VARCHAR(32) NOT NULL,
    backend_type      VARCHAR(32) NOT NULL,
    config            JSONB NOT NULL DEFAULT '{}'::jsonb,
    secret_ref        VARCHAR(512),
    isolation_level   VARCHAR(32) NOT NULL DEFAULT 'shared',
    migration_state   VARCHAR(32) NOT NULL DEFAULT 'active',
    version            BIGINT NOT NULL DEFAULT 1,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_backend_binding_active_app
    ON backend_binding (tenant_id, app_id, resource_type)
    WHERE app_id IS NOT NULL AND migration_state = 'active';

CREATE UNIQUE INDEX idx_backend_binding_active_tenant_default
    ON backend_binding (tenant_id, resource_type)
    WHERE app_id IS NULL AND migration_state = 'active';

CREATE TABLE channel_binding (
    channel_binding_id  VARCHAR(64) PRIMARY KEY,
    tenant_id           VARCHAR(64) NOT NULL REFERENCES tenant(tenant_id),
    app_id              VARCHAR(64) NOT NULL REFERENCES agent_app(app_id),
    channel_type        VARCHAR(32) NOT NULL,
    account_id          VARCHAR(255) NOT NULL,
    callback_key        VARCHAR(128) NOT NULL UNIQUE,
    config              JSONB NOT NULL DEFAULT '{}'::jsonb,
    secret_ref          VARCHAR(512) NOT NULL,
    status              VARCHAR(32) NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, channel_type, account_id)
);

CREATE TABLE external_identity (
    identity_id          VARCHAR(64) PRIMARY KEY,
    tenant_id            VARCHAR(64) NOT NULL REFERENCES tenant(tenant_id),
    channel_binding_id   VARCHAR(64) NOT NULL REFERENCES channel_binding(channel_binding_id),
    external_user_id     VARCHAR(255) NOT NULL,
    canonical_user_id    VARCHAR(128) NOT NULL,
    profile              JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (channel_binding_id, external_user_id)
);
```

`secret_ref` 保存 KMS 或 Secret Manager 路径，不保存 token、API key 或数据库密码。`backend_binding.app_id` 为空时表示租户默认配置，具体 App 配置优先。

## 4. 会话、消息和运行记录

```sql
CREATE TABLE conversation (
    conversation_id     VARCHAR(64) PRIMARY KEY,
    tenant_id            VARCHAR(64) NOT NULL REFERENCES tenant(tenant_id),
    app_id               VARCHAR(64) NOT NULL REFERENCES agent_app(app_id),
    channel_binding_id   VARCHAR(64) NOT NULL REFERENCES channel_binding(channel_binding_id),
    session_id           VARCHAR(255) NOT NULL,
    runtime_user_id      VARCHAR(255) NOT NULL,
    chat_type            VARCHAR(32) NOT NULL,
    external_chat_id     VARCHAR(255),
    external_thread_id   VARCHAR(255),
    pinned_revision_id   VARCHAR(64) REFERENCES agent_revision(revision_id),
    last_turn_seq        BIGINT NOT NULL DEFAULT 0,
    last_event_seq       BIGINT NOT NULL DEFAULT 0,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, app_id, runtime_user_id, session_id)
);

CREATE TABLE inbound_message (
    inbound_id            VARCHAR(64) PRIMARY KEY,
    tenant_id             VARCHAR(64) NOT NULL REFERENCES tenant(tenant_id),
    app_id                VARCHAR(64) NOT NULL REFERENCES agent_app(app_id),
    channel_binding_id    VARCHAR(64) NOT NULL REFERENCES channel_binding(channel_binding_id),
    external_message_id   VARCHAR(512) NOT NULL,
    request_id            VARCHAR(128) NOT NULL,
    conversation_id       VARCHAR(64) NOT NULL REFERENCES conversation(conversation_id),
    actor_user_id         VARCHAR(255) NOT NULL,
    message_type          VARCHAR(32) NOT NULL,
    payload               JSONB NOT NULL,
    payload_hash          VARCHAR(128) NOT NULL,
    status                VARCHAR(32) NOT NULL,
    received_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at          TIMESTAMPTZ,
    UNIQUE (channel_binding_id, external_message_id),
    UNIQUE (tenant_id, request_id)
);

CREATE TABLE agent_run (
    request_id            VARCHAR(128) PRIMARY KEY,
    tenant_id             VARCHAR(64) NOT NULL REFERENCES tenant(tenant_id),
    app_id                VARCHAR(64) NOT NULL REFERENCES agent_app(app_id),
    revision_id           VARCHAR(64) NOT NULL REFERENCES agent_revision(revision_id),
    conversation_id       VARCHAR(64) NOT NULL REFERENCES conversation(conversation_id),
    turn_seq              BIGINT NOT NULL,
    fencing_token         BIGINT NOT NULL,
    status                VARCHAR(32) NOT NULL,
    worker_id             VARCHAR(128),
    model_name            VARCHAR(255),
    prompt_tokens         BIGINT NOT NULL DEFAULT 0,
    completion_tokens     BIGINT NOT NULL DEFAULT 0,
    cost                  NUMERIC(20, 8) NOT NULL DEFAULT 0,
    error_type            VARCHAR(128),
    error_message         TEXT,
    trace_id              VARCHAR(64),
    started_at            TIMESTAMPTZ,
    completed_at          TIMESTAMPTZ,
    cancel_requested_at   TIMESTAMPTZ,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (conversation_id, turn_seq)
);
```

`external_message_id` 优先使用 IM 平台提供的稳定消息 ID。没有稳定 ID 的事件使用通道类型、账号、用户、聊天、事件类型和平台时间戳生成规范化哈希。

## 5. Session、Event 和 Summary

如果使用 tRPC-Agent-Go 原生 Session 适配器，实际表结构由对应子模块管理，平台只需要维护 `conversation` 和 `agent_run`。如果需要数据库级 event 幂等、严格 event sequence 或跨后端统一迁移，可以实现平台 Session Service，采用以下逻辑表：

```sql
CREATE TABLE agent_session (
    tenant_id         VARCHAR(64) NOT NULL,
    app_id            VARCHAR(64) NOT NULL,
    runtime_user_id   VARCHAR(255) NOT NULL,
    session_id        VARCHAR(255) NOT NULL,
    state             JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_event_seq    BIGINT NOT NULL DEFAULT 0,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at        TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, app_id, runtime_user_id, session_id)
);

CREATE TABLE session_event (
    tenant_id         VARCHAR(64) NOT NULL,
    app_id            VARCHAR(64) NOT NULL,
    runtime_user_id   VARCHAR(255) NOT NULL,
    session_id        VARCHAR(255) NOT NULL,
    event_id          VARCHAR(128) NOT NULL,
    event_seq         BIGINT NOT NULL,
    request_id        VARCHAR(128) NOT NULL,
    invocation_id     VARCHAR(128),
    author            VARCHAR(255) NOT NULL,
    role              VARCHAR(32),
    event             JSONB NOT NULL,
    state_delta       JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id, runtime_user_id, session_id, event_id),
    UNIQUE (tenant_id, app_id, runtime_user_id, session_id, event_seq)
);

CREATE INDEX idx_session_event_request
    ON session_event (tenant_id, request_id, event_seq);

CREATE TABLE session_summary (
    tenant_id          VARCHAR(64) NOT NULL,
    app_id             VARCHAR(64) NOT NULL,
    runtime_user_id    VARCHAR(255) NOT NULL,
    session_id         VARCHAR(255) NOT NULL,
    filter_key         VARCHAR(255) NOT NULL DEFAULT '',
    summary            TEXT NOT NULL,
    topics             JSONB NOT NULL DEFAULT '[]'::jsonb,
    high_watermark     BIGINT NOT NULL,
    last_event_id      VARCHAR(128),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id, runtime_user_id, session_id, filter_key)
);
```

更新 Session 时在事务内锁定 `agent_session` 行，分配下一个 `event_seq`，插入 Event 并合并 `state_delta`。Summary 只允许更大的 `high_watermark` 覆盖旧值。

## 6. Memory、Artifact 和工具执行

```sql
CREATE TABLE memory_record (
    tenant_id       VARCHAR(64) NOT NULL,
    app_id          VARCHAR(64) NOT NULL,
    subject_id      VARCHAR(255) NOT NULL,
    memory_id       VARCHAR(128) NOT NULL,
    memory_kind     VARCHAR(32) NOT NULL DEFAULT 'fact',
    content         TEXT NOT NULL,
    topics          JSONB NOT NULL DEFAULT '[]'::jsonb,
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_event_id VARCHAR(128),
    version         BIGINT NOT NULL DEFAULT 1,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at      TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, app_id, subject_id, memory_id)
);

CREATE TABLE artifact_metadata (
    artifact_id     VARCHAR(64) PRIMARY KEY,
    tenant_id       VARCHAR(64) NOT NULL,
    app_id          VARCHAR(64) NOT NULL,
    runtime_user_id VARCHAR(255) NOT NULL,
    session_id      VARCHAR(255) NOT NULL DEFAULT '',
    filename        VARCHAR(512) NOT NULL,
    version         BIGINT NOT NULL,
    object_key      VARCHAR(1024) NOT NULL UNIQUE,
    mime_type       VARCHAR(255),
    size_bytes      BIGINT NOT NULL,
    checksum        VARCHAR(128) NOT NULL,
    scan_status     VARCHAR(32) NOT NULL DEFAULT 'pending',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, app_id, runtime_user_id, session_id, filename, version)
);

CREATE TABLE tool_execution (
    execution_id     VARCHAR(64) PRIMARY KEY,
    tenant_id        VARCHAR(64) NOT NULL,
    request_id       VARCHAR(128) NOT NULL REFERENCES agent_run(request_id),
    tool_call_id     VARCHAR(128) NOT NULL,
    tool_name        VARCHAR(255) NOT NULL,
    arguments_hash   VARCHAR(128) NOT NULL,
    idempotency_key  VARCHAR(255),
    decision         VARCHAR(32) NOT NULL,
    status           VARCHAR(32) NOT NULL,
    result_ref       VARCHAR(1024),
    error_type       VARCHAR(128),
    started_at       TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ,
    UNIQUE (request_id, tool_call_id)
);
```

Artifact 内容放对象存储。当前实现使用进程 mutex，并在 PostgreSQL 控制面模式下使用 session-level advisory lock 覆盖多节点的 list-version + put 临界区；如果未来允许绕过 Runner 直接大规模并发上传，可再增加独立 artifact metadata/version allocator。

## 7. 审批、工具执行、后台任务和迁移

```sql
CREATE TABLE tool_approval (
    approval_id          VARCHAR(64) PRIMARY KEY,
    tenant_id            VARCHAR(64) NOT NULL,
    channel_binding_id   VARCHAR(64) NOT NULL,
    request_id           VARCHAR(128) NOT NULL,
    user_id              VARCHAR(512) NOT NULL,
    session_id           VARCHAR(512) NOT NULL,
    tool_call_id         VARCHAR(255) NOT NULL,
    tool_name            VARCHAR(255) NOT NULL,
    arguments_hash       VARCHAR(64) NOT NULL,
    status               VARCHAR(32) NOT NULL,
    decision_message_id  VARCHAR(512),
    expires_at           TIMESTAMPTZ NOT NULL,
    decided_at           TIMESTAMPTZ,
    resumed_at           TIMESTAMPTZ,
    UNIQUE (tenant_id, request_id, tool_call_id)
);

CREATE TABLE tool_execution (
    execution_id     VARCHAR(64) PRIMARY KEY,
    tenant_id        VARCHAR(64) NOT NULL,
    request_id       VARCHAR(128) NOT NULL,
    revision_id      VARCHAR(64) NOT NULL,
    tool_call_id     VARCHAR(255) NOT NULL,
    tool_name        VARCHAR(255) NOT NULL,
    arguments_hash   VARCHAR(64) NOT NULL,
    status           VARCHAR(32) NOT NULL,
    result_hash      VARCHAR(64),
    error_type       VARCHAR(128),
    started_at       TIMESTAMPTZ NOT NULL,
    completed_at     TIMESTAMPTZ,
    UNIQUE (request_id, tool_call_id)
);

CREATE TABLE background_job (
    job_id            VARCHAR(64) PRIMARY KEY,
    tenant_id         VARCHAR(64) NOT NULL,
    app_id            VARCHAR(64) NOT NULL,
    revision_id       VARCHAR(64) NOT NULL,
    job_type          VARCHAR(64) NOT NULL,
    dedupe_key        VARCHAR(512) NOT NULL,
    payload           JSONB NOT NULL,
    status            VARCHAR(32) NOT NULL,
    attempt_count     INT NOT NULL,
    max_attempts      INT NOT NULL,
    next_attempt_at   TIMESTAMPTZ NOT NULL,
    locked_by         VARCHAR(128),
    locked_until      TIMESTAMPTZ,
    trace_parent      VARCHAR(128),
    last_error        TEXT,
    UNIQUE (tenant_id, job_type, dedupe_key)
);

CREATE TABLE backend_migration (
    migration_id       VARCHAR(64) PRIMARY KEY,
    tenant_id          VARCHAR(64) NOT NULL,
    app_id             VARCHAR(64),
    resource_type      VARCHAR(64) NOT NULL,
    source_binding_id  VARCHAR(64) NOT NULL,
    target_binding_id  VARCHAR(64) NOT NULL,
    state              VARCHAR(32) NOT NULL,
    checkpoint         JSONB NOT NULL,
    verification       JSONB NOT NULL,
    repair_backlog     BIGINT NOT NULL,
    version            BIGINT NOT NULL
);
```

## 8. Outbox 和审计

```sql
CREATE TABLE outbox_event (
    outbox_id        BIGSERIAL PRIMARY KEY,
    tenant_id        VARCHAR(64) NOT NULL,
    aggregate_type   VARCHAR(64) NOT NULL,
    aggregate_id     VARCHAR(128) NOT NULL,
    event_type       VARCHAR(128) NOT NULL,
    payload          JSONB NOT NULL,
    traceparent      VARCHAR(255),
    available_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at     TIMESTAMPTZ,
    retry_count      INT NOT NULL DEFAULT 0,
    last_error       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_outbox_pending
    ON outbox_event (available_at, outbox_id)
    WHERE published_at IS NULL;

CREATE TABLE audit_log (
    audit_id          BIGSERIAL PRIMARY KEY,
    occurred_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    tenant_id         VARCHAR(64) NOT NULL,
    channel           VARCHAR(32),
    user_id           VARCHAR(255),
    actor_user_id     VARCHAR(255),
    session_id        VARCHAR(255),
    request_id        VARCHAR(128),
    agent_name        VARCHAR(255),
    revision_id       VARCHAR(64),
    tool_name         VARCHAR(255),
    decision          VARCHAR(64) NOT NULL,
    policy_version    VARCHAR(64),
    arguments_hash    VARCHAR(128),
    latency_ms        BIGINT,
    error_type        VARCHAR(128),
    cost              NUMERIC(20, 8),
    trace_id          VARCHAR(64),
    details           JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX idx_audit_tenant_time
    ON audit_log (tenant_id, occurred_at DESC);
CREATE INDEX idx_audit_trace
    ON audit_log (trace_id);
```

审计日志应按月分区并设置租户级保留期。`details` 只保存脱敏后的结构化信息；原始 prompt、工具参数和工具结果不直接写入该表。

## 9. 删除和保留策略

租户删除采用两阶段流程：先冻结写入和撤销密钥，再异步清理 Session、Memory、Knowledge、Artifact 和审计数据。每个后端产生删除清单和校验结果，完成前租户状态保持 `deleting`。

Session、Memory 和 Artifact 的保留期可以不同。Artifact 到期先删除对象，再标记元数据；如果对象删除失败，任务进入重试和人工对账，不能只删 SQL 记录后留下孤儿对象。
