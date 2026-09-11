# 数据模型设计

## 1. 模型分层

平台数据分为三类：

- 控制面数据：租户、Agent App、revision、通道、后端绑定和策略；
- 运行面数据：入站消息、Agent run、会话、事件、摘要、工具执行和回复 outbox；
- Agent 数据：Memory、Knowledge 元数据、Artifact 元数据和审计日志。

控制面和运行面推荐使用 PostgreSQL。Session、Memory、Knowledge 和 Artifact 的实际内容可以按租户路由到 Redis、SQL、向量库或对象存储。即使租户把 Session 放在 Redis，Control DB 中仍保留 inbound、agent run 和 delivery journal，用于幂等、审计和故障恢复。

以下 DDL 是最小逻辑模型，省略了组织成员、RBAC、计费明细和知识文档分片等扩展表。

实际建表和约束定义见[数据库迁移文件](../trpcservice/database/migrations)。核心实体及职责如下：

| 数据域 | 核心表 | 关系与职责 |
| --- | --- | --- |
| 租户与应用 | `tenant`、`agent_app`、`agent_revision` | 租户拥有应用，应用发布不可变配置版本 |
| 模型连接 | `model_connection` | 同租户连接 ID 固定模型与地址，凭据加密保存；配置版本与凭据版本分别管理 |
| 存储配置 | `backend_connection`、`backend_binding`、`backend_migration` | 具名连接与凭据引用、租户或应用绑定、迁移状态和校验证明 |
| 知识资料 | `knowledge_document` | Agent 资料摘要、处理任务和可检索状态；正文存入租户知识后端 |
| Skill | `skill_bundle` | 租户所属的不可变文件版本、校验值、审核状态和乐观锁版本 |
| 通道与会话 | `channel_binding`、`conversation` | 外部账号绑定应用，会话固定运行身份与发布版本 |
| 消息执行 | `inbound_message`、`agent_run`、`queue_outbox`、`outbound_message` | 入站去重、顺序调度、执行恢复与回复投递 |
| 治理 | `tool_approval`、`tool_execution`、`audit_log` | 参数绑定审批、执行事实、脱敏审计 |
| 后台处理 | `background_job`、`background_watermark` | 摘要、记忆提取和迁移任务及单调处理水位 |
| 控制台 | `admin_session`、`agent_draft`、`debug_snapshot`、`debug_session`、`debug_run` | 登录、草稿与独立调试；按 tenant/app/owner 授权 |
| 机器人连接 | `channel_connection`、`channel_credential`、`channel_connection_group`、`channel_connection_setting` | 连接状态、加密凭据、群元数据与部署地址 |

会话存储键由租户/应用 AppName、runtime_user_id 和 session_id 组成。Session、State、Event、Summary、Memory、向量和对象内容由对应的 tRPC-Agent-Go 后端管理；Control DB 保留消息、运行、审批、迁移和审计记录。

`agent_run.schedule_generation` 防止旧投递覆盖新调度；`next_attempt_at` 记录延迟恢复时间。`outbound_message.message_kind` 区分等待提示与最终回复，唯一约束为 `(request_id,message_kind)`。`finalized_at` 标识审计、用量和后台任务收尾完成；恢复已完成运行时只补收尾，不重新执行模型或工具。

调试身份与业务 IM 会话隔离。调试快照不作为发布版本使用，Worker 不读取登录凭据表。发布事务原子更新草稿、稳定版本指针和审计记录。

模型连接以 `(tenant_id,connection_id)` 为主键；`root_connection_id/config_version` 形成配置版本链，`credential_version` 标识密钥更新。AES-GCM 密文绑定租户、连接、模型和地址。主密钥由部署者保管，数据库仅保存指纹。Admin 拥有受限的修改权限，Worker/Jobs 只读。

连接凭据使用 `(tenant_id,reference)` 和用途约束。群元数据仅保存群名称与观察时间，不保存聊天正文。Telegram 登记确认后激活 Binding；企业微信消息 MCP 经逐群成员授权后激活，业务消息继续引用同一通道模型。

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

以下租户/对象批量销毁属于生产数据治理设计，当前没有租户级自动销毁接口。已实现的机器人移除采用软退役，见下一节。

租户删除采用两阶段流程：先冻结写入和撤销密钥，再异步清理 Session、Memory、Knowledge、Artifact 和审计数据。每个后端产生删除清单和校验结果，完成前租户状态保持 `deleting`。

Session、Memory 和 Artifact 的保留期可以不同。Artifact 到期先删除对象，再标记元数据；如果对象删除失败，任务进入重试和人工对账，不能只删 SQL 记录后留下孤儿对象。

## 10. 机器人连接维护

- `channel_binding.retired_at` 标记退役；约束要求退役绑定保持 disabled。当前有效绑定使用部分唯一索引，退役记录仍供旧 conversation、inbound、outbound 和 audit 引用。
- `channel_connection.status` 使用 removed 表示移除；目录隐藏移除项，当前账号唯一索引排除 removed，允许重新添加。`operation` 区分 activate/remove，不能将待确认移除误判成激活成功。
- 管理型企业微信 `config.group_grants` 以 chat_id 为键，每群记录 start_at 及成员的 id/name/since。新群和成员有各自时间下界；空成员列表表示不接收该群。接收进度与消息正文不存入授权配置。
- 凭据更新写入新的加密引用，旧会话、审计和退役绑定保留；更换企业微信地址会清理旧群列表缓存并重新确认授权。换绑 Agent 创建新的 BindingID，不修改历史会话所属应用。

实际 SQL 以 [031_connection_lifecycle.sql](../trpcservice/database/migrations/031_connection_lifecycle.sql) 为准。

## 11. 存储连接、Skill 与知识资料

`backend_connection` 以 `(tenant_id, connection_id)` 标识连接，保存显示名称、资源与后端类型、非秘密配置和内部凭据引用。真实值复用 `channel_credential` 的 AES-GCM 加密存储，授权用途扩展到 Session、Memory、Knowledge、Artifact 和 Embedding；连接接口不返回凭据引用或密文。绑定创建时校验凭据用途以及目标配置与已授权连接一致，不能把密钥改投到其他地址。

`skill_bundle` 以 `(tenant_id, name, version)` 为主键，保存 Markdown、脚本、SHA-256、提交者和审核者。`status` 为 pending、approved 或 revoked，`revision` 用于审核状态的并发更新；数据库触发器禁止修改版本内容。发布的 Agent 继续保存 name/version/checksum 引用，审核不会改写 Agent 版本。

表结构和约束见 [032_managed_resources.sql](../trpcservice/database/migrations/032_managed_resources.sql)。

`knowledge_document` 以 `(tenant_id, app_id, document_id)` 标识当前资料，保存名称、内容摘要、大小、元数据、使用的 Revision、后台任务和处理状态，不保存正文。正文仅进入有界后台任务并写入租户隔离的向量后端；任务状态变化通过数据库触发器同步目录，删除完成后移除目录记录。表结构见 [033_knowledge_management.sql](../trpcservice/database/migrations/033_knowledge_management.sql)。
