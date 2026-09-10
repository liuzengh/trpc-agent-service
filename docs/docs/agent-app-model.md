# Agent App 模型与发布边界

> 本文承接 Tenant 闭环，定义租户级 Agent App 的稳定身份、版本化执行配置、
> 发布与回滚语义，以及供 Agent Factory 消费的一次执行快照。实现跟踪见
> [Issue #17](https://github.com/XnLemon/trpc-agent-service/issues/17)。

> 当前交付：schema v1 支持 `llm` 和顺序 `chain` 两种 Agent kind；
> `trpcservice/agent/factory.go` 通过 `kind + schema_version` 注册并构造对应 Agent，
> 发布、回滚、灰度候选、执行快照和租户工具授权均由控制面与运行时测试覆盖。

## 目标与边界

Agent App 是租户创建、发布和路由 Agent 的控制面对象。它必须同时解决两类问题：

- 为 Admin API、Channel Binding 和审计提供稳定的应用身份。
- 为 Worker 和 Agent Factory 提供不可变、可复现的执行配置。

因此模型不能把名称、草稿、当前配置和历史版本混成一个可覆盖的 JSON 对象。当前采用
“稳定根实体 + 版本化 Revision”结构：`agent_app` 保存身份、生命周期和当前发布指针，
`agent_app_revision` 保存草稿或已发布的执行定义。

当前交付覆盖领域模型、发布/回滚、候选版本、PostgreSQL/InMemory Repository、执行快照、
Agent Factory 和服务级运行时装配。Issue #37 的 `migrations/0001_control_plane.up.sql`
与 `0002` 受控写入口复用本页 DDL，SQL Repository、Secret Resolver、Gateway 和 Channel
通过各自开发文档的集成测试共同验收。

## 核心决策

### 稳定身份与发布内容分离

`app_id` 和 `app_key` 创建后不可修改。展示名和描述可以修改，但不参与路由、鉴权、
Session 命名空间或缓存键。

App 根实体使用乐观锁 `version`；Revision 使用 App 内单调递增的 `revision`：

- `app.version` 检测元数据、状态和当前发布指针的并发修改。
- `revision` 标识可以被历史执行、审计和回滚稳定引用的 Agent 定义。

发布后的 Revision 永久不可修改。更新配置必须维护草稿，再发布为新的 Revision。回滚不覆盖
内容，只把 `current_revision` 切换到一个历史已发布 Revision。

### 第一阶段支持 LLMAgent 与顺序 Chain

模型保留 `agent_kind` 和 `schema_version`。schema v1 接受 `agent_kind = 'llm'` 和
`agent_kind = 'chain'`：Chain 由 2–32 个有序、命名且经过校验的 LLMAgent step 组成，
每个 step 继承父 Revision 的模型、工具 allowlist、generation 和 runtime policy。
Agent Factory 对 schema version 和 kind 做显式注册与校验，未知组合不会进入运行时。

### 引用能力，不持有运行时对象

Revision 保存 `model_profile_id`、工具授权和 Knowledge/Backend 引用，不保存
`model.Model`、`tool.Tool`、数据库连接、函数指针或进程内对象。

模型 API key、IM token、数据库密码和 Tool 凭据只存在于 Secret Manager。Agent Factory
在可信 `tenant_id` 已确定后，用 Revision 的同租户引用解析运行时依赖并按授权注入。

工具采用显式 allowlist。未列出的 Tool、ToolSet、MCP Server 或 Skill 不得因平台注册、
模型请求或名称碰撞自动出现在 Agent 中。

## PostgreSQL 数据模型

以下 DDL 与 migration 描述并落实完整性和事务语义；InMemory、PostgreSQL 和 MySQL Repository
共享同一领域边界。

### Agent App 根表

```sql
CREATE TABLE agent_app (
    tenant_id       TEXT NOT NULL REFERENCES tenant(tenant_id),
    app_id          TEXT NOT NULL
                    CHECK (app_id ~ '^app_[0-7][0-9A-HJKMNP-TV-Z]{25}$'),
    app_key         TEXT NOT NULL
                    CHECK (app_key ~ '^[a-z][a-z0-9-]{1,63}$'),
    display_name    TEXT NOT NULL
                    CHECK (length(btrim(display_name)) BETWEEN 1 AND 200),
    description     TEXT NOT NULL DEFAULT ''
                    CHECK (length(description) <= 2000),

    status          TEXT NOT NULL DEFAULT 'draft'
                    CHECK (status IN ('draft', 'active', 'suspended', 'disabled')),
    current_revision BIGINT,

    version         BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (tenant_id, app_id),
    UNIQUE (tenant_id, app_key),
    CHECK (
        (status = 'draft' AND current_revision IS NULL)
        OR (status IN ('active', 'suspended') AND current_revision IS NOT NULL)
        OR status = 'disabled'
    )
);
```

`app_key` 只在租户内唯一。不同租户可以使用相同 key；Gateway 和 Admin API 必须先从可信
边界解析 `tenant_id`，再解释 App key。

### Agent App Revision

```sql
CREATE TABLE agent_app_revision (
    tenant_id       TEXT NOT NULL,
    app_id          TEXT NOT NULL,
    revision        BIGINT NOT NULL CHECK (revision >= 1),

    state           TEXT NOT NULL DEFAULT 'draft'
                    CHECK (state IN ('draft', 'published')),
    draft_version   BIGINT NOT NULL DEFAULT 1 CHECK (draft_version >= 1),
    agent_kind      TEXT NOT NULL CHECK (agent_kind IN ('llm', 'chain')),
    schema_version  INT NOT NULL DEFAULT 1 CHECK (schema_version = 1),

    description        TEXT NOT NULL DEFAULT '' CHECK (length(description) <= 2000),
    instruction        TEXT NOT NULL
                       CHECK (length(btrim(instruction)) BETWEEN 1 AND 65536),
    global_instruction TEXT NOT NULL DEFAULT ''
                       CHECK (length(global_instruction) <= 65536),

    -- migration 使用同租户复合外键，防止跨租户 Profile 引用。
    model_profile_id TEXT NOT NULL,

    -- 只保存按 agent_kind + schema_version 校验过的无密钥配置。
    generation_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    runtime_policy    JSONB NOT NULL DEFAULT '{}'::jsonb,

    content_digest    TEXT,
    published_at      TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (tenant_id, app_id, revision),
    FOREIGN KEY (tenant_id, app_id)
        REFERENCES agent_app(tenant_id, app_id),
    CHECK (
        (state = 'draft' AND content_digest IS NULL AND published_at IS NULL)
        OR
        (state = 'published'
         AND content_digest ~ '^[0-9a-f]{64}$'
         AND published_at IS NOT NULL)
    )
);

ALTER TABLE agent_app
    ADD CONSTRAINT fk_agent_app_current_revision
    FOREIGN KEY (tenant_id, app_id, current_revision)
    REFERENCES agent_app_revision(tenant_id, app_id, revision);

CREATE OR REPLACE FUNCTION agent_app_reject_identity_change()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
       OR NEW.app_id IS DISTINCT FROM OLD.app_id
       OR NEW.app_key IS DISTINCT FROM OLD.app_key THEN
        RAISE EXCEPTION 'agent app identity is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER agent_app_identity_immutable
BEFORE UPDATE ON agent_app
FOR EACH ROW EXECUTE FUNCTION agent_app_reject_identity_change();

CREATE OR REPLACE FUNCTION agent_app_revision_reject_published_change()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.state = 'published' THEN
        RAISE EXCEPTION 'published agent app revision is immutable';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER agent_app_revision_published_immutable
BEFORE UPDATE OR DELETE ON agent_app_revision
FOR EACH ROW EXECUTE FUNCTION agent_app_revision_reject_published_change();
```

JSONB 用于容纳框架的有界可选参数，不是绕开领域校验。Go 层必须解码到具体类型，拒绝未知
字段、未知 schema version、非法数值和任何 Secret 字段。框架默认值必须在发布时物化或
纳入 digest 语义，避免框架升级后同一 Revision 静默改变行为。

### 工具授权和默认引用

```sql
CREATE TABLE agent_app_revision_tool (
    tenant_id  TEXT NOT NULL,
    app_id     TEXT NOT NULL,
    revision   BIGINT NOT NULL,
    tool_id    TEXT NOT NULL,
    required   BOOLEAN NOT NULL DEFAULT false,

    PRIMARY KEY (tenant_id, app_id, revision, tool_id),
    FOREIGN KEY (tenant_id, app_id, revision)
        REFERENCES agent_app_revision(tenant_id, app_id, revision)
        ON DELETE CASCADE,
    CHECK (length(btrim(tool_id)) > 0)
);

CREATE OR REPLACE FUNCTION agent_app_revision_tool_guard()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_tenant_id TEXT;
    v_app_id TEXT;
    v_revision BIGINT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        v_tenant_id := OLD.tenant_id;
        v_app_id := OLD.app_id;
        v_revision := OLD.revision;
    ELSE
        v_tenant_id := NEW.tenant_id;
        v_app_id := NEW.app_id;
        v_revision := NEW.revision;
    END IF;

    IF EXISTS (
        SELECT 1 FROM agent_app_revision
        WHERE tenant_id = v_tenant_id
          AND app_id = v_app_id
          AND revision = v_revision
          AND state = 'published'
    ) THEN
        RAISE EXCEPTION 'published agent app tool authorization is immutable';
    END IF;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER agent_app_revision_tool_immutable
BEFORE INSERT OR UPDATE OR DELETE ON agent_app_revision_tool
FOR EACH ROW EXECUTE FUNCTION agent_app_revision_tool_guard();

ALTER TABLE tenant
    ADD CONSTRAINT fk_tenant_default_agent_app
    FOREIGN KEY (tenant_id, default_agent_app_id)
    REFERENCES agent_app(tenant_id, app_id);
```

已发布 Revision 持久化显式 `tool_id` 授权；Runner 构造时通过已安装的 `ToolRegistry`
解析这些 ID，required 工具不可用会使构造失败。数据库外键保证默认 App 的归属和存在；
设置默认对象时，Admin API 还验证 App active 且存在有效当前 Revision。暂停或停用默认
App 时，不得静默回退到其他 App。

生产角色不能直接修改发布表或绕过上述触发器。与 Tenant 状态变更相同，发布、回滚和状态
迁移应通过固定 `search_path` 的受控函数完成，并先撤销 PostgreSQL 默认授予 `PUBLIC` 的
函数执行权限。Migration owner 不属于运行时连接池；Worker 只消费执行快照，不读取草稿或
枚举其他租户的 App。

## 生命周期

```text
                         首次发布
                  draft ─────────────> active
                                          │  ▲
                                 暂停     │  │ 恢复
                                          ▼  │
                                      suspended
                                          │
                         停用             │ 停用
                  draft ─────────────┐    │
                                    ▼    ▼
                                    disabled
                                      终态
```

- `draft`：没有已发布 Revision，允许维护草稿，不接受执行。
- `active`：存在 `current_revision`，允许创建新的执行快照。
- `suspended`：保留当前 Revision 和数据，拒绝新执行，可以恢复。
- `disabled`：终态，拒绝发布、回滚、恢复和新执行。

App active 后可以继续创建草稿，根状态仍为 active，现有流量继续使用当前 Revision，直到
新草稿成功发布。草稿不完整或发布失败不能影响当前流量。

Tenant 与 App 是连续的两道门禁：只有两者都 active 才能创建新执行快照。已创建快照按既定
取消和收尾策略完成，暂停不篡改进行中的快照。

## 草稿、发布与回滚

草稿更新同时校验 `expected_app_version` 和 `expected_draft_version`，分别防止状态/发布操作
与草稿编辑发生并发覆盖。实现可以允许多个草稿，但每次发布必须显式指定 Revision，不能
使用“最新一条”之类存在竞态的查询。

### 发布事务

发布必须在一个控制面事务中完成：

1. 以 `(tenant_id, app_id)` 锁定 App，校验 expected app version。
2. 锁定指定草稿，校验 expected draft version。
3. 验证 App 未 disabled、Tenant active、Revision schema 完整。
4. 验证模型、Tool、Knowledge 和 Backend 引用属于同一租户且可发布。
5. 对规范化执行配置计算 content digest。
6. 将 Revision 冻结为 published，写入 digest 和发布时间。
7. 原子更新 `current_revision`；首次发布把 App 从 draft 变为 active。
8. 递增 App version 并写 Publish Outbox/Audit 事件。

任一步失败都必须回滚，旧 `current_revision` 继续提供服务。

摘要使用 SHA-256 小写十六进制，输入为确定性序列化后的 agent kind、schema version、
Prompt、模型引用、排序后的工具授权、generation config、runtime policy 和 Chain steps。摘要不包含时间、
actor 或 draft version。Map key、集合顺序和空值语义必须规范化。摘要用于缓存和审计，不是
签名或授权凭据。

### 回滚

回滚只允许选择同一 Tenant、同一 App 的历史 published Revision。事务校验 expected app
version，切换 `current_revision`，递增 App version 并记录事件。目标 Revision 不复制、
不重编号、不修改 digest。回滚只影响后续快照，进行中的请求继续使用原 Revision。

## 审计事件

发布、回滚和状态迁移返回结构化事件：

| 字段 | 说明 |
| --- | --- |
| `event_type` | published、rolled_back 或状态事件 |
| `tenant_id` / `app_id` | 显式租户和应用边界 |
| `previous_revision` / `current_revision` | 变更前后发布指针 |
| `content_digest` | 当前 Revision 摘要 |
| `actor_type` / `actor_id` | 可信变更主体 |
| `reason` | 1–1000 字符的非空原因 |
| `correlation_id` | 管理请求或发布流程关联 ID |
| `previous_version` / `next_version` | App 乐观锁版本 |
| `occurred_at` | UTC 发生时间 |

InMemory Repository 把事件作为调用结果返回。生产 SQL Repository 必须在同一事务中更新
App 并写 Outbox，不能先发布成功再尽力写审计。

## Go 目录与 Repository

`trpcservice/app` 持有 App、Revision 和 Repository；`trpcservice/agent` 持有不可变执行快照与
Agent Factory。领域状态、仓储和运行时装配保持清晰边界。

```text
trpcservice/app/
├── app.go            # App 根实体、生命周期和校验
├── revision.go       # 草稿、发布版本及摘要
├── repository.go     # 控制面 Repository 契约
├── inmemory/         # 单进程开发/确定性测试实现
├── postgres/         # PostgreSQL Repository
└── mysql/            # MySQL Repository

trpcservice/agent/
├── execution.go      # 执行快照和 Factory 输入边界
└── factory.go        # 按 kind + schema version 构造 Agent
```

Agent Factory 与控制面 Repository 保持稳定的包边界，扩展实现继续通过
`kind + schema_version` 注册，不复制同一领域抽象。

```go
type Repository interface {
    Create(context.Context, CreateInput) (*App, error)
    Get(context.Context, string, string) (*App, error)
    CreateDraft(context.Context, CreateDraftInput) (*Revision, error)
    UpdateDraft(context.Context, UpdateDraftInput) (*Revision, error)
    GetRevision(context.Context, string, string, int64) (*Revision, error)
    Publish(context.Context, PublishInput) (*App, *Revision, ChangeEvent, error)
    Rollback(context.Context, RollbackInput) (*App, ChangeEvent, error)
    SetCanary(context.Context, SetCanaryInput) (*App, ChangeEvent, error)
    TransitionStatus(context.Context, TransitionStatusInput) (*App, ChangeEvent, error)
}
```

实际 API 应使用清晰参数名或输入结构体，避免混淆 tenant ID 与 app ID。任何按 app key 的查询
也必须同时接收 tenant ID。

实现必须遵守：

- 调用开始、等待锁后和提交修改前检查 `context.Context` 取消。
- 读写返回深拷贝，特别是工具切片、JSON Map 和可选指针。
- `app_key` 唯一索引按 tenant 分区。
- Revision 编号只在同一 App 内单调分配。
- version 冲突返回可识别的 sentinel 或 typed error。
- InMemory 用于单进程开发和确定性测试；PostgreSQL/MySQL Repository 持久化控制面状态，
  Worker 通过选定的共享 runtime capability 获得跨节点执行状态。

## 执行快照与 Agent Factory

`AgentExecutionSnapshot` 是一次 Worker 执行的不可变输入，至少包含 Tenant ID/version、
App ID/key/version、固定 Revision/content digest、无密钥 Agent 定义及依赖引用。

快照构造器连续验证：

1. Tenant snapshot 来自受保护构造边界且状态 active。
2. App 属于同一 Tenant、状态 active，并存在当前 Revision。
3. Revision 属于同一 App、状态 published，编号等于 `current_revision`。
4. 内容摘要有效，schema version 受支持。

快照内部状态必须封闭，访问器返回防御性副本。零值或手工构造的畸形值不得通过 Context
注入成为可信配置。

平台 `trpcservice/agent` Factory 根据快照完成运行时装配：

| Revision 配置 | tRPC-Agent-Go 边界 |
| --- | --- |
| App key / 展示元数据 | LLMAgent 或 Chain name 与 description；稳定运行身份仍使用 App ID |
| instruction / global instruction | LLMAgent 的 Instruction / GlobalInstruction；Chain 为父级运行说明 |
| chain steps | `chainagent.New` 的有序 LLMAgent 子 Agent |
| model profile ref | 同租户 Model Registry 解析为 `model.Model` |
| tool allowlist | 已安装 `ToolRegistry` 按已发布 Revision 授权解析为 `tool.Tool` / `tool.ToolSet` |
| generation config | `model.GenerationConfig` 的受支持字段 |
| runtime policy | Tool 并行、并发、循环和有界执行选项 |
| tenant/app/revision/digest | Factory 缓存键及 OTel attributes |

Factory 复用 tRPC-Agent-Go 的 LLMAgent、Chain、Agent、Runner、Tool、Session 和 Memory。
平台层只负责租户授权、配置解析、依赖注入、缓存和审计，不复制框架执行循环；未知的
`kind + schema_version` 不会回退到 LLMAgent。

Factory 缓存键至少包含：

```text
tenant_id + app_id + revision + content_digest
```

不能只用 app key、展示名或“当前版本”。发布和回滚产生新快照键，旧缓存项在已有执行释放
引用后按容量或 TTL 回收。

## 验证策略

实现测试至少覆盖：

- App ID/key、展示字段、状态、时间和 version 边界。
- 相同 app key 在不同租户允许、同租户拒绝。
- Repository 的显式租户隔离和深拷贝。
- 草稿 version 冲突和并发更新单一获胜。
- 首次/连续发布、失败发布保持旧版本、历史回滚。
- published Revision 不可修改。
- draft、suspended、disabled、无当前 Revision 以及 Tenant 非 active 的门禁。
- context 已取消和等待锁时取消。
- 发布、回滚、状态事件的 actor、reason、correlation 和版本字段。
- InMemory Repository race 测试。

## 交付关系

```text
tenant
  ├── agent_app + revision
  │     └── model/tool/knowledge references
  ├── backend_profile
  └── channel_binding -> agent_app
        └── Gateway / Worker 最小执行链路
```

Agent App Revision 通过同租户复合约束引用 Model Profile；ExecutionPlan 将同租户的
App/Revision、Model Profile 和 Backend Profile 固定为一次执行快照。Channel Binding 持有
自身的同租户 App 引用，可信 Channel 路由选定 App 后由 Gateway 使用该固定 ExecutionPlan 执行。
