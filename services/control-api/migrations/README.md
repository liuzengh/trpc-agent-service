# Control API migrations

Control API owns and embeds the migrations in this directory. Startup records
each applied file in `control_schema_migrations` while holding a PostgreSQL
transaction-level advisory lock. Ledger creation itself occurs after that lock
inside the migration transaction, so concurrent first startups cannot race on
creating the ledger. Production bootstrap uses the explicit
`CONTROL_MIGRATION_DATABASE_URL`, checks it against the independent runtime
connection's actual database/schema, and closes the migration pool before handing
the runtime pool to modules. Production startup fixes the names to the `control`
schema and `control_migrator`/`control_runtime` roles. It rejects global admin
attributes on either role; the migrator must own the schema while the runtime
must not own or create in it. Another workload's matching DSNs are rejected before
any DDL.

The schema is selected by the trusted connection/role `search_path`; historical
SQL remains unqualified and unchanged. `MigrateForRuntime` revokes the runtime
role's mutation privileges on the ledger inside the same transaction that creates
it, retaining only `SELECT`. `Migrate` remains available for existing dedicated
integration fixtures; it is not the production startup path. Provisioning must
preserve ledger grants and the runtime role must not inherit the schema owner.

`0001_baseline.sql` contains the V1 Identity, Platform Operator, Tenant, Membership,
Agent, Agent Draft, immutable Agent Version, Runtime Profile, Profile Draft,
immutable Profile Revision, private Profile Credential, Deployment, immutable
Deployment Revision and Runtime Manifest, command Receipt, and Control Outbox
schema. The baseline may grow while the greenfield V1 schema is still under
construction; after the first released deployment, every schema change must use
a new migration.

Changing this greenfield baseline does not replay it in a database where
`0001_baseline.sql` is already present in `control_schema_migrations`. Development
and test databases created from an older baseline must be rebuilt instead of
receiving a compatibility migration before the first release.

`0002_channel_preflights.sql` is an additive upgrade for Telegram read-only
diagnostics from the currently deployed Channel baseline. It adds only
`channel_preflights` and `channel_preflight_requests` plus their indexes. The
existing baseline is not rewritten; existing accounts, credentials, routes,
observations, command receipts, and Outbox records remain unchanged. Existing
databases apply this file once through the normal embedded migration runner.
The diagnostic wire protocol remains V1; a numbered SQL migration is not a
new product/API version. Rollback testing uses an isolated database/schema copy,
not table removal from an existing deployment.


`0003_telegram_receive_modes.sql` preserves old Telegram accounts as explicit
webhook and introduces exact physical-Bot uniqueness and mode-aware observations.

`0004_wecom_preflights.sql` replaces only the completed-check-count constraint on
`channel_preflights`: Telegram keeps eight checks, while WeCom requires three
checks plus `wecom_long_connection_v1`, `long_connection`, and an explicit true
connection-probe confirmation. The Provider branch rejects SQL NULL rather than
silently accepting missing metadata. No table, operational record or earlier SQL
file is rewritten. Apply this additive migration before accepting WeCom probes.

`0005_channel_principals.sql` adds the F01 account-scoped external principal
identity table. Tenant/account/provider references and external identity
uniqueness are enforced together; revocation retains the mapping. ACTIVE is an
identity state, not permission to invoke an Agent. This persistence foundation
does not register principal HTTP routes, publish policies, or enable admission;
those are separate pending optimization slices.

0005 同时扩展 Channel 命令回执的 operation 闭合集合，允许
`RegisterChannelPrincipal` 与 `SetChannelPrincipalState`。主体写入与成功回执在
同一 OWNER 事务内提交；本迁移仍不发布策略或改变运行时接纳行为。

`0006_channel_access_policies.sql` adds one account-scoped policy head, append-only
revision documents and a normalized principal membership relation. Deferred
constraints require a real head revision and an exact body/member set at commit;
triggers reject revision/member mutation and non-monotonic head updates.
The policy storage adapter writes revision, head, member rows and reference-only
publication/audit records in `control_outbox` within one OWNER transaction.
These new outbox types are not consumed by the existing route relay. The policy
publisher's Session/Quota/tool-owner resolution, command API and new relay/schema
integration remain pending; applying this migration does not enable authorization.

0006 also registers `PublishChannelAccessPolicy` in the closed command receipt
operation set. `PolicyPublisher` now writes its compact success receipt inside
the revision/outbox transaction. Its mandatory owner validator has no permissive
production default; owner adapters and public/bootstrap wiring remain pending.

`0007_channel_policy_definitions.sql` stores Control-owned immutable Session/Quota
policy definitions with exact tenant/kind/id/revision keys. The owner reader
checks the entire document digest after JSONB retrieval. History is append-only.
The same migration adds immutable, tenant/kind/id-scoped MAC command receipts.
The owner publisher now commits definition, publication/audit outboxes and receipt
atomically under OWNER and revision checks. Management API, production bootstrap
and event relay remain pending. These are policy configuration records, not
Worker Session facts or runtime quota consumption.

### 0008：账户授权快照 generation

`0008_channel_authorization_generation.sql` 为现存及新建账户维护数据库事务级单调
fence。主体、策略 head、账户变化都会失效旧快照分页；原有 0001–0007 内容不变。
新增 `COLLATE "C"` keyset 索引以匹配跨语言摘要顺序。generation 不是 Broker offset，
也不代表 Gateway/Worker 已安装当前授权。迁移不会启用策略投影或运行授权开关。

`0010_tenant_usage_policy.sql` stores one current, tenant-owned usage policy and
CAS/idempotency metadata. Its revision is only an update fence: V1 does not expose
policy history. Runtime enforcement remains in Gateway and Worker shared stores.
