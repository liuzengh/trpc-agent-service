# Issue #71：Tenant-scoped Provider Registries

本页记录运行时多租户装配的注册表与 Factory 契约。进程内注册表用于本地开发和确定性测试，
Vault、Redis、S3 和 Channel provider 使用相同接口完成生产装配。

生产 SecretManager 适配器位于 `trpcservice/model/vault`，面向 HashiCorp Vault KV v2。它按
`<tenant-id>/<secret-ref>` 读取 `value` 字段，只接受 HTTPS endpoint，Token 仅保留在运行时
HTTP header；Vault 的状态码、响应体和 transport 错误均转换为稳定的脱敏错误。

## Secret

`runtime/model.SecretRegistry` 使用 `(tenant_id, secret_ref)` 作为唯一 key，注册、替换、删除和解析均要求显式租户。解析失败统一返回脱敏错误，secret 值不会出现在错误、`String` 表示、计划、缓存 key 或持久化对象中。`Close` 会清空值并拒绝后续写入；`model` 只保留 `SecretScope`、`SecretValue` 和 `SecretResolver` 契约。

## Model Provider

`runtime/model.ModelProviderRegistry` 使用 `(tenant_id, provider)` 路由 `ModelFactory`。工厂输入会在调用边界 clone；未知租户或 provider fail closed，调用上下文取消优先于 provider 结果。

## Backend Provider

`runtime/storage/factory.ProviderRegistry` 使用 `(tenant_id, capability, provider)` 路由 `CapabilityProvider`。它只持有工厂引用，不持有已物化 capability；`backend` 只保留冻结的无密钥 `StorageFactoryInput` 和能力绑定契约。

## Channel Provider

`channels/provider.Registry` 使用 `(tenant_id, channel, provider_account_id)` 路由 `Factory`。
它是组合层适配器，只有这里依赖 `outbox.Provider`；`channels` 根包不再反向依赖
回复投递实现，因此 Telegram、WeCom 等具体 adapter 不会污染绑定领域模型。

## Storage / Session materialization

`runtime/storage/factory.StorageFactory` 接收 `ExecutionPlan.StorageFactoryInput()` 的防御性副本，按 capability/provider 从注册表解析实现，并将临时 secret 只传给当前工厂调用。返回的 `CapabilitySet` 由创建它的 Runner 持有；Runner 关闭时释放所有实现了 `Close` 的 capability。Session capability 必须实现 tRPC-Agent-Go 的 `session.Service`，否则 materialization fail closed。

旧版 `agent.NewRunner` 仍接受借用的 Session service；提供 StorageFactory 时启用多租户物化路径，
未提供时保持兼容。bootstrap 已把注册表和 StorageFactory 接入多租户生产装配。

所有注册表都是线程安全、可关闭的实现；持久化 capability、Secret rotation 和 cache invalidation
由 PostgreSQL/Redis/S3 adapter、bootstrap 和 runtime registry 共同提供。
