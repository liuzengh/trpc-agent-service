# Issue #69：Multi-tenant Bootstrap

生产 bootstrap 不再要求所有运行时对象绑定到一个 `RuntimeTenantID`。默认的单租户环境变量仍兼容；需要多个 API 租户时使用：

```text
TRPC_API_IDENTITIES=token-a|t_<tenant-a>|app_<app-a>|service-a,token-b|t_<tenant-b>|app_<app-b>|service-b
```

每个 identity 都会注册独立的 Model provider、Secret scope 和 Backend Session provider。Gateway 仍从控制面按可信 principal 解析完整 ExecutionPlan；Session capability 从该 plan 的 tenant scope materialize，因此多个租户可在同一进程并发执行。

该边界有一条无外部凭据的并发契约测试：两个 API identity 在同一 `Runtime` 中同时完成认证、计划解析和 Runner acquire；测试断言 Model factory 只能看到各自 tenant 的临时 secret，StorageFactory 对每个 tenant 只物化其自身的 Session capability。

`TRPC_API_IDENTITIES` 与旧的 `TRPC_API_TOKEN`/`TRPC_TENANT_ID`/`TRPC_APP_ID` 互斥：未设置 identity 列表时继续使用旧字段。单租户兼容路径继续使用 `TRPC_MODEL_API_KEY`；多租户部署使用 `TRPC_MODEL_API_KEYS=t_<tenant-a>=<key-a>,t_<tenant-b>=<key-b>`，每个 key 只在对应 `(tenant_id, secret_ref)` 的受信任 Factory 路径中注入 SecretRegistry，绝不会写入计划、缓存或数据库。

多租户 audit writer 按事件 `tenant_id` 懒加载 tenant-bound PostgreSQL store，并为每次写入设置对应的 RLS scope。WeCom channel runtime 使用按 `(tenant_id, account_id)` 作用域的 credential registry 与有界 worker group；对应注册、解析、关闭和并发调度均由 Channel 测试覆盖。

`WECOM_CALLBACK_TOKEN` 等单套 WeCom 环境凭据绑定一个 API identity。多 identity 配置对共享
WeCom 凭据执行显式校验，确保验签与 outbox worker 始终在确定的 tenant/account scope 内运行。
