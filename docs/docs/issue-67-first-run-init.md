# Issue #67：首次运行初始化

`trpc-service init` 是一次性、显式授权的首租户初始化命令。它解决“服务启动需要 Tenant/App ID，而 ID 只有创建资源后才产生”的首次运行闭环；普通服务启动不会自动创建资源。

## 命令与配置

命令只从 `TRPC_POSTGRES_DSN` 读取数据库连接。Tenant/App 的元数据可以放在环境变量中，也可以用同名命令行参数覆盖：

| 配置 | 用途 | 必需 |
| --- | --- | --- |
| `TRPC_POSTGRES_DSN` | PostgreSQL 控制面连接 | 是 |
| `TRPC_INIT_TENANT_KEY` / `--tenant-key` | 初始 Tenant key | 是 |
| `TRPC_INIT_TENANT_NAME` / `--tenant-name` | 初始 Tenant 显示名 | 是 |
| `TRPC_INIT_APP_KEY` / `--app-key` | 初始 Agent App key | 是 |
| `TRPC_INIT_APP_NAME` / `--app-name` | 初始 Agent App 显示名 | 是 |
| `TRPC_INIT_APP_DESCRIPTION` / `--app-description` | Agent App 描述 | 否 |
| `--confirm` | 明确授权执行初始化 | 是 |

数据库 DSN 不提供命令行参数，以免凭据出现在进程参数或 shell 历史中。`--confirm` 是额外的操作员确认；没有它时命令不会连接数据库。

## 本地首次运行

使用一个专用的本地 PostgreSQL 数据库：

```bash
createdb trpc_control_plane
export TRPC_POSTGRES_DSN='postgres://postgres:postgres@127.0.0.1:5432/trpc_control_plane?sslmode=disable'
export TRPC_INIT_TENANT_KEY='local'
export TRPC_INIT_TENANT_NAME='Local Tenant'
export TRPC_INIT_APP_KEY='assistant'
export TRPC_INIT_APP_NAME='Local Assistant'

go run ./cmd/trpc-service init --confirm
```

成功输出只包含可复制的两个 ID：

```text
export TRPC_TENANT_ID='t_...'
export TRPC_APP_ID='app_...'
```

将这两个值加入服务进程的环境后，再按[正常启动配置](issue-41-runtime-bootstrap-admin-api.md)提供 API token、Admin token、模型 SecretRef/密钥和运行时存储配置。`init` 不创建模型、API key、Revision 或 IM binding；初始 Agent App 保持为 draft。

## Staging 与 production

在 staging 或 production 中，将 init 作为受控的一次性 Job 或维护步骤执行：

1. 从 Secret Manager 注入 `TRPC_POSTGRES_DSN`，不要把 DSN 写入 Job 参数、镜像或仓库。
2. 通过受审批的 operator 身份运行 `trpc-service init --confirm`，使用具备 migration 和控制面受控写入口权限的数据库连接；不要使用仅用于正常流量的低权限运行时连接。
3. 保护 Job stdout 和审计记录。输出只有 Tenant/App ID，不包含 DSN、数据库凭据、模型/API/IM secret；只将 ID 写入后续服务的受控配置。
4. 初始化成功后，使用生成的 `TRPC_TENANT_ID` 和 `TRPC_APP_ID` 启动服务。启动仍要求这些 ID 显式存在，不会因为数据库已初始化而自动推断它们。

命令会先应用并校验内置 migration。不要在 production 中手工写表或绕过受控写函数；执行前应确认连接到目标数据库并遵循现有备份、变更窗口和回滚流程。

## 幂等、并发与异常状态

初始化事务持有首租户全局 PostgreSQL advisory lock，因此多个 Job 或 operator 同时运行时最多一个调用创建资源。完成后重复运行不会更新元数据或创建副本，而是返回数据库中稳定的 Tenant/App ID。

命令对数据库状态采取 fail-closed 规则：

- 没有 Tenant 且没有 App：创建一个 active Tenant 和一个 draft Agent App；ID 由领域构造器生成。
- 恰好一个 Tenant 且恰好一个属于它的 App：视为已完成，返回现有 ID。
- 多个 Tenant、多个 App、跨 Tenant 不一致，或只有 Tenant 没有 App：返回明确错误并停止，不猜测应该使用哪个对象。

正常服务启动路径仍调用 `bootstrap.NewFromEnvironment`，继续要求显式的 `TRPC_TENANT_ID` 和 `TRPC_APP_ID`；不会在每次启动时创建资源，也没有公开 setup endpoint。
