# Admin 子领域设计

- **边界状态**：已接受
- **细节状态**：V1 已收口
- **实现状态**：Operator、用户管理、Tenant 开通和首个 Operator 引导已实现
- **适用范围**：Control API 内的平台级管理能力

## 1. 目的

`admin` 负责平台级授权和跨子领域管理命令。它不是公共 Handler、工具或配置的杂项
目录。它回答两类问题：当前用户是否是 Platform Operator，以及平台管理命令怎样
通过 Application Port 调用真正拥有数据的模块。

Platform Operator 与 Tenant 用户共用 `identity` 的账号和 Session。共享登录不表示
共享授权：`/v1/admin/*` 同时验证 Identity Context 和实时 Operator Grant；Tenant
资源仍由 Tenant Membership 授权。

## 2. 统一语言

| 术语 | 定义 |
| --- | --- |
| Platform Operator | 获得平台级管理授权的 UserAccount，不是 Tenant 角色 |
| Operator Grant | 将平台级能力授予 UserID 的可撤销记录 |
| Admin Context | 有效 Identity Context 与有效 Operator Grant 的组合 |
| Tenant Provisioning | 创建 Tenant 并原子建立 Initial Owner 的平台命令 |
| System Bootstrap | 全新数据库首次启动时创建第一个账号和 Grant 的系统 Actor |

## 3. 所有权边界

`admin` 拥有：

- Operator Grant 生命周期和“不得撤销最后一个 Operator”的规则。
- `/v1/admin/*` 的授权、DTO 和平台管理用例。
- 创建全局用户和开通 Tenant 的跨模块编排。
- 首个 Platform Operator 的空库判断与结果分类。

`admin` 不拥有：

- UserAccount、Password Credential 和 Session；它们属于 `identity`。
- Tenant 和 Membership；它们属于 `tenant`。
- PostgreSQL 连接、HTTP Server 和生命周期；它们属于共享 `infra/bootstrap`。

Admin 通过 Application Port 调用 Identity 与 Tenant，不导入它们的 PostgreSQL
Adapter，也不修改它们拥有的表。

## 4. Operator Grant

```text
OperatorGrant
├── UserID
├── GrantedByActorType: USER | SYSTEM_BOOTSTRAP
├── GrantedByUserID
├── GrantedAt
├── RevokedByUserID
└── RevokedAt
```

V1 使用固定能力集合，不引入细粒度 RBAC：

```text
users:manage
operators:manage
tenants:manage
```

必须满足：

- 一个 UserID 只有一条当前 Grant 记录；重新授予会激活原记录。
- 禁用账号的 Session 不能建立 Admin Context。
- Grant 撤销后，下一个请求立即失效。
- PostgreSQL advisory lock 串行化“计数并撤销”，最后一个有效 Operator 不可撤销。

## 5. V1 Application 用例

### Command

- `EnsureInitialOperator`
- `CreateUser`
- `GrantOperator`
- `RevokeOperator`
- `ProvisionTenant`

### Query

- `IsOperator`
- `ListOperators`
- `ListUsers`
- `ListTenants`

Admin 使用的跨领域 Port 直接对应拥有方 Application：

```text
AccountService
├── CreateManagedAccount
├── GetAccount
└── ListAccounts

TenantService
├── ProvisionTenant
└── ListTenants
```

## 6. 首个 Platform Operator

Migration 完成后、HTTP Server 监听前，进程 bootstrap 在显式 `auto` 模式下执行：

```text
PostgreSQL transaction
  -> pg_advisory_xact_lock
  -> admin.EnsureInitialOperator
       -> transaction-bound Identity AccountManagement
       -> transaction-bound Admin OperatorStore
  -> commit account + credential + grant together
```

配置：

```text
CONTROL_BOOTSTRAP_MODE=auto | disabled
CONTROL_BOOTSTRAP_USERNAME=<username>
CONTROL_BOOTSTRAP_DISPLAY_NAME=<display name>
CONTROL_BOOTSTRAP_PASSWORD=<temporary password>
```

`auto` 状态矩阵：

| 数据库状态 | V1 结果 |
| --- | --- |
| 已有有效 Operator | 幂等 NOOP，不修改密码或 Grant |
| 没有账号且没有 Operator | 原子创建账号、临时凭证和 SYSTEM_BOOTSTRAP Grant |
| 已有账号但没有有效 Operator | `RECOVERY_REQUIRED`，启动失败且不自动提权 |

密码从进程配置读取，不进入日志、HTTP 响应或数据库明文字段。初始账号首次登录得到
Restricted Session，完成密码轮换后才能使用 Admin API。

V1 暂不引入 BootstrapState、审计 Outbox 和 Break-glass 命令；这些是明确的后续安全
切片，不用空接口占位。

## 7. Platform Operator 直接创建用户

```text
POST /v1/admin/users
  -> Admin Context
  -> admin.CreateUser
       -> Identity AccountService.CreateManagedAccount
```

新账号为 ACTIVE，使用临时密码且首次登录必须轮换。该命令不会自动授予 Operator
Grant，也不会自动创建 Tenant Membership。Operator 授权和 Tenant 准入分别通过其
拥有方用例完成。

## 8. Tenant Provisioning

```text
POST /v1/admin/tenants
  -> Admin Context
  -> admin.ProvisionTenant
       -> TenantService.ProvisionTenant
            -> INSERT Tenant + Initial OWNER Membership
```

V1 要求 `owner_user_id` 指向已有 ACTIVE UserAccount。Tenant 与 Initial Owner 由
Tenant PostgreSQL Adapter 使用一个 CTE 原子创建，不会留下无 Owner 的 ACTIVE
Tenant。

## 9. HTTP API

```text
GET    /v1/admin/capabilities
GET    /v1/admin/operators
POST   /v1/admin/operators
DELETE /v1/admin/operators/{user_id}
GET    /v1/admin/users
POST   /v1/admin/users
GET    /v1/admin/tenants
POST   /v1/admin/tenants
```

所有路由先经过 Identity Session Middleware，再经过 Admin Authorization
Middleware。Restricted Session、普通用户和已撤销 Operator 都不能执行平台命令。

## 10. 代码结构

```text
services/control-api/internal/admin/
├── domain/operator_grant.go
├── application/service.go
├── adapter/
│   ├── inbound/http/
│   │   ├── middleware.go
│   │   ├── operator_handler.go
│   │   ├── user_handler.go
│   │   ├── tenant_handler.go
│   │   └── response.go
│   └── outbound/postgres/store.go
└── wiring.go
```

业务 Adapter 统一使用 `package httpadapter` 和 `package postgresadapter`。

## 11. 后续切片

- Operator 全部丢失后的 Break-glass 恢复。
- 审计事件、Outbox 和管理操作可观测性。
- 细粒度 Operator RBAC、MFA 和高风险命令二次认证。
- Tenant Suspend/Restore 和受审计的业务数据访问。
