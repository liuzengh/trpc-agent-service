# Tenant 子领域设计

- **边界状态**：已接受
- **细节状态**：V1 已收口
- **实现状态**：Tenant Provisioning、Membership 查询和 Owner 成员管理已实现
- **适用范围**：Control API 的 Tenant、Membership 与租户授权

## 1. 目的

`tenant` 负责 Tenant 和平台用户之间的成员关系，以及由 Membership 建立的 Tenant
访问边界。它不验证密码，也不判断用户是否为 Platform Operator。

Tenant 是客户工作空间，不是登录账号。用户先通过 `identity` 认证，再由当前有效
Membership 获得 Tenant 内的权限。

## 2. V1 统一语言

| 术语 | 定义 |
| --- | --- |
| Tenant | 平台内彼此隔离的客户工作空间 |
| Membership | UserAccount 与 Tenant 之间的关系 |
| Tenant Owner | 角色为 `OWNER`、可以管理成员的用户 |
| Tenant Member | 角色为 `MEMBER` 的普通成员 |
| Initial Owner | Tenant 创建时原子建立的第一个 Owner |
| Tenant Context | 已认证 UserID、TenantID 和服务端查询到的 Membership |

Platform Operator 不是 Tenant 角色。Operator 创建 Tenant 属于 `admin` 工作流；
Tenant 和 Membership 的状态与不变量仍由 `tenant` 拥有。

## 3. 所有权边界

`tenant` 拥有：

- Tenant 创建、状态和稳定 ID。
- Membership 的唯一性、角色和成员管理规则。
- Tenant Context 的授权查询。
- 初始 Owner 与“普通删除不能移除 Owner”的 V1 规则。

`tenant` 不拥有 UserAccount、Password、Session 或 Operator Grant，也不拥有 Agent、
Runtime Profile、Deployment 和 Channel Binding 的领域规则。

## 4. V1 领域模型

```text
Tenant
├── ID
├── Slug
├── Name
├── Status: ACTIVE
├── CreatedAt
└── UpdatedAt

Membership
├── ID
├── TenantID
├── UserID
├── Role: OWNER | MEMBER
├── CreatedBy
└── CreatedAt
```

最低约束：

- `(TenantID, UserID)` 唯一。
- Tenant 创建和 Initial Owner 创建是同一个 PostgreSQL 语句中的原子结果。
- 只有 Owner 可以添加或移除成员。
- V1 只能把已有的 ACTIVE 平台用户添加为 `MEMBER`。
- 普通成员删除命令不能删除 `OWNER`；Ownership Transfer 后续单独实现。

V1 不提前引入 `ADMIN/DEVELOPER/VIEWER`、Membership 状态机、Invitation、Suspension
或 Tenant 删除状态机。

## 5. Application 用例

### Command

- `ProvisionTenant`：只供 Admin Application Port 调用，同时创建 Initial Owner。
- `AddMember`：Owner 添加一个已有 ACTIVE UserAccount，固定授予 `MEMBER`。
- `RemoveMember`：Owner 删除普通成员，拒绝直接删除 Owner。

### Query

- `ListMyTenants`
- `GetTenant`
- `ListMembers`：只允许 Owner
- `SearchMemberCandidates`：只允许 Owner，查询 ACTIVE 且尚未加入当前 Tenant 的账号
- `ListTenants`：只供 Admin Application Port 调用

Tenant Command 对 Identity 的依赖仍是使用方 Port：

```text
AccountLookup.IsActiveAccount(UserID) -> bool
```

成员候选搜索使用 Tenant Application 自己定义的只读 Port：

```text
MemberCandidateQuery.SearchMemberCandidates(TenantID, Query, Page)
  -> ACTIVE UserAccount public projection
  -> exclude existing Tenant Membership
```

该 Port 由专用 PostgreSQL Read Model 实现，只读取 `user_accounts` 和
`tenant_memberships`，仅投影 `user_id`、`username`、`display_name`。它不导入
Identity PostgreSQL Adapter、不读取凭证，也不修改 Identity 状态；最终添加命令仍会
通过 `AccountLookup` 重新确认 ACTIVE 状态，并由 Membership 唯一约束处理并发重复添加。

## 6. HTTP API

```text
GET    /v1/me/tenants
GET    /v1/tenants/{tenant_id}
GET    /v1/tenants/{tenant_id}/member-candidates?query=&offset=&limit=
GET    /v1/tenants/{tenant_id}/members
POST   /v1/tenants/{tenant_id}/members
DELETE /v1/tenants/{tenant_id}/members/{user_id}
```

这些接口先经过 Identity Session Middleware。候选搜索与成员列表、添加、删除都只允许
Owner。临时密码尚未轮换的 Restricted Session 只能修改密码或登出，不能建立 Tenant
Context。

```text
Identity Session
  -> UserID
  -> TenantID（路由选择器）
  -> Tenant Service 查询有效 Membership
  -> 执行业务用例
```

路由中的 `tenant_id` 不是授权证据，客户端传入的 Role 或 `isOwner` 标志也不是授权
证据。

## 7. 持久化所有权

Tenant PostgreSQL Write Adapter 独占：

```text
tenants
tenant_memberships
```

数据库使用 `UNIQUE(slug)`、`UNIQUE(tenant_id, user_id)`、角色 `CHECK` 和外键提供最后
一道一致性约束。业务错误仍由 Application 映射，Handler 不解释 PostgreSQL 错误。

候选人 PostgreSQL Read Adapter 是跨模块专用 Read Model：它只读查询公开账号字段并与
当前 Tenant Membership 做 anti-join，以在同一个数据库语句中得到准确的筛选、分页和
总数。

## 8. 代码结构

```text
services/control-api/internal/tenant/
├── domain/
│   ├── tenant.go
│   └── membership.go
├── application/
│   ├── ports.go
│   ├── provision_tenant.go
│   ├── manage_members.go
│   └── queries.go
├── adapter/
│   ├── inbound/http/
│   └── outbound/postgres/
│       └── member_candidate_query.go
└── wiring.go
```

业务 Adapter 统一使用 `package httpadapter` 和 `package postgresadapter`。

## 9. 后续切片

- Ownership Transfer 与最后 Owner 的完整规则。
- Invitation、受邀账号激活和投递 Adapter。
- 更细的 Tenant Role、Membership Suspension 和 Tenant Suspension。
- Agent/Profile/Deployment 权限映射。
