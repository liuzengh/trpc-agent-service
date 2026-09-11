# Identity 子领域设计

- **边界状态**：已接受
- **细节状态**：V1 已收口
- **实现状态**：账号创建、登录、Session 认证、登出和修改密码已实现
- **适用范围**：Control API 的本地账号认证与服务端会话

## 1. 目的

`identity` 负责回答“调用者是谁”，不回答“调用者能操作哪个 Tenant 或平台资源”。
Control API 自己管理用户名和密码，不依赖外部 OIDC 作为 V1 的账号来源。

Platform Operator、Tenant Owner 和普通 Tenant Member 使用同一种 `UserAccount`
和同一套登录流程。用户登录后得到的 Identity Context 只证明身份；Platform
Operator 权限由 `admin` 校验，Tenant 权限由 `tenant` 校验。

## 2. 统一语言

| 术语 | 定义 |
| --- | --- |
| UserAccount | Control API 中全局唯一、可登录的本地账号 |
| Password Credential | 账号当前使用的不可逆密码校验材料 |
| Session | 登录成功后由服务端持久化、可撤销的会话 |
| Identity Context | 由有效 Session 建立的 `UserID`、`SessionID` 等可信上下文 |
| Authentication | 验证账号与凭证并建立 Identity Context |
| Authorization | 判断 Identity Context 是否拥有某项平台或 Tenant 权限 |

Tenant 不是登录主体，Membership 也不是账号。一个 UserAccount 可以同时拥有
Operator Grant，并属于零个、一个或多个 Tenant。

## 3. 所有权边界

`identity` 拥有：

- UserAccount 的创建、禁用和基础资料。
- 用户名规范化与全局唯一性。
- Password Credential 的建立、更换和参数升级。
- Session 的创建、校验、轮换、过期和撤销。
- 登录、登出、修改密码和查看当前账号等 Use Case。

`identity` 不拥有：

- Platform Operator Grant；它属于 `admin`。
- Tenant、Membership、Invitation 或 Tenant 角色；它们属于 `tenant`。
- Agent、Runtime Profile、Deployment 或 Channel Binding 权限。
- 通用 HTTP Server、PostgreSQL 连接池或日志实现。

禁止重新建立 `internal/infra/auth`。密码哈希、Session Repository 和 HTTP 认证
Middleware 都是 `identity` 的具体 Adapter，因为它们服务于明确的业务能力。

## 4. 领域模型

### 4.1 UserAccount Aggregate

```text
UserAccount
├── ID
├── Username
├── NormalizedUsername
├── DisplayName
├── Status: ACTIVE | DISABLED
├── CreatedAt
└── UpdatedAt
```

V1 推荐使用平台全局唯一的用户名。Identity 内部始终以稳定 `UserID` 建立关联，
禁止让其他模块把可变 Username 当作外键。

### 4.2 Password Credential

```text
PasswordCredential
├── UserID
├── EncodedHash
├── MustChangeAtNextLogin
└── ChangedAt
```

数据库只保存 Argon2id PHC 字符串；PHC 字符串已经包含独立 Salt、算法版本和参数。
禁止保存明文、可逆密文、密码提示或快速 SHA-256/MD5 摘要。参数升级检测在后续
密码维护切片实现。

### 4.3 Session Aggregate

```text
Session
├── ID
├── UserID
├── TokenHash
├── Restricted
├── CreatedAt
├── ExpiresAt
└── RevokedAt
```

浏览器只持有高强度随机 Session Token，数据库只保存 Token Hash。Session 中禁止
缓存 Platform Operator 权限、Tenant 角色或“当前 Tenant”，避免权限变更延迟和
多个浏览器标签页相互覆盖。

## 5. 核心业务规则

- 禁用的 UserAccount 不能建立或继续使用 Session。
- 登录失败响应不得暴露 Username 是否存在、账号是否禁用或密码哪一部分错误。
- 登录必须限速，并记录不包含密码和 Token 的安全审计信息。
- 修改或重置密码时应支持撤销其他 Session。
- Initial Operator Bootstrap 或 Platform Operator 直接创建的账号使用临时密码；首次
  登录建立的受限 Session 只能修改密码或登出，轮换成功后才能访问其他能力。
- 服务端权限判断必须实时读取或可靠缓存拥有方状态，不能信任前端角色标志。
- 密码和 Session Token 禁止进入日志、Trace、事件、错误消息或 NATS Payload。

V1 密码策略只做 12–128 字符长度校验，不使用强制字符组合或周期性修改；登录由
HTTP Adapter 限速。弱密码 Blocklist 和泄露密码检查属于后续安全加固切片。

## 6. Application Use Case

### 6.1 Command

- `CreateManagedAccount`，供 Initial Operator Bootstrap 和已授权 Admin 使用。
- `LoginWithPassword`
- `LogoutSession`
- `ChangePassword`

V1 不公开匿名注册。账号只能由 Initial Operator Bootstrap 或已授权 Admin 通过
Application Port 创建，凭证必须标记为首次登录强制轮换。禁用账号、密码恢复、
Invitation 激活和会话管理 UI 属于后续纵向切片。

### 6.2 Query

- `AuthenticateSession`
- `GetCurrentUser`
- `GetAccount`、`ListAccounts`，只通过 Admin 使用方 Port 暴露

## 7. HTTP 调用链与接口

```text
POST /v1/auth/login
  -> Login Handler
  -> LoginWithPassword
       -> Account Repository
       -> Password Verifier
       -> Session Repository
  -> Set-Cookie

Authenticated Request
  -> Identity Session Middleware
  -> AuthenticateSession
  -> Identity Context
  -> Admin 或 Tenant 授权
  -> 目标业务 Use Case
```

V1 首批接口按纵向切片实现：

```text
POST   /v1/auth/login
POST   /v1/auth/logout
GET    /v1/me
POST   /v1/me/change-password
```

Control Web 使用 `HttpOnly`、`Secure`、`SameSite` Cookie 承载不透明 Session
Token。CSRF、Origin 校验、空闲过期与绝对过期的精确参数属于实现前配置决策。

## 8. Application Port

V1 只声明实际使用的 Port：

```text
AccountReader.FindLoginIdentity
PasswordVerifier.Verify
SessionWriter.CreateSession
SessionIdentityReader.FindSessionIdentity
PasswordChangeStore.ChangePassword
SessionRevoker.RevokeSession
AccountStore.CreateAccount/GetAccount/ListAccounts
```

Session Token 使用标准库 `crypto/rand` 生成。登录限速位于 Inbound HTTP Adapter，
当前是单进程固定窗口；多副本共享限额仍是部署前演进项。Clock、Audit 和其他
Repository 等到真实用例出现后再建立，避免预先形成只有一个实现的浅接口。

`admin` 与 `tenant` 只通过使用方 Application Port 调用 Identity。Admin 首次引导
和直接创建用户都使用 `AccountManagement`；初始引导把它绑定到同一 PostgreSQL
事务，调用方不访问 Identity 表，也不自行计算密码哈希。

## 9. 持久化所有权

建议由 Identity PostgreSQL Adapter 独占：

```text
user_accounts
password_credentials
user_sessions
```

最低唯一约束：

```text
UNIQUE(normalized_username)
UNIQUE(token_hash)
```

Admin 和 Tenant 可以保存稳定 `UserID` 引用，但禁止修改以上表。是否建立跨模块
数据库外键属于物理隔离设计，不改变代码所有权。

## 10. 目标代码结构

```text
services/control-api/internal/identity/
├── domain/
├── application/
├── adapter/
│   ├── inbound/http/
│   └── outbound/
│       ├── postgres/
│       └── argon2id/
└── wiring.go
```

## 11. 待确认细节

- Username 改名策略和保留名称。
- Invitation 新用户的激活流程。
- Session 续期和并发会话上限。
- 密码恢复渠道、MFA 与高风险 Admin Command 的二次认证。
- Login Rate Limiter 的共享存储和部署拓扑。
