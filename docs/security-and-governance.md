# 身份、权限与密钥治理

本文第 1–9 节描述当前**已实现**的安全机制：控制面认证、角色模型、Security Manifest、租户 Entitlement 和 Runtime 构建顺序，以 `trpcservice/identity`、`trpcservice/security`、`trpcservice/secretref`、`trpcservice/web/admin.go`、`trpcservice/agent/agent.go` 和 `start.sh` 的源码与测试为准。未实现能力及其安全影响见第 10 节，生产治理的执行约束见第 11 节。

## 1. 一句话边界

- 控制面（Admin API）和对话面（`/v1/chat/completions`）都要求 Bearer 凭据，且**使用两套互不相通的凭据体系**。
- 凭据是静态的：进程启动时一次读入，运行期不可变更，没有热加载、轮转、过期或撤销。
- 授权粒度是**角色 + 租户**（`platform_admin` / `tenant_admin`）和**租户 Entitlement**（某租户的 Revision 可以引用哪些 SecretRef 和 PolicyRef），不是动态 RBAC。
- 进程只允许绑定回环地址。当前服务使用明文 HTTP，可路由监听会使 Admin Bearer token 暴露在网络传输中；demo profile 可使用公开的开发 chat key 启动。TLS 由外部反向代理终止，本二进制不提供 TLS 配置。

## 2. 两条凭据链路

| | Chat Key | Admin Key |
| --- | --- | --- |
| 认证接口 | `identity.Authenticator.Authenticate` | `identity.AdminAuthenticator.AuthenticateAdmin` |
| 身份类型 | `identity.Identity`（tenant + principal + allowed_app_ids） | `identity.AdminIdentity`（role + principal + tenant） |
| 实现 | `identity.StaticAPIKeyAuthenticator` | `identity.StaticAdminAPIKeyAuthenticator` |
| 可达路径 | `/v1/chat/completions` | `/admin/**` |
| 最短长度 | 16 | 32 |
| Manifest `purpose` | `chat` | `platform_admin` / `tenant_admin` |

两个内置认证器使用不同的 Go 类型和方法名，`PlatformServer` 用独立字段和认证表区分入口。`security.Load` 在 Manifest 和 demo 模式下都拒绝不同凭据条目解析为同一个 key，因此 chat key 不在 Admin 认证表中，访问 Admin 返回 `401`。这是启动配置校验和运行时路由共同提供的保证；Go 类型可以同时实现两个接口，不能仅靠接口类型推出凭据隔离。测试见 `identity.TestAdminAndChatAuthenticatorsAreDistinctTypes` 与 `web.TestAdminAuthenticatesBeforeRouting`。

Chat Key 至少 16 个字符，绑定一个租户及其 `allowed_app_ids` 授权的 App 集合。Admin Key 至少 32 个字符，按 `platform_admin` 或 `tenant_admin` 角色允许创建租户或管理相应租户的 App/Revision；长度要求不替代随机性和安全保管。

### 2.1 凭据的长期存储与可传输性

两个静态认证器的长期认证表只保存 key 的 SHA-256 摘要：

- 误打印认证表不会直接暴露明文 key；环境变量、请求头和构造期间的缓冲仍可能持有明文，Go 字符串也不保证可清零，因此不提供进程内存转储防泄漏保证。
- 查表按摘要进行，消除了逐字节字符串比较的前缀时间信号。

两个认证器都会在**构造时**拒绝无法可靠作为 Bearer 传输的 key（`identity/credential.go`）：

- 空 key；
- 首尾带空白的 key——`Authorization` 头在解析时会被 trim 两次，配置的 key 和到达的 key 不是同一个字符串，摘要永远对不上；
- 含有不能出现在 HTTP 头值里的字节（CR、LF、NUL、DEL）。

规则刻意收窄到"可证明不可能工作"的集合，不是 RFC 6750 token68：`:` 和 `|` 不是 token68 字符但出现在真实 API key 里且逐字节传输正常，因此不拒绝。这条检查不可能拒掉一个今天能用的 key（`identity.TestStaticAuthenticatorsAcceptOrdinaryKeys`），但能把"进程正常启动、日志报告凭据数量正确、然后对着刚刚报告的 admin key 返回 401"这种故障提前成一条启动错误。长度检查在它之后：32 个空格能过长度下限却谁也认证不了，而"你的 key 是空白"才是有用的那一半信息。

## 3. Admin 角色模型

`AdminRole` 是**闭集**，manifest 不能发明新角色：

| 角色 | 租户绑定 | 可做 |
| --- | --- | --- |
| `platform_admin` | 不属于任何租户，且**必须**不携带 `tenant_id` | 创建租户；管理任意租户的 App / Revision |
| `tenant_admin` | 绑定且仅绑定一个租户 | 管理自己租户的 App / Revision |

`AdminIdentity.Validate` 拒绝任何无法完整定界的身份：带租户的 `platform_admin` 会读起来像"有作用域"却实际无作用域，因此直接判非法；`tenant_admin` 必须有合法 `tenant_id`。`AllowsTenant` 做**精确字符串比较**，没有前缀、通配或大小写折叠。

创建租户是唯一不属于任何租户的控制面操作，因此归属唯一不属于任何租户的角色：`tenant_admin` 调 `POST /admin/v1/tenants` 得到 `403 forbidden`（`web.TestAdminTenantCreationIsPlatformAdminOnly`）。这里用 `403` 而不是 `404`，是因为"我不能建租户"对这个调用方本来就不是秘密。

## 4. Admin 请求处理顺序

整个 `/admin` 子树**在 mux 之前**被 `adminFirst` 接管，然后由 `handleAdmin` 按下列顺序处理：

```text
1. 认证 Bearer 凭据                       → 401 unauthenticated（带 WWW-Authenticate: Bearer）
2. POST 必须声明 Content-Type: application/json → 415 unsupported_media_type
3. 路径前缀匹配（原始 URL.Path，精确比较）  → 404 not_found（admin route not found）
4. 租户作用域检查（路径含 tenant 时）       → 404 not_found（resource not found）
5. 方法匹配                                → 405 method_not_allowed（带 Allow）
6. 角色检查（仅 POST /admin/v1/tenants）    → 403 forbidden
7. Entitlement / Tool Registry / Repository
```

**认证在最前面**是这套设计的核心。没有有效凭据的调用方，在真实路由、不存在的路由和错误方法上都得到同一个 `401`，因此这个 API 不回答任何关于自己形状的问题。路由发生在认证之后，也意味着下游没有任何 handler 能被一个尚未被识别的调用方到达。

`/admin` 子树没有注册在 `http.ServeMux` 上，而是在 mux 之前拦截，原因是 `ServeMux` 会先清理路径：`/admin//v1/tenants`、`/admin/./v1/tenants`、`/admin/v1/tenants/../secrets` 都会得到一个 `301`，而这个重定向是写给一个**没有凭据**的调用方的。现在这些路径先认证，然后按原始路径精确比较——不解析任何 traversal，所以"拼写奇怪的真实路由"不是那条路由，而是一个 `404`（`web.TestAdminOddPathsAreRefusedBeforeTheRouterAnswers`、`TestAdminOddPathsAreNotRedirectedForAValidCredential`）。非 `/admin` 流量照常进 mux（`web.TestNonAdminTrafficStillRoutesThroughTheMux`）。

### 4.1 跨租户统一 404，且零 Repository 调用

`tenant_admin` 访问别的租户时，在**任何 Repository 调用之前**返回和"资源真的不存在"**逐字节相同**的 `404 not_found`。两半都重要：

- 状态码或响应体只要差一个词，这个接口就成了枚举租户是否存在的 oracle。因此 `writeNotFound`（`404 not_found` / `resource not found`）是**唯一一个资源级** not-found 写出口，跨租户短路和 `ErrTenantScope` / `ErrNotFound` 共用它，逐字节相同。路由层的 `admin route not found` 是另一条消息，但它只回答"这个 URL 形状不是一条路由"，与任何租户是否存在无关，而且同样在认证之后。
- 先到 Repository 再拒绝，会让一个租户的管理员驱使本进程代表另一个租户查库——这是它无权制造的负载和审计痕迹。

测试用一个"被调用即 fail test"的 `failingRepository` 把"零调用"变成正面观测而不是从错误信息倒推（`web.TestAdminTenantAdminCannotReachAnotherTenant`、`TestAdminCrossTenantRefusalMatchesARealNotFound`）。

### 4.2 Admin 面没有 CORS

Admin 的任何响应——成功和失败——都**不带任何 `Access-Control-Allow-*` 头**，也没有预检分支。`OPTIONS` 不被特殊处理：它和别的请求一样先认证，然后作为这些路由不接受的方法得到 `405`。

所有 POST 必须声明 `Content-Type: application/json`，包括不带 body 的 publish 请求。该类型不属于 CORS "simple request" 允许的类型，跨源浏览器写请求必须先通过预检；Admin 不提供 CORS 许可，因此跨源页面无法读取响应或提交写操作。JSON 内容由 `decodeAdminJSON` 单独校验。测试见 `web.TestAdminNeverPublishesCORSHeaders`、`TestAdminWritesRequireJSONContentType`。

对话面 `/v1/chat/completions` 仍然发布 CORS 头，两者是不同的边界。

### 4.3 `created_by` 来自认证 Principal

`createRevisionRequest` **没有** `created_by` 字段。作者身份不是请求可以声明的东西，它就是发出这次请求的凭据的 principal：

```go
CreatedBy: admin.PrincipalID,
```

字段是"不存在"而不是"接受后覆盖"，因此仍然携带 `created_by` 的请求体会被 `DisallowUnknownFields` 拒绝（`400 invalid_json`），而不是安静地表达一个和字面意思不同的语义。测试见 `web.TestAdminRevisionAuthorshipComesFromTheCredential`。

## 5. Security Manifest

### 5.1 两个来源，不混用

| `TRPC_SERVICE_SECURITY_CONFIG_FILE` | 生效配置 |
| --- | --- |
| 未设置（严格等于空串） | demo profile |
| 指向文件 | 该 manifest 是**全部**配置 |
| 只有空白 | **启动失败**，不回退 |

设置了 manifest 时，`TRPC_SERVICE_API_KEY` 和公开的开发 key **完全不参与**：写了 manifest 的部署没打算同时保留第二条环境凭据入口。设成空白直接拒绝而不回退，是因为回退正是"运维以为自己的 manifest 生效了，实际跑的是 demo profile"的成因。路径也不 trim——带杂散空格的路径不会被猜成它像的那个路径。

demo profile 是：一个 chat 凭据（`TRPC_SERVICE_API_KEY`，未设置时用公开的 `local-development-key-not-a-secret`）、一个 platform admin 凭据（`TRPC_SERVICE_ADMIN_API_KEY`，**没有任何默认值**，未设置则拒绝启动），以及一条 entitlement：`demo` 租户可以引用 `builtin.safe-tools`，**不能引用任何 SecretRef**。demo profile 走的是和 manifest 完全相同的校验路径。

chat key 有公开占位值而 admin key 没有，理由是不对称的：公开的 chat key 只能和一个 demo App 对话；公开的 admin key 能创建租户和发布 Revision，而它在任何人读源码的那一刻就已经泄漏。

### 5.2 格式

```json
{
  "version": 1,
  "credentials": [
    {
      "purpose": "chat",
      "principal_id": "tenant-a-user",
      "key_ref": "env:CHAT_KEY",
      "tenant_id": "tenant-a",
      "allowed_app_ids": ["assistant"]
    },
    {
      "purpose": "platform_admin",
      "principal_id": "ops",
      "key_ref": "env:ADMIN_KEY"
    },
    {
      "purpose": "tenant_admin",
      "principal_id": "tenant-a-ops",
      "key_ref": "env:TENANT_ADMIN_KEY",
      "tenant_id": "tenant-a"
    }
  ],
  "tenant_entitlements": [
    {
      "tenant_id": "tenant-a",
      "allowed_secret_refs": ["env:TENANT_A_MODEL_KEY"],
      "allowed_policy_refs": ["builtin.safe-tools"]
    }
  ]
}
```

字段规则：

| `purpose` | `tenant_id` | `allowed_app_ids` |
| --- | --- | --- |
| `chat` | 必填 | 必填，非空，不可重复 |
| `platform_admin` | **必须缺省** | **必须缺省** |
| `tenant_admin` | 必填 | **必须缺省** |

"缺省"和"必填"一样严格地检查：携带 `tenant_id` 的 `platform_admin` 条目说明作者对这条授权有一个不成立的信念，接受它同时忽略该字段等于确认这个信念。

**key 的值永远不在文件里**，只有 `key_ref: "env:VAR_NAME"`。

### 5.3 严格解析

security 文件是最不能容忍"解析器忽略了它不认识的那部分"的一类文件——被忽略的正是有人以为自己写下的授权。因此解析在每个可能藏错误的方向上都是严格的：

- **版本按相等比较**，不是"至少"。`version: 2` 被拒绝而不是尽力兼容：为后续版本写的文件可能含有语义已变的字段，而它会被这个 build 静默按旧语义读取。
- **未知字段拒绝**（`DisallowUnknownFields`）。
- **重复成员拒绝**，任意深度。`encoding/json` 对重复成员取最后一个且不报告，于是 `{"purpose":"chat","purpose":"platform_admin"}` 会解码成 platform admin，而文件里还说了别的这件事没有任何痕迹。比较用 `strings.EqualFold`，因为这正是 decoder 把成员名匹配到结构体字段时用的折叠关系——`key_ref`、`KEY_REF` 和 `Key_ref`（U+212A KELVIN SIGN）都落进同一个字段，最后一个获胜。只折叠 ASCII 会让非 ASCII 拼写作为"不同成员"漏过去，即同样的静默 last-win 多加一步。
- **文件里只能有一个 JSON 值**，尾随第二个对象或任何垃圾都拒绝。
- **必须是常规文件**，且不超过 256 KiB。句柄被 `Stat` 而不是路径，读取限制到上界 +1 字节，因此 stat 之后才增长的文件由长度检查兜住。
- 错误信息**从不引用文件内容**：重复成员只报字节偏移，不复述成员名。

### 5.4 凭据解析

- `key_ref` 经 `secretref.EnvName` 解析，只接受 `env:VAR_NAME`，且是**精确匹配**——不 trim、不折叠大小写、不做任何规范化。
- 两个条目引用同一个环境变量 → 拒绝。
- 环境变量未设置或为空串 → 拒绝（导出成空等于运维要了一个不存在的凭据）。
- 两个条目**解析出同一个 key**（按 SHA-256 比较，而不是按变量名）→ 拒绝。否则把 admin key 导出到某个租户 chat 凭据读的第二个变量里，就能让一个凭据静默地当两个用，而一次请求命中哪个取决于哪个认证器先看到它。
- 反过来不限制：**一个 principal 持有两个不同的 key 是轮转**，必须允许（`security.TestLoadAcceptsTwoKeysForOnePrincipal`）。
- manifest 至少要有**一个 chat 凭据和一个 platform admin**。没有 platform admin 的进程会启动进一个无人能管理的控制面。
- 任何错误都不携带 key，只给条目下标、purpose 和查找过的变量名。

## 6. 租户 Entitlement

`tenant_entitlements` 按**租户**而不是按凭据组织：能不能引用一个能力，是“使用这个 Revision 或 BackendProfile 的租户”的属性，而不是“碰巧创建它的人”的属性。

- `allowed_secret_refs`：该租户的 Revision 可在 `model.secret_ref`、BackendProfile 可在 PostgreSQL `dsn_ref` 或 Redis `url_ref` 中引用哪些 `env:VAR_NAME`。
- `allowed_policy_refs`：可以在 `policy_refs` 里引用哪些策略。
- 匹配是**精确字符串**，没有通配符、大小写折叠或规范化。
- 条目内重复、租户重复都拒绝。
- `allowed_policy_refs` 在**加载时**对着运行中的 Tool Registry 校验（`tool.Registry.HasPolicy`）。这必须发生在这里而不是首次使用时：按设计调用方分不出"策略不存在"和"你没被授权"，所以 manifest 里的一个拼写错误否则就是一条永久的、无从读起的静默拒绝。

### 6.1 凭据 / Entitlement 分离

两条**不可逾越**的规则：

1. 任何租户都不能被授权引用**持有本平台自身凭据**的那些环境变量（由 manifest loader 检查，因为只有它知道这些变量是哪些）。
2. 任何租户都不能被授权引用 `TRPC_SERVICE_` 命名空间里的任何变量（由 `Entitlements.add` 检查，因此在代码里构造的 Grant 也受同一条约束）。

没有第一条，把某个租户授权到它自己的模型 key，就足以让它发布一个 `secret_ref` 指向 admin key 的 Revision，并让 Runtime 把这个 key 发到它自己选定的 `base_url`。检查按**精确名字**进行——一条比它所保护的查找更聪明的匹配规则，是一条带缺口的规则（`security.TestLoadRefusesToEntitleATenantToAPlatformVariable`、`TestEntitlementSeparationMatchesExactNamesOnly`）。

### 6.2 多个执行点，同一个 authorizer 实例

| 位置 | 时机 |
| --- | --- |
| `POST .../backend-profiles` | Profile 校验之后、写库之前；不读取环境 |
| `POST .../revisions` | Revision 自身 entitlement 之后，解析它引用的 Profile 并再次检查其 SecretRef，全部发生在写库之前 |
| `POST .../revisions/{id}/publish` | 读出存储的 Revision 后，重新检查 Revision 与 Profile，发布之前 |
| Runtime 构建 | digest 校验之后，Tool Registry、模型 Secret 解析与 Storage Router 之前 |
| 动态 Storage Factory | 进程约束之后、读取 Profile 指向的环境变量之前 |

`cmd/trpc-service/main.go` 把 `securityCfg.Revisions` **同一个实例**同时交给 `web.NewPlatformServer`、Runtime 和 Storage Factory。不是三个等价的值：Admin 接受而数据面拒绝（或者反过来）是一次关于“这个租户能做什么”的分歧，而这种分歧在请求期没有正确的解法。

创建时校验可在保存 draft 前拒绝未授权引用，避免错误配置延迟到发布或运行时才暴露。

Admin 拒绝一律是同一个 `403 not_entitled`，措辞固定，不说明是哪个引用被拒。对“环境变量存在”和“不存在”、“策略已注册”和“从没听说过”都给出同一个答案；Profile 创建和 Revision 的两次门禁都不会读取环境。Runtime/Factory 的拒绝再统一塌缩为 `409 revision_unavailable`，不把平台配置原因告诉对话调用方。

`publish` 上 entitlement 先于 Tool Registry 检查，顺序和 Runtime 一致。Registry 失败返回 `400 invalid_revision` 并说明原因：走到这一步的调用方对它引用的每个 ref 都已被授权，剩下的故障在 Revision 自身，此时说清楚是"可修复的错误"和"谜"的区别。

## 7. Runtime 构建顺序

`agent.NewRuntime` / `planRuntime` 的检查顺序本身就是安全属性：

```text
1. 状态必须是 published
2. 身份与 config 形状（tenant / app / revision id、config 校验）
3. config_digest 重算并精确核验     → tenant.ErrConfigIntegrity
4. RevisionAuthorizer               → security.ErrNotEntitled
5. Tool Registry 解析
6. 模型构造与 SecretRef 解析
7. Storage Router 解析 BackendProfile 并获取 Bundle lease
```

**第 3 步**：Published Revision 是不可变的，所以它的 config 必须仍然哈希到创建时记录的值。对不上意味着这一行是在 Repository 之外被改过的，而这正是"评审之后再挪动 `secret_ref` 或 `base_url`"的做法。**空 digest 算不匹配，不算豁免**——无法被核验必须和被核验为错误以同样的方式失败。测试覆盖四种绕过 Repository 的篡改（挪 credential、挪 endpoint、改写 instruction、加 policy），且拒绝信息不回显被篡改的值（`agent.TestRuntimeReverifiesThePublishedDigest`、`TestRuntimeRefusesTamperingThatSurvivesTheDigest`）。

**第 4 步先于第 5、6、7 步**：一个租户无权运行的 Revision 必须在平台**没有读取那个环境变量**、没有透露策略是否存在、也没有查询或连接租户存储的前提下被拒绝。因此 `secret_ref` 指向未授权变量时，该变量是已设置、已导出为空还是根本不存在，得到的答案完全相同（`agent.TestRuntimeAuthorizesBeforeItReadsTheEnvironment`、`TestRuntimeResolvesTheCredentialOnlyAfterEntitlement`、`TestRuntimeRefusalDoesNotDistinguishRealPoliciesFromInvented`、`TestNewRuntimeRefusesBeforeTouchingStorage`）。

`RevisionAuthorizer` 是**必填、非 variadic、无默认值**的构造参数：谁可以运行一个引用凭据的 Revision，正是那个不能靠"忘了传"来决定的选择。没有能力配置的调用方显式传 `security.DenyCapabilities()`——它不是"关掉授权"，而是最严格的答案。不引用任何 SecretRef / PolicyRef 的 Revision 在它下面照样能跑，因为那恰好就是不需要 entitlement 的那一类（`agent.TestRuntimeRunsCapabilityFreeRevisionsWithNoEntitlement`、`TestRuntimeRequiresAnAuthorizer`）。

Runtime 构建期的 `ErrNotEntitled` 和 `ErrConfigIntegrity` 在 HTTP 层都塌缩成 `409 revision_unavailable`，措辞与"未发布"完全一致，不透露是三者中的哪一个。

## 8. 启动顺序

```text
校验监听地址 → security.Load → 校验存储配置 → 开池 / Ping / Migrate → Storage Router → Runtime Resolver → HTTP Server
```

`security.Load` 只读文件和环境，不碰数据库，因此是**最便宜的拒绝**：凭据配错的进程不应该在发现这件事的路上连过共享数据库或跑过一次迁移。它也在存储之前拿到 Tool Registry，因此 manifest 授权的策略是对着**真实存在的**策略校验的。

启动日志只打印 `securityCfg.Description`，它报告来源和数量，从不包含 key、摘要或任何解析出的值。

顺序被写成测试而不是只写进注释：给一个**同时**错两处的环境（manifest 指向不存在的文件 + postgres profile 缺 DSN），断言回来的是 security 的拒绝，并用一个记录每次调用的存储构造器 stub 把"没到存储"变成正面观测（`main.TestRunLoadsSecurityBeforeStorage`、`TestRunValidatesTheListenAddressFirst`）。

## 9. `start.sh` 与本地 Admin Key

demo profile 下 `TRPC_SERVICE_ADMIN_API_KEY` 没有默认值，所以 `start.sh` 在环境什么都没提供时生成一个，一次，并保留：

- 从 `/dev/urandom` 取 48 个 `[A-Za-z0-9_-]` 字符，远超 32 的下限。
- 以 `umask 077` 创建 `data/admin-api-key`，之后再显式 `chmod 600`，因此新建文件和早期宽松版本留下的文件都不会是组或全局可读。
- **重启复用同一个文件**，不重新生成。
- **key 本身从不打印**，终端和日志都没有；打印的只有路径，因为运维必须找得到它。

两种情况完全不生成文件：环境里已经有 `TRPC_SERVICE_ADMIN_API_KEY`（运维的 key 由运维自己管，把副本写进工作区等于这个脚本替他们决定 secret 放哪），或者设置了 `TRPC_SERVICE_SECURITY_CONFIG_FILE`（凭据来源由 manifest 决定，生成一个没人读的 key 只是看起来像配置的噪音）。

`data/` 已在 `.gitignore` 中（`data/*`，仅保留 `data/README.md`），生成的 key 不会进入 Git。

## 10. 明确未实现

当前未实现以下能力：

- **JWT / OIDC / 任何动态身份提供方。** 只有静态 API Key，没有轮转、过期、撤销、按 Principal 的配额与限流。轮转目前只能表现为"manifest 里给同一个 principal 配两个 key，然后重启"。
- **动态 RBAC。** 角色是编译期闭集的两个值，权限是代码里的分支，不是可配置的策略表。
- **热加载。** manifest 只在启动时读一次。改文件必须重启进程；已被 Resolver 缓存的 Runtime 保持它构建时的 entitlement 判定。
- **持久化审计。** 没有任何管理操作审计：谁在什么时候创建了租户、发布了哪个 Revision，都只有 `created_by` 这一个字段，没有独立、不可篡改、可查询、有保留期的 Audit Store。Tool 审计仍然只写结构化 `slog`。
- **生产 Secret Manager。** `secretref` 只解析 `env:VAR_NAME`。Vault / KMS / Kubernetes Secret 引用、租户级密钥托管和密钥轮转都未实现。
- **预算、审批与 Guardrail。** 没有 token / 成本预算，没有危险操作二次确认，没有 Plugin/Guardrail 治理链路。
- **全链路日志脱敏。** 已实现的是若干明确边界的不回显：SecretRef 与 manifest 错误不带值、Tool 审计不含参数/结果/错误正文、进程默认连接错误经 `sessionbackend.Scrub`、动态 BackendProfile 错误先整体替换解析后的 DSN/URL 再清理驱动改写的密码片段，并在发生替换时切断 unwrap 链。这仍不是一条覆盖上游内部日志、metrics、trace 和 stderr 的进程级脱敏管道。
- **`config_digest` 是无密钥摘要。** 它防住的是"能写库但没有本二进制"的写者。同时能改行**并重算指纹**的写者可以改掉 `base_url` 并让本进程照常构建——`base_url` 不是可授权能力，没有任何 manifest 字段授予或收回一个 endpoint，所以 digest 是它唯一的防线。这条残余风险以数据库写权限为界，要关闭需要 keyed digest 或对 config 的签名，而不是在构建函数里再加一个检查（`agent.TestRuntimeDigestDoesNotDefendAgainstAWriterWhoCanRecomputeIt`）。
- **传输安全。** 进程只服务明文 HTTP，因此只能绑定回环地址，且没有绕过开关。TLS 由外部反向代理终止。

## 11. 生产治理设计

以下为目标设计，尚未接入当前静态认证、Tool callback 或三阶段遥测。

### 11.1 策略与预算

租户 Policy、IM Principal 对 App 的授权及其版本以 PostgreSQL 为权威源。Run 受理与执行前核对有效授权，Model 计费调用和危险 Tool 执行前再次检查当前版本；Revision 记录的 PolicyRef 不豁免运行期撤销。缓存只加速已验证版本的读取，无法确认版本、授权已撤销或策略库不可用时拒绝新的受控调用。已发出的外部请求不能由撤销追溯取消。

预算使用共享账本，以 `(tenant_id, request_id, model_call_id)` 唯一标识一次逻辑模型调用。Plugin 在第一次外部请求前持久生成调用标识，原子检查租户余额并预占有界费用与 token；同一调用的内部重试复用记录，不重复预占。若重试本身可能再次计费，预占上界必须覆盖全部允许尝试，否则禁止该重试。结束后按各尝试的实际 usage 与价格版本进行一次幂等结算；结果未知时保留预占并对账，不能当作零费用释放。账本不可用时拒绝新的计费调用，不能依赖各 Worker 的本地计数继续放行。

### 11.2 危险操作审批

默认不在活跃 Run 中等待人工审批。Tool 前置检查发现缺少批准时不执行操作，登记待审批记录并结束本轮，排空 Event 后释放 Session 租约。用户完成审批后通过**新的 Run**引用该操作；这不是恢复或重跑原 Run，也不自动把原消息再次交给 Runner。

审批记录绑定 Tenant、请求主体、Tool、规范化参数摘要、业务操作 ID 和有效期；只有同租户且具有该操作审批权限的主体能够批准，并记录批准者与策略版本。消费前重新核对当前授权、参数和期限，以 CAS 将批准从可用变为已消费，且只有成功者能提交业务操作。业务操作 ID 在审批和执行之间保持稳定；若消费后进程退出、提交超时或副作用结果未知，保持已消费状态并查询业务结果或人工对账，不恢复批准、不盲目再次执行。模型不能创建批准或通过改写参数绕过检查。

### 11.3 Secret 与遥测出口

生产 Secret Resolver 使用经过认证的工作负载身份访问 Secret Manager，先验证 Tenant 对 SecretRef 及用途的授权，再获取指定版本的值。凭据只能用于获准的模型、IM 或数据库端点，并结合出站网络策略约束目标。缓存键包含 Tenant、引用和版本；轮转时拒绝新引用旧版本，重建相关 Runtime 并排空旧引用，紧急撤销按策略取消活跃 Run，但不承诺撤回已经发送的请求。

日志、Trace、审计和错误报告在导出前统一执行字段白名单及脱敏，只输出固定错误类别和必要内部关联标识；不直接透传供应商/SDK 原始错误、消息正文、Tool 参数或结果。第三方内部诊断默认关闭，必须启用时经过同一过滤边界。认证、Secret 解析或脱敏失败时拒绝相应调用或丢弃不安全记录并产生固定告警，不能回退输出原文。

Collector 只接受内网中经过工作负载认证的写入，例如 mTLS；遥测查询后端按租户授权过滤，不以客户端传入的 tenant 标签代替鉴权。按租户策略配置采样、保留期、到期删除、队列长度与导出超时，队列满时丢弃遥测并告警。遥测可用性不决定业务执行结果，费用与审批事实仍保存在权威账本中。

### 11.4 治理执行阶段

Runner 的 Plugin、Guardrail 和 Callbacks 承载请求级策略。执行前检查 IM 用户权限、Tool 白名单、预算和敏感输入；危险 Tool 进入人工审批；执行后进行输出脱敏和审计。密钥配置只保存 `secret_ref`，日志、Trace、错误和审计不记录明文密钥。Tool 必须显式声明是否可重放；具有副作用且允许自动重放的 Tool 使用跨模型 attempt 稳定的业务操作键，不能把可能重生的上游 `tool_call_id` 当成跨 attempt 保证。

| 治理阶段 | 生产决策与失败行为 |
| --- | --- |
| 入站 | 根据已验证 Tenant/Principal/Binding 检查用户是否可访问 App；缺失身份或权限拒绝，不允许模型决定租户或授权 |
| Model 前后 | Plugin 在调用前按租户预算原子预占本次有界额度，超额拒绝；完成后按供应商 usage 与版本化价格表结算。调用结果未知时保留预占并进入对账，不把未知费用当作零 |
| Tool 前 | Callback 以租户已授权 Policy 的交集裁决 Tool；危险操作的批准绑定 Tenant、主体、操作、参数摘要和有效期。参数变化、审批拒绝或过期均不执行，不以聊天中的一句确认直接放行 |
| 输出前 | Guardrail 在最终回复或每个可发布流式片段离开服务前检查。需要全文才能判断的策略缓冲完整输出并降级为最终回复；已发送内容无法靠事后脱敏撤销 |
| 审计 | 记录 allow/deny/approve/redact/limit 等决策与稳定错误分类，使用[审计字段](data-model.md#37-inboxrunoutbox-与-audit)。`latency_ms` 以毫秒记录延迟，费用或 Trace 尚不可得时明确为空/未知 |

## 12. 相关文档

- [Admin API 与动态路由](admin-api.md)：端点、请求示例、路由顺序和错误码。
- [Tool 与 Policy Runtime](tool-policy.md)：Tool Registry、Policy 交集、工具循环上限和 Tool 审计字段。
- [实现与验证](acceptance.md)：设计映射、参考实现、验证结果与已知限制。
