# Runtime Profile V1 子领域与当前实现契约

- **阶段**：开发阶段，当前 `/v1` 尚未稳定；只维护一套实现，不保留旧引用式协议。
- **控制面代码状态**：Canonical V1、Write/Read DTO 分离、11 个 HTTP 操作、Profile 内部
  加密凭据存储、Draft COW 与 live 更新已落入代码；完整集成验证以本轮测试报告为准。
- **运行面状态**：`CheckUsable`、`ResolveForAttempt`、授权 Application Port 与可选
  runtimehttp Adapter 已有代码和测试；真实 Deployment 调用、执行授权拥有方与 Worker 接线尚未完成。
- **协议说明**：[RuntimeProfileSpec](runtime-profile-spec.md)；
  [凭据写入、存储与消费](runtime-profile-credentials.md)。

## 1. 目的与所有权

runtimeprofile 管理 Tenant 内可复用的 Model、Tool、Knowledge、Storage 具体资源。
用户编辑普通配置与 write-only 凭据动作，服务端保存当前 Draft，并发布不可变
ProfileRevision。Profile 拥有凭据关联、当前加密值、状态、并发计数和操作 Receipt，
不要求用户先建立独立 Secret 对象或填写 SecretRef。

Profile 拥有：

- Runtime Profile 身份、名称、描述与 TenantID。
- 当前唯一 Draft、Draft Revision、不可变发布记录与 Spec Digest。
- 具体资源的类型化协议、静态校验、内部 CredentialID 与固定用途/目标关联。
- 凭据输入处理、加密存储、COW 关联、显式 live 更新和受控 Application 取值边界。

Profile 不拥有：

- AgentSpec、AgentVersion、Agent Slot 或 Agent/Profile 兼容性判断。
- DeploymentRevision、RuntimeManifest、运行生效指针和 Channel Binding。
- Environment、平台网络规则、执行后端管理或资源上限的授权。
- Provider 连通性探测、凭据对远端的真实有效性、Agent 对象构造。
- Run/Attempt 生命周期、lease 分配、Worker 执行、IM/Gateway 或回复投递。
- 独立 Secret CRUD、Vault/KMS、OAuth、账号池、凭据调度与历史值存储。
- Storage 实例创建、迁移、备份、Knowledge 摄取或索引生命周期。

Profile 发布仍是静态操作：不读取 AgentVersion、不解密、不请求 Provider，也不自动
创建 Deployment、Outbox 或 NATS 事件。它固定配置与关联身份，不冻结 live 凭据值。

## 2. 统一语言与三种表示

| 术语 | 含义 |
| --- | --- |
| RuntimeProfile | 稳定配置集合身份与展示元数据 |
| ProfileDraft | 当前可修改工作副本，允许暂时缺少发布所需内容 |
| Draft Revision | Draft 保存 CAS；每次成功保存递增 |
| ProfileWrite | 独立 HTTP 写入 DTO：普通 config 与 write-only credentials 动作 |
| Canonical RuntimeProfileSpec | 内部发布配置：非秘密资源字段、内部 CredentialID 与两个 V1 版本字段 |
| ProfileRead | 公开脱敏读取 DTO；不含 spec、内部 ID、值或密文 |
| ProfileRevision | 具有稳定 ID 的不可变 Canonical 发布记录 |
| ProfileRevisionSummary | Revision 元数据投影；不加载完整 Spec 或凭据状态 |
| Profile Credential | Profile 私有当前值记录，关联 Tenant/Profile/类别/资源/用途/目标 |
| Credential Revision | 单 CredentialID 的 live CAS，独立于 Draft Revision |
| Association Token | 绑定确定关联的条件写元数据，不是 ID，也不替代授权 |
| Spec Digest | 内部 Canonical Spec 的 SHA-256，不是公开 Read DTO 的摘要 |

~~~text
ProfileWrite(config + credentials actions)
    -> Application 授权 / CAS / 用途与目标验证
    -> 配置 + 服务器生成的 CredentialID -> Draft.spec_jsonb
    -> 当前加密值                         -> Profile 私有凭据表

发布：Draft -> 静态校验 / Canonicalization -> ProfileRevision
公开读：内部完整校验 -> ProfileRead(config + 可选 credential_states)
内部编译读：内部完整校验 -> Canonical Spec（仅可信 application port）
~~~

Write、Canonical、Read 不是两个 API 版本。当前继续使用 `/v1`、`schema_version=v1` 与
`credential_protocol_version=v1`。公开 config 不接受 `*_credential_id`、
`credential_id`、旧 `*_ref` 或普通位置的明文凭据字段。

## 3. 资源匹配与运行边界

Deployment 发布时选择准确的 AgentVersion 与 ProfileRevision，按资源类别和名称
精确匹配，再校验 Capability 和其他兼容条件，将解析结果固定到 RuntimeManifest。
用户不填写额外 Slot → Resource 映射表；Capability 不用于搜索候选资源，名称不模糊匹配。

~~~text
AgentSpec.requirements.models.primary -> RuntimeProfile.models.primary
AgentSpec.requirements.tools.search   -> RuntimeProfile.tools.search
AgentSpec.requirements.knowledge.docs -> RuntimeProfile.knowledge.docs
~~~

AgentSpec 表达逻辑需求，Profile 表达具体资源。Profile 可以包含额外资源，但这些资源
不自动暴露给 Agent；Manifest 只纳入实际需求、必要运行资源及已接受 Kind 的必要依赖。
Profile `tools.search` 与 MCP `tool_name=search_web` 是不同概念，更换远程工具名不要求
修改 AgentSpec。Storage 不属于 AgentSpec V1 Slot；运行角色的 Storage 选择由 Deployment
定义，示例 `storage.session` 不成为 Profile 必填资源或隐式选择规则。

V1 不创建 Environment 实体、表、管理 API、`environment_id`、隐藏 `default`、Overlay、
继承或多层合并。测试与生产使用不同 Profile 各自录入 Endpoint、凭据与存储配置。
凭据内部作用域为 Tenant + Profile + CredentialID + Purpose，并校验固定目的范围；
相同资源名或相同值不产生跨 Tenant/Profile 共享与复用。

## 4. 聚合与发布粒度

一份完整 Canonical Spec 是一次发布的粒度，但 RuntimeProfile 不加载全部历史 Revision。
发布仅加载当前 Profile/Draft、幂等记录与下一个编号所需状态；历史元数据独立分页查询。
Model、Tool、Knowledge、Storage 是 Spec 内具名资源，不为其分别创建独立 REST CRUD、
Draft、Revision 或 Repository 聚合。

ProfileDraft 不会变成 ProfileRevision。发布创建快照，Draft 继续编辑；Profile 不使用
混合的 DRAFT/PUBLISHED/ACTIVE/DEPLOYED 状态机。当前生效配置属于 Deployment。
不提供并行 Draft、审批、归档、克隆或自动重新部署。

## 5. HTTP API：11 个操作

所有路径均以 `/v1/tenants/{tenant_id}/runtime-profiles` 为前缀。

| 方法 | 后缀 | 当前行为 |
| --- | --- | --- |
| POST | 空 | 创建 Profile 与 revision=1 的初始 Draft，201，返回 profile + 脱敏 draft |
| GET | 空 | Profile 元数据分页列表 |
| GET | /{profile_id} | Profile 元数据详情 |
| PATCH | /{profile_id} | 修改 name/description，至少一项 |
| GET | /{profile_id}/draft | 脱敏 ProfileRead，附当前 credential_states |
| PUT | /{profile_id}/draft | ProfileWrite + Idempotency-Key，返回 DraftWriteResult |
| POST | /{profile_id}/credentials/update | 已发布关联的显式 live replace/clear |
| POST | /{profile_id}/draft/validate | expected_revision，返回静态 Validation Report |
| POST | /{profile_id}/revisions | expected_revision，首次 201、幂等重试 200 |
| GET | /{profile_id}/revisions | ProfileRevisionSummary 分页，无 spec/config/凭据状态 |
| GET | /{profile_id}/revisions/{revision_number} | 完整内部校验后返回脱敏 ProfileRead 与当前状态 |

Draft PUT 请求顶层为 `expected_draft_revision`、`credential_protocol_version`、`config`、
`credentials`。config 四类集合显式提供，资源省略表示从 Draft 删除，不是深层合并。
Validate 与 Publish 继续使用 `{expected_revision}`，不混用为 PUT 的字段名。
PUT 与 live POST 必须带 `Idempotency-Key`（1～128 个可见 ASCII 字符，不含空格）。

PUT 成功返回 `{profile_id, draft_revision, updated_at}`；live POST 返回
`{credential_revision, status}`。Receipt 重放返回原非秘密结果，不刷新动态状态。
Publish 返回 `{revision: ProfileRead}`，不包含 `credential_states`，保证发布重试的
结果不随 live 更新变化。详情 GET 的 credential_states 是独立当前投影，不进入 Canonical。

公开 Draft/Revision 响应均不含 `spec`、内部 CredentialID、原值、密文、掩码化原值或值摘要。
`spec_digest` 仅标识内部 Canonical 内容，客户端不能用脱敏 config 重算它。

## 6. 授权与错误语义

- 身份来自可信 Session；Restricted Session 必须先完成密码轮换。
- ACTIVE Tenant 的 OWNER/MEMBER 可读、修改普通配置、Validate 和 Publish。
- Draft 凭据 replace/clear、删除含关联资源与 live 更新要求 OWNER。
- 只有普通配置编辑且凭据全部 keep 的 MEMBER 不因此获得凭据写权限。
- Platform Operator 身份不自动绕过 Tenant Membership/OWNER 边界。
- 事务与 Receipt 重放复核当前授权；路径中的 tenant_id 不是授权证据。

| HTTP | 当前 Error Code | 语义 |
| ---: | --- | --- |
| 400 | INVALID_REQUEST | HTTP JSON、字段形状、分页、请求头或基本动作输入非法 |
| 400 | INVALID_CREDENTIAL_REQUEST | Application 凭据输入/动作/值非法 |
| 400 | INVALID_RUNTIME_PROFILE | 名称或描述非法 |
| 401 | UNAUTHENTICATED | 无有效 Session |
| 403 | PASSWORD_CHANGE_REQUIRED / TENANT_FORBIDDEN | Session 或 Tenant 授权不满足 |
| 404 | RUNTIME_PROFILE_NOT_FOUND / RUNTIME_PROFILE_REVISION_NOT_FOUND | Profile/Revision 不存在 |
| 409 | RUNTIME_PROFILE_DRAFT_REVISION_CONFLICT | Draft CAS 失败 |
| 409 | CREDENTIAL_ASSOCIATION_CONFLICT | 关联 token、用途或固定目标不符 |
| 409 | CREDENTIAL_REVISION_CONFLICT | live Credential CAS 失败 |
| 409 | CREDENTIAL_UNAVAILABLE | live 关联不可用或已 cleared |
| 409 | IDEMPOTENCY_CONFLICT | 同 key 对应不同请求 |
| 422 | RUNTIME_PROFILE_SPEC_INVALID | 发布结构或领域校验失败，附 Validation Report |
| 500 | INTERNAL_ERROR | 不暴露内部详情的服务器错误 |

分页默认 20、最大 100。凭据请求总大小最大 512 KiB，单个 value 最大 64 KiB。
输入拒绝重复 Key、未知/大小写变体字段、null、非法 UTF-8、尾随 JSON 和非法动作/值；
错误不回显输入。任意请求日志或 SQL 参数日志不得记录秘密。

## 7. Draft COW、live 更新与幂等

| 操作 | 行为 | 对已发布配置的影响 |
| --- | --- | --- |
| Draft keep / 凭据字段缺省 | 保留相同用途与目标的当前关联；无关联仍未配置 | 不改旧 ID/值 |
| Draft replace | 新建 CredentialID、Credential Revision=1，绑定新 Draft | 不影响旧 Revision/Deployment |
| Draft clear / 删除资源 | 仅移除 Draft 关联 | 不撤销已发布引用 |
| live replace | 已发布关联定位 + OWNER + 独立 CAS，同 ID 更新值 | 配置/关联/Digest 不变，新取值批次使用新值 |
| live clear | active -> cleared，移除当前密文，Credential Revision 加一 | 后续取值失败；不原地恢复该 ID |

Draft 动作的 expected_credential_revision 仅允许 0/省略，Draft CAS 才是关联修改条件。
目的范围变化禁止 keep 偷渡旧值，需显式 replace/COW。live 更新目标来自确定的发布编号、
类别、资源名、purpose_field 与 Association Token；不接受客户端 CredentialID。

一个 Profile 拥有方事务覆盖 Draft CAS、普通配置、新凭据记录与 Receipt，或 live CAS、
状态/密文与 Receipt。失败整体回滚。请求比较使用服务端 keyed MAC，不存请求明文或裸值摘要。
Receipt 的主键作用域是 Tenant/Profile/actor/key；MAC 同时覆盖命令种类和请求。
同 key 同请求返回原结果，不同请求（包括另一命令）冲突。

发布幂等键保持 `(tenant_id, profile_id, source_draft_revision)`。先查询已发布记录，
再读取可变 Draft，因此 Draft 已前进后重试旧发布仍返回原 Revision。未发布且 Draft
计数已变才报冲突。Spec Digest 不设发布唯一约束；相同内容可以有不同业务 Revision。

## 8. 校验与完整性

- **Write 输入层**：严格解析独立 DTO，控制字段、动作和值大小，解析受限 DSN。
- **L0**：内部 Draft JSON 的可存储性与秘密字段拒绝，允许缺少发布必填关联。
- **L1/L2**：两个 V1 版本、关闭资源字段、内部 ID 格式、Capability 与类型化语义。
- **Canonicalization**：类型化规范化后执行 JCS 与 SHA-256；不注入隐藏资源或 Provider 默认。
- **动态凭据检查**：所属、Purpose、Audience、active 状态不属于 Profile 静态发布；
  `CheckUsable` 与运行取值在消费时校验，且不把配置可用性当作远端真实有效性。
- **Deployment 兼容性**：同名资源、Capability、Generation、平台网络规则、Manifest/Worker
  协议等属于后续 Deployment 实现，不在 Profile 发布中执行。

任何内部完整 Revision 读取以及公开 Revision 脱敏前，都必须重新 Canonicalize 并核对
Schema Version 与 Spec Digest。公开详情先校验来源，再投影；Publish 也使用此规则。
Revision 列表只选 Summary 列，不选择/聚合/反序列化 `spec_jsonb`，不加载凭据状态，
不对整页重复完整发布校验。Summary 的 Digest 是元数据，不代表该次查询验证了内容。

## 9. 持久化与加密配置

当前基线中由 Profile 拥有的表：

| 表 | 职责 |
| --- | --- |
| runtime_profiles | 身份、元数据、latest_revision_number |
| runtime_profile_drafts | 单当前 Draft 与 CAS，spec_jsonb 只存内部非秘密配置/ID |
| runtime_profile_revisions | 不可变 Canonical Spec、Digest、编号与发布审计 |
| runtime_profile_credentials | 当前密文、状态、Credential CAS、不可变所属/Purpose/Audience |
| runtime_profile_credential_receipts | actor/key、request_mac 与非秘密固定结果 |

凭据记录只保留当前值，不保存历史密文；cleared 为终止状态。基线 Trigger 限制凭据身份/
用途不变、计数每次加一和终止状态；Revision Trigger 拒绝 UPDATE/DELETE。
当前开发基线直接更新，没有旧 ref-only Reader、补录门禁或历史算法兼容分支。

`CONTROL_PROFILE_CREDENTIAL_KEY` 必须是外部提供的 Base64 编码 32 字节 Key；启动时缺失
或非法即失败，不生成默认 Key。`credentialcrypto` 使用 AES-256-GCM，封装为
`version || fresh nonce || ciphertext-and-tag`。版本和拥有方 AAD 受认证；加密与 MAC
使用独立派生域，幂等 MAC 与 Association Token 再按用途分域。Key 不存数据库，
配置与恢复责任留给部署；不建设 KMS 或自动 Key 轮换平台。

Storage 的 write-only DSN 当前只接受 PostgreSQL URI，固定目的字段为
`{host, port, database, username, sslmode}`，只加密 password。sslmode 显式取
`disable`、`require` 或 `verify-full`；其他 options 不接收。细节见凭据文档。

## 10. 组装与跨领域消费

业务 Adapter 随 runtimeprofile 子领域，进程只有一个 bootstrap。runtimeprofile 的
`wiring.go` 组装 PostgreSQL Store、CredentialCipher、Tenant/Owner 授权和公开 Handler。
跨领域使用 Application Port，禁止直接导入其他子领域 Repository 或读取其私有表。

- `CheckUsable` 已实现 ProfileRevision 与用途/状态检查，不解密；Deployment 调用尚待接线。
- `ResolveForAttempt` 已实现批量校验/解密与失败清理，依赖
  `ExecutionAuthorizationVerifier` 查询可信 Run/Attempt 拥有方。
- 可选 runtimehttp Adapter 已实现 `POST /internal/v1/runtime-profiles/credentials/resolve`。
  只有 AuthenticateWorker 与 ExecutionAuthorizationVerifier 成对注入时才注册；身份由
  可信 middleware 通过 context 写入，不相信请求体/请求头自述。
- 默认 bootstrap 未注入真实执行 verifier，不注册该内部路由；缺少 verifier 或有效
  执行上下文即拒绝，没有默认许可。该路由不计入 11 个管理操作。
- Run/Attempt 当前 lease/epoch 授权、Manifest 闭包、Worker batch 验收、single-flight、
  迟到响应与新 Attempt 重试属于后续运行面任务，现有 Application 测试不等于生产接线完成。

## 11. 代码结构与验证边界

~~~text
runtimeprofile/
├── domain/                         # Spec / Credential record / Validation / Canonicalization
├── application/                    # Authoring / COW / live / public read / consumer ports
├── adapter/
│   ├── inbound/http/               # 11 个公开操作
│   ├── inbound/runtimehttp/        # 可选内部批量解析，默认不注册
│   └── outbound/
│       ├── postgres/               # Profile 事务和私有凭据表
│       └── credentialcrypto/       # AES-GCM 与用途分域 MAC
└── wiring.go                       # 子领域组装，不启动第二个进程
~~~

本轮已有 Domain、Application、HTTP、加密与持久化测试代码；最终通过情况以实际测试
输出为准，不把“文件存在”写成完整集成通过。运行授权拥有方与 Worker 接线仍明确未完成。

## 12. 后续工具扩展

当前工具仅 `kind=mcp_streamable_http`、`capability=web.search`，不接收自由 Capability。
平台内建工具与工作区命令执行工具属于后续协议设计：AgentSpec 必须显式声明需求，
节点用 `tool_slots` 选择，Profile 选择平台已接入的类型化实现，Worker 只装配 Manifest/
节点需要的部分。平台实现标识不用 Go import 路径或任意 SDK Option。
工作区执行可以复用 Worker 内 tRPC CodeExecutor，不要求独立 Sandbox Manager。
本轮未增加新工具 Kind、Capability、执行配置或 Registry。
