# Runtime Profile 凭据直接录入：当前 V1 实现与运行面契约

- **控制面状态**：独立 Write/Read DTO、Canonical CredentialID、11 个管理 HTTP 操作、
  Profile 私有加密表、COW/live/CAS/幂等代码已落地；完整验证结果以本轮测试报告为准。
- **消费状态**：CheckUsable、ResolveForAttempt 和执行授权 Application Port 已实现；
  真实 Run/Attempt 拥有方与 Worker 接线仍属后续任务，不能视作运行面已经贯通。
- **版本**：直接调整当前 `/v1`、`schema_version=v1` 与 `credential_protocol_version=v1`，
  不维护旧引用式字段/Reader/Digest/Receipt 兼容分支。
- **范围**：Profile 拥有轻量凭据输入、当前加密值与消费边界，不建设独立 Secret 平台。
- **关联**：[runtime-profile.md](runtime-profile.md)、[runtime-profile-spec.md](runtime-profile-spec.md)。
- **部署协作**：资源同名匹配、Manifest 编译/闭包与运行角色选择归 Deployment 任务。

## 1. 当前实现与待接线部分

用户直接录入 API Key、MCP Bearer Token、Qdrant/Embedding Key 与 PostgreSQL URI。
公开 config 与 write-only credentials 分离；ProfileDraft/Revision 只保存非秘密配置和
服务器生成的内部 CredentialID，值在 Profile 私有表加密保存。公开 Read 不返回 spec、
内部 ID、值或密文。当前 Schema、DTO、Validator、Fixture、Golden 与开发数据库基线
已经按单一 V1 直接调整，不保留旧引用输入或补录迁移路径。

普通 SaveDraft 凭据变更采用 copy-on-write，显式 live 更新才改变同 ID 当前值/状态。
这部分控制面代码与测试已经存在；完整 Go/PostgreSQL 集成结果以本轮实际执行为准。
正常业务的 ProfileRevision/RuntimeManifest 仍是不可变快照，开发期不做历史兼容不等于允许
绕过发布规则。本文中的旧 Revision 是正常业务快照，不是要求保留开发期旧格式。

内部消费方法已有代码与单元测试；真实执行身份、Run/Attempt 当前授权拥有方、Manifest
验证与 Worker 批次验收尚未接线。后文运行生命周期条目明确是后续接入必须遵守的契约，
不是当前已经运行的 Worker 行为。授权依赖缺失时拒绝解析，绝无默认许可。

## 2. 不变的领域边界

- AgentVersion 继续表达逻辑需求，ProfileRevision 继续表达具体运行资源。
- Deployment 继续按资源类别和名称精确匹配；Capability 不搜索候选资源，用户不填写映射表。
- Profile 发布不读取 AgentVersion，不测试远端连通性，不验证 Provider 是否接受某个 Key。
- Profile 可以包含额外资源；Worker 只取得 Manifest 固定闭包所授权的资源和凭据。
- V1 没有 Environment 实体、`environment_id`、隐式 default 或 Overlay。
- 测试与生产使用不同 Profile；复制普通配置不自动继承来源 Profile 的凭据。
- Profile 不因此获得网络策略、执行后端管理或资源上限的授权权力。
- 当前工具仍只有 `kind=mcp_streamable_http`、`capability=web.search`，本文不放宽枚举。
- 平台内建工具与工作区执行仍是后续协议设计，框架能力不自动暴露给每个 Agent。

## 3. 所有权与统一语言

| 名称 | 含义与所有权 |
| --- | --- |
| RuntimeProfileWrite | 用户写入 DTO；包含普通配置和 write-only 凭据动作，不是 Canonical Spec |
| RuntimeProfileRead | 公开脱敏投影；包含普通配置和凭据状态，不包含内部 CredentialID 或值 |
| CanonicalRuntimeProfileSpec | 内部不可变发布文档；普通配置、类型化用途与内部凭据关联进入 Digest |
| CredentialID | 服务器随机生成的不透明内部身份；用户不创建、不填写、不复制该 ID |
| ProfileCredential | Profile 拥有的内部加密凭据记录，不是用户可独立管理的 Secret 聚合 |
| Credential Revision | 单个 CredentialID 的值/状态修改 CAS；不同于 Draft Revision 与发布编号 |
| Purpose | 服务端由资源类型与字段位置确定的用途，不是用户任意字符串或资源名称 |
| Purpose Scope | 某种用途允许的端点、协议、认证类型与必要目标范围，关联后不可静默改变 |
| Association Token | 公开可见的条件写入元数据，用于证明观察到的关联，既不是 CredentialID 也不是授权凭证 |
| Attempt Credential Batch | 某个 Worker 成功初始化时接受的一批内存凭据；不形成持久 Spec 或业务事件 |

Profile 拥有普通配置、凭据关联、密文与凭据状态的写事务和数据表。
Deployment 只经 application port 检查用途与可用性；它不解密、不读 Profile 私有表。
Worker 只经内部解析边界取值；它不跨模块或跨库 SQL 读取凭据。
这些接口都不是新服务。内部存储可直接使用 Control API 已有 PostgreSQL，但表归 Profile 拥有。
服务共享数据库连接不意味着共享表访问权；跨领域由 bootstrap 组装 application port。

## 4. 当前 V1 的表示分离与字段命名

| 边界 | 当前实现 | 责任 |
| --- | --- | --- |
| 用户 HTTP DTO | `/v1/.../runtime-profiles`，RuntimeProfileWrite / RuntimeProfileRead | 直接修订现有路由的输入输出，不维护 ref-only 双栈 |
| 持久 Canonical Spec | `schema_version: "v1"` | 直接更新现有 Schema、类型化校验与 Canonical 规则；不保留开发期旧字节或 Digest 算法分支 |
| 内部凭据协议 | `credential_protocol_version: "v1"` | 描述内部关联与解析协议，不是旧 SecretRef 的兼容层 |
| RuntimeManifest | 声明采用的 Spec 和凭据协议 | 具体 Manifest Schema 与消费校验归 Deployment 任务；发布后保持不可变 |

以上 Write、Canonical、Read 已有独立类型与控制面实现；没有改写 Identity、Tenant、
Agent 的 API。RuntimeManifest 编译与实际 Worker 消费仍待接入。
用户写入 `config` 表达普通配置，`credentials` 表达凭据动作；二者由服务器组合为内部 Canonical 候选。
当前 `PUT /v1/tenants/{tenant_id}/runtime-profiles/{profile_id}/draft` 全量替换普通配置，
必须携带 `Idempotency-Key`；成功仅返回 profile_id、draft_revision、updated_at。
`config` 的 models、tools、knowledge、storage 四个集合都显式出现；资源项省略表示从草稿删除，
不是 Overlay、深合并或部分 Patch。`credentials` 中缺字段=keep 只适用于本次仍保留的
同一资源实例，不会保留已从 config 删除的资源或给同名新资源续用原有关联。
服务器不接受用户在 `config` 注入任何 `*_credential_id`、`credential_id`、旧 `*_ref` 或明文凭据字段。
公开读取返回 `config` 与 `credential_states`，不把内部 Canonical Spec 当作公开 GET 的响应模型。
公开 `spec_digest` 指向内部 Canonical 文档；客户端不能通过公开脱敏投影自行重算该 Digest。
只有可信内部完整编译 Query 返回 Canonical 文档；公开完整详情在校验其来源后再脱敏。

## 5. Write DTO：表单里的 keep / replace / clear

凭据定位使用 `credentials.<category>.<resource_name>.<purpose_field>`，不让用户手填引用或 ID。
Purpose 字段由服务器为已接入资源类型预定义；名称只是定位，不授予访问另一个 Profile 的权限。
当前表单用途字段如下；它们是关闭的预定义位置，不扩大工具 Kind/Capability 协议。

| 用户用途字段 | 适用资源 | 内部 Canonical 关联位置 |
| --- | --- | --- |
| `api_key` | Model `models.primary` | `models.primary.api_key_credential_id` |
| `bearer_token` | MCP Tool `tools.search`，`auth.kind=bearer` | `tools.search.auth.credential_id` |
| `qdrant_api_key` | Knowledge `knowledge.docs` | `knowledge.docs.qdrant_api_key_credential_id` |
| `embedding_api_key` | 同一个 Knowledge 的 Embedding | `knowledge.docs.embedding.api_key_credential_id` |
| `dsn` | Storage `storage.session` | `storage.session.dsn_credential_id`，另有固定目标元数据 |

每个已提供的动作只允许 `action=keep|replace|clear`；`value` 仅允许随 replace 出现。
缺少整个凭据字段等同 keep，不等于清空，不把空字符串解释成 clear。
replace 必须带非空真实输入；`******`、既有掩码占位与 UI 未修改标记不当作新值。
服务器必须在写 DTO 层拒绝掩码占位，且不通过回显输入说明错误。
clear 必须显式表达；其影响由“保存草稿”与“更新已使用凭据”两种命令分别规定。
HTTP 总输入最大 512 KiB，单 value 最大 64 KiB；拒绝 null、重复 Key、未知/大小写变体
字段、非法 UTF-8、尾随 JSON、空白/NUL/CR/LF 值、掩码、redacted/unchanged 与占位输入。
Idempotency-Key 是 1～128 个可见 ASCII 字符，不含空格；错误不回显原值。
普通配置的资源删除依照 SaveDraft 的配置替换规则处理，与“缺少凭据字段=keep”不是同一件事。

~~~json
{
  "expected_draft_revision": 7,
  "credential_protocol_version": "v1",
  "config": {
    "models": {
      "primary": {
        "kind": "openai_compatible",
        "model": "example-chat-model",
        "base_url": "https://model.example.test/v1",
        "capabilities": [
          "chat",
          "tool_call"
        ]
      }
    },
    "tools": {},
    "knowledge": {},
    "storage": {}
  },
  "credentials": {
    "models": {
      "primary": {
        "api_key": {
          "action": "replace",
          "expected_credential_revision": 0,
          "value": "<write-only real input>"
        }
      }
    }
  }
}
~~~

上述 `<write-only real input>` 是文档占位，不是应被接受为实际凭据的测试值。
示例完整给出四个普通配置集合；空集合表示该类别在新草稿中为空，不触发隐藏默认配置。

## 6. 命令 A：SaveDraft 只修改草稿，不暗中轮换生产凭据

SaveDraft 的 `expected_draft_revision` 保护普通配置及当前草稿关联。
同一表单中的 keep/replace/clear 遵守 copy-on-write，而不是直接更新已发布 CredentialID。

| SaveDraft 动作 | 内部结果 | 对已有发布的影响 |
| --- | --- | --- |
| keep 或缺字段 | 保留当前草稿原有关联；无关联时仍未配置 | 不更新值，不改变旧 Revision |
| replace | 服务器创建新 CredentialID 与初始 Credential Revision=1，再绑定草稿 | 旧 ID/值不变；发布新 ProfileRevision 后才使用新关联 |
| clear | 只移除当前草稿关联 | 不撤销仍被已有发布引用的凭据，不让已有 Deployment 突然失效 |
| 删除资源 | 移除草稿资源及关联 | 已发布 Revision 及其原 CredentialID 保持不变 |

新建关联使用 `expected_credential_revision=0`，表示目标是“新凭据尚不存在”，不表示覆盖旧 ID 的版本。
即使当前草稿关联的凭据版本为 12，SaveDraft replace 仍以新 ID 的 expected=0 创建版本 1。
keep/草稿 clear 不改凭据值，因此不把 live Credential Revision 当作这两种动作的并发写入计数。
服务器用 Draft CAS 与该草稿的当前关联验证目标，不能仅按同名资源找到某个 CredentialID。
保存成功后 Draft Revision 递增；新凭据记录、关联和普通配置一起落库。
必需凭据缺失的草稿可保存，但后续发布的静态结构校验需要指出缺失的关联位置。
Profile 发布不读 AgentVersion，不解密；当前 live 配置/撤销状态仍是 Deployment 检查时的动态条件。
如果本次普通配置改变目的范围，keep 或缺少凭据动作必须报需重新录入；不能将旧关联默默迁往新目标。
凭据 replace/clear（包括草稿 COW 与 live 命令）要求 OWNER；ACTIVE Tenant 的 OWNER/MEMBER
可以编辑普通配置且保持凭据 keep。普通配置权限不隐含读取、替换或撤销凭据的权力。删除含关联资源也按草稿
clear 授权处理，不能通过省略 config 资源绕过凭据操作权限。

## 7. 命令 B：显式更新已使用凭据，才影响既有 Deployment

独立命令统一称 `UpdateUsedProfileCredential`，它仍是 Profile 的内部业务能力，不是 Secret CRUD。
当前公开路径是 `POST /v1/tenants/{tenant_id}/runtime-profiles/{profile_id}/credentials/update`，
必须携带 `Idempotency-Key`；表单必须显式提示这不是普通“保存草稿”。
目标由已发布 Revision 编号、资源类别、名称、用途字段和 Association Token 确定，用户仍不填写 ID。
服务器先读取可信不可变 Revision，定位其中资源实例与内部 ID，再验证 token 与当前用途范围。
资源实例指该不可变 Revision 中的确定位置及服务器记录的关联，不是当前草稿里一个同名 map key。
`expected_credential_revision` 独立执行 live CAS；旧 Draft Revision 不是 live CAS 的替代品。

~~~json
{
  "target": {
    "profile_revision_number": 3,
    "category": "models",
    "resource_name": "primary",
    "purpose_field": "api_key",
    "association_token": "<server-issued conditional-write metadata>"
  },
  "action": "replace",
  "expected_credential_revision": 5,
  "value": "<write-only replacement>"
}
~~~

Association Token 由服务器完整性保护并绑定 Tenant、Profile、已发布 Revision、资源实例、用途和关联。
它不暴露 CredentialID，也不取代登录身份、Tenant membership、OWNER 权限和请求目标核对。
默认仅当前 Tenant 的 OWNER 可执行 live replace/clear；管理员身份不自动绕过租户边界。
OWNER 的 actor 身份必须来自可信认证上下文，不来自请求体的可伪造字段。

| live 动作 | 允许的状态 | 结果 |
| --- | --- | --- |
| replace | 当前凭据 active、目的范围不变且 CAS 匹配 | 同 CredentialID 更新加密值，Credential Revision 加 1 |
| clear | 当前凭据 active 且 CAS 匹配 | 状态终止为 `cleared`（撤销），移除可解密当前值，Credential Revision 加 1 |
| replace 已 cleared ID | 不允许恢复旧 ID | 用户需通过 SaveDraft 新建 ID，再发布新 ProfileRevision |
| keep | live DTO 不接受 | 无需执行 live 写入，查询状态不会修改版本 |

同 ID live replace 不改 Draft、ProfileRevision、Manifest 或它们的 Digest，不要求重新发布 Deployment。
live clear 后，旧 Manifest 的新 Attempt 取值明确失败；它不是“先空着，自动回退其他 Key”。
已经成功初始化的 Attempt 可继续使用内存值；此版本不承诺 clear 即时终止所有运行中的调用。
同一 ID 被多个旧 Revision 合法引用时，live 更新影响这些引用的新 Attempt，表单应明确该影响。
禁止由“修改草稿中的 Key”隐式调用 live 命令；copy-on-write 与生产轮换必须在交互和接口上分离。

## 8. 稳定关联、删除重建与复制

删除后再创建同名 `models.primary` 必须分配新 CredentialID，禁止复用旧名称查询到的历史 ID。
当前没有 Profile 复制 API；若后续加入复制，只复制非秘密配置，凭据关联默认清空，
不把来源密文或 ID 带入目标。
相同值或相同字段名也不触发跨 Profile 的凭据去重、ID 复用或自动关联。
跨 Tenant、同 Tenant 跨 Profile 的 CredentialID 注入均被拒绝，公开接口原本就不接受此类 ID。
内部最小作用域是 `TenantID + ProfileID + CredentialID + Purpose`，用途范围还需与可信资源目标吻合。
不能仅凭 CredentialID 的不可猜测性授权，也不能仅凭同 Tenant 或资源同名授权。
endpoint、kind、auth 或其他目的范围变化需要显式重新录入并创建新 ID，不能静默沿用旧值。
若新配置仍需要凭据，则必须 replace 为新 ID；若 `auth.kind` 改为 `none` 或新 Kind 不再有该
凭据用途，则在同一个 SaveDraft 中显式移除关联，不创建空值凭据，也不把旧值转移到其他用途。
这属于草稿配置/关联变化，不是对历史 ID 的 live clear；旧部署继续受原关联约束。
旧 Revision 不改写；旧 ID 的用途范围不会为了新 Draft 的端点而原地修改。
Association Token 与确定 Revision 的绑定防止删除/重建后对“同名新资源”执行过期 live 操作。
仍被已发布 Revision 引用的凭据记录由 Profile 拥有方管理；普通 Draft 删除不撤销这些关联。
这不是 Credential 历史值存储：live 轮换只维护当前值，不保存轮换前的历史密文。
本轮不设计凭据垃圾回收系统，后续清理不得破坏仍被不可变 Revision 合法使用的记录。

## 9. Storage DSN：受限 PostgreSQL URI 与固定目标

当前只接受 `postgres://` 或 `postgresql://` URI，不接受 keyword DSN、Unix socket、
多 host、fragment、任意驱动参数或 options。URI 必须包含 username、password、database，
并显式提供唯一 query `sslmode`，允许 `disable`、`require`、`verify-full`。
端口省略时规范化为 5432；host 规范化为小写；端口范围 1～65535。

服务端解析后仅加密 password，完整 URI 不进入数据库 Spec、Receipt 或日志。
非秘密 `destination = {host, port, database, username, sslmode}` 固定在 Canonical Spec；
公开 config 可返回这些目标字段。config 同时提供 destination 时，必须与 DSN 解析结果
完全一致；没有任意 options 字段或允许名单扩展。本轮不增加无密码信任连接策略。

- password 改变、完整 destination 一致：显式 live replace 可以同 ID 更新。
- host/port/database/username/sslmode 改变：live replace 拒绝，需 Draft 新配置、
  COW 新 ID、新 ProfileRevision 与后续 Deployment 发布。
- `dsn` 是用户输入用途名称；内部 dsn_credential_id 实际指向密码值，不存整条 DSN。
- 固定目的指纹只摘要非秘密字段，不包含密码或裸密码摘要。
- `storage.session` 只是示例，不是 Profile 必填项或隐式运行绑定；选择权归 Deployment。

## 10. 持久化、加密与原子性

公开 Handler 使用 DecodeProfileWrite/DecodeCredentialUpdate 严格解析输入，Application
处理动作、用途/目的、OWNER 授权与 CAS 后构造内部 Spec。只含非秘密配置和内部 ID 的
候选经过 Draft L0 才写入 spec_jsonb，不把整个 ProfileWrite 或 value 写入持久 Spec。

当前基线新增 `runtime_profile_credentials` 与 `runtime_profile_credential_receipts`。
凭据记录保存 Tenant/Profile/ID、category/resource_name/purpose/audience_digest、
Credential Revision、active/cleared、当前 ciphertext 与审计元数据。Receipt 保存
actor/key/request_mac/非秘密固定结果，不保存请求正文或密文。没有历史密文表。

`CONTROL_PROFILE_CREDENTIAL_KEY` 为必需外部配置，Base64 解码后必须为 32 字节；缺失
或非法时启动失败，不生成默认开发 Key。Key 与数据库分离，不写入行或仓库。
credentialcrypto 使用 AES-256-GCM：封装为 `version || nonce || ciphertext-and-tag`，
每次加密生成新随机 nonce，当前 envelope version=1；版本与调用方 AAD 都受认证。
AAD 绑定 Tenant、Profile、CredentialID、类别、资源名、Purpose 与 audience_digest。
加密与 MAC 通过不同派生域分离，Receipt 与 Association Token 再以各自用途分域。
篡改、版本不支持或 AAD 不匹配均返回不含值的错误；本轮不建设 KMS/自动 Key 轮换。

PostgreSQL WithinProfile 在事务内锁定所属 Profile 行，序列化该 Profile 的配置、凭据、
Receipt 与批量读取；这里不声称使用跨服务或全数据库固定快照。
SaveDraft 事务覆盖配置、Draft CAS、新 ID/密文与 Receipt；live 事务覆盖 OWNER 复核、
关联与 Credential CAS、状态/密文与 Receipt。任何失败整体回滚。
数据库 Trigger 保持所属/用途不变、每次 Credential Revision 加一、cleared 终止状态；
已发布 ProfileRevision 仍由不可变 Trigger 保护。事务内不调用 Provider 或发送秘密事件。

## 11. 幂等请求、诊断和日志

SaveDraft 与 live 更新必须提供 `Idempotency-Key`。Receipt 主键作用域为租户、Profile、
actor、key；请求 MAC 还覆盖命令种类，因此同 key 跨命令重用会冲突。
相同 key 和相同请求返回同一非秘密结果；相同 key 对不同请求报冲突，不重复创建 ID 或再次轮换。
服务器需要比较带秘密请求时，使用服务端 keyed MAC，而不是裸 SHA-256 或把请求体存入 Receipt。
MAC key 与业务标识绑定、按服务配置管理；MAC 本身不向公开 API 暴露为可验证密码摘要。
Receipt 仅保存命令身份、输入 MAC、确定的 Draft/Credential Revision 等结果，不保存明文或密文。
GET 的动态 credential_states 不进入 Receipt；live Receipt 中的 status 是该命令当时的
固定结果，不因后续变化而刷新。当前状态经详情 GET 获取，旧 Receipt 不伪装为实时状态。

公开 GET、验证报告、普通日志、错误栈、SQL 参数日志、追踪 span、审计事件与业务 Outbox 都排除凭据值。
HTTP 和 PostgreSQL instrumentation 不能先记录原始请求/参数再声称由业务响应脱敏。
诊断只记录字段位置和稳定错误码，不回显 value、连接串、密文、解密错误内容或加密 Key。
调试模式遵循同样规则；生产密钥不存在“只在 debug 日志打一下”的例外。
后续 Worker 内部解析响应必须使用受认证加密传输，不被通用 HTTP body logger 采集；
当前 Application 方法与可选内部 Adapter 不代表生产执行身份/授权已经接线。
最小化明文驻留时间，不把“GC 最终回收”描述成可证明的即时内存擦除。

## 12. Read DTO 与完整性边界

~~~json
{
  "profile_id": "profile-example",
  "draft_revision": 8,
  "config": {
    "models": {
      "primary": {
        "kind": "openai_compatible",
        "model": "example-chat-model",
        "base_url": "https://model.example.test/v1",
        "capabilities": [
          "chat",
          "tool_call"
        ]
      }
    },
    "tools": {},
    "knowledge": {},
    "storage": {}
  },
  "credential_states": {
    "models": {
      "primary": {
        "api_key": {
          "configured": true,
          "status": "active",
          "credential_revision": 1,
          "association_token": "<conditional-write metadata>"
        }
      }
    }
  },
  "schema_version": "v1",
  "credential_protocol_version": "v1"
}
~~~

公开状态既不返回完整 ID，也不返回密文、值、掩码化原值、前缀、后缀或可离线验证值的摘要。
Revision 详情的普通配置来自固定 Revision；它附带的 credential_states 是独立的当前状态投影。
轮换后 credential_revision/status 可以变化，但 ProfileRevision 内容和 spec_digest 不变。
公开响应必须标清这一点，不能把动态状态再 canonicalize 进历史 Revision。
内部完整读取继续根据对应 Schema Version canonicalize，并核对 Schema Version 与 Digest 后才编译。
公开完整详情也应验证其完整 Canonical 来源后再脱敏，不能以“只公开部分字段”为由绕过损坏检测。
Revision 摘要列表保持独立摘要类型，不加载完整 Spec，不对每项重新执行完整发布校验。
列表也不为了计算每个资源状态引入无界凭据遍历；需要状态时使用有界详情/状态查询。
发布幂等性仍返回原不可变发布结果；Credential 状态不参与“相同 Draft 是否已发布”的身份判断。
Publish 通过 Source Draft Revision 找到原不可变发布记录，再生成固定脱敏响应，不新建
动态状态 Receipt。configured/status/credential_revision 不放入发布响应或摘要；当前状态
通过 Draft/Revision GET 的独立投影查询，不因发布幂等重放而刷新。
公开 spec_digest 仍是内部 Canonical 的完整性标识，而不是脱敏响应内容的摘要。

## 13. 消费接口一：Deployment 检查，不解密

~~~text
ProfileCredentialChecker.CheckUsable(
    tenant, actor, profileID, profileRevisionNumber, uses
)
~~~

该 Application 方法已有实现与测试，真实 Deployment 调用待接入。`uses` 应由 Deployment
对完整 Canonical Revision 和 Manifest 闭包计算，不由浏览器直接宣称授权范围。
跨领域 use 的核心字段统一为 `credential_id`、`purpose`、`audience_digest`，并携带其所属
Profile/Revision 和确定资源实例的可信上下文。`purpose` 由已接入 Kind 和位置推导，
`audience_digest` 只摘要非秘密的归一化目的范围；它不是凭据值的摘要，也不是授权令牌。
Canonical 中的 `*_credential_id` / `auth.credential_id` 与 Manifest use 是不同层次的表达，
由编译器根据确定资源及不可变用途关联构造，不要求用户提交这些字段。
Checker 必须核对 ID 确实属于所选 Tenant/Profile/Revision，并且用途与所选资源完全相符。
它读取是否已配置、active/cleared 状态、当前 Credential Revision 与所属/用途授权，不返回值或密文。
Checker 不调用 Provider，也不把“可配置使用”说成 Key 对远端真实有效。
Deployment 编译将必要的内部关联固定到 Manifest，不把当前 Credential Revision 当成永久冻结值。
Checker 成功只是发布时检查；之后可能发生 live clear，所以 Worker 初始化仍必须独立检查。
这不是对凭据未来永久可用的承诺，也不要求 Deployment 在 Profile 所有者之外解密。

## 14. 消费接口二：Worker 基于可信执行上下文解析

~~~text
RuntimeCredentialResolver.ResolveForAttempt(
    可信执行上下文, ManifestID/Digest, uses
)
~~~

可信执行上下文由 Run/Attempt 拥有方签发；Profile 解析前还必须通过拥有方的可信授权查询
核验 Attempt active、当前 lease epoch、Worker 归属及授权状态。签名尚未过期不证明旧 epoch
仍然有效，工作负载身份也不等于具体 Run 的授权。
授权至少绑定 Tenant、Run、Attempt、Worker 身份、lease epoch/fencing、expiry 和确定的 Manifest ID/Digest。
若采用签发载荷，Profile 必须校验可信 issuer、受众 audience、完整性与时效；Worker 不得自行签发租户授权。
Worker 提交的 tenant_id、uses 或任意 Manifest JSON 不能自行形成授权，工作负载认证本身也不是租户授权。
Profile 通过 `ExecutionAuthorizationVerifier` application port 查询 Run/Attempt 拥有方；跨进程时由该拥有方既有 Workload 的
内部接口承接，不通过 Profile 或 Worker 直读其表。这里的签名校验不能替代当前授权查询，
也不引入独立鉴权服务。若将来改为纯离线短期授权，必须另行定义提前撤销和重新分配的限制。
Manifest 内容/闭包必须来自可信查询，或由受信签名覆盖；若携带方是 Worker，则先按受信
Manifest Digest 校验完整 Canonical 内容，再计算闭包，不能只验证 ID 后相信它提交的 uses。
请求 uses 必须是已验证固定 Manifest 凭据闭包的子集，不能依据 Worker 自述新增用途或资源。
解析时逐项核对 use 位于固定 Manifest 闭包、内部关联属于同 Profile 且用途和固定目标匹配。
同租户另一个 Profile 的值、未声明工具的值、额外 Profile 资源的值均不因此可读取。
失败返回稳定结构化错误；未配置、已撤销、关联不符、Manifest 不符或 lease 失效均不得回退其他值。
业务使用方只取得已授权批次，不取得列举所有凭据的接口。

Profile 拥有方提供可选 `runtimehttp` Adapter，路由为
`POST /internal/v1/runtime-profiles/credentials/resolve`，不同于公开管理 GET。
只在可信工作负载认证 middleware 与 ExecutionAuthorizationVerifier 成对注入时注册；
身份来自可信 middleware 经 `WithWorkerIdentity` 写入的 context，不信任请求体或
请求头自述 Worker 身份。当前 bootstrap 在显式配置 `CONTROL_RUNTIME_CONFIG_FILE` 后
装配 mTLS 工作负载身份、真实 Worker Run/Attempt verifier 与此路由；未配置时不注册，
缺少依赖不提供默认许可。Worker outbound Adapter 已按固定 Manifest 解析整批并复核
当前 Fence。真实跨进程 fixture 已验证该路径，实际外部 Provider/Telegram 验收另行记录，
不把仅有内部 Adapter 或配置存在视为真实渠道交付。
Deployment 与 Profile 同进程时可直接组装 Checker，不需要通过自己的公开 HTTP 绕行。
Worker 独立进程时继续使用上述内部接口；共享 PostgreSQL 不构成跨模块直读表的理由。
此方案不创建 Secret 微服务，不复制值进 Outbox/NATS，也不建设通用凭据分发平台。

## 15. Attempt 初始化：Worker V1 已接线的失败语义

当前 Application 在同一 Profile 锁定事务中核对全部 use，随后全有或全无地解密并返回；
Worker V1 已实现后述完整批次验收、单次初始化与 Attempt 生命周期；具体跨进程测试范围
见 [Worker 实现状态](../agent-worker/implementation-status.md)，不据单元测试推断外部模型验收。
任何一项失败时不返回部分值。Worker 构造本 Attempt 的全部所需凭据集合，验收响应的
Tenant/Profile/Attempt/ManifestDigest 等于请求，且 CredentialID/Purpose/audience_digest 集合
完全相同，无缺失、多余或重复；接收时还持有有效 lease，全部通过才初始化客户端与 Agent。
任一验收失败丢弃整批；首版不支持开始执行后再追加批次或补取某个遗漏凭据。
一个 Attempt 只允许一次 single-flight 初始化；初始化成功后所有节点使用同一个进程内批次。
内部响应只进入 Worker 内存；接收缓冲、日志、临时文件和 tracing 不保存持久值副本。
Worker 不在每次模型/工具调用前重新解析，也不在同 Attempt 中混用轮换前后的值。

为避免增加历史密文或初始化 pin 表，本版采用明确的失败边界：

1. 完整批量响应验收成功，才算该 Attempt 凭据初始化成功。
2. 请求超时、响应丢失、部分响应或初始化结果不确定时，终止当前 Attempt；下一次执行分配新 AttemptID。
3. 不在同 Attempt 上再次 Resolve 以猜测上次用了哪个版本；内部 HTTP Adapter 关闭透明自动重试。
4. Worker 进程崩溃、lease 失效、凭据缓存丢失时，同样结束该 Attempt，恢复使用新 AttemptID。
5. 初始化响应携带请求标识与 Attempt/lease 绑定；已结束 Attempt 的迟到响应被丢弃，不启动 Agent。
6. 新 Attempt 重新读取当前值；active 同 ID 轮换只影响新 Attempt，不改变旧 Manifest。

在线 verifier 的网络/读取故障与明确拒绝分开：前者经
`ErrExecutionDependencyUnavailable` 返回 Resolve 503，Worker 记录
`DEPENDENCY_UNAVAILABLE` 并在固定期限和显式次数内分配新 Attempt；真实 owner 403
经 `ErrExecutionUnauthorized` 返回 Resolve 403，Worker 记录 `CREDENTIAL_DENIED`，
不自动重试该 Run。完整200响应中的批次身份/uses验收失败同样整批拒绝，不初始化客户端。

live clear 与并发 Resolve 通过拥有方同一 Profile 的行锁事务序列化。
clear 提交之后开始授权的新批次必须失败；clear 之前已授权的在途批次可能完成初始化。
Run 授权查询与 Profile 数据库读取不是分布式原子事务；查询点之后发生的 lease 撤销存在在途
竞争，Worker 必须在接受响应和开始执行时复核执行侧 fence。此设计不声称瞬时撤销在途网络响应。
已初始化 Attempt 不逐调用回查撤销状态；这不是强制立即停机或远端 Token 即时失效协议。
若需要即刻终止运行，后续需单独的 Attempt 取消能力，不能声称仅清空数据库就实现了它。

该最小选择不存储 Credential 历史值，不存储持久批次 pin，也不支持跨进程同 Attempt 透明恢复。
代价是网络抖动可能造成未执行 Agent 的 Attempt 失败并重试，必须由 Run 生命周期定义有界重试策略。
重试新 Attempt 不得绕过 Run 的幂等、副作用管理或累计重试限制；这些规则不在 Profile 内实现。

## 16. 后续运行接线的可用性与部署代价

新 Attempt 首次取值依赖 Profile 拥有方内部接口、PostgreSQL、解密配置，以及 Run/Attempt
拥有方的当前授权查询可用性。独立 Worker 到 Profile 是一次批量请求；Profile 到执行拥有方
的授权核验是另一条显式依赖，不能把它省略成“验签就等价于实时 lease 检查”。
Control API/Profile 拥有方中断时，新 Attempt 可能在初始化阶段失败；不能继续承诺所有新 Run 不受影响。
已成功取得值的当前 Attempt 不逐调用依赖 Control API，因此可在其 lease 与执行策略允许时继续。
同进程调用 Checker 是比 HTTP 更小的接线，但它不能替代独立 Worker 的运行取值网络边界。
把 Worker 直接连 Profile 表、把秘密复制进 Manifest 或让 NATS 广播秘密不是符合所有权的“简化”。
本版不引入历史保留/凭据缓存服务；运维需配置内部端点、认证、传输、DB 和加密 Key。
加密 Key 的备份/恢复需要避免永久丢失现有密文解密能力，但本轮不实现 KMS 或 Key 轮换平台。

## 17. 开发阶段直接修订，不建设历史兼容层

1. 直接调整当前 `/v1` 的 RuntimeProfileWrite、RuntimeProfileRead 与内部 Canonical
   `schema_version=v1`，只保留一套目标协议和正常读写路径。
2. 实现阶段同步更新现有 OpenAPI、JSON Schema、类型化 DTO、Validator、Fixture、Golden
   和数据库基线；新增业务所需的加密存储可以落入基线，不为开发期旧数据专设迁移或历史表。
3. 旧 ref-only 输入不继续接受，不维护旧 Reader、Validator、Canonicalizer、Digest 或
   Receipt 重放分支，也不提供旧引用补录、混合版本列表或双栈门禁。
4. 不把旧 SecretRef 字符串重新解释成凭据值，不隐式从外部 Secret 平台迁值；用户按目标
   Profile 表单直接配置资源和凭据，无需先创建 Secret 对象或执行开发期数据转换流程。
5. 开发期旧 Draft、Revision、Receipt、Digest 与 Fixture 不构成必须保留的产品历史。
   当前实现直接更新开发基线并新增必要私有表；不建立旧数据补录或混合读取路径。
6. 目标协议下正常发布的 ProfileRevision/RuntimeManifest 仍是不可变快照；普通配置或
   关联变化继续走 Draft CAS、COW 和新发布，不能用“开发期无兼容”绕过发布边界。
7. active 同 CredentialID 的纯值 live replace 不修改快照或 Digest，仍需显式命令、用途
   校验、独立 CAS 与幂等结果；撤销不原地恢复旧 ID。
8. 摘要列表仍不加载 Spec，内部完整读取和公开脱敏前的完整来源验证仍执行当前协议的
   Canonicalization 与 Digest 核对；不保留旧算法分支并不等于取消完整性校验。
9. 控制面已落地代码与未接线的运行消费边界分别标记，不把它们当成两个要并行维护的
   产品版本。真正稳定发布后的演进策略另行设计，不作为本次实施门槛。

## 18. 最小验收矩阵

本表是实现验收要求，不是所有条目已经端到端通过的报告；Worker/Attempt 条目属于
后续接线验收。实际控制面/数据库测试结果以本轮执行记录为准。

| 场景 | 必须观察的结果 |
| --- | --- |
| 当前 V1 协议 | `/v1` 与 `schema_version=v1` 使用单一目标协议，不接受旧 ref-only 输入或保留兼容分支 |
| 直接录入 Key/Token/DSN | 保存成功只返回元数据，公开读取不含 ID、值或密文 |
| 持久化检查 | Draft.spec_jsonb、Revision、Manifest、Receipt、日志与事件无明文/密文值 |
| SaveDraft keep | 关联保留，不更新 live Credential Revision |
| SaveDraft replace | 创建新 ID 和初始版本 1，旧 Revision/旧 Deployment 的关联不变 |
| SaveDraft clear/删除 | 只解绑草稿，仍被已有发布引用的凭据不受影响；缺少必需关联时发布提示明确 |
| live replace | OWNER、关联校验和独立 CAS 成功；旧 Manifest 不变，新 Attempt 使用新值 |
| live clear | 终止状态，旧 Manifest 的新 Attempt 失败，原 ID 后续不可 live 恢复 |
| 普通编辑员 live 操作 | 拒绝，不依靠按钮隐藏实现授权 |
| 并发修改 | Draft CAS 与 Credential CAS 各自生效，冲突不吞掉已有更新 |
| 加密/SQL/提交失败 | 整体回滚，没有新孤立关联、半份配置或错误递增版本 |
| 同 key 幂等重放 | 返回同一非秘密结果，不重复新建/轮换；不同请求冲突 |
| 跨 Profile/跨 Tenant | 相同资源名、伪造用途、ID 或 token 都不能读取/修改别人的值 |
| 删除同名重建 | 新 ID，不劫持旧 Revision；过期 association token 不改新关联 |
| Profile 复制 | 普通配置可复制，凭据未配置，不自动获得源 Profile 值 |
| 目的范围变化 | endpoint/kind/auth/DSN 目标变更必须 COW 和新 Revision |
| DSN 仅密码轮换 | 固定目标一致时才允许 active 同 ID live replace |
| 初始化全有或全无 | 任一 use 失败不启动 Agent、不返回可使用的部分批次 |
| 响应丢失或 Worker 崩溃 | 当前 Attempt 结束，下一次使用新 AttemptID，不重解旧 Attempt |
| 已初始化时轮换/clear | 当前批次不变；新 Attempt 分别用新值/明确失败 |
| 迟到响应 | Attempt/lease fence 不符即丢弃，无重复启动 |
| 摘要列表与完整读取 | 列表不读 Spec；完整读取继续 canonicalize 并核对 Digest |
| 无独立 Secret 对象 | 用户仅在 Profile 内录入即可形成有效发布候选 |

## 19. 已落地内容与仍待接入的事项

当前代码已有：11 个管理 HTTP 操作、独立 Write/Read DTO、两个 V1 版本字段、内部
CredentialID/用途模型、受限 PostgreSQL URI、输入大小与动作校验、AES-GCM/MAC、必需
外部 Key 配置、Profile 事务表/Receipt、COW/live/CAS/幂等、公开读与摘要完整性边界。
CheckUsable、ResolveForAttempt、ExecutionAuthorizationVerifier 与可选 runtimehttp
Adapter 是消费接入基础，不代表真实执行拥有方与 Worker 已经接入。

仍待后续任务完成：

- 可信 Run/Attempt 当前 active lease/epoch 与 Manifest 授权查询的真实实现和注入。
- 工作负载认证、内部传输配置、Worker Adapter/批次验收和 Attempt single-flight 生命周期。
- Deployment 实际编译/Checker 调用、固定 Manifest 凭据闭包与 Storage 运行角色选择。
- 平台内建工具、工作区执行的新 Kind/Capability/配置；本轮不扩大工具协议。
- OAuth 刷新、账号池、通用 Secret CRUD、Vault/KMS、跨进程同 Attempt 透明恢复。

默认 bootstrap 不开启内部解析路由，不注入许可型 verifier。完整集成验证以实际输出
为准，本文不把测试替身或可选 Adapter 记作生产运行面完成。

## PostgreSQL／Redis Memory 最小密码契约

`managed_memory` 选择平台 PostgreSQL 或 Redis 后端时，公开 `config.storage.memory`
仍只有 `kind`、`backend_id`、`backend_revision`。写入实际密码使用：

```json
{"credentials":{"storage":{"memory":{"dsn_password":{"action":"replace","value":"<password>"}}}}}
```

`value` 是密码而不是完整 DSN；`keep`／`clear` 沿用既有操作语义。
平台目标固定 host、port、database、username、TLS；本运行契约要求
`managed-postgres-v1` 或 `managed-redis-v1`、Memory 隔离及 `memory_runtime` 执行身份。
其他托管角色不因此获得密码输入能力。Redis 是正式持久 Memory 后端，不配置隐式 TTL；
密码仅用于认证，不能改变 Snapshot 固定的 Host/Port/Username/Database/TLS。

Profile 使用方端口 `ManagedCredentialTargetResolver.ResolveManagedCredentialAudience`
从可信目录解析租户作用域、确定 backend ID/revision 的 PostgreSQL／Redis Memory 目标，返回
`Snapshot.Digest()`。服务端将这个摘要与生成的 `dsn_credential_id` 一起保存到
内部 canonical 的 `credential_audience_digest`，不接受用户提交或在公开 config 返回。
密码复用现有 Profile 私有加密存储，purpose 为 `dsn_password`。

Deployment 实际启用托管 PostgreSQL／Redis Memory 时要求两项内部关联存在，并要求 Profile 保存的
摘要与固定 Manifest Backend 的摘要完全一致。Manifest 的资源 `credential` 只保存
`credential_id`、`purpose=dsn_password`、`audience_digest`；编译 required uses 包含它。
Worker 复用现有受认证 `ResolveForAttempt` 取得密码，再用固定后端描述构造连接，
不从环境变量补密码，不把密码写入 Manifest、Outbox 或事件。

更换 backend ID/revision 后，`keep` 不会跨目标复用旧密码；必须显式 `replace`。
未配置或已清除密码的 Draft 可保存，但实际托管 Memory 发布编译会返回凭据诊断。
已发布 Revision 的状态和轮换使用固定关联、association token、主体授权与凭据 CAS；
不读取当前目录，目录下架不自动清除状态或阻止既有引用轮换。轮换不改 Revision。
公开状态键为 `credential_states.storage.memory.dsn_password`。

现有凭据表对 purpose 只有非空约束，本改动不需要数据库迁移，也不修改历史迁移。

Redis Memory 的历史无凭据 Manifest descriptor 保持 Canonical／详情读取与原公开投影；
这不授予执行能力，新编译／发布仍要求固定凭据关联，Worker gate 也拒绝无凭据执行。
本批使用协调同批发布；新 Redis Memory Manifest 不投递给旧 PG-only Worker，
不滚动混用这两种实现。当前不新增协议字段或扩大运行契约版本。

## Redis 托管 Session 密码与 Summary 边界

`managed_session` 选择 Redis 时复用 `credentials.storage.session.dsn_password`，
对应 `credential_states.storage.session.dsn_password`。输入值只用于认证，不是 DSN。
公开配置仍为 kind/backend_id/backend_revision，内部同样成对保存
`dsn_credential_id` 与 `credential_audience_digest=Snapshot.Digest()`。

`ResolveManagedCredentialAudience(ctx, tenant, backendID, revision, role)` 按角色验证：
Memory 允许 PostgreSQL／Redis 的 `memory_runtime`；Session 仅允许 Redis 的
`session_runtime`。托管 PostgreSQL Session 暂不启用，旧 `postgres_state` 不受影响。
保存／发布使用固定 Snapshot 的 Host/Port/Username/Database/TLS 与租户、角色范围。
更换目标时 keep 拒绝、replace 重绑；已发布凭据的状态与轮换继续按不可变关联，
不重新查询当前目录，不新增凭据生命周期。

Worker 发布托管 Redis Session 必须携带匹配的密码引用；历史无凭据描述仍可读取，
公开投影不额外增加 credential_present，Worker 拒绝执行缺少凭据的描述。
Summary 与消息和状态共用同一 Session 后端快照。Session 身份仍包含
DeploymentRevisionID，不跨 Revision 迁移 Session，也不引入新的 identity。
本批仍协调同批发布，不将新 Session Manifest 交给旧 Worker。

## Artifact S3 双凭据

`managed_artifact` 选择固定平台 S3 Snapshot，公开配置仍是
kind/backend_id/backend_revision。写入使用
`credentials.storage.artifact.access_key_id` 与 `secret_access_key`，
分别复用 keep/replace/clear 与现有已发布凭据轮换协议。
两项均加密保存，内部字段为 access_key_id_credential_id、
secret_access_key_credential_id 和 credential_audience_digest=Snapshot.Digest()。
Draft 可暂存部分配置；新 Worker 发布必须两项齐全且匹配目标摘要。
更换目标时所有保留的旧关联都会拒绝 keep，必须 replace 或 clear；
发布后逐字段轮换独立于目录且不改 Revision，不宣称两个独立请求原子换对。

Manifest 使用 credentials.access_key_id / credentials.secret_access_key，
每项仅含 credential_id/purpose/audience_digest，purpose 与字段同名。
endpoint、bucket、region、path_style、versioning 都来自固定 Snapshot；
密码不得覆盖目标。公开 Manifest 只显示 credential_present。
历史无 credentials 描述保持可读，但不通过新 Worker 执行门禁。
Artifact 元数据继续沿用既有 Worker PostgreSQL metadata_contract，
不增加用户数据库配置；需要 Artifact 的节点必须选择支持 tool_call 的模型。

## 托管 Knowledge 的固定凭据

目标解析接口泛化为 `ResolveManagedCredentialAudience`，存储角色语义不变；
knowledge 角色只解析租户获准使用的固定 Qdrant Snapshot。
`managed_knowledge` 复用 qdrant_api_key 输入、状态及轮换，内部成对保存
qdrant_api_key_credential_id 与 credential_audience_digest=Snapshot.Digest()。
Manifest.Credential 使用 qdrant_api_key 与该摘要，Embedding.Credential 继续使用
既有 embedding_api_key 和 kind/BaseURL audience 算法，不改已有身份绑定。
新 Worker 发布必须显式 Qdrant Key；历史无 Key 描述保持可读但不可执行。

Knowledge 的 Worker scope 包含 tenant/profile/resource/backendDigest 与固定
embedding model/baseURL/dimensions，不包含 DeploymentRevisionID；同配置重复发布可复用。
导入绑定固定发布Revision与被Agent选择的resource，不接受用户提供scope或后端目标。
不新增 chunk 配置、知识任务平台或后台导入调度。
