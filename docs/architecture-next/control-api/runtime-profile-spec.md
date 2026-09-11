# RuntimeProfileSpec V1：内部 Canonical 与公开 DTO 契约

- **阶段**：开发阶段的单一 V1，当前 Schema/Go 类型已切换为内部 CredentialID；无旧引用兼容层。
- **代码范围**：Write、Canonical、Read 分离与控制面代码已落地，完整集成结果见本轮报告。
- **协议位置**：`api/schemas/runtimeprofile/v1/` 表达内部 Canonical Spec，不是公开写入/读取 DTO。
- **版本**：`schema_version=v1` 与 `credential_protocol_version=v1`。
- **运行面**：Deployment Compiler、真实 Run/Attempt 授权与 Worker 接线尚未完成。

## 1. 三种表示，不是三个版本

| 表示 | 入口/用途 | 凭据内容 |
| --- | --- | --- |
| ProfileWrite | PUT draft 的独立命令 DTO | credentials 中的 write-only 动作和值 |
| Canonical Spec | 内部 Draft 候选、不可变 Revision、可信编译 Query | 服务器生成的 CredentialID；无值、密文或可变状态 |
| ProfileRead | 公开 Draft/Revision GET 与发布响应 | config；详情可有 credential_states；无内部 ID 或 spec |

输入不会直接 marshal 到 spec_jsonb。Application 先处理凭据动作、授权、用途、CAS 与
加密记录，才构造内部 Spec。公开读取在内部完整性验证后投影，不能使用脱敏 config
重算内部 Digest。公开接口不接受旧 `*_ref`、`credential_id` 或 `*_credential_id` 注入。

用户不创建独立 Secret 对象；Profile 拥有内部轻量加密存储。详细命令与消费协议见
[runtime-profile-credentials.md](runtime-profile-credentials.md)，11 个 HTTP 操作和事务
边界见 [runtime-profile.md](runtime-profile.md)。

## 2. 不变的边界

AgentSpec 表达逻辑需求，RuntimeProfile 表达具体资源。Deployment 按资源类别和名称
精确匹配 `models.primary`、`tools.search`、`knowledge.docs`，不搜索 Capability 候选，
不模糊匹配，不要求额外用户映射表。MCP 远程 `tool_name=search_web` 不必等于资源名。
Profile 发布不读取 AgentVersion，也不证明该 Profile 满足某个 Agent。

额外资源不自动暴露给 Agent；Manifest 只包含实际需求与必要运行资源/已接受类型依赖。
Storage 不属于 AgentSpec V1 Slot，其运行角色选择留给 Deployment；`storage.session`
仅为示例。V1 没有 Environment 实体、管理 API、environment_id、default、Overlay 或继承。
不同运行配置使用不同 Profile；Profile 不拥有平台网络、执行后端或资源上限授权。

## 3. Canonical 内容边界

Canonical Spec 只存普通资源字段和内部关联，不存明文、密文、Credential Revision、
active/cleared、configured、Association Token、用户动作或请求 Idempotency-Key。
也不存 Agent Prompt/节点、AgentVersion、DeploymentRevision、Run/Worker 状态或框架任意 Option。
已发布 Revision/Manifest 冻结配置与关联身份，不冻结 live 值。

Write-only API Key、Token、DSN 的允许输入位置由独立 DTO 限制；这不放宽 Canonical
层的递归秘密字段检测。普通字段不接收任意 headers、query、options、config Blob。

## 4. 内部文档模型

~~~json
{
  "schema_version": "v1",
  "credential_protocol_version": "v1",
  "models": {},
  "tools": {},
  "knowledge": {},
  "storage": {}
}
~~~

发布时六个顶层字段都必填，四类集合可为空。允许只包含部分类别的 Profile，这只证明
文档自身合法，不证明可部署。所有资源与嵌套对象都是关闭、类型化的判别联合。
初始 Draft 的内部 `{}` 可以保存，公开初始读投影为空 config 和两个 V1 版本字段。

## 5. Write DTO 与动作

PUT draft 必须带 Idempotency-Key，顶层字段为：

| 字段 | 约束 |
| --- | --- |
| expected_draft_revision | 正整数，Draft CAS |
| credential_protocol_version | 固定 v1 |
| config | models/tools/knowledge/storage 四个集合显式出现，全量替换普通配置 |
| credentials | 按 category/resource/purpose 定位的动作；省略某用途表示 keep |

config 使用各资源非秘密字段；不含下面 Canonical 字段表的内部 ID。凭据用途为
Model `api_key`、MCP `bearer_token`、Knowledge `qdrant_api_key`/`embedding_api_key`、
Storage `dsn`，不能用任意目的名建立关联。

- keep/clear 不带 value；Draft expected_credential_revision 只能为 0/省略。
- replace 必须带非空真实 value；Draft replace 总是新 ID，初始 Credential Revision=1。
- 整个输入最大 512 KiB；单 value 最大 64 KiB。
- 拒绝 null、重复/未知/大小写变体字段、非法 UTF-8、尾随 JSON、非法动作与用途。
- value 拒绝空白、NUL/CR/LF、掩码、redacted/unchanged 标记和文档占位。
- 目的范围变化必须重新录入并 COW，keep 不会向新 Endpoint 转移旧凭据。

live POST 是独立 DTO，目标包含发布编号、类别、资源名、purpose_field、Association Token；
动作只为 replace/clear，携带正数 expected_credential_revision 和 Idempotency-Key。

## 6. 内部标识与用途

Resource Key 格式：`^[a-z][a-z0-9_-]{0,63}$`，只在类别内唯一。内部 CredentialID 格式：
`^crd_[0-9a-f]{32}$`，由服务端生成，不作为公开表单字段。

一个 CredentialID 固定 Tenant、Profile、类别、资源名、Purpose 与 audience_digest。
Audience 仅摘要非秘密目标，不摘要凭据值：

| 用途 | 当前目标指纹输入 |
| --- | --- |
| Model api_key | kind + base_url |
| MCP bearer_token | kind + server_url + auth.kind |
| Qdrant qdrant_api_key | kind + host + port + tls |
| Embedding embedding_api_key | kind + embedding.base_url |
| Storage dsn | kind + 完整固定 destination |

相同值、名称或 Tenant 不构成跨 Profile 凭据共享授权。当前 Kind 没有跨 Resource 引用，
不预建任意深度依赖图、Cycle Detector 或通用 ResourceRef。

## 7. 凭据与不可变性

内部 ID 出现在确定 Kind 的固定位置；值只在 Profile 私有表加密保存。
普通 Draft replace 创建新 ID；clear 只解除 Draft 关联，不撤销旧 Revision 使用的 ID。
显式 live replace 仅在用途/目标相同且独立 CAS 成功时更新同 ID 当前值，不改 Spec/Digest。
live clear 终止 ID，后续取值失败；恢复使用需新 ID 和新发布。

公开详情的 credential_states 是当前投影，不进入 Canonical。它可以包含 configured、
status、credential_revision、association_token，但不包含值、密文或内部 ID。
Publish 响应不带这些动态状态；Summary 列表也不加载它们。

## 8. 当前关闭 Kind 与 Capability

| 类别 | Kind | Capability |
| --- | --- | --- |
| Model | openai_compatible | chat/tool_call 非空子集，必须含 chat，最多 2 项且不重复 |
| Tool | mcp_streamable_http | 固定 web.search |
| Knowledge | qdrant_openai | 类型推导 knowledge.search，输入不接收 capability 字段 |
| Storage | postgres_state | 类型推导 storage.session + storage.memory |

没有任意自由 Capability、全局 Registry、Alias、继承或语义搜索。框架内建工具与
工作区执行仍是后续扩展：AgentSpec 声明需求、节点 tool_slots 选择、Profile 类型化配置、
Worker 按 Manifest/节点最小装配。实现标识不是 Go import 路径或任意 SDK Option。
工作区执行可复用 Worker 内 tRPC CodeExecutor，不要求独立 Sandbox Manager。
本轮不新增工具 Kind、Capability 或执行字段。

## 9. Model Canonical：openai_compatible

| 字段 | 类型 | 发布约束 |
| --- | --- | --- |
| kind | string | openai_compatible |
| model | string | 1～256，Provider 模型名 |
| base_url | string | 1～2048，HTTP/HTTPS、非空 Host，无 UserInfo/Query/Fragment |
| api_key_credential_id | internal CredentialID | 必填，服务器关联 |
| capabilities | string[] | chat/tool_call Allowlist，必须 chat |

Write/Public config 不含 api_key_credential_id。Model 不包含 Agent Generation、Prompt、
任意 Header 或框架 Option。静态校验不证明模型在线或 Worker 已接入对应 Adapter。

## 10. Tool Canonical：mcp_streamable_http

| 字段 | 类型 | 发布约束 |
| --- | --- | --- |
| kind | string | mcp_streamable_http |
| server_url | string | 同上 HTTP URL 规则 |
| toolset_name | string | `^[a-z][a-z0-9_]{0,63}$` |
| tool_name | string | 同上名称规则；例如 search_web |
| auth | object | `{kind:none}` 或 `{kind:bearer, credential_id:...}` |
| capability | string | 固定 web.search |

Bearer 的 credential_id 是内部值；Write/Public config 只提供 auth.kind。
none 分支没有不活跃的凭据字段。不存在平台工具默认全量授权。

## 11. Knowledge Canonical：qdrant_openai

| 字段 | 类型 | 发布约束 |
| --- | --- | --- |
| kind | string | qdrant_openai |
| host | string | 1～253 |
| port | integer | 1～65535 |
| tls | boolean | 显式提供 |
| collection | string | 1～128 |
| qdrant_api_key_credential_id | internal CredentialID | 可选 |
| embedding | object | 关闭的嵌套对象 |

Embedding 必填 model（1～256）、base_url（受限 HTTP URL）、api_key_credential_id
（内部 ID）与 dimensions（1～65536）。Write/Public config 不包含两个内部凭据字段。
Profile 不管理文档摄取、切块、索引或在线连通性。

## 12. Storage Canonical：postgres_state

| 字段 | 类型 | 发布约束 |
| --- | --- | --- |
| kind | string | postgres_state |
| dsn_credential_id | internal CredentialID | 必填；关联加密 password，而非完整 URI |
| destination.host | string | 1～253 |
| destination.port | integer | 1～65535 |
| destination.database | string | 1～128 |
| destination.username | string | 1～128 |
| destination.sslmode | string | disable / require / verify-full，显式提供 |

Write 的 credentials.storage.<name>.dsn 接收 PostgreSQL URI，服务端解析出 destination
并仅加密 password。只接受 postgres/postgresql，username、password、database 和 sslmode
均必需；port 省略时明确规范化为 5432。拒绝 keyword DSN、多 host、Unix socket、fragment、
额外/重复 query options 和隐式 TLS 策略。config 若同时提供 destination，必须与解析结果一致。
公开 config 包含非秘密 destination；live replace 必须保持完整 destination 不变。
本轮没有 Artifact Storage Kind，也不增加驱动 options。

## 13. 文档上限与默认行为

- 内部 Spec 最大 512 KiB；models/tools/knowledge/storage 最多分别 16/64/32/16 项。
- 所有发布集合必须显式出现，可以为空；Draft 可以语义不完整。
- Resource/内部凭据 ID/远程工具名采用各自固定语法，不互换含义。
- 不注入默认 Model、Provider、Endpoint、Capability、工具或认证配置。
- Storage URI 缺省端口规范化为 5432 是明确的 DSN 解析规则，不是环境 Overlay。

## 14. 拒绝开放配置与越界字段

当前 Schema/Go Validator 拒绝未知字段。config/Spec 不接受旧引用字段、任意 sdk_options、
headers、query、回调、Go import、Tenant 可执行代码或开放式 Map。
凭据明文只能位于 write-only value；密文只能位于 Profile 私有凭据记录。
HTTP、Application、SQL tracing、日志、诊断和事件都不保存秘密正文。

## 15. 可修改性

| 数据 | Draft / live 操作 | 发布后 |
| --- | --- | --- |
| 普通配置和 CredentialID 关联 | Draft CAS + COW | 原 Revision 不变，发布新 Revision |
| Credential 当前值/状态 | OWNER + live CAS + 用途验证 | 可变，但不改已发布内容或 Digest |
| Profile 名称/描述 | 元数据操作 | 可修改，不进入 Spec Digest |
| CredentialStates | 读取投影 | 当前状态，不是不可变快照字段 |
| Revision Spec/Digest/发布审计 | 服务端发布 | 不更新、不删除 |

## 16. 校验与消费分层

1. Write 输入解码：512 KiB 总大小、64 KiB value、关闭字段、动作、用途和值规则。
2. Draft L0：内部 JSON 可存储、重复 Key/编码/秘密字段检测，允许语义不完整。
3. 发布 L1/L2：当前 Canonical V1 结构、Kind、内部 ID、Capability 与类型化语义。
4. Canonicalization/Digest：确定性规范化与 JCS/SHA-256，不请求外部服务。
5. CheckUsable：有界完整 Revision 读取与关联/active 检查，不解密、不调用 Provider。
6. ResolveForAttempt：可信授权验证后有界批量检查与解密，不接受调用方自述授权。

Profile 静态发布不检查 live 状态或远端真实有效性。Deployment 仍需单独完成资源匹配、
平台策略与 Manifest 兼容性检查。消费方法、测试与可选 runtimehttp Adapter 已存在；
内部路由只在工作负载认证与 ExecutionAuthorizationVerifier 成对注入时注册。默认
bootstrap 不注册，真实执行拥有方和 Worker 批次验收尚未接线；缺少 verifier 直接拒绝。

## 17. Validation Report

所有可预期的文档问题都返回结构化诊断，不把裸 Go Error 文本作为 API 协议。

~~~json
{
  "valid": false,
  "schema_version": "v1",
  "draft_revision": 4,
  "diagnostics": [
    {
      "code": "RUNTIME_PROFILE_SPEC_SENSITIVE_FIELD",
      "severity": "error",
      "pointer": "/models/primary/api_key",
      "resource_kind": "model",
      "resource_key": "primary",
      "message": "RuntimeProfileSpec 不能包含明文凭据字段。"
    }
  ]
}
~~~

字段：

| 字段 | 说明 |
| --- | --- |
| valid | 没有 Error 时为 true；Warning 不影响 |
| schema_version | 可识别的请求 Schema Version；缺失或无法识别时返回空字符串 |
| draft_revision | 本次验证对应的 Draft Revision |
| diagnostics | 排序稳定的 Error 与 Warning |
| code | 稳定机器码，不随中文文案变化 |
| severity | error 或 warning |
| pointer | RFC 6901 JSON Pointer |
| resource_kind | model、tool、knowledge、storage 或 null |
| resource_key | 问题属于具体 Resource 时为 Key，否则为 null |
| message | 面向用户的安全说明，不作为客户端逻辑判断依据 |

所有字段都必须出现在响应中；resource_kind 与 resource_key 是 required + nullable。
当前 OpenAPI 中 schema_version 是普通 string，不限定为 enum: [v1]，因此缺少或非法
版本的诊断响应仍符合响应协议。

诊断按 pointer、severity、code、resource_kind、resource_key 排序。同一输入和同一
Validator Version 必须返回相同顺序。每个输入问题只能由一个校验层拥有；同一 JSON
Pointer 不得因 Schema 与 Domain 重复检查而返回两条等价诊断。

## 18. 稳定诊断码

### 18.1 Draft 内容安全

| Code | Severity | 含义 |
| --- | --- | --- |
| RUNTIME_PROFILE_SPEC_INVALID_JSON | error | 不是合法 JSON |
| RUNTIME_PROFILE_SPEC_DUPLICATE_KEY | error | JSON Object 包含重复 Key |
| RUNTIME_PROFILE_SPEC_DOCUMENT_REQUIRED | error | 顶层不是 Object |
| RUNTIME_PROFILE_SPEC_DOCUMENT_TOO_LARGE | error | 超过文档大小限制 |
| RUNTIME_PROFILE_SPEC_SENSITIVE_FIELD | error | 出现禁止的 Credential 字段 |

### 18.2 Schema

| Code | Severity | 含义 |
| --- | --- | --- |
| RUNTIME_PROFILE_SPEC_UNSUPPORTED_VERSION | error | Schema Version 不受支持 |
| RUNTIME_PROFILE_SPEC_REQUIRED_FIELD | error | 缺少必填字段 |
| RUNTIME_PROFILE_SPEC_UNKNOWN_FIELD | error | 出现 V1 未定义或越界的字段 |
| RUNTIME_PROFILE_SPEC_INVALID_TYPE | error | 字段类型错误 |
| RUNTIME_PROFILE_SPEC_INVALID_IDENTIFIER | error | Resource Key 或 Tool 名称非法 |
| RUNTIME_PROFILE_SPEC_DUPLICATE_CAPABILITY | error | Capability 重复 |
| RUNTIME_PROFILE_SPEC_CREDENTIAL_ID_INVALID | error | 内部 CredentialID 格式非法 |
| RUNTIME_PROFILE_SPEC_LIMIT_EXCEEDED | error | 长度、数量或数值超过限制 |
| RUNTIME_PROFILE_SPEC_UNSUPPORTED_KIND | error | Resource Kind 不受支持 |
| RUNTIME_PROFILE_SPEC_INVALID_URL | error | URL 非法、非 HTTP(S)、缺少 Host，或包含 UserInfo、Query、Fragment |
| RUNTIME_PROFILE_SPEC_INVALID_VALUE | error | 关闭字段值不在允许范围，例如 sslmode |

additionalProperties=false 发现的普通未知字段和 agent_id、environment 等已知越界
字段都统一映射为 RUNTIME_PROFILE_SPEC_UNKNOWN_FIELD，同一 Pointer 不再额外返回
“forbidden boundary”诊断。内部 CredentialID 只有出现在 Kind 允许位置且值格式非法时才映射为
RUNTIME_PROFILE_SPEC_CREDENTIAL_ID_INVALID；出现在不允许位置时仍是 UNKNOWN_FIELD。

### 18.3 领域语义

| Code | Severity | 含义 |
| --- | --- | --- |
| RUNTIME_PROFILE_SPEC_CAPABILITY_KIND_MISMATCH | error | Capability 不在 Kind 的固定集合或 Allowlist |

若以后某个具体 Kind 引入类型化资源引用，应随该 Kind 的 Schema 增加职责明确的稳定
诊断，而不是预先建立通用 REF_NOT_FOUND、REF_CYCLE 或 UNUSED_RESOURCE。

## 19. Canonicalization 与 Digest

内部完整 Spec 经 L0/L1/L2、typed decode、类型化规范化、RFC 8785 JCS 和 SHA-256。
Model Capability 集合确定排序；对象 Key 按 JCS；普通数组不随意重排；Resource Key、
Endpoint 等值不静默 trim/lowercase（受限 DSN 的明确规范化规则除外）。

Digest 包含 schema_version、credential_protocol_version、非秘密配置与内部 CredentialID；
不包含值、密文、live 状态、Credential Revision、Association Token、元数据或时间戳。
相同 Spec 可以从不同 Source Draft Revision 发布，Digest 不设业务唯一约束。

任何完整 Revision 读取、发布幂等返回、CheckUsable、Resolve 和公开脱敏前均先验证
内部 Canonical 内容与保存的 Schema Version/Digest。公开 Read DTO 不作为 Digest 输入。
Summary 仅返回元数据，不加载 spec_jsonb 或凭据状态，也不重新执行整份校验。

## 20. 发布与 Read

Validate/Publish 均使用 expected_revision。发布先按 Source Draft Revision 查询已发布
记录，再检查可变 Draft；首次返回 201，延迟幂等重试返回同一 Revision 和 200。
发布无动态 credential_states，不刷新 live 状态；可变详情状态由 GET 独立投影。
发布后 Draft 可继续编辑，旧 Revision 不变。公开完整 Read 不返回内部 spec 或 ID。

## 21. Schema 与 Fixtures

`runtime-profile-spec.schema.json`、embed.go、有效/无效 fixtures 与 Go Domain 类型共同
表达内部 Canonical V1。fixtures 中 CredentialID 是内部格式测试值，不是公开 PUT 样例；
fixture 通过静态发布校验不证明该 ID 在数据库存在或可运行。
公开写入/读取 DTO 以 OpenAPI 和 HTTP/Application 测试为准。当前仅维护单一协议，
不接收旧引用式输入或保存开发期 Digest/Reader 兼容分支。

## 22. 当前实现与验证范围

本轮已更新 Schema、Go 类型、Canonicalization、DTO、HTTP Handler、Profile 私有表与
加密 Adapter；已有对应测试代码。实际测试通过范围以本轮验证输出为准，不将消费
Application Port 测试等同为真实 Deployment/Worker 端到端完成。

验收应覆盖：公开 DTO 无值/密文/内部 ID、非法输入、COW/live/CAS/幂等、失败回滚、
AES-GCM AAD 与篡改拒绝、完整读损坏检测、Summary 不读 Spec、真实 PostgreSQL 事务。

## 23. 开发期单一协议与发布不变量

当前 `/v1`、schema_version=v1 与 credential_protocol_version=v1 已直接调整。
不另开版本，不保留旧 Reader/Validator/Digest/Receipt、迁移补录或历史密文。
实际新表直接进入开发基线，不要求开发期旧记录兼容。

目标协议内正常发布的 Revision/Manifest 仍不可变；配置或关联变化通过 Draft/COW
和新发布完成，live 仅更改同 ID 当前值/状态。关闭字段与工具枚举继续保留。
真正稳定发布后的演进策略另行设计，不作为当前开发调整门槛。

## 24. 后续待完成事项

- Deployment 同名匹配、Manifest 字段/闭包、Storage 运行角色选择与 Checker 调用。
- 真实 Run/Attempt 拥有方授权查询、lease/epoch、工作负载认证与 Worker 内部 HTTP 接线。
- Worker 批次验收、一次初始化、迟到响应与新 Attempt 有界重试；当前没有默认许可。
- Provider/工具/Knowledge/Storage 的显式在线诊断，不进入 Profile 静态发布。
- 平台内建工具与工作区执行的新类型化协议；不先加入自由 Capability 或 SDK Blob。
