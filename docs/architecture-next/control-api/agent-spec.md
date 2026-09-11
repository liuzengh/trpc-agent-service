# AgentSpec V1 Schema 与 Validation 设计

- **状态**：已接受并实现，由 JSON Schema、Go 校验器和测试共同固化
- **所属子领域**：Control API / Agent
- **协议位置**：`api/schemas/agentspec/v1/`
- **适用对象**：AgentDraft 与不可变 AgentVersion 中的 AgentSpec 文档
- **不适用对象**：RuntimeProfileSpec、ProfileDraft、ProfileRevision、
  DeploymentRevision、RuntimeManifest、Worker 运行实例与编辑器视图状态

## 1. 目的

AgentSpec 是 `agent` 子领域拥有的声明式业务协议，用于描述一个 Agent 的逻辑行为、
节点组合关系，以及运行时需要满足的逻辑资源槽位。AgentSpec 不包含具体运行环境、
Provider 凭证、Secret、模型连接参数或 Worker 配置。

AgentSpec V1 必须同时满足以下目标：

1. 一份合法 AgentSpec 可以被确定性规范化并计算稳定 Digest。
2. Draft 可以暂时不完整，但只有通过发布校验的 Canonical AgentSpec 才能进入
   AgentVersion。
3. Control API 可以在不构造实际 tRPC-Agent-Go 对象的情况下完成环境无关校验。
4. ProfileRevision 可以提供具体资源，Deployment 可在不修改 AgentVersion 的情况下
   按资源类别和名称精确匹配到同名逻辑 Slot，无需用户提供额外映射表。
5. 未知字段、框架专属 Option 和任意可执行表达式不能偷偷进入协议。
6. 编辑器布局变化不能改变 AgentSpec 的运行语义或 Digest。

## 2. V1 非目标

AgentSpec V1 不定义：

- Graph、Edge、Reducer、Join、Condition 或任意表达式语言。
- Tenant 上传的 Go 代码、插件、回调函数或节点模板。
- 具体模型 Provider、Base URL、API Key、Token、Password 或其他 Credential。
- ProfileDraft、ProfileRevision、DeploymentRevision 或 RuntimeManifest。
- Worker 并发、超时、队列 Consumer、资源限制和执行状态。
- 节点坐标、视口、选择、折叠和面板状态等 Editor State。
- 多 Draft 分支、合并、审批或实时协同编辑。
- 框架内部所有 tRPC-Agent-Go Option 的 JSON 映射。

## 3. 文档模型

V1 使用“单个 Root + 扁平节点表”的结构：

```text
AgentSpec
├── schema_version
├── root ───────────────┐
├── requirements        │
│   ├── models          │
│   ├── tools           │
│   └── knowledge       │
└── nodes               │
    ├── main ◀──────────┘
    ├── researcher
    └── writer
```

V1 的节点引用在结构上必须形成一棵单父节点树。`loop` 描述运行时重复，不允许通过
节点引用在定义结构中形成环。

完整示例：

```json
{
  "schema_version": "v1",
  "root": "main",
  "requirements": {
    "models": {
      "primary": {
        "capabilities": ["chat", "tool_call"]
      }
    },
    "tools": {
      "search": {
        "capability": "web.search"
      }
    },
    "knowledge": {
      "docs": {
        "capability": "knowledge.search"
      }
    }
  },
  "nodes": {
    "main": {
      "kind": "sequence",
      "name": "研究与回答",
      "children": ["researcher", "writer"]
    },
    "researcher": {
      "kind": "llm",
      "name": "资料检索",
      "instruction": "检索并整理与用户问题相关的信息。",
      "model_slot": "primary",
      "tool_slots": ["search"],
      "knowledge_slots": ["docs"],
      "generation": {
        "temperature": 0.2,
        "max_output_tokens": 4096
      }
    },
    "writer": {
      "kind": "llm",
      "name": "结果整理",
      "instruction": "根据已有信息生成结构清晰、可核验的回答。",
      "model_slot": "primary",
      "tool_slots": [],
      "knowledge_slots": []
    }
  }
}
```

## 4. 顶层 Schema

AgentSpec V1 的逻辑 JSON Schema 为：

```text
AgentSpec
├── schema_version  required, const "v1"
├── root            required, NodeID
├── requirements    required, Requirements
└── nodes           required, map<NodeID, Node>
```

所有对象均使用 `additionalProperties: false`。字段变化必须同步 Schema、Validator 与 Fixture，
不能依赖任意扩展字段；当前开发期可直接调整 V1，稳定发布后的演进规则见第 18 节。

### 4.1 `schema_version`

```json
{
  "schema_version": "v1"
}
```

- 必填。
- V1 只能取字面量 `v1`。
- 客户端需要随文档提交，但服务端决定是否支持该版本。
- 不能通过修改该字符串绕过校验或宣称 Runtime 兼容。

### 4.2 `root`

- 必填。
- 必须是 `nodes` 中存在的 NodeID。
- Root 不能同时作为其他节点的 Child 或 Body。

### 4.3 `requirements`

`requirements` 声明 AgentSpec 使用的逻辑资源槽位，不提供具体运行配置。

```json
{
  "requirements": {
    "models": {},
    "tools": {},
    "knowledge": {}
  }
}
```

三个集合均必填，可以为空对象。这样可以避免 `null`、字段缺失和空对象具有不同
语义。

### 4.4 `nodes`

- 必填且至少包含一个节点。
- Key 是稳定 NodeID，不使用数组下标作为身份。
- Value 必须是 V1 支持节点类型的判别联合。
- 节点数、深度和文本长度受平台上限约束。

## 5. 标识符规则

NodeID 和 Slot Name 使用相同的基础格式：

```regex
^[a-z][a-z0-9_-]{0,63}$
```

示例：

```text
main
researcher
support_agent
writer-v2
```

非法示例：

```text
Main                 # 大写字母
1st-agent            # 不能以数字开头
agent/researcher     # 不允许斜杠
agent researcher     # 不允许空格
```

标识符区分大小写，但 V1 仅允许小写，从输入层消除大小写歧义。

## 6. Requirements Schema

### 6.1 Model Requirement

```json
{
  "models": {
    "primary": {
      "capabilities": ["chat", "tool_call"]
    }
  }
}
```

字段：

| 字段 | 类型 | 必填 | 约束 |
| --- | --- | --- | --- |
| `capabilities` | `string[]` | 是 | 至少一个、不得重复、满足 Capability 语法 |

AgentSpec 不包含 Provider、实际模型名、Endpoint 或 Credential。Deployment 发布时将
`requirements.models.primary` 精确匹配到所选 ProfileRevision 的
`models.primary`，再验证 Capability 是否满足。匹配不跨类别、不模糊匹配，也不按
Capability 搜索其他候选资源。

### 6.2 Tool Requirement

```json
{
  "tools": {
    "search": {
      "capability": "web.search"
    }
  }
}
```

字段：

| 字段 | 类型 | 必填 | 约束 |
| --- | --- | --- | --- |
| `capability` | `string` | 是 | 必须满足 Capability 语法 |

AgentSpec 不记录 Tool Server URL、认证头、Token 或进程启动参数。

### 6.3 Knowledge Requirement

```json
{
  "knowledge": {
    "docs": {
      "capability": "knowledge.search"
    }
  }
}
```

AgentSpec 只声明逻辑能力；具体 Knowledge Resource、索引配置、Storage 依赖及内部
凭据关联由 ProfileRevision 提供。Deployment 按类别和名称精确匹配 Knowledge
Resource，再固定必要依赖。Storage 不属于 AgentSpec Slot，其 session/memory
运行角色选择由 Deployment 单独定义，不通过通用工具映射引入。Profile 拥有凭据
输入与加密存储，AgentSpec 不包含 SecretRef、CredentialID 或凭据值。

## 7. Node 判别联合

V1 支持四种节点：

```text
llm
sequence
parallel
loop
```

`kind` 决定允许出现的其余字段。节点不能携带万能 `config`，也不能出现
`[key: string]: unknown` 一类任意扩展字段。

公共可选字段：

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| `name` | `string` | 1 到 128 个 Unicode Code Point，仅用于节点可读名称 |

NodeID 才是稳定引用身份，`name` 可以修改且不承担引用作用。

### 7.1 LLM Node

```json
{
  "kind": "llm",
  "name": "客服",
  "instruction": "准确回答订单相关问题。",
  "model_slot": "primary",
  "tool_slots": ["order_lookup"],
  "knowledge_slots": ["faq"],
  "generation": {
    "temperature": 0.2,
    "max_output_tokens": 4096
  }
}
```

字段：

| 字段 | 类型 | 必填 | 发布约束 |
| --- | --- | --- | --- |
| `kind` | `string` | 是 | 必须为 `llm` |
| `name` | `string` | 否 | 非空且不超过 128 个 Code Point |
| `instruction` | `string` | 是 | Trim 后非空，不超过 65536 个 UTF-8 字节 |
| `model_slot` | `string` | 是 | 必须引用 `requirements.models` 中的 Slot |
| `tool_slots` | `string[]` | 是 | 不得重复，全部引用已声明 Tool Slot |
| `knowledge_slots` | `string[]` | 是 | 不得重复，全部引用已声明 Knowledge Slot |
| `generation` | `object` | 否 | 只允许 V1 明确列出的行为参数 |

`generation` V1 字段：

| 字段 | 类型 | 范围 | 说明 |
| --- | --- | --- | --- |
| `temperature` | `number` | `0 <= x <= 2` | Agent 行为参数，进入 AgentVersion |
| `max_output_tokens` | `integer` | `1 <= x <= 262144` | 平台上限；具体 Model 上限在 Deployment 校验 |

模型不支持某个 generation 参数属于 Deployment 兼容性错误，不属于 Draft 保存错误。

### 7.2 Sequence Node

```json
{
  "kind": "sequence",
  "name": "处理流程",
  "children": ["classify", "answer"]
}
```

- `children` 必填，至少一个，最多 64 个。
- Child 不得重复。
- 数组顺序属于运行语义，规范化和 Digest 计算必须保留顺序。
- V1 只承诺按顺序调用子 Agent，不额外定义任意 JSON 输出到下一节点输入的映射语言。

### 7.3 Parallel Node

```json
{
  "kind": "parallel",
  "name": "并行检查",
  "children": ["security", "quality"]
}
```

- `children` 必填，至少一个，最多 64 个。
- Child 不得重复。
- 所有 Child 接收同一次调用上下文。
- V1 不定义自定义 Reducer、Join 或结构化聚合表达式。
- 需要综合多个结果时，应在外层 `sequence` 中显式增加后续 LLM 节点。
- `children` 顺序仍进入 Digest，并作为诊断和事件展示的稳定顺序。

### 7.4 Loop Node

```json
{
  "kind": "loop",
  "name": "审查迭代",
  "body": "review-and-revise",
  "max_iterations": 3
}
```

- `body` 必填，必须引用一个存在的节点。
- `body` 在结构树中是 Loop 的唯一直接子节点。
- `max_iterations` 必填，范围为 1 到 32。
- V1 不接受 `stop_policy_ref`、`condition_ref` 或任意表达式。
- `max_iterations` 是硬上限；是否在标准 Agent 完成信号后提前终止，由固定的
  Worker 执行语义定义，不允许每份 AgentSpec 自定义。
- 多步骤 Body 必须显式建模为 `sequence`，而不是让 Loop 同时拥有多个 Child。

## 8. 明确禁止的字段

V1 AgentSpec 不接受下列旧原型字段：

```text
runtime_compatibility
edges
editor_metadata
config
model_ref
tool_refs
knowledge_ref
children              # 在 llm 和 loop 节点上禁止
stop_policy_ref
state_ref
reducer_ref
join_ref
condition_ref
node_template_ref
mapping
```

替代关系：

| 旧字段 | V1 处理 |
| --- | --- |
| `model_ref` | 改为逻辑 `model_slot` |
| `tool_refs` | 改为逻辑 `tool_slots` |
| `knowledge_ref` | 改为逻辑 `knowledge_slots` |
| `chain` | 改名为 `sequence` |
| `cycle` | 收敛为受限 `loop` |
| `graph`、`edges` | V1 不支持 |
| `config.name` | 改为节点显式字段 `name` |
| `config.instruction` | 改为 LLM 显式字段 `instruction` |
| `editor_metadata` | 移入独立 Editor State |
| `runtime_compatibility` | 由服务端和 RuntimeManifest 管理 |

## 9. 可修改性边界

| 数据 | Draft 中可修改 | 发布后可修改 | 是否进入 Spec Digest |
| --- | --- | --- | --- |
| `root` | 是 | 否 | 是 |
| `requirements` | 是 | 否 | 是 |
| `nodes` 与节点字段 | 是 | 否 | 是 |
| Agent `name`、`description` | 通过 Agent 元数据用例修改 | 可以 | 否 |
| Editor State | 通过独立用例修改 | 不适用 | 否 |
| `schema_version` | 只能选择服务端支持版本 | 否 | 是 |
| Draft Revision | 否，服务端递增 | 不适用 | 否 |
| Version Number | 否，服务端分配 | 否 | 否 |
| Spec Digest | 否，服务端计算 | 否 | 结果本身 |
| Deployment 资源解析结果 | 不属于 AgentSpec | 通过新 DeploymentRevision 固定 | 否 |

修改已发布 AgentVersion 的正确流程是：读取旧 Version 的 Spec，保存为新的 Draft
Revision，完成修改后发布新的 AgentVersion。禁止更新旧 Version 的 `spec_jsonb`。

## 10. Validation 分层

### 10.1 L0：传输与 Draft 保存校验

`SaveDraft` 在持久化前执行，目的是安全保存和并发保护，不要求 Draft 已经可以发布。

检查：

1. 请求体可以按 UTF-8 JSON 解析，顶层值必须是对象。
2. JSON Object 中不允许重复 Key。
3. 原始 AgentSpec 文档不得超过 524288 字节。
4. 必须携带 Expected Draft Revision。
5. Expected Revision 必须等于数据库当前 Revision。
6. 对象 Key 不得使用已知 Credential 字段，如 `api_key`、`password`、`token`、
   `authorization`、`credential`、`client_secret`。
7. HTTP、应用日志和审计日志不得记录完整 AgentSpec 请求体。

L0 成功只代表文档可以保存。缺少 Root、节点不完整或 Slot 未声明的 Draft 仍可保存。
对于 instruction 等自由文本，系统不能可靠判断任意字符串是否恰好包含 Secret；
Schema 通过不给 Credential 提供合法字段来收紧边界，界面和日志仍必须避免诱导或
传播明文 Secret。

### 10.2 L1：JSON Schema 校验

`ValidateDraft` 和 `PublishAgentVersion` 都执行 L1：

- `schema_version` 必须受支持。
- 必填字段存在且类型正确。
- 所有对象 `additionalProperties: false`。
- 字符串、数组、数值和集合大小满足限制。
- `kind` 必须匹配相应 Node Schema。
- 节点不能携带其他类型专属字段。
- Identifier 符合统一 Pattern。

Draft 保存不要求通过 L1；发布必须通过。

### 10.3 L2：Agent 领域语义校验

L2 不访问 RuntimeProfile、Deployment 或 Worker：

1. Root 必须存在。
2. Root 不能被其他节点引用为 Child 或 Body。
3. 每个非 Root 节点必须且只能有一个父节点。
4. 所有节点必须从 Root 可达。
5. 节点引用结构不能形成环。
6. Sequence 和 Parallel 的 Child 必须存在且不得重复。
7. Loop Body 必须存在且不能引用自身。
8. LLM 引用的 Model、Tool、Knowledge Slot 必须已声明。
9. 所有已声明但未使用的 Slot 产生 Warning。
10. 节点总数、最大结构深度和直接子节点数不得超过平台限制。

V1 平台限制：

| 项目 | 上限 |
| --- | ---: |
| AgentSpec 原始 JSON | 524288 字节 |
| Node 数量 | 128 |
| 结构深度 | 16 |
| 单个组合节点直接 Child | 64 |
| Model Slot | 16 |
| Tool Slot | 64 |
| Knowledge Slot | 32 |
| Loop `max_iterations` | 32 |
| 单个 Instruction | 65536 UTF-8 字节 |

### 10.4 L3：Deployment 兼容性校验

L3 不属于 Agent 发布校验，由 Deployment 发布执行：

- AgentVersion 的全部 Model/Tool/Knowledge Slot 是否在所选 ProfileRevision 中具有
  同类别同名 Resource；不存在时返回明确诊断，V1 无用户映射表。
- 同名 Model 是否满足声明的 Capability。
- 同名 Tool 和 Knowledge Capability 是否满足需求。
- 最小资源闭包的内部凭据关联是否经 `ProfileCredentialChecker.CheckUsable` 验证属于
  所选 Tenant/Profile/Revision、用途与固定目的范围匹配且当前可用；检查只读元数据，
  不解密或请求 Provider。Profile 的该 Application Port 已实现，Deployment 调用仍待
  本切片接入。V1 不要求 Environment。
- Worker 新 Attempt 通过 `RuntimeCredentialResolver.ResolveForAttempt`，在当前
  Run/Attempt 拥有方授权通过后批量取值；不能把发布时检查复用为永久许可，执行中只
  复用已校验的固定凭据集合。
- Generation 参数是否被具体 Model 支持。
- Deployment Compiler 是否支持所选 AgentSpec/RuntimeProfileSpec Schema Version，
  并能把实际使用的 Resource Kind 编译为 RuntimeManifest Adapter Kind/Version。
- 平台配置提供的固定 PlatformExecutionContract 是否支持生成的 RuntimeManifest
  Schema Version、Adapter Kind/Version、执行范围与资源上限；不实时枚举 Worker
  或把 Provider 在线探测作为发布必需步骤。Worker 不直接消费 AgentSpec 或
  RuntimeProfileSpec。
- 是否可以生成只含实际资源和必要依赖、保留节点工具分配且不可变的 RuntimeManifest。

V1 不引入 Environment 业务对象；自动匹配只发生在发布时，Worker 不跟随最新
ProfileRevision，也不在执行时重新猜测绑定。已解析关系属于 Manifest 编译产物。
Worker Adapter 对 Executor、Skills 等扩展派生的工具和入口也必须显式控制，避免
绕过节点工具分配。当前 Tool 配置协议只有 MCP web.search，内建和命令工具的
具体资源协议属于后续扩展。Profile 直接录入与内部加密凭据已由当前 V1 实现，不增加开发期历史兼容；
本节不修改 AgentSpec V1 字段或 JSON 示例。

因此：

```text
AgentVersion 发布成功
≠
它可以搭配任意 ProfileRevision 部署
```

Runtime Profile 的资源与校验边界见
[`runtime-profile-spec.md`](runtime-profile-spec.md)，Deployment 设计见
[`deployment.md`](deployment.md)。

## 11. Validation Report

所有可预期的文档问题都返回结构化诊断，不使用裸 Go Error 文本作为 API 协议。

```json
{
  "valid": false,
  "schema_version": "v1",
  "draft_revision": 7,
  "diagnostics": [
    {
      "code": "AGENT_SPEC_ROOT_NOT_FOUND",
      "severity": "error",
      "pointer": "/root",
      "node_id": null,
      "message": "根节点 main 不存在。"
    },
    {
      "code": "AGENT_SPEC_UNUSED_TOOL_SLOT",
      "severity": "warning",
      "pointer": "/requirements/tools/search",
      "node_id": null,
      "message": "Tool Slot search 已声明但未被任何节点引用。"
    }
  ]
}
```

字段：

| 字段 | 说明 |
| --- | --- |
| `valid` | 没有 Error 时为 `true`；Warning 不影响该值 |
| `schema_version` | 服务端实际验证的 Schema 版本 |
| `draft_revision` | 本次验证对应的 Draft Revision |
| `diagnostics` | 排序稳定的 Error 与 Warning 列表 |
| `code` | 稳定机器码，不随中文文案变化 |
| `severity` | `error` 或 `warning` |
| `pointer` | RFC 6901 JSON Pointer |
| `node_id` | 问题属于具体节点时返回，否则为 `null` |
| `message` | 面向用户的说明，不作为客户端逻辑判断依据 |

诊断排序必须确定：先按 `pointer`，再按 `severity`，最后按 `code`。同一输入和同一
Validator 版本必须得到顺序一致的诊断列表。

## 12. V1 诊断码

### 12.1 Draft 保存

| Code | Severity | 含义 |
| --- | --- | --- |
| `AGENT_SPEC_INVALID_JSON` | error | 不是合法 JSON |
| `AGENT_SPEC_DUPLICATE_KEY` | error | JSON 对象包含重复 Key |
| `AGENT_SPEC_DOCUMENT_REQUIRED` | error | 顶层不是对象 |
| `AGENT_SPEC_DOCUMENT_TOO_LARGE` | error | 超过文档大小限制 |
| `AGENT_SPEC_SENSITIVE_FIELD` | error | 出现明确禁止的 Credential 字段 |
| `AGENT_DRAFT_REVISION_CONFLICT` | error | Expected Revision 已过期 |

### 12.2 Schema

| Code | Severity | 含义 |
| --- | --- | --- |
| `AGENT_SPEC_UNSUPPORTED_VERSION` | error | Schema Version 不受支持 |
| `AGENT_SPEC_REQUIRED_FIELD` | error | 缺少必填字段 |
| `AGENT_SPEC_UNKNOWN_FIELD` | error | 出现 V1 未定义字段 |
| `AGENT_SPEC_INVALID_TYPE` | error | 字段类型错误 |
| `AGENT_SPEC_INVALID_IDENTIFIER` | error | NodeID 或 Slot 不合法 |
| `AGENT_SPEC_LIMIT_EXCEEDED` | error | 长度、数量或数值超过限制 |
| `AGENT_SPEC_UNSUPPORTED_NODE_KIND` | error | Node Kind 不受 V1 支持 |

### 12.3 领域语义

| Code | Severity | 含义 |
| --- | --- | --- |
| `AGENT_SPEC_ROOT_NOT_FOUND` | error | Root 不存在 |
| `AGENT_SPEC_ROOT_HAS_PARENT` | error | Root 被其他节点拥有 |
| `AGENT_SPEC_NODE_REFERENCE_NOT_FOUND` | error | Child 或 Body 不存在 |
| `AGENT_SPEC_NODE_MULTIPLE_PARENTS` | error | 节点被多个父节点拥有 |
| `AGENT_SPEC_NODE_UNREACHABLE` | error | 节点无法从 Root 到达 |
| `AGENT_SPEC_STRUCTURE_CYCLE` | error | 定义结构存在环 |
| `AGENT_SPEC_DUPLICATE_CHILD` | error | 组合节点重复引用 Child |
| `AGENT_SPEC_MODEL_SLOT_NOT_FOUND` | error | LLM 引用了未声明 Model Slot |
| `AGENT_SPEC_TOOL_SLOT_NOT_FOUND` | error | LLM 引用了未声明 Tool Slot |
| `AGENT_SPEC_KNOWLEDGE_SLOT_NOT_FOUND` | error | LLM 引用了未声明 Knowledge Slot |
| `AGENT_SPEC_UNUSED_MODEL_SLOT` | warning | Model Slot 已声明但未使用 |
| `AGENT_SPEC_UNUSED_TOOL_SLOT` | warning | Tool Slot 已声明但未使用 |
| `AGENT_SPEC_UNUSED_KNOWLEDGE_SLOT` | warning | Knowledge Slot 已声明但未使用 |

应用层可以增加请求、授权和资源状态错误码，但不能伪装成 AgentSpec 文档诊断。

## 13. Canonicalization 与 Digest

发布按固定顺序执行：

```text
Parse without duplicate keys
        ↓
L1 JSON Schema Validation
        ↓
L2 Domain Validation
        ↓
Semantic Normalization
        ↓
RFC 8785 JSON Canonicalization Scheme
        ↓
SHA-256
        ↓
spec_digest = "sha256:<lowercase-hex>"
```

语义规范化规则：

- `children` 保留原顺序，因为 Sequence 顺序属于语义，Parallel 也使用稳定展示顺序。
- `tool_slots`、`knowledge_slots` 和 `capabilities` 是集合，发布前按字典序排序。
- Schema 已禁止重复集合元素，Canonicalizer 不静默删除重复值。
- 不自动 Trim 或重写 `instruction`，避免无提示地改变 Prompt。
- 不注入 Provider 默认值；未出现的可选字段保持未出现。
- Agent 元数据、Editor State、Revision、时间戳和发布者不进入 AgentSpec Digest。
- Canonicalizer 和 Validator 的行为必须有 Golden Fixtures 固定。

Digest 不建立唯一约束。相同内容可以在不同 Draft Revision 上发布为不同业务 Version。

## 14. Save、Validate 与 Publish 行为

### 14.1 SaveDraft

- 执行 L0。
- 使用 Expected Draft Revision 做 Compare-And-Swap。
- 成功后 `spec_revision + 1`。
- 可以保存尚未通过 L1/L2 的文档。
- 不计算正式 Spec Digest，不创建 AgentVersion。

### 14.2 ValidateDraft

- 读取指定 Draft Revision。
- 执行 L1 和 L2。
- 返回完整 Validation Report。
- 不修改 Draft，不创建 Version，不访问 RuntimeProfile。

### 14.3 PublishAgentVersion

- 必须携带 Expected Draft Revision。
- 执行 L1、L2、Canonicalization 和 Digest。
- 任何 Error 都阻止发布；Warning 不阻止。
- 在事务内重新检查 Draft Revision。
- 同一 Source Draft Revision 重复发布，幂等返回已有 AgentVersion。
- 发布后 Draft 仍可继续修改。
- 已发布 AgentVersion 的 AgentSpec 永远不能更新。

## 15. 有效示例

### 15.1 单 LLM

```json
{
  "schema_version": "v1",
  "root": "assistant",
  "requirements": {
    "models": {
      "primary": {
        "capabilities": ["chat"]
      }
    },
    "tools": {},
    "knowledge": {}
  },
  "nodes": {
    "assistant": {
      "kind": "llm",
      "name": "通用助手",
      "instruction": "准确回答用户问题。",
      "model_slot": "primary",
      "tool_slots": [],
      "knowledge_slots": []
    }
  }
}
```

### 15.2 Parallel 后汇总

```json
{
  "schema_version": "v1",
  "root": "main",
  "requirements": {
    "models": {
      "primary": {
        "capabilities": ["chat"]
      }
    },
    "tools": {},
    "knowledge": {}
  },
  "nodes": {
    "main": {
      "kind": "sequence",
      "children": ["reviews", "summary"]
    },
    "reviews": {
      "kind": "parallel",
      "children": ["security", "quality"]
    },
    "security": {
      "kind": "llm",
      "instruction": "检查安全问题。",
      "model_slot": "primary",
      "tool_slots": [],
      "knowledge_slots": []
    },
    "quality": {
      "kind": "llm",
      "instruction": "检查结果质量。",
      "model_slot": "primary",
      "tool_slots": [],
      "knowledge_slots": []
    },
    "summary": {
      "kind": "llm",
      "instruction": "汇总已有检查结果。",
      "model_slot": "primary",
      "tool_slots": [],
      "knowledge_slots": []
    }
  }
}
```

## 16. 无效示例

### 16.1 Root 不存在

```json
{
  "schema_version": "v1",
  "root": "missing",
  "requirements": {
    "models": {},
    "tools": {},
    "knowledge": {}
  },
  "nodes": {
    "main": {
      "kind": "sequence",
      "children": ["child"]
    }
  }
}
```

至少返回：

```text
AGENT_SPEC_ROOT_NOT_FOUND
AGENT_SPEC_NODE_REFERENCE_NOT_FOUND
```

### 16.2 LLM 引用未声明 Slot

```json
{
  "schema_version": "v1",
  "root": "assistant",
  "requirements": {
    "models": {},
    "tools": {},
    "knowledge": {}
  },
  "nodes": {
    "assistant": {
      "kind": "llm",
      "instruction": "回答问题。",
      "model_slot": "primary",
      "tool_slots": [],
      "knowledge_slots": []
    }
  }
}
```

返回：

```text
AGENT_SPEC_MODEL_SLOT_NOT_FOUND
```

### 16.3 节点属于多个父节点

```json
{
  "schema_version": "v1",
  "root": "main",
  "requirements": {
    "models": {},
    "tools": {},
    "knowledge": {}
  },
  "nodes": {
    "main": {
      "kind": "sequence",
      "children": ["left", "right"]
    },
    "left": {
      "kind": "sequence",
      "children": ["shared"]
    },
    "right": {
      "kind": "sequence",
      "children": ["shared"]
    },
    "shared": {
      "kind": "llm",
      "instruction": "执行共享步骤。",
      "model_slot": "primary",
      "tool_slots": [],
      "knowledge_slots": []
    }
  }
}
```

返回：

```text
AGENT_SPEC_NODE_MULTIPLE_PARENTS
```

## 17. Schema 与实现一致性

V1 完成时必须同时存在并相互验证：

```text
api/schemas/agentspec/v1/agent-spec.schema.json
api/schemas/agentspec/v1/examples/valid/*.json
api/schemas/agentspec/v1/examples/invalid/*.json
services/control-api/internal/agent/domain/spec.go
services/control-api/internal/agent/domain/validation.go
```

测试至少覆盖：

1. 所有有效 Fixture 同时通过 JSON Schema 与 Go Domain Validator。
2. 每个无效 Fixture 返回预期稳定诊断码和 JSON Pointer。
3. Go Marshal 后仍通过公开 JSON Schema。
4. Canonicalization Golden File 与 Digest 固定。
5. Object Key 顺序变化不改变 Digest。
6. Set 类数组顺序变化不改变 Digest。
7. Sequence Child 顺序变化必须改变 Digest。
8. Editor State 不属于 AgentSpec；`editor_state` 进入 AgentSpec 必须作为未知字段失败，因而不能改变 Digest。
9. Schema 未定义字段必须失败。
10. 发布事务只接受无 Error 的 Canonical AgentSpec。

## 18. Schema 演进

当前尚未形成稳定对外版本，遵循 ARC-000：可以直接调整现有 V1 Schema / Fixture，
无需保留开发期旧数据或另起协议代际。以下规则只适用于未来稳定对外发布之后；
目标系统运行中的 AgentVersion / RuntimeManifest 不可变约束始终保留。

- 稳定发布后，协议语义不可静默修改。
- 兼容性说明和新的可选能力也必须经过 Fixture、Validator 与 Deployment Compiler
  映射测试；若输出的 RuntimeManifest 改变，还必须经过 Worker Manifest 兼容性测试。
- 破坏性字段变化发布 `v2`，而不是修改历史 AgentVersion。
- Draft 可以通过显式、可测试且用户确认的迁移转换到新版本。
- 已发布 AgentVersion 不原地迁移；Deployment Compiler 必须使用记录的 AgentSpec
  Schema Version，并拒绝自己不支持的 Version。
- Worker 只接收 RuntimeManifest；不受支持的 Manifest Schema/Adapter Kind/Version
  必须明确报告 incompatible，不能静默降级。

## 19. 待后续协议解决的问题

以下内容不阻塞 AgentSpec V1，但不能通过新增任意字段临时绕过：

- Delegation 型 SubAgent 与固定组合节点之间的区别。
- Graph State、Reducer、Condition、Join 和平台 Runtime Registry。
- 结构化输入输出与跨节点 Mapping。
- Model、Tool、Knowledge Capability Registry 的版本策略；V1 当前只校验语法并在
  Deployment 中做精确字符串集合匹配。
- Loop 标准提前终止信号的 Worker 执行语义精确定义。
- Deployment Compiler 的 AgentSpec 支持窗口与 RuntimeManifest 映射策略，以及 Worker
  的 Manifest Schema/Adapter Kind/Version 支持窗口与滚动升级策略。
