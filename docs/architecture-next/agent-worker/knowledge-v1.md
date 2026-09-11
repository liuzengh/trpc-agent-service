# Knowledge V1：SDK 文本导入与固定检索链

## 装配，不重写 RAG

```
AgentSpec requirements.knowledge + LLM knowledge_slots
 → Profile knowledge resource（固定 Qdrant + Embedding + 两类凭据）
 → immutable Manifest
 → Worker Reader / Plan / Factory
 → llmagent.WithKnowledge(knowledge.Knowledge)
 → SDK knowledge_search
 → SDK Embedder / Retriever / 固定 Qdrant named-vector 查询
```

独立同步导入：

```
OWNER 纯文本 HTTP / Web
 → 固定已发布 Manifest 的知识资源
 → SDK text.Reader / 默认 FixedSizeChunking
 → SDK BuiltinKnowledge.AddSource
 → SDK OpenAI-compatible Embedder
 → Qdrant REST thin VectorStore.Add
```

Qdrant 只负责向量与 payload 的存储/检索，不负责解析、分块或 embedding。
SDK root 保持 v1.11.2。text reader 导入补齐 goldmark v1.4.13 间接依赖。
SDK 官方独立 Qdrant adapter 的 gRPC endpoint / unnamed vector 与现有固定 REST
Endpoint / VectorName 不等价；本实现只补末端 REST 薄 VectorStore，不猜 6334 端口，
不丢 VectorName，不因名称差异开启 BM25。集合由部署准备，Open 只验证维度、名字和
距离度量，不建集合、不 recreate。

## 显式契约

V1 单 LLM 绑定一个显式 Knowledge resource。多个引用在发布门禁明确拒绝，
不会默默选第一个。一个 resource 可导入多份文本。Profile 固定：

- `kind: managed_knowledge` 与 backend ID/revision。
- embedding 的 `model`、`base_url`、`dimensions`；不从聊天模型猜测。
- 独立 `qdrant_api_key` 与 `embedding_api_key` 只写动作及既有轮换流程。
- Qdrant credential audience 为完整 backend Snapshot.Digest；Embedding 沿用
  原 `CredentialAudienceDigest("managed_knowledge", embedding.base_url)`。

Manifest 保留 `knowledge/<resource>` callable_entries 的节点授权。
既有 provider-callable-name-v1 要求 provider 名为 `fn_` 加 logical entry SHA-256
前 60 位十六进制；SDK 默认 `knowledge_search` 不是这个 provider 名。
Worker 使用公开 model.Model 薄 wrapper 仅映射声明、历史和响应流名称，
SDK 本体仍由 WithKnowledge 装配；分片名称也校验完整白名单，不访问 SDK 私有字段。
Memory/Artifact 工具原名保持不变，共用原 Manifest max_tool_calls。

## 隔离与失败

- scope 包含 tenant/profile/resource/backend digest/embedding model+base URL+dimensions；
  凭据轮换不改变 scope，同维度换模型也不复用旧 vectors。
- 存储层强制 scope filter，回读再校验 payload scope；SDK UserID/SessionID 不决定权限。
- Import 同名同正文使用确定性 point IDs；改变正文是追加，不声明替换旧文档。
- Import 同步等待 SDK AddSource，并检查 SDK progress error；后段 chunk 失败时前段
  points 可能已写入，返回失败，不宣称批次原子、补偿或后台重试。
- 空、维度错误或非有限 embedding 拒绝写入。非成功 provider 诊断先脱敏再交 SDK。
- SDK 普通搜索工具总传非 nil 的空 SearchFilter；V1 接受语义空 filter，并在副本上
  归一化，非空自定义过滤/混合检索等未实现模式明确拒绝，不静默忽略条件。
- 无匹配与后端故障区分；无匹配沿 SDK 工具纠错/回答语义，后端故障阻止成功 Final。
- 查询与导入使用同一固定 embedding model/维度，显式传值，不向第三方模型隐式填 1536。

## 正式导入入口

```
POST /v1/tenants/:tenant/deployments/:deployment/revisions/:revision/knowledge/:resource/import
{"name":"guide.txt","text":"plain text content"}
```

成功 200 `{documents:N}`。现有 OWNER 身份与固定 PublishedRevision 授权后，
Control 内部解析原 ProfileRevision 的两项凭据，以现有 mTLS 调用 Worker
`POST /internal/v1/knowledge`。Worker 独立核对 Manifest ref/digest/revision/tenant 与
唯一显式知识资源，不接受调用方 namespace/endpoint 或未发布资源。

纯文本正文传输 1 MiB、JSON 2 MiB，另遵守固定 backend.MaxBytes；同步管理请求上限
60 秒且受后端 timeout 约束。没有 job ID、后台任务、自动恢复、多格式解析或 OCR。
HTTP/UI 失败提示可能已有部分 chunk 写入，不自动重试。

## 验收入口与真实模型边界

```
services/agent-worker/internal/execution/adapter/outbound/knowledgestore/test-integration.sh
python3 -B scripts/test-worker-knowledge-joint.py --race --artifacts /tmp/knowledge-joint
```

真实独立 Qdrant 测试已覆盖 SDK 导入、named vector、scope/model 隔离、重复导入、
部分失败、实际 SDK search tool（含默认空 SearchFilter），以及 frozen provider name
完整/分片调度。正式 HTTP + Worker + Channel Lab 联合 7 Run 已通过：6 成功、
1 模型失败；空 embedding 和停机导入返回 503、不新增 points；第二 Profile 隔离，
切回原发布仍检索其原数据。

以上 embedding provider 是显式确定性 HTTP fixture，不能替代真实外部 embedding
验收。用户提供实际 endpoint/model/维度/凭据之后，统一跑真实 embedding 与四能力
组合回归；当前 localhost:57856/v1/models 不可达，不推测聊天 API key 能跨服务使用。
实际 GUI 已完成 Agent v2 / Profile r2 / Deployment r2 发布、纯文本文件输入与
单次同步导入（HTTP 200，1 document）。同一 GUI 发布 Manifest 的实际 SDK Run
从真实 Qdrant 取回 `GUI imported canary LILY-526.`，正式 Session 成功接受并由
Channel Lab 收到 Final。该 GUI 的模型和 embedding 均为 HTTP fixture。

四能力组合回归已完成：同一正式 Manifest 与 Session 连续 3 个 Run，Memory 正式
APPLIED、Artifact 版本 0 读回、Knowledge 检索与 Summary 接受/下轮消费同时通过。
实际 DeepSeek 主模型 7 次、摘要模型 6 次 HTTP 200，最终 Gateway transport 与
Delivery 均 ACCEPTED，Lab 正文逐字等于真实模型 Final。组合的 4 次 embedding
调用仍为 fixture；这证明真实模型工具选择和回答装配，不替代真实向量语义验收。

后续：提供真实 embedding 配置后完成外部 embedding 与检索语义验收。
文档替换/删除、多个知识资源组合、定制分块、混合检索、多格式与后台导入不在本版实现。
