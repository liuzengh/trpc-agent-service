# Artifact V1：单文件服务的正式运行闭环

## 已实现的路径

AgentSpec LLM 节点显式 `artifact: {enabled: true}` → Profile
`storage.artifact: {kind: managed_artifact, backend_id, backend_revision}` →
编译固定 S3 Snapshot、双凭据用途和 `worker-artifact-metadata-v1` → Worker
Reader/Plan/Factory → SDK `artifact.Service` → `runner.WithArtifactService`。

内容保存在固定 S3 兼容后端，元数据在已有 Worker PostgreSQL 的
`worker_artifact_files` / `worker_artifact_versions`。没有再部署元数据库。
SDK root 仍为 v1.11.2；S3 使用 MinIO Go 客户端，不自行实现签名。

Profile 使用两个独立只写动作 `credentials.storage.artifact.access_key_id` 和
`secret_access_key`。固定 Manifest 的 `credentials` 保存对应 CredentialUse，
两项 audience 都等于完整 backend Snapshot.Digest。执行凭据仍通过现有 mTLS
Attempt 校验一次性初始化，不读取可变 Profile 目标。历史无凭据描述可读，
新发布/执行必须两项齐全。固定 S3 endpoint/bucket/region/path_style 不由密钥改变。

## 文件工具与 SDK 服务不是一回事

SDK 的 `runner.WithArtifactService` 只注入服务。V1 明确提供四个 **Worker 薄工具**，
它们调用 SDK `agent.CallbackContext` 的公开方法，不声称 SDK 内建这些工具：

| 工具 | 输入 | 输出 |
|---|---|---|
| artifact_save | name、content_base64、mime_type | name/version/ref/size_bytes/sha256/mime_type |
| artifact_load | name、可选 version | 元数据和 content_base64；缺失为 found:false |
| artifact_list | 空对象 | keys |
| artifact_delete | name | deleted:true；撤销全部可见版本 |

Artifact 启用要求主模型声明 tool_call。Memory 和 Artifact 工具共同遵守现有
Manifest `max_tool_calls`，不增加隐式预算；两类工具同时启用不会相互覆盖。
模型参数错误可进入纠错循环，后端错误会阻止成功 Final，模型随后声称成功也不接受。
文件名为单个名字，不接受路径。工具只接触当前 SDK Session 的文件。

## 版本、正式性与失败窗口

- 首版本 0，之后成功写入分配递增版本；显式 version 可重读，nil 读取最新可见版本。
- PG 文件行锁串行分配版本，先完成 S3 唯一对象写入，再提交元数据和版本序列。
- 读回验证长度和 SHA-256；元数据存在但对象缺失、损坏或后端失败，不返回成功。
- **Save 是立即外部副作用，不等待 Session Complete。** 工具保存成功后模型失败，
  文件版本可能继续存在，但失败轮 Session 仍不被接受。没有声称两者原子。
- S3 成功而 PG 提交失败/结果不确定可能留下孤儿。没有跨存储事务或补偿队列。
- 不确定提交后重试可能生成新版本，因为原 SDK API 没有请求幂等键。
  HTTP/UI 不自动重试上传，提示先核对已存版本。
- Delete 是逻辑撤销：删除可见元数据，保留版本序列；S3 字节可能保留，后续新版本
  不复活旧版本。V1 不包含物理擦除和孤儿 GC。
- scope 包含 tenant、固定 backend digest、SDK App/User/Session。现有 Session identity
  包含 DeploymentRevisionID，新发布建立新 Session，不自动迁移文件。

## 鉴权 HTTP 与 Web

在固定 DeploymentRevision 详情使用真实 RunID 上传与按版本读取：

```
PUT /v1/tenants/:tenant/deployments/:deployment/revisions/:revision/artifacts/:name?run_id=:run
GET /v1/tenants/:tenant/deployments/:deployment/revisions/:revision/artifacts/:name?run_id=:run&version=0
```

Control 按现有 OWNER 身份授权，从已发布 ProfileRevision 解析双凭据，通过现有
Control→Worker mTLS 调用 `/internal/v1/artifacts`。不伪造 Attempt，不公开密钥读取 API。
Worker 从 ledger 查 Run，独立核对 tenant / Manifest ref+digest / DeploymentRevisionID，
由正式 Run 派生 SDK Session scope。浏览器不能声明 user/session/namespace 或后端地址。

PUT 接收原 bytes 并返回 200 metadata；GET 返回原 bytes、attachment、no-store、nosniff。
Web BFF 也保持 attachment/nosniff，避免 HTML/SVG 在 Console 同源作为页面执行。
原文件传输上限 16 MiB，内部 JSON 上限 24 MiB，同时遵守已发布 backend.MaxBytes；
传输界限不是新增 LLM 输出/Token policy。IM Final 可展示 artifact ref，ref 不是公开
下载授权，也不要求 Telegram 发附件。

## 验收与后续

```
services/agent-worker/internal/execution/adapter/outbound/artifactstore/test-integration.sh
python3 -B scripts/test-worker-artifact-joint.py --race --artifacts /tmp/artifact-joint
python3 -B scripts/test-worker-memory-web.py --scenario artifact --help
```

真实 MinIO + Worker PG 测试覆盖字节、并发版本、tenant/backend/Session 隔离、损坏、
S3/PG 失败、超时/TLS和逻辑删除。正式 HTTP 发布 + SDK + Channel Lab 联合验收覆盖
8 Run（6成功、2失败），包括文件保存后模型失败仍保留版本，不误写为回滚成功。
模型是确定性 HTTP fixture；实际 GUI 与最终组合真实模型验收单独记录，不混称。

后续计划：Knowledge 的独立文本导入与检索链；总包统一组合回归。
多格式解析、预览平台、对象 GC、物理删除和恢复队列不属于当前 V1。
