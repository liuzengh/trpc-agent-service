# 固定 Knowledge 的 Owner 文本导入

`POST /v1/tenants/:tenant_id/deployments/:deployment_id/revisions/:revision_number/knowledge/:resource/import`
只接受 `{ "name": "note.txt", "text": "纯文本内容" }`，不接受 query、目标、scope、
凭据字段或分块参数。执行同步返回 `{ "documents": N }`，不新增后台任务。

Control 使用现有 cookie 与 OWNER 授权，完整校验固定 DeploymentRevision/Manifest，
只选取该 Manifest 中 Agent 声明并分配给节点的 Knowledge 资源。
Runtime Profile 内部 Owner 解析接口在 Profile 锁内再次授权并校验资源名称、
固定 ProfileRevision、凭据用途与 audience，解密 Qdrant Key 与 Embedding Key。
不查询最新Profile/目录，不伪造Attempt token。

Control 通过现有 Execution URL 与 mTLS 凭据向 Worker 的
`/internal/v1/knowledge` 发送 import 请求；不开放普通用户凭据读取接口。
Worker 从固定 Manifest 派生 tenant/profile/resource/backendDigest 与
embedding Model/BaseURL/dimensions scope，使用 SDK Reader/Chunker/Embedding
与 Knowledge 链处理文本，强制租户过滤。scope 不绑定 DeploymentRevisionID，
同配置重复发布可复用；不同配置不会静默合并。

本包文本上限 1 MiB、JSON 传输 2 MiB，且受固定 Backend.Limits.MaxBytes 约束；
客户端60秒同步请求，禁重定向、不自动重试，无隐式幂等保证。
Qdrant REST固定Endpoint及VectorName来自Snapshot，不猜测gRPC端口。
GET搜索不作为本次Control公开API；Agent检索由Worker现有执行链调用。
