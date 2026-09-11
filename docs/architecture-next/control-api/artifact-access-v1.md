# Artifact Owner 文件访问

本接口属于已有 Deployment 的固定发布物访问，不提供任意 S3 key、endpoint、
user_id、session_id 或 Credential ID 输入。Worker 仍拥有 Artifact 存储与 Run scope。

- `PUT /v1/tenants/:tenant_id/deployments/:deployment_id/revisions/:revision_number/artifacts/:filename?run_id=...`
  接收原始 bytes 与 Content-Type，返回 200 元数据 JSON。
- `GET` 同一路径返回原始 bytes，可选 `version=0` 或更大版本；未指定由 Worker 加载最新。
- filename 仅单 basename，禁止 slash、backslash、空、`.`、`..`。
- Query 只接受单个 run_id 和 GET 可选单个 version；PUT 不接受 version。
- 传输内容上限 16 MiB，同时受固定 Manifest Backend.Limits.MaxBytes 约束。

Control 沿用 cookie 登录与租户 OWNER 权限，读取并完整验证指定 DeploymentRevision，
从固定 Manifest 取得 Artifact 资源、ProfileRevision 和两项凭据 use。
Runtime Profile 的内部 `ResolveArtifactForOwner` 再次确认 Owner，锁定 Profile 后
核验固定 Revision 的凭据关联并解密。该方法不是公开的 Secret 读取 API，
不接受或伪造 Attempt token。其结果只在本次 Worker 请求中使用。

Control 复用 RuntimeConfig.ExecutionURL origin 与现有 Execution mTLS 证书配置，
调用共享 `api/runtime/execution/v1.ArtifactRequest` 的 `/internal/v1/artifacts`。
禁止重定向；没有新配置对象、租户数据库或授权平台。
Worker 独立验证 Run ledger 的 tenant、manifest_ref、manifest_digest 和
DeploymentRevisionID，并从 Run 派生 SDK Artifact Session scope，拒绝目标覆盖。

PUT 创建新版本，当前无隐式幂等或自动重试。GET 的 SHA256、大小、名称与版本由
Control 验证，响应 no-store、nosniff、attachment，凭据不进入 API 响应或日志。
Worker 的 invalid/forbidden/not-found/limit/backend 错误映射到
400/403/404/413/503；缺失 Run 同样为 403。版本从 0 开始。

运行数据接入验收必须经过真实 Control HTTP -> Worker mTLS -> 固定 Manifest ->
Run scope -> S3/既有 PostgreSQL Artifact metadata；mock 或单元测试仅证明契约。
