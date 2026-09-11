# 已提交 Final 的 Artifact 专用凭据解析

新增内部路由：`POST /internal/v1/runtime-profiles/credentials/resolve-final-artifact`。
该路由只注册到现有 Worker mTLS 内部监听器，不进入公共管理 API。
本包实现 Control 入口与证明消费；Worker Final 证明服务、附件下载及 Gateway 投递另行联调。

## 请求

```json
{
  "final": {
    "intent_id": "intent", "digest": "sha256:<64 lowercase hex>",
    "admission_id": "admission", "run_id": "run", "attempt_id": "attempt",
    "completion_id": "completion", "execution_generation": 1, "sequence": 1
  },
  "tenant_id": "tenant", "manifest_id": "manifest",
  "manifest_digest": "sha256:<64 lowercase hex>",
  "deployment_id": "deployment", "deployment_revision_id": "revision",
  "profile_id": "profile", "profile_revision_number": 1
}
```

`final.digest` 为 ReplyIntent／Final 证明摘要，不是 Manifest 摘要。所有标识必须精确匹配。
不接受 `uses`、`execution_token`、用户身份或请求体中的 Worker 身份；不接受未知、重复或 null 字段。
Worker 身份只来自既有认证中间件。请求不产生新授权票据或持久状态。

## 授权链

1. Control 从可信认证上下文取得 Worker workload identity。
2. 通过现有 Execution mTLS client 调用 `/internal/v1/execution/finals:verify`。
   Worker 必须支持已提交的 Final／Completion 查询，不借用已过期 Attempt lease。
3. 严格比对整个 FinalRequest，以及证明中的 tenant_id、manifest_digest；拒绝跳转，单次有界请求。
4. 按 TenantID＋ManifestID 从 Deployment 自有表读取固定 Revision 与 Manifest。
   检查 DeploymentID、DeploymentRevisionID、ManifestID、TenantID、content digest 和 Profile 身份／Revision。
5. 校验 Manifest 内容完整性，要求已显式启用 Artifact，且 Artifact 固定为受支持的 managed_artifact。
6. 从该 Artifact 资源派生且只派生 `access_key_id`、`secret_access_key` 两项 Use。
7. Runtime Profile 再检查固定 ProfileRevision 的凭据关联、当前状态及用途，执行解密。
   已清除／不可用凭据仍拒绝；Final 证明不会使旧凭据永久有效。

Profile 应用层只消费 FinalArtifactAuthorizer 端口；Bootstrap 组合 Execution verifier 与 Deployment
只读查询。Profile 不读 Deployment 表，Deployment 不读 Profile 凭据表。
旧 Owner Artifact 路由授权和旧 active-Attempt resolver 均不放宽。

## 响应及失败

响应复用现有内部 CredentialBatch JSON：tenant_id、profile_id、profile_revision_number、run_id、
attempt_id、worker_id、manifest_id、manifest_digest、credentials；每项含 credential_id、purpose、
audience_digest、credential_revision、value。

`lease_epoch` 固定为 0，明确本响应不是 active lease 授权。Worker 新下载路径应独立解码，
不复用旧 Attempt 响应必须 lease_epoch>0 的判断。真实凭据只在内部受认证通道传输，
不进入 Manifest、Final、Outbox、日志或公共 API。成功及错误响应均为 `Cache-Control: no-store`。

失败复用稳定分类：WORKER_UNAUTHENTICATED、INVALID_REQUEST、EXECUTION_UNAUTHORIZED、
EXECUTION_DEPENDENCY_UNAVAILABLE、CREDENTIAL_UNAVAILABLE、RESOLUTION_FAILED。
失败不返回部分凭据，已分配的值缓冲区按既有规则清理。

本包定向验证覆盖证明字段篡改、租户／Manifest／Deployment／Profile身份不符、额外 Uses／旧token、
HTTP身份伪造、缺凭据或已清除、模型用途注入、关闭redirect、读库参数租户隔离、以及成功双凭据解析。
TLS测试服务器和数据库stub不是生产mTLS／真实数据库附件联调验收。
