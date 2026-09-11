# P0b1 managed Profile Web 验证边界

契约：99b030a0f9b1f4cddcc31dc0878ec6bd4a864575。
目录：`GET /api/control/v1/tenants/{tenant_id}/runtime-backends`，真实登录 Cookie，
200 `{items:[{id,revision,label,kind,roles,available}]}`。无前端模拟选项，空目录、
失败目录和不可选条目不触发自动选择、降级或修订升级。404 只描述暂不可用，不判定根因。
`available` 是目录选择资格，不是 Worker 健康检查。

Storage 的 managed 类型与固定键 session/memory/artifact 一致，仅保存 backend_id 和
backend_revision（正安全整数）。managed Knowledge 保留 Embedding 的模型、地址、维度和
原有 write-only embedding_api_key 凭据操作；不显示 Qdrant 目标、Collection 或平台凭据。
公开 Config 不是 canonical Spec，浏览器不提交 api_key_credential_id。

通过新增资源入口勾选 managed，随后显式选择目录项。不提供现有资源的一键跨类型迁移；
需要替换时走原有删除确认、权限及 Draft CAS 保存流程。删除已发布配置或平台后端不在此入口。
不可变版本只显示保存的绑定，不请求当前目录来重写旧版本。

单元/组件测试可以模拟 HTTP 返回以验证错误状态；这些结果不等于真实 HTTP 验收。

```sh
npm --prefix web test -- --maxWorkers=2 lib/runtime-backend-api.test.ts lib/managed-profile.test.ts components/runtime-profiles/managed-backend-select.test.tsx components/runtime-profiles/managed-profile-editor.test.tsx
```

本批真实 HTTP 状态：NOT_RUN，等待统筹提供已接线的隔离 Control/Web、测试 Tenant 和账号。
后续验收需读取真实目录，保存并 GET 回读 Profile，服务端校验/发布，再 GET 不可变版本核对
backend_id/backend_revision 与非秘密 embedding 配置，检查 stale revision/不可用/跨 Tenant。
P0b1 bootstrap 的 Backends nil 时不能据 Schema/组件通过声称 managed 发布可用。
Deployment 仍等待 P0b2，P0a gate 提示保留。
