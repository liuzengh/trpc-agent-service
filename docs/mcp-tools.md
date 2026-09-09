# 生产 MCP 与业务工具

外部工具作为租户/App 的不可变发布配置接入 Runtime Bundle。这是受治理的生产运行能力，不允许模型临时连接任意 MCP 服务。

## 安全边界

- MCP 仅支持管理员发布的命名 Streamable HTTP 服务；生产配置必须是 HTTPS。不支持 stdio，也不接受模型或普通请求传入 endpoint、header 或 SecretRef。
- 客户端关闭可选的 GET SSE server-push 通道，只使用有界的 Streamable POST 请求/响应；这样既兼容有状态和无状态服务，也避免后台通知 goroutine 进入 Bundle 生命周期之外。工具发现和调用不依赖 GET SSE。
- 同一个有状态 MCP client 在当前上游版本中不保证并发安全，因此一个 Bundle 内对同一 server 的调用按 session 串行化；不同 MCP server、Bundle、服务节点仍可并发。需要更高单 server 吞吐时应扩展为有界连接池，并先通过 race 与服务端限流验收。
- MCP endpoint 和业务工具 endpoint 禁止 userinfo、query、fragment；HTTP client 禁止跟随重定向。每个远端调用都有 30 秒以内的有界超时。
- 认证只允许 `Authorization: Bearer`（默认）或 MCP 的 `X-API-Key`。生产真实值由符合 tenant/app namespace 的 Vault/KMS SecretRef 在 Bundle 创建时解析，不进入配置版本、API 响应、错误或工具结果。
- MCP 远端工具使用 `mcp__<server_id>__<remote_tool>` 名称。发布配置必须同时把这个完整名称加入 `tools.allow`；业务工具使用其 `name`。内置 `echo`、`calculator` 和 `knowledge_search` 不能被覆盖。
- 所有可调用工具在最终 handler 再执行租户 allow/deny 与审批校验，并复用 Tool trace、审计和预算上下文。业务接口还必须按 `X-Idempotency-Key` 实现幂等。

## 配置示例

以下片段位于租户 `demo` 的 App `assistant` 下。凭据值只放外部 Secret Manager，不要写入 YAML：

```yaml
tools:
  allow:
    - calculator
    - mcp__crm__lookup_customer
    - create_ticket
  deny: []
  require_approval:
    - create_ticket

mcp_servers:
  - server_id: crm
    endpoint: https://mcp.example.com/mcp
    credential:
      provider: vault
      key: demo/assistant/crm-mcp-token
    credential_header: Authorization
    credential_scheme: Bearer
    allowed_tools:
      - lookup_customer
    # MCP 无法与平台数据库组成事务；服务端必须按调用语义保证幂等。
    idempotent: true
    timeout_seconds: 10
    enabled: true

business_tools:
  - name: create_ticket
    description: Create a support ticket through the approved service.
    endpoint: https://tools.example.com/v1/tickets
    credential:
      provider: vault
      key: demo/assistant/ticket-api-token
    timeout_seconds: 10
    enabled: true
```

业务工具向固定 endpoint 发送 `POST application/json`，模型参数只能成为 JSON body。服务自动添加 Bearer credential 和 `X-Idempotency-Key: <request_id>:<tool_name>:<invocation>`，持久 ledger 另以框架 `tool_call_id` 防止崩溃接管时自动重放未知副作用。请求和响应上限均为 64 KiB，响应必须是 JSON object。非 2xx、超时、重定向、非 JSON 或超限响应都会变成不包含上游正文的通用错误。

启用的 MCP server 必须声明 `idempotent: true`，表示远端对重复调用具备幂等语义。平台不能把 MCP 网络副作用与 Inbox 事务做成原子提交；不能满足该契约的写操作必须使用 Business HTTP adapter，并由远端按稳定 `X-Idempotency-Key` 去重。

## 发布、切换与故障行为

Admin `validate` 和 `publish` 都先执行静态校验，再解析 SecretRef、连接启用的 MCP 服务、执行 Initialize/ListTools，并确认所有 `allowed_tools` 都存在。任一步失败都拒绝发布，当前版本不变。进程启动也对数据库中的当前发布版本执行同样预检，因此不能带着不可用工具进入 ready 状态。

发布成功后无需修改本地 YAML或重启 Compose。新请求取得新版本 Bundle；已经运行的请求继续持有旧 Bundle，完成并释放最后一个引用后才关闭旧 MCP session。回滚同样创建新配置版本和新 Bundle，不修改历史版本。

## 验收

- 静态测试覆盖 HTTPS、SecretRef、认证 header、名称冲突和 allowlist 校验。
- Catalog 测试覆盖同名工具命名空间、部分初始化失败资源回收、错误与结构化结果脱敏。
- 真实协议测试启动本地 Streamable HTTP MCP Server，完成 Initialize、ListTools 和 CallTool。
- HTTPS 业务工具测试覆盖策略上下文、Bearer、幂等键、JSON object、输入限制和响应脱敏。
- Runtime 测试验证外部工具随 Bundle 关闭且 Close 幂等；既有 Manager 测试继续验证 v1 请求完成前 v1 不关闭、v2 新请求进入新 Bundle。
