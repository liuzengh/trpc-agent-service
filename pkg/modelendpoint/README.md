# 运营侧模型端点

`TRPC_OPENAI_BASE_URL` 为进程级 OpenAI-compatible API 地址，例如 `https://api.provider.example/v1`。部署时替换为已批准的实际服务地址，只注入 Worker 与 Summary Worker；Admin、Gateway、Delivery 无需该变量。模型密钥继续使用租户绑定的 SecretRef，租户 `model.endpoint` 必须留空，不能覆盖目标。未配置时使用官方 OpenAI API。

自定义地址必须是 HTTPS，不含用户名、密码、query、fragment 或编码路径；实际请求仅允许配置的 origin 与 API 路径前缀。Transport 禁止环境代理和重定向，每次新建连接验证完整 DNS 结果并向已验证公网 IP 拨号，TLS 仍验证原域名证书。私网、回环、链路本地、保留地址及混合公私网 DNS 结果均拒绝。网络代理通过假 IP 解析的环境需为该域名配置真实公网 DNS；不能关闭上述校验。

SDK 自动重试保持关闭，退避、预算和审计仍由平台统一处理。上游错误在进入 SDK 日志与模型事件前净化；成功回复和 usage 保留，SSE 保持流式。非 SSE 回复上限 8 MiB，连接闲置 30 秒后回收。不同端点必须保持批准模型的名称、上下文上限与计费契约；此变量不扩大模型目录或租户工具权限。
