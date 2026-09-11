# WeCom 本机验证页

从仓库根运行 `go run ./services/channel-gateway/cmd/wecom-smoke -bot-id BOT_ID`，打开输出的本机 URL。
Secret 只在页面输入，勾选真实订阅影响确认，再选择认证探测或 marker 收发验证。

- 仅监听 127.0.0.1；随机不可预测路径、Host/Origin 检查、no-store 响应。
- 官方 WSS 端点；不接收用户提供的 endpoint/proxy；保留证书验证与默认出口代理。
- 认证探测无自动重连、不发送消息；独立收发模式仅回复自己的测试 marker。
- 不创建平台账户/Binding，不启用运行目标，不调用 Worker。
- 15 分钟连接/30 分钟进程自动关闭；Ctrl-C 或“停止并释放连接”结束本工具连接。
- 控制台不打印 Bot Secret 或业务消息；状态页只返回白名单计数和布尔事实。

详细机制和产品预检契约见
[WeCom 接入验证设计](../../../../docs/architecture-next/channel-gateway/wecom-preflight-v1.md)。
