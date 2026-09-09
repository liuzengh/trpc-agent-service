# 企业微信智能机器人连接器

连接器使用企业微信官方智能机器人长连接协议，接收私聊文本，经现有 Gateway 的通道认证、用户白名单与限流进入 PostgreSQL Inbox。Consumer、Worker、Session、Memory、审计和 Outbox 仍由平台处理。机器人回复通过独立的 `/reply` 内部接口送回同一条 WebSocket；成功状态要求收到官方 `errcode=0` 确认。

Windows 本地部署先将 `deploy/wecom-bot.local.example.json` 复制到仓库外，填写长连接凭据与允许用户，再运行 `scripts/wecom_bot_local_bootstrap.ps1 -ConfigPath <配置文件路径>`。`ConfigPath` 必须显式传入；`-ValidateOnly` 完全离线检查配置，`-SkipBuild` 复用已有镜像。

机器人配置使用 `wecom_bot` 类型，`accountId` 为 Bot ID，`tokenRef` 指向本地桥接认证令牌。官方 Bot Secret 只注入连接器，不能填到渠道的 `secret` 字段。Gateway 和 Delivery 必须注入该租户 `channel_token` 的精确 SecretRef 授权，生成名称使用 `tenant.SecretBindingEnvironmentName(tenantID, "channel_token", "wecom_bot", botID)`。模型凭据另外授权。

必要环境变量：

| 变量 | 用途 |
| --- | --- |
| `WECOM_BOT_ID`、`WECOM_BOT_SECRET` | 官方 API 模式中长连接凭据 |
| `TRPC_SECRET_WECOM_BOT_BRIDGE_TOKEN` | 与 Gateway / Delivery 的内部认证令牌，32–4096 位 URL-safe 字符 |
| `WECOM_BOT_GATEWAY_BASE_URL` | Gateway origin，例如 `http://gateway:8080` |
| `WECOM_BOT_WEBHOOK_ROUTE_KEY` | 指向该租户渠道绑定的路由键 |
| `WECOM_BOT_STATE_DIR` | 持久卷目录，运行 UID 必须可写 |
| `PORT` | 内部回复与健康接口端口，默认 8090 |

组合 `deploy/docker-compose.yml`、`deploy/docker-compose.wecom-bot.yml` 与租户授权 override 部署。一个 Bot 只部署一个连接器，Worker 可以独立扩容。跨主机主备需要另行提供单一所有者控制，文件锁只覆盖共享本地状态目录；官方不保证离线消息全部补发。

消息先写本地 spool 再交给 Gateway。只在 Gateway 返回 200 后清除正文并保留去重元数据；此状态代表完成接入处理，可能是已入队，也可能是已审计拒绝或忽略，不代表 Agent 已执行。未完成接入最多保留 256 条，记录总数上限 10000。成功状态元数据保留至少 25 小时；派发结果不明确的回复记录不自动删除。容量耗尽、磁盘错误或消息标识冲突会停止并保留状态，需运维处理。

回复正文上限为平台规定的 20 KiB，以一次 `finish=true` 流式更新提交；尚未开放图片、文件、语音、群聊、实时增量流式、卡片及主动群推送。HTTP 503 且 `status=not_sent` 表示明确未发送，可重试；官方明确返回限流错误 `45009` 时，桥接返回 HTTP 429、`status=not_sent` 和 `Retry-After`，Outbox 按退避时间重试同一投递。确认超时返回结果未知，Outbox 停止自动重发以避免重复消息。连接被另一客户端替换、凭据被拒绝时停止，避免反复争抢同一 Bot。

官方协议：[智能机器人长连接](https://developer.work.weixin.qq.com/document/path/101463)。普通群消息推送 Webhook 不是此协议。
