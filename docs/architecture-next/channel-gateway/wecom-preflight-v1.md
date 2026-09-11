# 企业微信智能机器人接入验证 V1

日期：2026-09-07。实现位置为 Channel Gateway 工作树；使用 `platform/im/wecom`
公开 Go 包，直接嵌入 Gateway，无独立 Node Connector 镜像或部署单元。

## 1. 与 Telegram 的差异

Telegram 的预检调用 `getMe/getWebhookInfo`，不改变接收方式。企业微信智能机器人
长连接通过 `aibot_subscribe` 验证 Bot ID + Secret；这是一次真实订阅，可能替换同一
Bot 的其他连接，**不是只读身份查询**。不能把 Telegram 的说明直接换成企微名称。

官方协议与 SDK 参照：
- [企业微信长连接配置指引](https://open.work.weixin.qq.com/help?doc_id=21661)
- [官方 Node SDK](https://github.com/WecomTeam/aibot-node-sdk)
- [官方长连接实现](https://github.com/WecomTeam/aibot-node-sdk/blob/main/src/ws.ts)

本实现用同一 Go 协议库完成认证，端点固定为 `wss://openws.work.weixin.qq.com`。
账户、请求及浏览器不能提供任意探测 URL。只允许部署自己的普通出口代理，保留 TLS
验证并禁止 HTTP 跳转。私有部署端点尚未开放为账户级参数。

## 2. 产品操作

1. 在企微创建 API 模式机器人，选择“使用长连接”，获取 Bot ID 和 Secret。
2. 在平台创建停用的 WeCom ChannelAccount，保存 `wecom.bot_secret`。
3. OWNER 打开“接入验证”，确认短连接认证可能替换其他客户端、诊断期间可能收到但
   不处理业务消息；显式发起检查。普通成员可读结果，不可发起。
4. 查看凭据配置、连接认证、真实消息验证三项事实。认证通过不表示路由、Worker 或
   Agent 回复已完成；启用、Binding 与真实消息验收是后续独立动作。

POST 与 Telegram 共用账户下的 `/preflights`，但凭据版本字段不同：

```json
{
  "expected_account_revision": 7,
  "expected_connection_revision": 4,
  "expected_bot_secret_version": 2,
  "allow_connection_probe": true
}
```

`expected_bot_secret_version` 与 Telegram 的 `expected_bot_token_version` 互斥。
确认位进入幂等摘要，缺少确认不启动订阅。WeCom 的预检派生模式为 `long_connection`；
不向现有 WeCom Account.config 强行添加 Telegram 的 receive_mode 字段。

## 3. 三侧实现

| 组件 | 职责 |
| --- | --- |
| Control | OWNER/停用/CAS、确认位、任务与短时租约、凭据版本固定、撤销重查、结果存储 |
| Gateway Connection | 独立 WeCom runner、认证探测、释放连接、白名单事实上报 |
| Web | 提供企微入口、Secret 版本与影响确认、发起/恢复/轮询、未知与过期展示 |
| `platform/im/wecom` | `ProbeAuthentication` 与正常 `Client` 共用认证/ACK/关闭协议实现 |

WeCom runner 只声明 `diagnostic_policy=wecom_long_connection_v1`，Control 只授权
`wecom_preflight` consumer 领取该策略任务。Resolve/Complete 按已保存任务 Provider 再次
校验授权。Telegram runner 保持旧策略与 `telegram_preflight`，不能领取 WeCom 凭据。

两个 runner 的 mTLS/出口连接池、执行槽独立；**claim 限速共享**。Control 对同一
principal/instance 合计允许 2 次/秒，Gateway 对所有 provider 的 claim 和不确定响应重试
共用 510ms 间隔，不因增加一个 runner 将请求速率翻倍。

短时认证最多 10 秒，不自动重连；返回前取消并关闭。诊断 handler 丢弃业务帧，不发送
业务回复，不调用普通 Account enable/Permit/Admission，不产生 RunRequested。
Control 仍固定 120 秒任务 deadline、30 秒 claim lease，版本/授权变化按原规则失效。

## 4. 检查结果

| 检查 | 状态 / 原因 | 能证明的事实 |
| --- | --- | --- |
| credential_configuration | PASS/CREDENTIALS_CONFIGURED；FAIL/BOT_SECRET_MISSING | 保存元数据有无 Secret |
| connection_authentication | PASS/WECOM_AUTHENTICATED | 对应 Bot ID + Secret 的订阅 ACK 明确成功 |
| connection_authentication | FAIL/WECOM_AUTH_REJECTED | 收到匹配订阅请求的明确非零 ACK |
| connection_authentication | UNKNOWN/PROVIDER_TIMEOUT、PROVIDER_NETWORK 等 | 尚未得到确定凭据结论；不冒充 Secret 无效 |
| delivery_verification | UNKNOWN/DELIVERY_NOT_TESTED | 预检未执行真实消息/Agent 回复验收 |

缺 Secret 时跳过网络。错误回包、错 req_id、断线、被替换、超时与明确拒绝分别保留
稳定分类；不把 Secret、远端错误文本、用户身份或消息正文放入预检结果。

摘要固定 policy、scope、source epoch、官方 WS 端点；effective digest 额外包含模式与
connection_revision。WeCom public origin 固定为 null / PUBLIC_ORIGIN_NOT_APPLICABLE，
更改 Telegram 公网入口不改变企微认证条件。

## 5. 部署

沿用现有 `channel-gateway` 部署单元，没有新增 Connector workload。
Control 模式增加 `GATEWAY_WECOM_PREFLIGHT_ENABLED`，默认 true；可独立关闭。
实际启用任务领取还需在 Control 对该 Gateway workload 配置 `wecom_preflight` consumer。
示例文件变更不等于既有服务已升级；本次代码交付不自动重启当前服务。

## 6. 本机真实 Bot 验证工具

```bash
go run ./services/channel-gateway/cmd/wecom-smoke -bot-id BOT_ID
```

命令只监听 `127.0.0.1` 随机端口，输出一次随机路径的本地页面地址。Secret 在密码框
输入，不放到命令参数、环境、Git、报告或聊天。页面和响应 no-store，精确 Host/Origin
及随机路径校验，禁止任意探测 URL。连接 15 分钟自动停止，进程最多运行 30 分钟。
Go 字符串生命周期受进程控制，不声称保证内存零化。

“仅验证认证”调用公开 ProbeAuthentication 并释放连接；“连接并验证收发”先用同一
认证探测，释放后另开消息验证连接。只对本次生成的 marker 回复固定测试文字，不记录
其他聊天。状态分别展示认证、Bot ID 匹配、收到 marker、Final ACK，不能互相替代。
停止操作释放本工具连接；不删除企微机器人，也不修改其他部署。

## 7. 验收记录

自动化验证覆盖协议 ACK/错误/超时/关闭、应用授权栅栏、真实 loopback WebSocket、
mTLS claim→resolve→complete、两种 runner 限速、Web 确认与恢复、Control 持久化契约。
与第三方真实机器人认证和收发分开记账。

本轮证据在工作树 `artifacts/wecom-preflight-20260907/VERIFICATION.txt`。
本轮已完成两条独立的真实验收：

- **产品预检**：Web → Control Session/OWNER → PostgreSQL 17（独立 migrator/runtime
  角色）→ mTLS Gateway runner → 官方 WeCom WSS → Complete → Web 固定链接。
  `cpf_zj0SsSycUhbUuDRqVZXofqqN` 返回 COMPLETED / PASS，凭据配置与认证 PASS，
  第三项仍是 UNKNOWN / DELIVERY_NOT_TESTED。未确认、取消各为 0 个任务，明确确认
  后为 1 个；MEMBER POST 返回 403，重新打开页面为只读；8 张运行相关表摘要不变。
- **协议真实收发**：公开 Go connector 使用已保存的 `wecome-test`，认证成功；
  收到 marker `wecom-smoke-12d93eb8fa51`，发送的 Final 获企微 ACK，企微客户端
  实际显示 `企微接入验证通过：wecom-smoke-12d93eb8fa51`。随后释放测试连接。

真实收发记录见 `LIVE_DELIVERY.json`、`LIVE_STOPPED.json`、`LIVE_UI.txt`；产品联合
记录见 `BROWSER_ACCEPTANCE.json`、`BROWSER_FINAL.json`。Bot Secret 不进入这些记录。
测试数据库与进程在验收后销毁，原企微机器人保留；既有部署未升级、未替换。
这两条链路不构成 Binding → RunRequested → Agent Worker → ReplyIntent 的真实 Bot
端到端执行验收；后者没有在这次预检任务中启用。

### 7.1 本次联合测试修复

1. **官方端点 HTTP/1 请求头大小写兼容**：同一端点上，Node `ws` 握手为 101，
   Go 默认 `Sec-Websocket-*` 为 404；仅改为官方 SDK 的 `Sec-WebSocket-*` 后，Go
   握手为 101。`platform/im/wecom/handshake.go` 在克隆的 client/request 上保留
   该拼写，不改变 URL、代理、TLS 校验、超时或跳转策略。真实 wire 回归先红后绿。
2. **旧联合测试夹具对齐当前数据库契约**：原夹具使用临时 schema/管理员连接，
   当前 Control 已要求 `control` schema、独立 `control_migrator`/`control_runtime`。
   联合夹具改为独立数据库、独立角色及与部署脚本一致的默认 DML 授权，并采用
   PostgreSQL 17；没有修改或弱化生产启动校验。初始启动失败及修正结果保留。
3. **保留部署迁移历史**：Control 新增 `0004_wecom_preflights.sql`，允许明确标注
   WeCom policy/mode/consent 的 3 项完成结果；不改既有 0001–0003，旧 Telegram
   8 项完成记录升级后保留。联合启动确实应用 0001–0004。
4. **认证 ACK 与连接状态解耦**：匹配订阅请求的明确成功/拒绝 ACK 单独保存；即使
   Provider 随后立即关闭连接，或状态通知缓冲被覆盖，预检仍保留已观察的认证事实。
   普通 Client 对明确认证拒绝保持终止，不因紧随其后的断线自动重连。正反 ACK 后
   立即断线的 loopback 压测纳入回归。
