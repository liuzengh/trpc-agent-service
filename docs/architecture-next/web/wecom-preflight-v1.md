# 企微接入诊断：Web V1

## 交付状态与代码边界

- 基线：Web `45927895089ccc9ed2db850928f8b2eb687a7e2a`，保留 Worker execution/reply 合并。
- 使用 Control 冻结的 `wecom_long_connection_v1` 契约；API-only 补丁 SHA256：
  `84f42141a2c4eab28a95e410229651859ff5c869deef14b5441a9c7ee8b61e4a`。
- 本文对应 Web UI、API client、恢复状态与测试实现，不声明真实企微认证、消息到达或 Agent 回复成功。
- 联合启动需要完整 Control/Gateway 实现。Control 已明确要新增
  `0004_wecom_preflights.sql`，将完成结果检查数约束按 provider 区分为 Telegram 8 / WeCom 3；
  API-only 或前端更新不替代该迁移，也不代表服务已部署。

## 用户路径

1. 在企微账户详情中选择「查看企微接入诊断」或「连接诊断」。MEMBER 可打开已知任务固定链接。
2. 查看已保存 Bot ID、Bot Secret 的版本及配置状态。页面不读取或展示 Secret 明文，
   不要求先创建 Binding，不修改现有 WeCom `ChannelAccount.config`。
3. 只有 OWNER、账户停用、版本完整且没有其他待确认写入时，才可准备新诊断。
   已启用账户必须回到账户接入操作单独确认停用，诊断面板不自动停用。
4. 点击「开始连接诊断」先打开影响确认窗口，此时没有 POST，也不写入待提交恢复记录。
   窗口展示 Bot ID、账户/连接/Secret 固定版本，复选框默认未选中。
5. 用户勾选影响确认后，才发送带新 Idempotency-Key 的四字段请求。准备期间账户版本、
   Secret 版本、启用状态、OWNER 资格或其他写入状态变化，会使确认失效。
6. 收到 202 后只按任务 ID 读取结果。配置、认证和真实消息/回复分层展示，不以
   `COMPLETED` 或 `WECOM_AUTHENTICATED` 表示持续在线、上线完成或 Agent 已回复。

## 为什么不沿用 Telegram 的只读说明

企微 `subscribe` 是真实连接认证。短连接可能替换同 Bot 在其他客户端的连接；
本平台账户停用不等于外部客户端已经断开。诊断期间可能收到业务消息，Gateway 应丢弃这些
消息，不创建 Run、不发送回复；诊断结束后断开，不自动恢复被替换的外部客户端。

上述影响同时出现在入口、面板和提交确认窗口。Telegram 仍保留原说明：只读已保存信息与
现有 Webhook，不执行 setWebhook/deleteWebhook/getUpdates，不增加企微确认步骤。

## 冻结的浏览器协议

POST `/v1/tenants/{tenant_id}/channel-accounts/{account_id}/preflights`：

```json
{
  "expected_account_revision": 3,
  "expected_connection_revision": 2,
  "expected_bot_secret_version": 4,
  "allow_connection_probe": true
}
```

- 与 Telegram 的 `expected_bot_token_version` 互斥；client 不推断、补全或默认同意。
- Create 202 receipt、GET 地址、固定链接和任务 deadline 与既有协议一致。
- WeCom 结果必须包含 `provider=wecom`、`receive_mode=long_connection`、
  `diagnostic_policy=wecom_long_connection_v1`、`bot_secret_version`、
  `allow_connection_probe=true`；不接受 `bot_token_version`。
- `long_connection` 仅为预检结果协议的派生值，不写入 WeCom Account config。
- `expected_public_origin` 必须为 null；claim 后须有 Gateway/effective digest，
  Gateway freshness 仍为 `UNCONFIRMED`。
- client 严格检查字段、provider、版本、状态、固定顺序、code/details 组合和结果聚合。
  同一账户 GET 如果返回另一 provider，面板不会渲染为成功。

### 固定三项检查

| 检查 | 允许结论 | 页面解释 |
| --- | --- | --- |
| `credential_configuration` | PASS/CREDENTIALS_CONFIGURED 或 FAIL/BOT_SECRET_MISSING | 仅凭据保存事实 |
| `connection_authentication` | PASS/WECOM_AUTHENTICATED、FAIL/WECOM_AUTH_REJECTED，或明确 UNKNOWN | 本次短连接认证；不是 getMe 身份对照或持续在线证明 |
| `delivery_verification` | 固定 UNKNOWN/DELIVERY_NOT_TESTED | 真实消息与 Agent 回复未验证 |

认证 UNKNOWN 允许 `PROVIDER_NETWORK`、`PROVIDER_TIMEOUT`、`PROVIDER_RESPONSE_INVALID`、
`PROVIDER_UNAVAILABLE`、`WECOM_CONNECTION_REPLACED`。只有缺 Secret 才允许
`SKIPPED/NOT_EXECUTED`。总 outcome 仅聚合前两项；第三项始终保留未知状态。

## 未知结果、恢复与生命周期

- 未确定 POST：保存原 key、三个原版本和 true 确认位，不自动提交，不改用最新版本。
  WeCom 原请求重试也重新展示影响确认；重放保持原 body/key，不产生新的请求身份。
- 已收到 202：恢复记录只保留任务 ID/deadline；后续失败仅重复 GET。
- 保留现有 user/tenant/account 存储键及登出清理。缺确认位、false 确认、跨 provider 或损坏
  的恢复记录被阻断，需用户核实后明确清除；不默默补写 consent。
- 自动 GET 为 2 秒间隔，隐藏页面暂停，终态/卸载/权限拒绝时停止；120 秒上限与服务端
  deadline 共同限制轮询，超限仅停止读取，不虚构服务端终态或新建任务。
- Secret/连接版本变化立即标记旧结果失效；元数据变更仅作提示；TTL 到期显示历史结果过期。
- Telegram 的旧记录、双模式固定八项、N/A、UNKNOWN、恢复与轮询测试继续通过。

## 本地验证入口

在 `web` 目录执行：

```sh
npm test
./node_modules/.bin/tsc --noEmit --incremental false
```

新增 consumer 测试直接读取 Control 的 `preflight-wecom-view-valid.json`，检查 DTO 与三项
checks 未被前端改写；表驱动反例覆盖隐式/错误确认、provider 混用、伪造 PASS 和原始秘密字段。
UI 测试覆盖 OWNER/MEMBER、启用状态、确认快照失效、版本冻结、未知 POST 原请求恢复、
202 后 GET-only、Secret 轮换、UNKNOWN 与终止轮询。

浏览器验收使用本树真实组件与样式构建的隔离静态页面；Control 响应全部由本地 fixture
模拟，页面明确标识「不连接企微」。桌面及手机检查确认窗口、三层结果、禁用入口、只读视图
及无横向溢出。临时页面/服务器在验收后关闭，不操作既有运行服务。
