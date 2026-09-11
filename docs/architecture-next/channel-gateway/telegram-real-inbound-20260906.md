# 真实 Telegram → Gateway → RunRequested 联合验收

- 验收日期：2026-09-06，Asia/Shanghai；入站时间 **06:55:45.562692**。
- Gateway：实际 `codex/channel-gateway` 工作树构建的 `/private/tmp/channel-gateway-gci2`。
- Control：本轮消息由实际 Control 管理接口发布账户、凭据与固定 Binding；Control 任务报告
  已提升实际工作树并重建运行二进制。本文直接核验 Gateway PG、真实 Telegram 与 NATS，
  不把对端报告的测试数字当成本任务独立重跑结果。
- 本次目标：真实 IM 入站到固定目标的持久 RunRequested；没有发送虚假机器人回复。

## 1. 结果

**PASS**：指定消息在 Gateway 持久账本中唯一接纳，PG Outbox 已发布，NATS FileStorage
保存了严格 Schema 合法且与 Outbox 规范化后完全相同的 RunRequested。

| 标识 | 实测值 |
| --- | --- |
| 测试前缀 | `gateway-v1-real-20260906-0637` |
| Telegram update_id | `309271229` |
| Telegram message_id | `6` |
| Receipt decision | `admit-run` |
| admission_id | `c00f77d6b3a94dcd98b77ccf071b047b` |
| run_id | `45ccc1ec2d4b600a656ed0b489497e5c` |
| RunRequested event_id | `c00f77d6b3a94dcd98b77ccf071b047b` |
| account / connection_revision | `cha_ZHZwYc0n54Tc0ZW3KlxBaemO` / `2` |
| binding / route generation | `chb_m6ZeGHUmZzf_zfSNV-cd4020` / `2` |
| DeploymentRevision | `dpr_TN-yxwXsqO0LyR9G3gfPlbpt` |
| RuntimeManifest | `rmf_CK7i4jR0iI5NBpJJmkNP02H5` |
| Manifest digest | `sha256:e10102a9f63cabe35c465f9ddc19c8396c62b4ebce4085767e70602732c3c5bf` |

账户内 Inbox=1、Admission=1、Outbox=1、published Outbox=1、DeliveryIntent=0。
真实消息完整文本在内存中逐字核对成功；报告只保留测试前缀、摘要和明确请求的事件标识，
省略发送者/聊天 ID、Token、凭据值、DSN 与正文全文。

## 2. 持久发布与 PubAck 证据

- Subject：`execution.run-requested.v1`。
- Stream：`RUN_REQUESTS_V1`，`FileStorage` / `WorkQueuePolicy`。
- 实际 stream sequence：`1`；匹配该 event_id 的持久消息数：`1`。
- `Nats-Msg-Id` 与 event_id/admission_id 相同。
- Outbox 发布尝试次数：`1`；published_at：**06:55:45.681012 +08:00**。
- 消息已保存时间：`2026-09-05T22:55:45.666731296Z`。
- 严格 `DecodeRunRequested` 校验通过；与 PG Outbox 重新编码后的规范字节相同。
- 规范 payload SHA256：`7519bf75c468a5f1223f470fa3dfa202a1c9e1236991aec0e240ec5e7a19d0fe`。

**原始 PubAck 响应帧没有单独持久化。** 当前源码只在 `JS.Publish` 返回且
`ack.Stream == RUN_REQUESTS_V1` 后，才调用 `ledger.Published` 写 `published_at`。
因此这里使用实际已发布标记、该代码路径和可读取的同事件持久消息交叉确认，不声称捕获
了原始 PubAck 帧。检查只执行 PG SELECT、NATS StreamInfo/GetMsg，没有建 consumer、
ACK、重发、purge 或 reset，保留 RunRequested 给后续执行方。

## 3. 注册与运行前置证据

真实 Telegram `getMe` 与 `getWebhookInfo` 已验证机器人身份和精确 webhook 地址；当时
pending_update_count=0、allowed_updates 包含 message/callback_query、无最近错误时间。
Gateway 连接版本2注册已 FINISHED/READY；mTLS目录、账户 floor2 和受限 Control Relay
生成的路由已匹配。公网未带 Secret 请求返回401，admin readyz 返回204，公开管理路径404。

公共入口为临时 tunnel，属于本次运行配置而非长期部署保证。真实注册证据保留于
`/private/tmp/gateway-control-joint-20v6w7tr/telegram-registration-evidence.json`。

## 4. 验收边界

真实 Control 发布用例对 Agent/Profile/Deployment 进行了拥有方静态编译校验，Gateway
固定的是真实已发布目标身份；本次模型与 Storage 配置明确为 **admission-only fixture**。
这不证明 Worker 已运行、Manifest 正文已由 Gateway 下载/重哈希、模型或 Storage 真实
可执行，也不证明完整机器人回复链路。Gateway 没有恒 true TargetReady 或伪造 Worker。

- 真实 Telegram 入站 → 持久 RunRequested：本报告已验收。
- Worker 执行、Manifest 内容读取/完整性验证、真实模型/Storage、完整回复：后续独立验收。
- 企微真实账户外部联调：不是本次 Telegram 结果的覆盖对象。

## 5. 可复核证据

- [脱敏 JSON 证据](evidence/telegram-inbound-20260906.json)
- 本机审计归档：`artifacts/channel-gateway-control-gci3-20260906/VERIFICATION.txt`（Git 忽略；可共享证据为上方已提交的脱敏 JSON）。
- 只读实查脚本：`/private/tmp/gateway-control-joint-20v6w7tr/inspect-inbound.go`。

实查命令在实际 Gateway 工作树运行，凭据仅从0600基础设施配置文件注入：

```bash
set -a
source /private/tmp/gateway-control-joint-20v6w7tr/gateway-database.env
source /private/tmp/gateway-control-joint-20v6w7tr/reconciler-nats.env
set +a
go run /private/tmp/gateway-control-joint-20v6w7tr/inspect-inbound.go
```

退出状态0，字面结果 `REAL_TELEGRAM_INBOUND=PASS`。JSON与本文均已在交付前重新读取核对。
