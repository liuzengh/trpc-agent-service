# Channel V1 联合验收记录（2026-09-06）

## 结论

Control ChannelAccount/ChannelBinding 与 Gateway ControlSource 已在各自独立工作树实现，
真实 Telegram 消息已完成 `Webhook → Inbox/Receipt → Admission → Outbox → NATS RunRequested`。
本次不是将HTTP fixture当作Telegram事件；消息从用户电脑上的Telegram应用发给
`@jfschannellab260904bot`，通过真实Telegram服务器投递到Gateway公开HTTPS webhook。

## 本次实际状态

| 项目 | 已验证结果 |
| --- | --- |
| Control 工作树/分支 | `/Users/jfs/Projects/trpc-agent-service-channelbinding` / `codex/control-channelbinding-v1` |
| Gateway 工作树/分支 | `/Users/jfs/Projects/trpc-agent-service-channel-gateway` / `codex/channel-gateway` |
| 公开Control | `http://127.0.0.1:18080`，health 204 |
| 内部Control | `https://127.0.0.1:18081`，真实mTLS snapshot 200 |
| Gateway | public 18090 / admin 18091，readyz 204；公开origin不暴露admin |
| Telegram注册 | 真实getMe身份匹配；getWebhookInfo目标URL精确匹配；远端注册持久READY |
| Credential | Control数据库中两条模块私有加密值；Gateway只经mTLS按用途取得同版本值 |
| Control Route Outbox | generation2，真实JetStream持久确认后PUBLISHED |
| Gateway观测 | connection_revision2 / READY / reason NONE；不与路由应用ACK混淆 |

临时公开origin由Gateway任务的cloudflared提供，只转发public listener；它不是长期生产地址。
运行中的本地进程、数据库和Broker为专用联合实例，不与可重置回归测试环境共用。

## 真实消息的持久证据

- 唯一标记：`gateway-v1-real-20260906-0637`。
- 实际发送及Gateway接收时间：2026-09-06 06:55（Asia/Shanghai）。
- Telegram update_id：`309271229`；message_id：`6`。
- Receipt decision：`admit-run`。
- Admission/Event ID：`c00f77d6b3a94dcd98b77ccf071b047b`。
- Run ID：`45ccc1ec2d4b600a656ed0b489497e5c`。
- Inbox=1、Admission=1、Outbox=1、published Outbox=1；Delivery intents=0。
- NATS：`RUN_REQUESTS_V1` / `execution.run-requested.v1` / stream_sequence=1。
- `Nats-Msg-Id`与Event ID一致；持久FileStorage中恰好一条匹配消息，严格wire Schema通过；
  规范化正文与PG Outbox完全相同。独立复核只调用StreamInfo/GetMsg，没有创建Consumer或ACK。

固定目标：

```text
Tenant: tnt_o4hb8SN9Hjfrmtlj4fdfN9dm
Account: cha_ZHZwYc0n54Tc0ZW3KlxBaemO
Binding: chb_m6ZeGHUmZzf_zfSNV-cd4020
RouteGeneration: 2
DeploymentRevision: dpr_TN-yxwXsqO0LyR9G3gfPlbpt
RuntimeManifest: rmf_CK7i4jR0iI5NBpJJmkNP02H5
ManifestDigest: sha256:e10102a9f63cabe35c465f9ddc19c8396c62b4ebce4085767e70602732c3c5bf
```

该目标通过真实Agent/Profile/Deployment管理API生成并由拥有方静态编译校验，再被Binding读取。
Control持久Manifest摘要、Binding目标和Gateway RunRequested三元组已交叉匹配。
原始PubAck网络帧没有单独留存；代码在收到正确stream的PubAck后才写published_at，数据库
published_at与可读取的对应持久消息形成交叉证据，不声称捕获了原始ACK帧。

## 实现与回归覆盖

| 要求 | 当前实现及直接证据 |
| --- | --- |
| 账户与绑定领域、闭合输入、版本/CAS | Domain + canonical JSON Schema/fixtures + 7个管理命令Application |
| 成员读/OWNER写、真实Session | public HTTP + Identity/Tenant拥有方Adapter；真实HTTP集成 |
| 租户、唯一身份、凭据用途、配额 | PostgreSQL外键/唯一键/CHECK + 提交期授权 + quota/null guard测试 |
| 原子变更、Receipt、Route及目录floor | 同一PG事务；失败注入回滚、同key并发、启停/绑定竞争测试 |
| 完整不可猜测快照 | RR一致性读、固定source epoch、Canonical Digest、数量/字节上限 |
| 真实凭据供应和撤销/轮换 | mTLS URI SAN映射；准确用途/连接版本；取值与disable/replace串行化 |
| 观测而非授权 | seq冲突/重放不刷新freshness、server received_at、TTL/清理测试 |
| Outbox真实发布 | 受限Producer、15秒lease/token fencing、5秒PubAck预算、失败保留/重投测试 |
| Gateway对齐 | 生产Bootstrap、ControlSource、可信Credential Bridge、动态注册/fence、Admission、观测 |
| 真实Telegram收信 | 上述真实update/receipt/run和NATS持久证据 |

Control实际树已执行真实PG/受限NATS下race回归：36个测试包、953个测试通过事件
（包含子测试）、0条测试skip。修改前及独立回滚副本：35个包、928个测试通过事件、0 skip。
全仓`go test ./...`、`go vet ./services/control-api/... ./api/...`及Control构建也已通过；
普通无环境变量测试不能代替上述真实PG/NATS记录。

## 证据位置与不作的推断

- Control四件套及原始测试输出：`/private/tmp/channel-postgres-http-20260906.w11vpc41/`。
- Control独立复核：该目录`JOINT_CONTROL_EVIDENCE.json`、`JOINT_GATEWAY_EVIDENCE.json`、
  `JOINT_GATEWAY_VERIFY.log`。
- Gateway拥有方报告：`/private/tmp/gateway-control-joint-20v6w7tr/telegram-inbound-evidence.json`；
  Gateway代码验证报告位于其`artifacts/channel-gateway-control-gci2-20260906/`。
- 报告不包含真实Token、DSN、sender/chat ID和用户消息全文。

本验收完成收信和固定目标接纳，不包含Worker执行、模型调用或机器人回复。模型/Storage
配置使用明确标注的admission-only fixture，静态发布成功不代表这些Provider真实可用。
未引入恒真TargetReady、动态注册中心、Secret产品、Environment或用户资源映射表。
代码保留在两边各自工作树；没有自动commit/push/合入main。源代码回滚仅恢复本轮源码，
不假装撤销已产生的业务数据库、NATS消息或Telegram远端配置。
