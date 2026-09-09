# 双 IM 与多节点生产验收报告模板

> 只填写脱敏元数据和 PASS/FAIL。禁止粘贴用户消息正文、回调 payload、回复正文、SecretRef、
> Token、Key、下载 URL、数据库 DSN/密码或平台截图。

## 基线

| 字段 | 值 |
| --- | --- |
| 验收时间（UTC） | `<YYYY-MM-DDThh:mm:ssZ>` |
| Commit | `<full sha>` |
| Image digest | `<sha256:...>` |
| Compose project | `<non-secret project name>` |
| 配置版本 | `<number only>` |
| 验收人 | `<team/role>` |

## 自动化与 Compose 证据

| 门禁 | 结果 | 脱敏证据 |
| --- | --- | --- |
| `./check.sh` | PASS / FAIL | `<job/run id>` |
| Gateway `/healthz`、`/readyz` | PASS / FAIL | `<timestamp>` |
| Gateway-only 无 Runner/consumer | PASS / FAIL | `<test name>` |
| Worker-only 无生产 HTTP 端口 | PASS / FAIL | `<test name>` |
| `worker-a`、`worker-b` 唯一注册和心跳 | PASS / FAIL | `<count only>` |
| Redis Streams 被两个 Worker 消费 | PASS / FAIL | `<distinct worker count>` |
| 同一 RunRequest 只执行一次 | PASS / FAIL | `<test name>` |
| Worker 停止后的新请求/过期 claim 接管 | PASS / FAIL | `<bounded duration>` |
| 重复 `message_id` 不重复运行 | PASS / FAIL | `<Inbox row count>` |
| 旧 fencing token 拒绝 Session/Event/Memory/Outbox 提交 | PASS / FAIL | `<test name>` |
| 同 session 跨 Worker 顺序 | PASS / FAIL | `<test name>` |
| PostgreSQL Session/Event/Memory 持久 | PASS / FAIL | `<counts before/after>` |
| 配置版本重启后不回退 | PASS / FAIL | `<version before/after>` |
| 两租户同 app/session/message ID 隔离 | PASS / FAIL | `<test name>` |
| 企业微信/飞书同外部 ID 与 binding 隔离 | PASS / FAIL | `<test name>` |
| 日志、错误和验收输出无 secret/正文 | PASS / FAIL | `<review method>` |

## 人工真实平台验收

| 通道 | 结果 | 链路（不得附正文或凭据） |
| --- | --- | --- |
| 企业微信 | PASS / FAIL | Callback → Inbox → Runner → Model → PostgreSQL → Outbox → WeCom |
| 飞书 | PASS / FAIL | Callback → Inbox → Runner → Model → PostgreSQL → Outbox → Feishu |

人工验收只记录匿名 tenant/binding 标签、时间和结果。任何平台截图保留在受控工单系统，不提交仓库。

## 故障与恢复

| 故障 | 注入方式 | 恢复上限 | 结果 |
| --- | --- | --- | --- |
| 单 Worker 停止 | `docker compose ... stop worker-a` | `<seconds>` | PASS / FAIL |
| Worker 重新加入 | `docker compose ... start worker-a` | `<seconds>` | PASS / FAIL |

确认未执行 `down -v`、`volume rm`、数据库 truncate/drop，且 PostgreSQL/Redis 数据卷仍为原卷：`是 / 否`。

## 结论

- 发布建议：`GO / NO-GO`
- 未通过项：`<仅写受控错误类型和 issue 链接>`
- 回滚点：`<previous image digest / config version>`
