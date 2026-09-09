# Inbox / Outbox 故障恢复

Admin API 为 PostgreSQL 中已经隔离的 Inbox DLQ、Outbox DLQ 和 Outbox `uncertain`
提供生产 Admin API。接口复用 Admin Bearer 认证和 URL tenant scope；请求体没有
`tenant_id`，服务也不会返回消息正文、外部用户、收件人、session 或 `last_error`。

## 状态与安全边界

- Inbox/Outbox `dlq` 可以执行 `redrive`，状态以
  `(tenant_id, message_id, expected_status=dlq)` CAS 更新到 `retry`。并发操作只有一个成功，
  其余返回 `409 Conflict`。
- Outbox `uncertain` 表示平台可能已经收到了消息，普通 `redrive` 不接受该状态。管理员必须
  调用 `resolve`，选择 `sent`（人工确认已经送达）或 `retry`。选择 `retry` 时必须显式提交
  `acknowledge_duplicate_risk=true`。
- 人工恢复会清空旧 worker claim，把单轮自动尝试计数归零；历史操作不依赖该计数，始终以
  追加式审计日志保存 actor、action、message id、旧/新状态、decision、error type、latency、
  trace id、reason 和 timestamp。
- reason 用于要求管理员明确说明操作依据；审计只保存其 SHA-256 指纹，不保存自由文本原文。
  仍不要在 reason 中填写 token、密钥、消息正文或个人信息。

## API

以下令牌都是占位符。生产令牌只通过环境或 Secret Manager 注入，不写入仓库。

```bash
# 查询 Inbox DLQ（最多 200，默认 50）
curl -H 'Authorization: Bearer <admin-token>' \
  'http://127.0.0.1:8080/v1/tenants/demo/operations/inbox?status=dlq&limit=50'

# 查询 Outbox DLQ 和结果不确定项
curl -H 'Authorization: Bearer <admin-token>' \
  'http://127.0.0.1:8080/v1/tenants/demo/operations/outbox?status=dlq,uncertain'

# 重放 DLQ
curl -X POST -H 'Authorization: Bearer <admin-token>' \
  -H 'Content-Type: application/json' \
  -d '{"id":"<outbox-id>","expected_status":"dlq","reason":"provider incident recovered"}' \
  'http://127.0.0.1:8080/v1/tenants/demo/operations/outbox/redrive'

# 人工核对后确认 uncertain 已送达
curl -X POST -H 'Authorization: Bearer <admin-token>' \
  -H 'Content-Type: application/json' \
  -d '{"id":"<outbox-id>","expected_status":"uncertain","decision":"sent","reason":"confirmed in provider console"}' \
  'http://127.0.0.1:8080/v1/tenants/demo/operations/outbox/resolve'

# 人工核对后承担重复风险并重试 uncertain
curl -X POST -H 'Authorization: Bearer <admin-token>' \
  -H 'Content-Type: application/json' \
  -d '{"id":"<outbox-id>","expected_status":"uncertain","decision":"retry","reason":"provider confirms no delivery","acknowledge_duplicate_risk":true}' \
  'http://127.0.0.1:8080/v1/tenants/demo/operations/outbox/resolve'
```

`404` 表示该租户下不存在此 ID，`409` 表示消息已被其他管理员或 Worker 改变状态，
`422` 表示状态/决策不合法。重试 `409` 前应重新查询，不应盲目重复提交。
