# 租户使用治理 V1

状态：已实现（2026-09-10）

## 1. 目标与边界

租户后台成员身份、IM 消息发送者身份和运行资源额度是三条不同的授权链：

- Control 的 `Tenant Membership` 决定谁能查看和修改策略；
- Gateway 根据 Bot/Channel Account、Binding、用户或群清单决定消息能否进入；
- Worker 根据租户共享状态决定 Run 能否占用并发和 Token 额度。

V1 只提供一个租户当前策略，不恢复历史的复杂授权与预算框架。策略 Revision 只用于
CAS 并发控制；更新覆盖当前行，不保留策略历史。V1 不包含充值、支付、发票、套餐、
组织目录同步、任意策略表达式或供应商账单对账。

## 2. 职责和数据流

```text
Tenant Owner -> Control Web -> Control API -> tenant_usage_policies
                                            |
IM update -> Channel Gateway -- mTLS GET ---+
     | 入口 allowlist + 共享每分钟计数
     v
RunRequested(固定 execution/token 策略快照) -> Worker
     | 共享租户并发检查 + Token 预留
     v
Provider usage -> 已知值结算 / 未返回则 UNKNOWN 并保留预留
     |
Worker usage projection -> Control API -> Control Web
```

Control 是当前策略 Owner，只向 Active Member 提供读取，只有 Active Owner 可以替换。
Gateway 不读取 Control 数据库；它通过已有 mTLS Control Client 读取策略。Worker 不在执行
时回读可变策略；Gateway 把这次接纳所用的执行与 Token 策略写入不可变
`RunRequested.v1`。Gateway 和 Worker 各自只访问自己的共享 PostgreSQL 表。

## 3. 当前策略

`Policy(schema_version=1)` 包含：

- `im.allow_all` 或至多 256 条精确规则；规则以 `account_id` 为必填范围、
  `binding_id` 为可选窄化范围，并分别列出 `user_ids` 和 `group_ids`；
- `requests.tenant_per_minute` 与 `requests.user_per_minute`；
- `execution.max_concurrent_runs`；
- `tokens.period_seconds`、`limit` 和 `reservation_per_run`；
- 输入/输出每百万 Token 的微美元估算单价。

身份均按不透明字符串精确匹配，不猜测、不归一化。列表必须排序、去重。启用策略必须
完整给出四组约束；关闭策略不得隐藏非零限制。没有策略时 Control 返回 Revision 0 的
显式关闭策略，保持旧租户兼容。

## 4. 入口授权与限流

Gateway 在路由解析后取得 Tenant、Account 和 Binding，再读取当前策略：

1. `allow_all=true` 时允许任意 IM 身份；否则当前 Account/Binding 必须命中一条规则，且
   `SenderID` 命中用户清单或 `ConversationID` 命中群清单；
2. 被拒绝的请求不生成 Run；策略读取失败时 fail closed；
3. 获准请求在 Gateway 自有事务中通过租户 advisory lock，同步增加租户和用户两个
   固定分钟窗口；任一达到上限，整个事务回滚，不消耗另一维计数；
4. 成功接纳时把同一策略 Revision 的 Worker 所需字段固定进 Run 事件。

计数表由所有 Gateway 副本共享，因此扩容不会给每个副本额外一份额度。窗口清理是后续
运维任务；历史窗口不会参与当前分钟判断。

## 5. Worker 并发、预留与结算

Worker Claim 在原有 Run 行锁之外获取租户 advisory lock，所有 Worker 副本按同一事务
顺序执行以下检查：

1. 活跃且 lease 未过期的租户 Attempt 数必须小于 `max_concurrent_runs`；
2. 当前 epoch 对齐周期内，已知用量按 `total_tokens` 计算，UNKNOWN 或 PENDING 按
   `reservation_per_run` 计算；
3. 新预留加入后不得超过事件快照中的 `token_limit`；
4. Claim 成功才创建 Attempt 与预留，两者同事务提交。

模型返回可信 usage 时，Worker 以输入/输出/总 Token 幂等结算，并用策略快照里的单价
计算估算成本。Provider 未返回 usage、模型调用结果不确定或调用失败时，结算为
`usage_known=false`，Token 字段保持 `NULL`，预留继续占用；绝不将未知记为零。模型调用
开始前明确失败可结算为已知零。进程中断后未结算 Attempt 维持 PENDING 和预留，避免
故障绕过额度。

这是一种保守的配额门禁，不宣传为绝对硬预算：Provider 可能在失败窗口消耗用量，输入
计数可能与 Provider 不同，且在途外部调用无法被 PostgreSQL 原子撤销。

## 6. 用量与成本投影

Worker 暴露租户最新周期的：`used_tokens`、`reserved_tokens`、
`unknown_usage_count`、`pending_usage_count` 和 `estimated_cost_micros`。Control 完成成员授权
后代理读取，Web 分开展示已知用量和预留/未知；投影不可用时明确显示不可用，不伪装成零。

估算成本只使用策略配置单价和已知 Provider usage，是平台内估算，不是 Provider 账单；
UNKNOWN/PENDING 不产生虚假成本数，但仍保留 Token 预留。

## 7. HTTP、事件和持久化

公开 Control API：

- `GET /v1/tenants/{tenant_id}/usage-policy`：Active Member；
- `PUT /v1/tenants/{tenant_id}/usage-policy`：Active Owner，要求
  `expected_revision` 和 `Idempotency-Key`；相同 key/载荷返回原结果，不同载荷返回 409；
- `GET /v1/tenants/{tenant_id}/usage-summary`：Active Member，从 Worker 读取。

内部接口为 Gateway 提供 mTLS 鉴别的当前策略读取。事件只携带执行门禁需要的非秘密
快照，不携带 IM allowlist。数据库迁移分别归属 Control `0010`、Gateway `0017` 和
Worker `0021`；不存在跨 Workload SQL。

## 8. 最小验收案例

- Tenant Owner 保存策略后，Member 可读但不能修改，过期 CAS 返回冲突；
- 指定 Account/Binding 的允许用户或群可生成 Run，未列出的身份被拒绝；
- 多 Gateway 副本共用租户/用户分钟计数，第二维失败不留下半次计数；
- 多 Worker 副本同时 Claim 时，租户 advisory lock 保证并发和 Token 预留不超配；
- 已知 usage 释放预留并按真实总量计入，缺失 usage 显示 UNKNOWN 且保留预留；
- Web 同时显示已用、预留、未知、待结算和估算成本，并声明不是供应商账单；
- 关闭/缺失策略保持现有租户通路，不产生隐藏限制。

## 9. 后续扩展

后续可独立增加滚动窗口、Redis 计数 Adapter、按模型/Agent 的细分额度、管理员清理过期
UNKNOWN、Provider 对账和告警。只有出现真实产品需求时才讨论策略历史、审批、套餐或
计费中心；这些扩展不得改变 Gateway 和 Worker 只消费窄运行契约的解耦边界。
