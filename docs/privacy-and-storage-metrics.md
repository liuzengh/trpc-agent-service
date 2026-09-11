# 租户文本治理与后端延迟指标

## 本轮补齐

2026-09-11 增加租户级文本隐私策略，以及 Session/Memory/平台存储的 Prometheus 延迟直方图和示例告警。此前三个 Review 缺陷的修复保持不变。

### 启用文本策略

在新的租户 revision 中配置，按现有 release/rollback 流程发布：

```yaml
privacy:
  input: redact
  output: block
```

| 值 | 行为 |
| --- | --- |
| `off` 或省略 | 保持旧版本行为 |
| `redact` | 将命中的文本替换为 `[REDACTED]` |
| `block` | 命中时拒绝当前输入/回复；不因策略拒绝自动重试模型 |

旧配置省略 `privacy` 时 JSON 编码不新增空字段，避免破坏已持久化 revision 的内容摘要。新增/修改策略必须使用新的 version，不会悄悄改变现有租户或已接收 Task 的策略快照。

当前规则识别邮箱、中国大陆手机号、`token/secret/password/api_key` 等带值字段，以及当前租户可解析的运行时密钥。手机号按数字边界匹配，避免把长整数 ID 的中间部分当手机号；JSON 参数用 `UseNumber` 保留大整数。仅当前租户的密钥参与匹配，不共享其他租户的值。迁移凭据、禁用通道不属于此处运行时密钥解析范围；启用的 `secret://` 引用解析失败时拒绝继续，错误只包含稳定类别。未设置的本地 env 交给原有后端初始化规则检查，不妨碍已配置的 mock fallback。

输入先移除审批 nonce，再对完整用户提示词（含附件元数据）应用策略，之后才传给 Runner。模型适配器外再包一层策略，对每次模型请求中的 system/history/tool 文本、reasoning、文本 ContentParts 和工具调用 JSON 参数处理，覆盖历史记录、预载 Memory、Knowledge 结果及 Summary 的文本入模路径。它位于预算预留/provider dispatch 之前，拒绝时不会先发出模型请求。原始共享消息切片、文本指针和参数字节不会原地修改。

出站在 Runner 事件排空、回复聚合后处理完整正文，因此跨流式片段的邮箱/手机号不会从 IM 输出端绕过规则。处理结果再进入输出安全检查和 canonical replay/Outbox。`block` 不提交该轮回复；`redact` 的 SQL canonical 及消息重放均使用已脱敏正文。已经发生的模型消费不会因出站拒绝而退款。Worker 的遮蔽/拒绝只记录 `privacy_decisions_total` 和内容 hash 审计，不记录命中值。

### 明确边界

这是可配置的规则型文本治理，不是语义 DLP、OCR、病毒扫描或可逆 tokenization。它不改写历史数据库存量，不加密 Inbox/Session，也不承诺识别姓名、地址、所有国际号码或任意未知密钥。模型边界遇到未检查的二进制/媒体内容会拒绝；当前 IM 附件仍是元数据路径。工具向外部业务系统写数据的资源级授权与沙箱需要独立治理，不能将本策略当作其替代。

### 指标与告警

`Metrics.Observe` 现在输出标准 histogram：`_bucket{le=...}`、`_sum`、`_count`，一次观察在同一把锁内更新全部累计样本。保留既有 sum/count 名称，同时支持 P95/P99 查询。

所有 `StartStorage` 边界接到当前服务的 `/metrics`，指标名为 `storage_operation_duration_seconds`。标签仅 tenant 的 hash、backend、固定 operation（component）和稳定 result 类别，没有 user/session/request/revision 标签。错误原文、DSN 和凭据不进入指标。同一个 finish 重复调用不会重复计数。

Runner 的 Session SPI 包装器覆盖创建、读取、列表、状态、事件追加及摘要方法，适用于 InMemory/Redis/SQL。Worker 仍保留原始 Session 对象上的 strict transaction/database identity 能力，避免包装器改变原子提交协议。InMemory/Redis Memory 的读、搜索、增删改与队列提交边界也有度量；队列提交耗时不应解读成后台提取全部完成的耗时。已有 SQL、Artifact、向量库、Audit、Inbox/Outbox 等存储 span 同步产生指标。外部 Mem0 内部网络/后台 ingestion 的完整细分指标仍不在本轮范围。

示例查询：

```promql
histogram_quantile(0.95,
  sum by (le, tenant, backend, component)
    (rate(storage_operation_duration_seconds_bucket[5m])))
```

`observability/alerts.yaml` 提供存储 P95 > 1 秒、错误率 > 5% 持续 5 分钟的告警，并要求每秒观测量 > 0.1，避免低流量单样本误报。Compose 已挂载该规则文件，Prometheus 保留 exporter 自带的 component 标签。阈值是可调整的起点，通知接收方/Alertmanager 仍需部署环境配置。

## 剩余缺口

| 范围 | 尚未完成的闭环 |
| --- | --- |
| IM 多媒体 | 真实图片/文件内容获取与安全扫描、出站附件/卡片、编辑与撤回；目前仍以文本回复及安全元数据为主 |
| IM 流控 | 共享的 provider/binding 出站主动限频；已有 Retry-After、退避和 unknown 人工决议 |
| 治理深化 | 语义 DLP、可逆 tokenization、资源级工具授权/隔离沙箱、真实密钥轮换联调 |
| 外部一致性 | provider 查询/自动 reconciliation、unknown 费用公共修正接口；已存在保守停车和人工流程 |
| 迁移与灾备 | 在线双写及路由自动切换、真实备份恢复/PITR、真实外部服务网络分区和故障切换验收 |
| 交付 | GitHub 远端发布状态仍未验证；当前父仓库中的 solution 仍为未跟踪目录 |

赛题中“设计/说明”与“实际能力”应继续分开评价：例如维护窗口迁移已提供，不必为了列举在线双写缺口而宣称赛题的数据迁移要求完全没有完成；同样也不能把多媒体限制说明等同于已实现多媒体收发。
