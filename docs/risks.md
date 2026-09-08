# 生产风险清单

## 风险分级

- P0：可能造成跨租户泄露、不可逆业务损失或大面积不可用；
- P1：影响多个租户或导致持续数据不一致；
- P2：局部功能退化，有明确恢复路径。

| 编号 | 风险 | 等级 | 触发场景 | 监控信号 | 缓解措施 |
| --- | --- | --- | --- | --- | --- |
| R01 | 同一 Session 被两个 Worker 同时执行 | P1 | 队列 rebalance、租约续期失败、网络分区 | lease conflict、同 conversation 活跃 run 大于 1 | Redis 租约、fencing token、提交 CAS；队列分区仅作可选优化 |
| R02 | IM 重投导致重复模型调用和重复回复 | P1 | ACK 丢失、上游超时重试 | inbound unique conflict、重复 request | 外部消息唯一索引、稳定 request_id、复用 agent_run/outbound |
| R03 | 副作用 Tool 重复执行 | P0 | Worker 在 Tool 成功后崩溃，恢复时盲目重跑 | 同 idempotency key 多次调用、uncertain tool execution | tool journal、业务幂等键、先查询后重试、补偿与对账 |
| R04 | 自动 Summary 或 Memory 任务丢失 | P1 | 使用进程内 channel，Pod 在消费前退出 | summary/memory lag 长期增长、水位不推进 | durable queue、outbox、任务重试和死信 |
| R05 | 旧 Summary 覆盖新 Summary | P1 | 异步任务乱序、不同 Worker 同时总结 | summary watermark 回退、上下文突然重复 | high watermark 条件更新、按 Session 合并任务 |
| R06 | 向量检索漏加租户过滤 | P0 | 调用方覆盖 filter、直接按 document ID 读取 | 搜索结果 scope 不匹配、审计命中跨租户 ID | ScopedVectorStore 强制 filter、ID 命名空间、返回结果复核 |
| R07 | `ToolFilter` 被误当作授权 | P0 | 框架工具或动态工具绕过可见列表 | 未授权 tool execution audit | PermissionPolicy、Tool PermissionChecker、MCP wrapper |
| R08 | 群共享会话写入个人 Memory | P0 | `runtime_user_id` 使用合成群主体，自动提取未关闭 | 群主体出现个人事实、跨成员召回 | 明确群聊模式，共享模式禁用个人自动 Memory，个人工具独立 scope |
| R09 | 密钥进入日志、trace 或 Session | P0 | 打印配置、HTTP header、下游错误体 | DLP 扫描发现 token 形态 | SecretRef、统一 redaction、payload drop、日志采样审查 |
| R10 | S3 同名 Artifact 并发覆盖 | P1 | 多 goroutine/多节点同时计算下一版本 | 同 filename/version checksum 不同 | 进程锁、PostgreSQL advisory lock、Runner Session lease；直传场景再加 SQL allocator |
| R11 | Redis async persist 返回成功但实际未落盘 | P1 | 开启 `WithEnableAsyncPersist(true)` 后节点崩溃 | Session 缺 Event、后台 persist error | 生产保持同步 persist，把异步放到上层 durable queue |
| R12 | 配置灰度使同一会话来回切版本 | P1 | 每次请求重新按百分比选 revision | 同 session revision 变化、工具列表跳变 | conversation pin revision、稳定哈希、显式迁移 |
| R13 | 数据迁移双写不一致 | P1 | 主写成功、次写失败或回填遗漏 | repair backlog、checksum mismatch | repair outbox、水位、影子读、切读门禁、快速回滚 |
| R14 | Session 数据库故障时创建空会话继续回答 | P0 | 为了可用性忽略 GetSession 错误 | 突然出现新 session、上下文丢失 | Session 失败时 fail-closed，任务退避，不静默新建 |
| R15 | Event channel 未排空导致 goroutine 泄漏 | P1 | 客户端断开后停止读取 Runner Event | goroutine、active run、内存持续增长 | 独立消费 goroutine、取消后 drain、超时指标、优雅关闭 |
| R16 | IM 发送限流造成回复堆积 | P2 | 突发流量、长回复拆分过多 | delivery lag、429、outbox pending | 通道级令牌桶、retry-after、合并/切分策略、独立 Sender |
| R17 | 模型超时后切备用模型重新执行 Tool | P0 | fallback 从头运行完整 ReAct 流程 | 同 tool_call 业务操作重复 | Tool 结果先持久化，fallback 只基于已有结果生成回答 |
| R18 | 高基数监控拖垮指标系统 | P2 | tenant/user/session/request 全作为 label | Prometheus series 暴涨、采集延迟 | 指标只用聚合 label，具体 ID 放 trace/log/exemplar |
| R19 | 恶意文件、压缩炸弹或 SSRF | P0 | IM 文件、模型 URL、知识源指向危险地址 | 扫描失败、异常下载流量、内网探测 | 文件大小/MIME/压缩比限制、病毒扫描、URL allowlist、DNS 复核 |
| R20 | 租户成本失控 | P1 | Tool 循环、超长上下文、恶意用户刷请求 | token/cost 突增、模型调用次数异常 | 预算预留、最大循环、Session summary、租户和用户限流 |

## 上线门禁

以下检查未通过时不能进入生产：

- 跨租户 Session、Memory、Knowledge、Artifact 隔离测试全部通过；
- 同一 external message 并发投递 100 次只产生一个逻辑 run；
- Worker 在用户 Event、Tool 成功、最终 Event、outbound 写入等故障点退出后均能恢复；
- 危险 Tool 没有 approval 时无法执行；
- 日志和 trace DLP 扫描不包含测试密钥；
- Redis 到 SQL、Qdrant 到新 collection 的演练可以回滚；
- SIGTERM 后没有持续增长的 goroutine 或未释放租约；
- 备份可以在独立环境恢复并通过数据校验。

## 故障演练建议

每季度至少演练：

1. 随机杀死正在运行 Agent 的 Worker；
2. Redis 主从切换和短时不可用；
3. PostgreSQL 主库切换；
4. 模型供应商持续超时或限流；
5. Tool 返回超时但业务实际成功；
6. IM 发送 API 429 和 5xx；
7. 配置 revision 紧急回滚；
8. 向量库切读和回退；
9. Secret 轮换和旧凭据撤销；
10. 审计存储不可用时的 fail-open/fail-closed 行为。
