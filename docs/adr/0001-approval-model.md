# ADR-0001: 人工审批采用「同步挂起」模型

- 状态：已确认（2026-09-02，阶段 13）
- 决策者：用户 + AI 对齐（grill-with-docs）
- 关联术语：见根目录 `CONTEXT.md`「审批治理」

## 背景

平台复用 tRPC-Agent-Go `plugin/guardrail/approval` 提供工具级审批。该插件的评审契约是**同步**的：`review.Reviewer.Review(ctx, *Request) (*Decision, error)` 在工具执行前（BeforeTool 钩子）被调用，返回决策后工具才放行/拒绝。框架不提供异步人审状态机。

需要人工审批（而非自动 AI 评审）时，Reviewer 必须能"等待人类在 IM 会话中回复批准/拒绝"。

## 决策

采用**同步挂起**模型：

1. 平台自实现 `review.Reviewer`（human reviewer）：外发审批通知 → 阻塞轮询 Redis 结果键 → 拿到人工决策后返回 `Decision`。
2. 触发策略**双轨**：Agent 发布勾选 `approval_tool_ids` ∪ 工具元数据 `risk_level=high` 自动触发（任一命中即需审批）。
3. 交互为**会话级单 pending**：用户直接回复「批准/拒绝」等自然语言变体，无编号。
4. 为使同步挂起可行，配套三项基础设施改动：
   - `bus.ConsumeInbound` 改为**有界并发消费**（每消息 goroutine + 信号量上限），否则单 worker 顺序消费下，用户的批准回复排在阻塞轮次之后形成死锁；
   - 审批等待期间**自动续期会话锁**（Lua compare-token + EXPIRE），防止 30s 锁 TTL 过期导致同会话并发处理；
   - 审批回复在 `handle()` **取锁之前**被无锁识别（`tryResolveApproval`），只写决策键并 ack，不进 agent 对话。

## 被否决的选项

- **异步重放**：触发审批即结束本轮，批准后把原消息重放给 agent 再跑一轮。优点是不改并发模型；缺点：模型每轮审批多消耗一轮推理、agent 可能以不同参数重发、语义与"真实暂停该工具调用"不完全等价。否决。
- **纯自动 AI 评审**（框架内置 `review.New` guardian）：适合风险自动过滤，但无法满足"人类拍板"的治理诉求，作为后续增强而非本决策替代。

## 权衡与影响

- 精确性：工具调用被真实暂停，批准后原参数继续，审计语义清晰。
- 并发面：bus 消费模型从顺序变为有界并发；同会话正确性仍由会话锁串行 + Redis/MySQL 双幂等兜底，不新增正确性风险。
- 资源：等待期占用一个 worker 槽位（信号量上限内），由超时（默认 5 分钟）收敛。
- 超时/拒绝：reviewer 返回拒绝类 Decision，approval 插件以 `CustomResult` 回给 LLM，agent 可继续对话解释。

## 验证

- bus：有界并发消费 + 审批键/续期锁集成测试（testcontainers Redis）。
- worker：审批全链路集成测试（testcontainers MySQL + Redis）——需审批工具触发 → 外发通知 → 模拟用户批准回复 → 原调用放行。
