# ADR-0006：租户可观测性与会话正文权限分离

- 状态：**已接受**
- 日期：2026-09-10

Tenant Admin 需要能够排查自己 Tenant 下 Agent 的运行状态，但“管理运行”不等于“默认读取其他用户的聊天正文”。因此平台把租户会话管理、执行追踪和会话内容审计拆成三类能力。

1. Tenant Sessions 默认只展示会话元数据，例如用户、Agent、Channel、会话类型、消息数量、最近活动与状态；其他用户私聊正文默认不可读。
2. Tenant Execution Records 对 Tenant Admin 开放，用于排障、成本和可靠性观察。安全投影可以包含状态、耗时、模型、Token、成本、工具名称、错误类型、Trace ID、重试与投递状态，但不默认保存或展示原始 Prompt、完整模型输出和敏感工具参数。
3. 如果部署场景确实需要客服质检、合规调查等正文读取能力，单独启用 Conversation Content Audit；该能力默认关闭，必须显式授权，并且每次读取行为本身也进入审计日志。
4. System Admin 不因平台级身份自动获得上述 Tenant 业务视图；仍需成为当前 Tenant Admin。

这个边界避免把“可运维”错误实现成“可任意读用户内容”，同时保留 Tenant Admin 对 Agent 失败、成本、Tool 调用和投递问题的实际排障能力。
