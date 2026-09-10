# Tenant 运行时边界

本页描述 issue #5 的租户运行时边界：怎样把 tenant 包的配置快照交给 Gateway、Worker、IM
Channel 和 tRPC-Agent-Go Runner。平台层负责可信租户快照和身份隔离，执行循环继续复用上游框架。

## 一次执行的边界

1. Channel Adapter 验签、用绑定关系解析可信的 tenant_id，不得接受请求头或消息载荷中的 tenant ID。
2. 控制面以该 ID 读取 tenant，先执行 `tenant.Validate` 校验完整根实体不变量；只有 `active` tenant 才能创建新的 `tenant.ConfigurationSnapshot`。快照包含固定 version，不含模型 API key、IM token 或后端密码；创建快照后 tenant 被暂停不会影响该次执行收尾。
3. Gateway 用 tenant.WithConfigurationSnapshot 将快照放入执行的 context.Context，然后将同一个 context 传给 tRPC-Agent-Go 的 runner.Runner、Tool 和 Storage 调用。
4. Worker 在执行期间只消费快照。配置更新会产生新的 tenant version，只影响后续执行，避免一次执行混用两套配置。

ConfigurationSnapshot 只允许通过受校验的构造器创建，不暴露可直接写入的 tenant 字段；Tenant() accessor 和 ConfigurationSnapshotFromContext 都返回副本，因此调用方不能伪造或修改其他组件看见的租户状态。未初始化的快照不会被放入 context，也会遮蔽 context 中已有的快照。

## Runner 身份

现有 `tenant.NewRunnerIdentity(tenantID, externalUserID, externalSessionID)` 没有 `binding_id`
参数，因此平台 Adapter 必须先构造 binding-scoped 输入，再调用它：

```text
binding_scoped_user = Encode(binding_id, external_user_id)
binding_scoped_session = Encode(binding_id, external_session_id)
tenant.NewRunnerIdentity(tenant_id, binding_scoped_user, binding_scoped_session)
```

`Encode` 使用长度前缀或结构化编码，避免简单字符串拼接发生碰撞。IM Adapter 决定
`externalSessionID`：单聊通常由外部用户标识组成，群聊必须包含外部群标识；平台再把
`binding_id` 纳入 UserID 和 SessionID。Binding Adapter 的 conformance test 必须验证同一
Tenant、用户和外部 Session 在不同 Binding 下生成不同的 Runner UserID/SessionID，同一 Binding
重放保持稳定。无论 Runner 使用什么字符串，平台的持久化查询仍必须将独立的 `tenant_id` 和
`binding_id` 作为条件，不能把 key 前缀视为唯一授权隔离手段。

## 已交付运行时边界

| 需求 | 直接复用 tRPC-Agent-Go | 平台实现 |
| --- | --- | --- |
| 执行取消和链路传递 | context.Context 与 runner.Runner | 将可信配置快照附加到 context |
| 用户/会话命名空间 | Runner 的 userID / sessionID | 生成无歧义的租户前缀身份 |
| 开发期会话/记忆 | InMemory Session / Memory 能力 | InMemoryRepository 保存 tenant 控制面数据 |
| Agent、Tool、Channel | Runner、Agent 编排、Tool/MCP、OpenClaw Channel | Gateway、Channel Adapter 与 binding-aware dispatch |

InMemoryRepository 用于开发和确定性测试；PostgreSQL、Redis、S3-compatible capability、队列和
Outbox 通过各自 runtime adapter 提供持久化与跨节点协作路径。所有实现继续以显式 tenant 条件
和双租户 conformance test 验证隔离。

状态迁移要求非空的 actor、reason 与 correlation ID；reason 最长 1000 个字符。返回的
StatusChangeEvent 会进入审计与 Outbox 记录，形成控制面变更的可追踪事件。
