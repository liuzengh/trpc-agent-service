# ADR-0008：租户与 Agent 使用停用而非直接删除

- 状态：**已接受**
- 日期：2026-09-10

## 背景

Tenant 和 Agent 下挂 Session、Memory、Knowledge、Artifact、Channel Binding、Execution 与 Audit。把普通管理操作直接定义为物理删除，会把运行停用、访问撤销、审计保留和数据销毁混为一件事。

## 决策

1. Tenant 当前只提供 `active / suspended` 生命周期。System Admin 可以停用和恢复 Tenant；当前产品不提供一键物理删除 Tenant。
2. Tenant 创建由 System Admin 发起，并与至少一个初始 Tenant Admin Membership 一起建立，避免产生无人管理的 Tenant。
3. Agent 使用 `draft / active / disabled`。Draft 发布后才能接受新消息；Disabled 不接受新的 Web/IM 请求，但保留历史 Session、Execution、Audit 与配置版本。
4. Tenant/Agent 的最终数据销毁属于单独的 retention/purge 能力，不复用普通停用按钮。
5. Tenant 或 Agent 被停用时只阻止新的 Admission。停用前已经成功受理的 Run 正常排空、提交和回复，避免制造“消息已接收但无可追溯结果”的中间状态。
6. Active Tenant 始终至少保留一个 Active Tenant Admin；最后一个管理员不能通过普通成员管理动作被移除、降级或停用。
7. Platform User 被移出 Tenant 后，历史 Session、Artifact、Audit 等事实继续保留原 owner，但该用户立即失去当前 Tenant 的访问权。若其 External Identity 仍可通过 public Binding 发消息，也不能因为旧 `platform_user_id` 关联而读取已经失去授权的私人 Tenant Memory。

## 后果

- 管理操作可逆，不会因为一次误点破坏审计和历史事实。
- 数据保留与真正销毁可以独立满足合规、成本和恢复需求。
