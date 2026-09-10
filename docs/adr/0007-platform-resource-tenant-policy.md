# ADR-0007：平台资源与租户策略分层

- 状态：**已接受**
- 日期：2026-09-10

## 背景

题目要求不同 Tenant 可以选择不同模型、数据后端和工具能力，但模型 API Key、数据库密码、Redis 凭据、对象存储 Secret 等敏感资源又不能自然下放给每个 Tenant Admin。若 Tenant 直接维护所有底层凭据，会把租户自治和平台基础设施管理耦合在一起，也会扩大密钥暴露面。

## 决策

1. **平台实例拥有基础资源，Tenant 拥有使用策略**。System Admin 管理 Model Asset、Backend Profile、Secret Provider 等平台级资源；Tenant Admin 只能在明确授权给当前 Tenant 的资源集合中选择。
2. **模型采用两层选择**：平台维护 Provider、模型能力和凭据；Tenant Model Policy 决定当前 Tenant 可用模型；Agent 配置再从 Tenant 可用模型中选择实际模型。
3. **数据后端采用两层选择**：平台维护 PostgreSQL、Redis、向量库、对象存储、外部 Memory 等 Backend Profile；Tenant Storage Policy 决定 Session、Memory、Knowledge、Artifact 等数据域使用哪个已授权 Profile。
4. Tenant Admin 只看到资源名称、能力和可用状态，不读取平台级明文凭据。凭据更新使用替换/轮换语义，不提供回显。
5. 这种分层不改变 Tenant 的运行时隔离：所有运行时解析仍必须先固定 `tenant_id`，再解析该 Tenant 当前已发布的 Policy 和 Resource Reference。
6. Channel Secret 也遵循同样的密钥边界：Tenant Admin 为自己 Tenant 的 Binding 提交或轮换凭据；System Admin 只管理 Secret Provider / Vault / KMS 等基础设施。任何管理员都不通过 Console 回读既有明文 Secret。

## 后果

- 满足不同 Tenant 使用不同数据后端和模型的题目要求，同时把 Secret 生命周期保持在平台边界。
- System Admin 可以运维基础设施，但不会因此自动获得 Tenant 业务数据读取权限。
- Tenant Admin 可以独立调整自己的 Agent、模型选择和 Storage Policy，而无需接触底层共享凭据。
