# ADR-0001：平台身份与租户授权边界

- 状态：**已接受**
- 更新：2026-09-10
- 关联：`CONTEXT.md`、`docs/complete-development-plan.md`

## 背景

平台实例可能部署在企业、学校、实验室、项目组或托管环境中，因此产品模型不能假设最上层边界一定是“企业”。同时，登录身份不能直接充当平台用户主键，否则 Local、企业微信、飞书和 OIDC 无法稳定汇聚到同一个平台用户模型，系统权限与租户权限也会重新耦合。

## 决策

1. **一个部署实例 = 一个 Platform Instance**。不建 Organization/Enterprise 作为产品前置实体；Tenant 是平台内第一等业务、配置与数据隔离单元，UI 与后端都统一称“租户”。
2. **平台用户是 canonical identity**。`platform_users.platform_user_id` 表示平台中的“人”；`login_identities(provider_id, subject_id)` 只负责把某种登录身份映射到平台用户。用户基本资料与登录凭据分离。
3. **正式登录支持 Local、企业微信、飞书、OIDC**。Local 由系统管理员创建账号，不开放匿名自助注册；企业微信、飞书和 OIDC 在受信任 Provider 首次成功登录时可以创建新的 Platform User。Mock 仅用于开发/测试。
4. **不同 Login Identity 不自动合并**。企业微信、飞书、OIDC 分别校验各自 Provider 边界；OIDC 使用 `(issuer, sub)`。邮箱、姓名等弱标识不得触发账号合并；一个用户需要增加新的登录方式时，必须在已登录状态下再次完成目标 Provider 认证后显式关联。
5. **权限分两层**：实例级 `system_admins`；租户级 `tenant_members(admin/member)`。System Admin 管理平台资源与租户生命周期，但不会仅因全局身份自动获得所有租户的业务数据读取权限；需要访问某租户业务数据时仍必须具有对应 Tenant Membership 或专门的审计授权。
6. **首个系统管理员**通过部署自举机制建立，后续系统管理员由已有 System Admin 在产品内授权。自举方式可以使用 Local 或已配置的受信任 Provider，但授权最终都落在 `system_admins`，而不是每次依赖环境变量即时判断。
7. **没有租户 Membership 也允许登录**。受信任第三方身份首次登录后可以只有 Platform User，没有任何 Tenant Membership；之后由 System Admin 或 Tenant Admin 按规则加入租户。
8. **会话使用服务端 Session**：Redis + HttpOnly Cookie + SameSite=Lax，写请求使用 CSRF Double-Submit；Session 可按用户整体吊销。系统角色或部门权限变化后立即吊销该用户已有 Session。
9. **企业微信网页登录使用 CorpApp 扫码入口** `wwlogin/sso/login?login_type=CorpApp...`；飞书使用 Web OAuth，并可通过内嵌二维码 SDK 在登录页内扫码（`LOGIN_FEISHU_QR_MODE`，授权后仍回到同一个 `/api/v1/auth/callback`，不新增第二套会话链路）；OIDC 使用 Authorization Code + PKCE + nonce；Local 使用平台自有用户名/密码凭据。
10. **外部 OAuth/OIDC Provider 共用平台登录回调入口** `LOGIN_CALLBACK_URL`，固定指向 `/api/v1/auth/callback`；Local 不经过该回调。
11. **外部 IM 身份与 Console 登录身份分离**。Channel Identity 可以在没有 Platform User 的情况下独立与 Agent 对话；Channel Binding 根据 `public / allowlist / member_only` 控制准入。需要把 IM 身份与 Web/Console 身份统一时，再进行显式 Identity Link。
12. **租户成员默认可使用租户内已启用 Agent**。如需对单个 Agent 收紧访问范围，使用 Agent ACL/Policy，而不是增加更多 Tenant Role。
13. **System Admin 不等于 Tenant Admin**。System Admin 可以创建、停用 Tenant，管理 Platform User、模型资产与身份源，也可以维护 Tenant Membership；但如果没有当前 Tenant 的 `admin` Membership，不能读取或修改该 Tenant 的 Agent、知识、会话正文、执行记录等业务数据。
14. **Tenant Provisioning 由 System Admin 发起**。创建 Tenant 时至少指定一个 Tenant Admin；普通 Platform User 不能自行创建 Tenant，也没有子 Tenant 概念。
15. **最后管理员受保护**。Active Tenant 不能移除、降级或停用最后一个 Active Tenant Admin；平台实例不能撤销或停用最后一个可用 System Admin。Bootstrap 只用于首次建立管理员，不作为长期隐藏权限来源；灾难恢复应使用独立 break-glass 运维流程。
16. **当前租户不进入登录 Session**。浏览器路由/页面上下文决定请求所处 Tenant；每个请求携带 `tenant_id`，服务端基于当前 Platform User 实时校验 Membership 与角色，允许多个标签页并行使用不同 Tenant。
17. **Platform User suspension 是实例级禁用**。已关联到 suspended Platform User 的 External Identity 也不能继续通过 public Channel 绕过停用；未关联的 public 外部身份仍按 Channel Policy 处理。
18. **开发期不保留旧身份模型兼容层**。旧的“企业 → 部门”“admin/operator/member”等概念直接按当前模型清理，不保留双轨；Mock 仅作为开发测试入口。
16. **Local 是正式登录能力，不是 Mock 的替代品**。首次部署若尚无可用 System Admin，可通过部署 Secret 自举一个 Local 管理员；初始化完成后权限以 `system_admins` 为事实源。Local 新用户由 System Admin 创建，不开放匿名注册，初始密码只显示一次并要求首次登录后修改。
17. **Tenant Membership 是当前授权，不改写历史归属**。用户被移出租户后立即失去该 Tenant 的 Console、Memory、Artifact 与业务数据访问，但已有 Session、Audit、Artifact 等历史事实仍保留原 Platform User 归属；是否最终删除由独立的数据保留/清理策略决定。
18. **Identity Link 不等于 Tenant Authorization**。即使某个 Public Channel 的 External Identity 已关联到 Platform User，只要该用户当前没有 Active Membership，就不能据此读取该 Tenant 过去的私人 Memory 或重新获得成员权限。
19. **用户可以维护自己的身份关联，但不能破坏可登录性**。已登录用户可以增加新的 Login Identity、修改 Local 密码以及连接/解除自己的 External Identity；不能解除最后一个可用登录方式。管理员可以禁用账号或重置凭据，但不得伪造用户对外部身份的所有权证明。

## 后果

- 权限模型收敛为“平台用户身份 → 系统管理授权 / 租户成员授权”，不再把现实组织结构编码进产品。
- Local、企业微信、飞书和 OIDC 都可以作为正式登录方式；Telegram 等消息 Channel 不会因为能聊天就自动成为 Console 登录方式。
- 系统管理中的“用户”和“租户”分离；租户“成员”页只处理当前租户的 Membership。
- 外部 IM 可以服务未注册的平台终端用户，同时保持租户与 Agent 的访问策略约束。

## 明确不做

| 方案 | 原因 |
|---|---|
| 产品内预设 Enterprise / Organization | 部署实例可能对应多种现实组织，不应强绑定企业语义 |
| Local 匿名自助注册 | 平台管理场景需要受控建号；Local 用户由管理员创建 |
| Telegram 登录 | Bot 身份不是企业员工身份源 |
| 根据邮箱自动合并不同 Provider 账号 | 邮箱不是跨 Provider 的权威稳定主体 |
| JWT 无状态会话 | 难以即时吊销，且本系统已有 Redis |
