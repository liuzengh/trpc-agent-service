# ADR-0001：首个 Platform Operator 数据库引导协议

- **状态**：已接受，按最小 V1 实现
- **日期**：2026-08-31
- **适用范围**：Control API 首次启动、Identity 账号创建和 Admin Operator Grant

## 1. 背景

普通平台管理命令要求有效 Session 和 Operator Grant，但全新数据库中两者都不存在。
系统因此需要一个仅用于空库的首个 Operator 引导路径，同时避免把平台角色塞入
UserAccount、通过配置文件标记“已安装”，或在已有账号时静默提权。

## 2. 决策

Migration 完成后、HTTP Server 监听前，进程 bootstrap 在显式 `auto` 模式下调用
`admin/application.EnsureInitialOperator`。Identity 继续拥有账号、密码策略和哈希；
Admin 继续拥有 Operator Grant 和引导状态判断。

```text
process bootstrap
  -> begin PostgreSQL transaction
  -> pg_advisory_xact_lock
  -> compose transaction-bound Identity AccountManagement
  -> compose transaction-bound Admin OperatorStore
  -> admin.EnsureInitialOperator
  -> commit
  -> start HTTP server
```

bootstrap 只组装事务绑定的 Port，不包含用户名、密码或授权业务规则，也不直接写业务
表。

## 3. 输入

```text
CONTROL_BOOTSTRAP_MODE=auto | disabled
CONTROL_BOOTSTRAP_USERNAME=<initial-operator-username>
CONTROL_BOOTSTRAP_DISPLAY_NAME=<optional-display-name>
CONTROL_BOOTSTRAP_PASSWORD=<temporary-password>
```

- `auto` 要求 Username 和 Password。
- `disabled` 不执行自动创建，适合已经初始化的部署和测试环境。
- 新生产数据库必须使用 `auto` 完成首次初始化。
- Password 从进程配置读取；日志、Trace、响应和数据库不保存明文。
- 创建的凭证标记为首次登录必须轮换。

## 4. V1 状态矩阵

状态检查在取得事务级 advisory lock 后执行：

| 数据库状态 | 结果 |
| --- | --- |
| 已存在有效 Operator | 幂等 NOOP，不修改账号、密码或 Grant |
| 无任何 UserAccount 且无有效 Operator | 原子创建账号、凭证和 SYSTEM_BOOTSTRAP Grant |
| 已有 UserAccount 但无有效 Operator | `RECOVERY_REQUIRED`，事务回滚且 HTTP Server 不启动 |

`SYSTEM_BOOTSTRAP` 的 `GrantedByUserID` 为空，不能伪造不存在的用户 Actor。

## 5. 并发和原子性

V1 的三个写入在同一 PostgreSQL 事务提交：

1. 取得固定键的 `pg_advisory_xact_lock`。
2. 重新读取有效 Grant 和账号数量。
3. Identity Application 创建 ACTIVE UserAccount 和临时 Password Credential。
4. Admin Repository 创建 Operator Grant。
5. 一次提交；任一步失败全部回滚。

多个副本并发启动时，后到副本取得锁后看到有效 Grant 并返回 NOOP。

## 6. V1 明确不做

首版不增加 BootstrapState、审计 Outbox、密码恢复或 Break-glass 命令。Operator 全部
丢失后不会重新打开空库引导条件；数据库已有账号且没有有效 Operator 会稳定进入
`RECOVERY_REQUIRED`。恢复认证、审批和审计作为后续独立 ADR。

## 7. 验证场景

1. 空库和有效 Bootstrap 配置只创建一个账号、凭证和 Grant。
2. 正常重启幂等，不重置用户名或密码。
3. 多副本并发执行只产生一个首个 Operator。
4. Identity 或 Grant 写入失败时事务不留下部分状态。
5. 已有普通账号但没有 Operator 时返回 `RECOVERY_REQUIRED`。
6. Admin 创建普通用户不会附带 Operator Grant 或 Tenant Membership。
