# 管理控制台

控制台由 Admin 同源提供，入口为 `/console/`。无需前端构建服务或外部 CDN；页面使用现有 Principal、租户权限与发布接口。Bearer 凭据仅驻留当前页面内存，刷新或退出后重新登录。

## 运行

在源码根目录执行：

```powershell
.\scripts\run_console_lab.ps1 -Build -SeedExamples
```

默认入口为 `http://127.0.0.1:38081/console/`，脚本输出本地登录凭据。`-SeedExamples` 创建两个真实配置租户，分别采用 Redis Session / PostgreSQL Memory 和反向组合；每个应用发布两个版本，并设置 10% canary。示例没有绑定 IM，也不会调用模型。接入实际业务前，在租户配置中提供有效模型凭据与通道绑定。

停止本次项目并保留数据卷：

```powershell
.\scripts\run_console_lab.ps1 -Down
```

## 操作流程

1. 登录后选择授权租户，查看持久化应用、版本、执行和投递状态。
2. 创建租户时分别选择 Session 与 Memory 引擎及 operator profile；模型 API key 通过一次性密码字段提交，或使用已授权 SecretRef。确认页不回显密钥，取消、提交或退出后清除；模型配置与工具权限接受服务端准入校验。
3. 在应用发布页创建应用与版本快照，发布后选择 stable/canary 版本及比例。每一步完成后从服务端重新读取，刷新页面可以继续操作。
4. 在数据后端页对照四个数据域的引擎、Profile、绑定状态与数据职责。没有配置 Knowledge 或 Artifact 时明确显示未配置。
5. 在请求视图打开 Inbox，查看关联执行尝试、Outbox 和可关联审计。视图来自持久记录，显示状态、版本、fence 和追踪标识，不返回消息内容、模型输入或凭据。未知执行结果需要操作人员核对，不能仅凭页面状态推断外部动作未发生。
6. 查看迁移状态，以及来自 `control_plane_audit` 的应用、发布和执行对账审计，包含操作者、动作和资源标识；使用刷新与分页读取后续记录。

## 接口与验证

`GET /api/v1/operations/me` 返回当前 Principal 的身份、权限和租户范围。其余 `/api/v1/operations/*` 读取端点要求明确的 `tenantId`；列表采用有界分页，游标绑定租户、视图与过滤条件。响应使用 `Cache-Control: no-store`。列表与请求详情只暴露明确选择的运行元数据。

写操作复用 `/api/v1/tenants`、`/api/v1/agent-apps`、`/api/v1/agent-versions` 和 `/api/v1/deployments`，权限仍由服务器判断。控制台不将未获授权的按钮显示为可操作功能，也不把客户端按钮隐藏作为权限控制。

真实数据库回归位于 `test/integration/operations_test.go`，覆盖租户隔离、分页顺序、请求关联和敏感数据不出现在响应。Inbox 与执行历史的分页使用租户、时间、ID 组合索引；迁移 `048` 应在部署维护窗口构建索引。生产部署继续使用受控 Admin 入口和 HTTPS。

真实进程恢复回归见 [进程恢复验收](../test/integration/process_recovery.md)：启动当前源码编译的 Worker 和 Consumer，强制中断进程，验证租约接管与已提交结果复用。测试使用 PostgreSQL、Redis 和本地模型协议服务，不发送外部 IM 消息。

部署表单的浏览器回归见 [浏览器验收](../test/browser/README.md)，覆盖分页配置读取、灰度比例边界、读取失败重试、配置变更与退出清理。
