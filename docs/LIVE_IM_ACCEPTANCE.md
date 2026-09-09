# IM 接入与部署验收报告

## 验收结论

Telegram 和企业微信智能机器人均已通过正式应用容器完成真实消息接收、tRPC Runner 执行、模型调用与官方回复投递。PostgreSQL、Redis、Qdrant、MinIO 和 TLS OpenTelemetry Collector 采用独立 Docker 服务，全量集成与 race 测试通过。

公开记录保留测试结论、技术环境和复现入口；不包含执行日期、账号身份、聊天正文、本机路径、服务商地址、真实 trace 标识或凭据。原始运行日志与配置在受控环境中保存。

## 测试结果

| 验证项目 | 结果 | 执行方式与范围 |
| --- | --- | --- |
| 全量集成与 race | 46 个测试包、1,260 个顶层测试通过；含子测试 2,176 项通过；0 失败、0 测试跳过；5 个包无测试 | Windows Go 1.26.7 测试进程连接真实 Docker 后端，执行 `go test -race -tags=integration -json -count=1 -p 4 -timeout=25m ./...` |
| Linux 容器单元测试 | 44 个测试包、1,211 个顶层测试通过；含子测试 2,080 项通过；0 失败 | Linux 容器执行全部无 integration tag 的 Go 测试；2 项依赖 Bash/PowerShell 的测试在容器内跳过，已在 Windows 全量测试及部署回归中执行通过 |
| Go 兼容性 | Go 1.25.14 构建通过；44 个测试包、1,213 个顶层测试通过；0 失败、0 测试跳过 | 验证模块最低工具链兼容性，不含 integration tag 用例 |
| 应用镜像 | 10 个镜像构建通过 | Admin、Gateway、Worker、Consumer、Delivery、Summary Worker、schema 迁移、数据迁移及两类 IM 连接器；运行用户均为 UID/GID 65532 |
| schema 迁移 | 升级、回退与重新升级通过 | 48 条迁移记录回退至 47，再升级至 48；2 个索引恢复 |
| 静态与部署门禁 | 全部通过 | 格式、静态检查、依赖完整性、`go vet`、两类 Bootstrap、SecretRef 作用域、Compose 最终结构与 CI 证据契约 |
| 漏洞扫描 | 当前调用路径与导入包均为 0 漏洞 | govulncheck 另报告所需模块中 8 项当前代码未调用的漏洞 |
| 告警规则 | 17 条规则通过 | Docker 中使用 Prometheus `promtool check rules` |

Docker Engine 运行 Linux amd64 容器。上述“全量”对应命令选中的完整测试集合；测试进程运行位置、真实依赖及协议替身按下文分别列明。复现环境和完整命令见[验证指南](VERIFICATION.md)，功能断言见[验收矩阵](ACCEPTANCE_EVIDENCE.md)。

## 真实后端与故障恢复

集成测试覆盖跨节点读写与租户隔离、Session 并发执行 fencing、Inbox/Outbox 事务、重复消息幂等、公平调度、租约过期接管、工具审批与审计，以及 Summary 持久化后进入下一次 Runner 请求。

| 场景 | 验证内容 | 测试入口 |
| --- | --- | --- |
| 交叉后端组合 | 两租户使用相反的 Redis/PostgreSQL Session、Memory 组合；跨实例可见性、逻辑 ID 与 actor 隔离 | `test/integration/cross_backend_storage_test.go` |
| Session 在线迁移 | 增量捕获、删除恢复、目标不可用时回滚、完成后旧缓存写入方跟随目标路由 | `test/integration/online_session_migration_test.go` |
| Knowledge / Artifact 迁移 | PostgreSQL fence、真实 Qdrant/MinIO 投影、profile 切换、版本及 tombstone 保留 | `test/integration/online_dataplane_migration_test.go` |
| 进程恢复 | 实际启动 Worker/Consumer，模拟领取后崩溃及 receipt 已持久化后崩溃，检查新 fence 接管与单一 Outbox 结果 | `test/integration/process_recovery_test.go` |
| 摘要执行 | 生成、持久化及下一轮 Runner 读取摘要 | `test/integration/summary_runtime_test.go` |
| TLS tracing | 使用受信 CA 向 TLS Collector 导出 span | `pkg/telemetry/otel_tls_integration_test.go` |

确定性的进程恢复测试使用本地 OpenAI 协议服务。ProcessRecovery 经临时 Go build overlay 注入限定地址与路径的 HTTP client；Summary 使用既有 ModelBuilder 注入。正式镜像保持生产模型网络策略，真实模型调用由官方 IM 链路独立验证。

## 官方 IM 消息链路

### Telegram

官方 Bot API 长轮询接收消息，连接器持久化原始更新后交给 Gateway；经 Inbox、Consumer、Worker/tRPC Runner、模型服务及 Outbox Delivery 完成回复。6 条消息均达到 Inbox `COMPLETED`、Execution `SUCCEEDED`、Outbox `REPLIED`；接收、执行审计与回复投递的 trace_id 全部对应。

连接器验证 Bot 身份与 webhook 状态，使用持久化 spool、offset 和单实例锁。Gateway 确认接入后推进 offset；故障重放保留原始消息字节，由 Inbox 完成去重。实现与配置见[Telegram 连接器](../pkg/telegramingress/README.md)。

### 企业微信智能机器人

API 模式长连接成功订阅官方服务。两条用户消息均完成真实模型执行及官方回复确认，Inbox、Execution 和 Outbox 均成功且没有重试；每条消息的审计 trace_id 与接收、投递记录一致。

首条消息验证记忆写入与计算回答，`memory_add` 调用成功，PostgreSQL 中的持久化记录及 tenant/app/actor 归属检查通过。后续同会话追问正确返回已提供的信息。该追问未调用 `memory_search`，因此验收结论分别为：记忆写入与持久化通过、同会话后续回答通过；跨会话检索未在此次官方通道测试中验证。

已验证范围为单聊文本与一次最终回复。群聊、图片文件、撤回和实时增量回复不计入此次验收。实现及官方协议入口见[企业微信连接器](../pkg/wecombot/README.md)。

### 凭据与模型边界

两通道均由平台 Runner 调用配置的 OpenAI-compatible 模型服务。模型凭据通过租户 SecretRef 解析；Admin 仅持有引用授权，模型 Key 与操作方 Base URL 仅注入 Worker/Summary Worker。企业微信官方 Bot Secret 仅注入连接器，Gateway/Delivery 使用独立桥接令牌。

公开源码的配置、示例和测试使用通用占位符或合成数据，真实凭据扫描未命中。模型目标的 HTTPS、DNS、固定 origin、TLS、重定向及错误净化策略见[运营侧模型端点](../pkg/modelendpoint/README.md)。

## 发布与目标环境

GitHub Actions 状态以 PR 对应提交的检查结果为准；本报告中的本地测试结果与远端 Runner 结果分别记录。生产 Kubernetes、身份与密钥系统、跨区域灾备和持续容量测试按[接入与部署验收](EXTERNAL_ACCEPTANCE_RUNBOOK.md)执行。
