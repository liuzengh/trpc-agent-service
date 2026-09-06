# 企业微信 MCP 平台接入开发记录

日期：2026-09-06。用户要求将已验证的 MCP 读发接口接入平台，本轮未启动真实群持续访问。

## 实现

- `wecom_mcp` Adapter：tRPC MCP Client、精确方法限制、租户/用途 Secret grant、指定群和人类成员、文本 @ 规则、完整分页、消息指纹及脱敏。
- Gateway 轮询：显式目标、默认关闭、租约、检查点 CAS、重叠窗口、接受后标记 seen；沿用已有配额与审批入口。
- Sender：每段 OutboundID 的共享发送记录、已成功重放不联网、未知结果不自动重发，不补造 ProviderMessageID。租户/应用暂停后不再发送待处理回复。
- Memory/PostgreSQL 状态与迁移 014、角色用途授权、启动装配、配置模板、指标审计和运行说明。

## 验证

1. `go test -race ./...`：模拟 MCP → Inbox/Outbox → 实际 Runner/LLMAgent 与 Session → Sender；同时覆盖原有 Telegram/审批/HTTP 回归。没有消费真实模型或企业微信凭据。
2. `TEST_POSTGRES_URL=... go test -race ./trpcservice/channels/wecommcp -run TestPostgresMCPStateIntegration -count=1`：本机 PostgreSQL 随机 `mcp_test_*` schema 中执行迁移、并发状态测试。临时 schema 自动清理，没有迁移或清理日常业务 schema。
3. `./lint.sh`、`go build ./...`、`git diff --check`：静态检查、编译与补丁格式检查。

完整 trace 测试检查 MCP 拉取、Gateway 接收、队列发布、Worker、框架 Agent、Session event 和发送属于同一 trace。重复轮询不增加 Agent 任务，相同出站记录的重复处理不再次发送。

## 尚未执行的外部操作

本开发轮次未修改 `.env`、创建实际 Binding、启用目标列表、调用真实模型/MCP 或重启服务。随后用户授权实际启用，后续操作单独记录在[启用记录](wecom-activation-2026-09-06.md)，不回写扩大本轮自动测试的证据范围。

真实源没有消息 ID，使用显式 `fingerprint-v1` 降级，同人同秒同内容可能合并。窗口只补偿有限延迟；旧于保留期、未识别媒体或分页不符会停止推进。发送 `attempting/unknown` 保守保留，不自动重新获取发送资格。详细边界见[运行说明](../wecom-mcp-runtime.md)，此前真实单次读发不需重测。
