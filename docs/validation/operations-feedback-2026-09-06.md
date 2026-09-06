# 业务工具与 IM 回执开发记录

日期：2026-09-06。

## 开发范围

- 第 3 项：业务幂等、内部 `create_work_item`、结果查询、只读对账、Runner 审批和后端异常恢复；提交 `5891a43`。
- 第 4 项：重复审批状态提示、决定消息去重账本、失败终态与回执原子提交、媒体/编辑的固定拒绝提示、控制消息限流、旧 Worker 写入保护。
- 仅增强本地平台能力，未接入外部订单/工单系统，未修改真实 Bot Binding/Revision。

## 验证命令

```bash
go test -race ./...
./lint.sh
./build.sh
TEST_POSTGRES_URL='postgres://trpc_agent:trpc_agent_dev@127.0.0.1:5432/trpc_agent?sslmode=disable' \
  go test -race ./trpcservice/approval ./trpcservice/toolexec \
  -run 'TestPostgres(DecisionContract|BusinessOperations|ToolJournal)Integration' -count=1
./scripts/e2e-telegram-tracing.sh
git diff --check
```

PostgreSQL 测试使用随机 `tool_test_*` 和 `approval_test_*` schema，测试结束后删除自己的 schema。没有迁移、清空或写入日常业务表。模拟协议测试不访问真实 Bot，不调用真实模型。

关键证明：16 个并发请求复用一笔业务；输入冲突被拒；丢失响应后只查询事实，不重复产生工作项；非幂等后端只发起一次；失败回执写入/提交响应失败不会增加模型执行次数；审批的新消息有新提示而旧消息重投没有第二次投递；媒体不能触发审批。

本轮模拟 Telegram 的追踪预检也从真实 Tempo 读回了完整链路：

```text
initial_trace_id: 20b22c435ff728afefc69bdb29fad64c
decision_trace_id: 32c81ba0bc367f0c4f40f2d7d685fb6c
initial_spans / decision_spans: 17 / 19
origin_link: true
artifacts: data/trace-preflight.w7uDUn/
```

结束时确认测试 schema 剩余数量为 0，本地 `/readyz` 与公网 `/healthz` 正常。现有服务没有被重启或停止。`.env` 的自动迁移开关为 true，下一次正常启动新 binary 会应用新迁移；当前运行中的旧版本不会自动变化。

## 尚需外部配置

下一项是企业微信真实账号联调，需要用户拥有可配置自建应用的企业账号和管理权限，以及本地应用/回调凭据。现有协议实现不能代替这些账号配置。真实生产部署、账号级权限、告警通知、容量与恢复承诺仍按执行清单后续处理。

本机只读检查显示企业微信 Binding 数量为 0，`.env` 中也没有 `WECOM_` 配置项；没有检查或输出任何密钥值。因此当前需要用户提供真实账号侧的配置，而不是继续模拟一次企业微信收发。
