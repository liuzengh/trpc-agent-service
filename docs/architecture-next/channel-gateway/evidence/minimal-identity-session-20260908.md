# 最小身份与连续会话自动化验收（2026-09-08）

## 结论与边界

- 验收版本：`7e957578af9bc4769b9b936e5c77b6b489a318a9`，执行时已核对远端 main。
- 自动化正式服务链路通过；**真实 Telegram／企微 Bot 与真实模型两轮聊天尚未验收**。
- 真实运行 Control、Gateway、Worker、PostgreSQL、NATS。通过 Control HTTP 发布 AgentVersion、ProfileRevision、DeploymentRevision、账户和 Binding。
- Telegram Bot API 与模型 HTTP/SSE 使用测试替身；企微本次验证数据库身份路径与 adapter，不包含真实 WebSocket 聊天。
- 本次验收没有修改业务源码或线上部署，没有新增 allowlist、审批、策略发布等治理能力。

## 业务结果

| 场景 | 观察结果 |
| --- | --- |
| 两位用户各首发一条消息 | 2 个社交身份、2 个独立 Session |
| 两位用户各继续一轮 | 同用户复用 Session，序号为 1、1、2、2 |
| 首轮后停止 worker-one，启动 worker-two | 第二轮正常执行，共 4 个 SUCCEEDED Run |
| 历史延续 | 第二轮实际 SDK 模型请求包含自己的首轮消息和回答 |
| 用户隔离 | 第二轮模型请求不包含另一位用户的首轮历史 |
| 重投首条 Telegram 消息 | HTTP 200，admission_count=1；Run 和模型请求仍各 4 次 |
| 回复目标与去重 | 4 条回复分别对应原 chat 和源消息，重投后无新增回复 |
| Telegram／企微真实 PG 身份测试 | 每个平台 2 个身份、2 个 Session；双连接池并发同用户 8 条消息，序号唯一 |
| 数据库 receipt 幂等 | 重复消息复用 receipt，不刷新身份 last_seen_at；正式 Claim 成功 |

这证明历史正确进入下一轮模型请求，不等同于真实模型的语义回忆已经通过。

## 执行命令与原始结果

以下命令在仓库根目录执行。数据库集成测试必须使用隔离测试库的
`WORKER_TEST_MIGRATION_URL` 和 `WORKER_TEST_RUNTIME_URL`，并先完成独立角色及 schema provisioning；
缺少这两个变量时测试会 skip，不能计为通过。本轮使用临时 PostgreSQL 容器执行，两个 provider 子测试均实际运行。

### 联合业务链

```sh
GOCACHE=/private/tmp/go-preflight-design-cache python3 scripts/test-minimal-identity-session.py --artifacts artifacts/minimal-identity-acceptance/joint
```

退出码：`0`。

```text
SESSION_PREPARATION=PASS
JOINT_CONTROL_HTTP_PUBLICATION=PASS AgentVersion + ProfileRevision + DeploymentRevision + Manifest Relay
MINIMAL_IDENTITY_SESSION_JOINT=PASS identities=2 sessions=2 rounds=4 worker_replacement=YES second_round_history=VERIFIED cross_user_history=ABSENT replay_model_calls=0 formal_finals=4
WORKER_JOINT_CLEANUP=PASS processes stopped; containers removed; private fixture files removed
```

### 双平台身份持久化

隔离测试环境就绪后，执行的 Go 测试命令为：

```sh
go test -count=1 -v ./services/agent-worker/internal/execution/adapter/outbound/postgresadapter -run '^TestMinimalSocialIdentityPostgres$'
```

退出码：`0`。

```text
=== RUN   TestMinimalSocialIdentityPostgres
=== RUN   TestMinimalSocialIdentityPostgres/telegram
=== RUN   TestMinimalSocialIdentityPostgres/wecom
=== NAME  TestMinimalSocialIdentityPostgres
    social_identity_integration_test.go:132: MINIMAL_SOCIAL_IDENTITY=PASS providers=2 pools=2 identities=2_per_provider sessions=2_per_provider concurrent_same_user=8 duplicate_side_effects=0 formal_claim=PASS repeat_dispatch=DENIED capacity_orphans=0
--- PASS: TestMinimalSocialIdentityPostgres (0.20s)
    --- PASS: TestMinimalSocialIdentityPostgres/telegram (0.07s)
    --- PASS: TestMinimalSocialIdentityPostgres/wecom (0.03s)
PASS
ok  	github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/postgresadapter	0.406s
```

### 领域与接收适配器

```sh
GOCACHE=/private/tmp/go-preflight-design-cache go test -count=1 ./services/agent-worker/internal/execution/domain ./services/channel-gateway/internal/admission/application ./services/channel-gateway/internal/admission/adapter/inbound/telegramadapter ./services/channel-gateway/internal/admission/adapter/inbound/wecomadapter
```

退出码：`0`。

```text
ok  	github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain	0.890s
ok  	github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/application	0.910s
ok  	github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/adapter/inbound/telegramadapter	1.248s
ok  	github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/adapter/inbound/wecomadapter	3.316s
```

## 关联证据与清理

[结构化证据](minimal-identity-session-20260908.json)包含本轮隔离测试的账户、Binding、
DeploymentRevision、Session、身份、Run 和回复对应关系；这些 ID 不是线上业务对象。

本轮临时服务进程及 PostgreSQL／NATS 容器均已退出，并核验不存在。
临时凭据由 harness 清理，线上业务库未用于测试。此提交只收录验收文档与非敏感结构化证据。

## 剩余验收

- Telegram 长轮询真实两轮聊天。
- 企微长连接真实两轮聊天。
- 真实模型利用首轮信息回答第二轮问题。
- 真实 Bot 链路中的 Worker 重启后会话延续。

上述项目需实际运行 Gateway／Worker 并连接真实 Bot，不能用协议预检或本报告的替身结果代替。
