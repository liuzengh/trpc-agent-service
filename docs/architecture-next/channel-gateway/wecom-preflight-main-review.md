# WeCom 接入验证：main 合入审查

日期：2026-09-07。远端基线固定为 `f61d49b09b8ae534f39f8065ddec699aa35ee10e`。
审查与集成使用独立工作树，不修改本地 main 的未提交内容，不重启既有部署。

## 1. 变更范围

本次候选沿用 Gateway 分支的 Telegram 双接收模式及 Worker V1 同步历史，新增
WeCom 公开 Go connector 认证探测、Control/API/迁移与 Web 接入验证。没有独立
Node Connector 镜像，也没有把认证 PASS 扩大为 Agent 或真实消息交付证明。

审查按两个独立轴进行，并补充继承提交的真实 PostgreSQL/NATS 集成检查。

## 2. Standards

明确架构规范违规 0 项。公开协议包没有服务/业务数据库依赖，Application 经自有 Port
调用 Adapter，生产 Runner 由 bootstrap 管理；OWNER、停用账户、确认位、精确版本及
闭合诊断结果保留。

两项非阻断 P3 已整理并复核：

1. ARC-102 与 Gateway 总览补入 Telegram 单次有界协议包的已接受例外；它不拥有
   持久 cursor、receiver owner 或业务重试。同步区分分布式 Webhook 收件与 receiver owner。
2. Gateway 通用凭据 Port 从 `ResolveBotToken` 统一为 `ResolveCredential`；外部
   URL、DTO、purpose、版本及租约核验保持不变。

## 3. Spec

发现 1 项 P2，并修复、独立复验：

- 明确订阅 ACK 后立即断线时，session 的首个终止错误和瞬时 Ready 状态会覆盖认证事实。
- 最小 loopback 回归在成功与拒绝两个分支的第 0 次即失败；独立拒绝复现的 300 次
  输出为 `map[PROVIDER_UNAVAILABLE:300]`。
- Reader 现在只对当前 generation、匹配 pending req_id/命令的认证 ACK 保存独立事实；
  新 generation 清空，重复、错 ID 或已超时 ACK 不覆盖。Probe 不依赖可丢失的状态通知。
- 正常 Client 对明确拒绝保持终止，不因紧随其后的断线启动自动重连。
- 修复后成功/拒绝各 300 次均正确，分别为 `map[WECOM_AUTHENTICATED:300]` 与
  `map[WECOM_AUTH_REJECTED:300]`；`-race -count=10` 正反立即断线回归通过。
  独立错 req_id 30 次仍 UNKNOWN，迟到 ACK 30 次仍 TIMEOUT。

## 4. 集成门禁补查

补齐原先跳过的 Gateway V1 数据库角色夹具后，发现继承测试仍固定迁移数为 11；
Telegram 与 Worker 已发布的两份 0011 按完整文件名共存，真实账本为 12。

修正测试为比较 embed 中全部 SQL 文件名与实际有序迁移账本，而非仅把常量改为 12。
历史 SQL、摘要、迁移执行器与权限检查不变。该测试先红后绿，真实 PostgreSQL 17
Gateway 角色测试 6 项通过；Control 角色/并发首次迁移/跨 workload 隔离测试 13 项通过。

## 5. 验证记录

各行是独立命令结果，测试计数存在重叠，不能累加。Go 全库无外部夹具的一般测试中，
显式依赖 PG/NATS 的用例会跳过；下面单独列出实际运行的集成测试。

| 检查 | 结果 |
| --- | --- |
| 远端 main 基线 `go test -count=1 -json ./...` | 2366 pass / 174 skip / 0 fail |
| 修复后全库 `go test -count=1 -json ./...` | 2876 pass / 210 skip / 0 fail |
| Web 基线，限制两个测试 worker | 38 files / 525 tests passed |
| Web 候选，限制两个测试 worker | 42 files / 622 tests passed |
| Web `npm run lint` 与 `npm run build` | exit 0 |
| WeCom/Connection/bootstrap/Control channelbinding/Worker `-race` | exit 0 |
| `go vet ./...` | exit 0 |
| Control PostgreSQL 17，三个 package | 87 pass / 0 skip / 0 fail |
| Gateway PostgreSQL/NATS 串行集成 | 1324 pass / 4 skip / 0 fail |
| Worker PostgreSQL/session/manifest/vertical/bootstrap | 120 pass / 0 skip / 0 fail |
| Worker NATS ACL/transport/replay | 11 pass / 0 skip / 0 fail |
| 额外 Gateway V1 角色契约 | 6 pass / 0 skip / 0 fail |
| 额外 Control V1 角色契约 | 13 pass / 0 skip / 0 fail |

Gateway 常规集成命令的四个跳过项中，两项数据库角色测试已通过上述额外命令补跑；
`TestReplyHandoffPostgresNATSIntegration` 与 `TestNATSTrustedTLSIntegration` 未配置其
专用夹具，仍不计入通过。Worker 的真实 PG/NATS/SDK 测试使用模型与 Control HTTP
夹具，不是第三方模型或真实 Bot 的 Agent 端到端验收。

首轮同时跑两份 Web 全套与 Go/集成编译导致已有 UI 异步等待超时；未改产品代码或
增大断言超时，改为逐套运行、每套两个 worker 后，基线与候选全部通过。失败日志保留。

## 6. 真实 Bot 证据与发布边界

前一联合验证阶段已完成官方 WeCom WSS 认证、原生客户端 marker 收发/Final ACK，
以及真实 Web → Control → Gateway runner → 官方 WSS → Complete 固定链接。
详见 [WeCom V1 的验收记录](wecom-preflight-v1.md#7-验收记录)。本轮审查没有重新订阅
真实机器人，使用 loopback 复现与隔离 PG17/NATS 复核；原机器人、既有部署不变。

可复核日志、原始 main 归档、最终源码归档、完整差异、源码回滚副本及发布结果位于
Gateway 工作树的 `artifacts/wecom-main-review-20260907/`。`ROLLBACK.sh` 只允许恢复
标记后的隔离副本，不对活跃工作树、远端 Git 或数据库执行降级。
