# 验证指南

本指南适用于 Enterprise Multi-Tenant Agent Platform 的源码包和仓库。功能断言见 [验收证据](ACCEPTANCE_EVIDENCE.md)，目标部署见 [验收 Runbook](EXTERNAL_ACCEPTANCE_RUNBOOK.md)。所有命令在源码根目录执行。

## 1. 环境

模块兼容基线为 Go 1.25.14，构建与安全门禁使用 Go 1.26.7。完整验证还需要 Bash、Docker 和 Docker Compose。解压源码不包含 Git 元数据，Go 命令使用 `-buildvcs=false`。

## 2. 提交包源码验证

```powershell
go test -buildvcs=false -count=1 -p 1 ./...
go vet -p 1 ./...
go test -buildvcs=false -race -count=1 -p 1 ./...
go run -buildvcs=false ./cmd/demo
```

交付包 `verification-evidence/current-validation.log` 保存上述命令的工具链、时间、输出和退出码。常规测试包含测试内启动的本地 HTTP/MCP 服务；`cmd/demo` 的观察项与范围见[故障演示](JUDGE_QUICKSTART.md#2-两分钟故障演示)。

## 3. 完整源码门禁

```bash
go version
go mod verify
test -z "$(gofmt -l cmd pkg migrations test)"
go build -buildvcs=false -p 1 ./cmd/...
go vet -p 1 ./...
go test -buildvcs=false -count=1 -p 1 ./...
go test -buildvcs=false -race -count=1 -p 1 ./...
bash ./scripts/static_verify.sh
```

race 检查需要当前平台支持的 C 工具链。每个门禁分别记录退出码；CI 配置位于 [verify.yml](../.github/workflows/verify.yml)。

## 4. 后端集成与部署检查

使用 Docker 启动测试所需的 PostgreSQL、Redis、Qdrant 和 MinIO，按测试配置提供连接参数，然后执行：

```bash
go test -buildvcs=false -race -tags=integration -count=1 -p 1 ./test/integration
```

集成测试覆盖队列持久化、Session 迁移、Summary→Runner、Knowledge、Artifact 和数据投影，模型及 embedding 使用本地协议服务。具体环境变量与自动启动流程以 [validate.sh](../scripts/validate.sh) 为准；该脚本同时汇总源码检查、后端集成、镜像构建和 Prometheus 规则检查：

```bash
bash ./scripts/validate.sh
```

在同一测试环境下，可以单独验证租户级交叉后端组合：

```bash
go test -buildvcs=false -tags=integration -count=1 \
  -run '^TestCrossBackendStorageAdapterSharesDataAcrossNodesAndIsolatesScopes$' ./test/integration
```

该测试使用真实 Redis/PostgreSQL、两个独立生产存储适配器和 PostgreSQL execution fence。两租户分别使用 Redis Session/PostgreSQL Memory 与相反组合，检查跨客户端可见性、tenant/app/actor 隔离、未选择后端无数据以及租约释放后的资源关闭。

完整业务链路可单独执行：

```bash
go test -buildvcs=false -race -tags=integration -count=1 \
  -run '^TestWeComEncryptedCallbackRunnerDeliveryE2E$' ./test/integration
```

该测试覆盖加密企业微信回调→Gateway→PostgreSQL Inbox→Consumer→Runner→Memory Tool→Outbox→Delivery，分别运行 Redis Session/PostgreSQL Memory 与 PostgreSQL Session/Redis Memory。每种组合验证正常回复、42001 token 失效刷新及活动模型调用期间取消 context，检查重复回调、trace 关联、持久化结果与 goroutine 排空。模型和 IM 使用 loopback 协议服务，Consumer→Worker 使用 LocalClient；Worker HTTP 鉴权与 execution fence 由对应测试单独验证，真实企业微信账号按部署验收执行。

连接池并发回归 `TestOnlineSessionConcurrentReadsUseIndependentGatePool` 使用真实 PostgreSQL/Redis，在迁移完成后以受限 Control/Gate 容量运行 25 并发读取，并检查连接归还与关闭。

Windows 可使用隔离环境脚本启动应用栈：

```powershell
.\scripts\run_c_local_stack.ps1 -ProjectName agent-platform-local -Build
.\scripts\run_c_local_stack.ps1 -ProjectName agent-platform-local -Down
```

脚本按自身位置定位源码，使用独立端口和一次性测试配置。启动后检查 migration 退出码、服务 health、Prometheus targets/rules 和容器 restart count。正式环境的密码与 Provider 凭据由部署方注入。

## 5. 演示与基准

故障恢复演示见[评委快速入门](JUDGE_QUICKSTART.md#2-两分钟故障演示)。目标部署的队列延迟、成功率、错误预算和告警处置见 [SLI/SLO](SLO.md)。

### 5.1 本地可靠性基准

`BenchmarkMemoryStoreInboxOutbox` 测量同一 `MemoryStore` 中的完整链路：创建唯一消息和 Session→`EnqueueInbox`→`ClaimInbox`（lease/fence）→`CompleteInbox` 并创建 Outbox。

```powershell
.\scripts\benchmark_local.ps1 -Count 5
```

`Count` 指定 Go benchmark 的目标计时秒数。跨平台等价命令：

```bash
go test -buildvcs=false ./pkg/reliable -run '^$' -bench '^BenchmarkMemoryStoreInboxOutbox$' -benchtime=5s -benchmem
```

| 输出 | 含义 |
|---|---|
| `ns/op` | 每次完整链路的平均耗时 |
| `B/op` | 每次链路分配的堆内存字节数 |
| `allocs/op` | 每次链路的堆分配次数 |
| `N` | 该轮校准后执行的操作次数 |

保存源码快照、操作系统、CPU、内存、Go 版本及完整命令。消息和 Outbox 会保留在同一 Store 中，测量包含累计队列状态的成本；比较时保持计时和运行条件一致。

该基准不包含数据库 I/O、网络、模型或 IM 延迟。正式容量验证使用目标部署和业务 payload，记录吞吐、p50/p95/p99、队列积压、资源与成本，按[故障、回滚与容量](EXTERNAL_ACCEPTANCE_RUNBOOK.md#8-故障回滚与容量)执行。

### 5.2 PostgreSQL 公平队列容量基线

`cmd/queue-bench` 提供可复测的真实数据库队列基线。它只接受名称以 `queuebench_` 开头的独立 PostgreSQL 数据库，启动前要求 `queuebench-*` 租户命名空间为空，并持有数据库 advisory lock；不会清理其他租户数据。每个案例预加载两个租户，分别执行普通领取和公平领取，覆盖均匀、4:1 加权、热点会话三类负载及 1/4/8/16 Consumer。输出包含吞吐、领取/处理/完成时间 p50/p95/p99、租户完成量、重复领取、连接池尾态和 Inbox/Outbox 持久化核验。

```powershell
.\scripts\queue_bench.ps1 -DatabaseUrl 'postgres://agent:<password>@127.0.0.1:35432/queuebench_local?sslmode=disable' -Output work\queuebench-results.json
```

该工具的结果用于解释 `max_inflight`、公平权重和连接池预算，不把单机结果写成生产 SLO。正式验收仍需在目标规格下记录 SQL pool wait、CPU、I/O、队列清空时间和故障接管结果；JSON 中的 `verified` 必须为 `true`，否则命令失败。

### 5.3 受控知识导入

`pkg/knowledgeingest` 是平台层导入原语，验证入口为 `go test -buildvcs=false ./pkg/knowledgeingest`。调用方必须提供已完成租户/app 绑定并接入迁移装饰器的 `StoreProvider`，请求不允许携带 DSN、Qdrant 地址或凭据。导入限制 UTF-8、1 MiB、256 个分片和保留 metadata；source 内容或 metadata 未变化时跳过 embedding，变化时按稳定 chunk ID 更新并删除旧分片。若同一 source 已存在超过 256 个分片，入口拒绝继续并要求显式修复，避免无界扫描或删除。

## 6. 结果记录与打包

结果记录包含源码快照、Go 版本、环境、开始/结束时间、命令、退出码和必要后状态。源码测试、真实后端、部署与外部账号验收分别标注运行范围。

CI 的 `go`、Go 1.25 兼容性和漏洞扫描任务通过 `scripts/ci_evidence.py` 生成不可变证据，绑定 `GITHUB_SHA`、运行号、工具链、逐项日志哈希和测试结果。Windows 打包任务必须等待三个门禁任务完成，下载并核验全部证据后才生成归档；缺失、篡改、源码 SHA 或运行身份不一致时拒绝打包。

[打包脚本](../scripts/package_all_materials.ps1) 生成文件清单和 SHA-256，检查新目录解包结果及秘密签名。归档包含源码、当前文档和验证证据，排除真实环境文件、运行时数据库、凭据、缓存和构建产物。交付清单中的源码身份用于对应仓库和压缩包。
