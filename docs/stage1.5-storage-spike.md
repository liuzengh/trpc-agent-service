# Phase 1.5 存储兼容性与 Redis 接入结论

> 验证日期：2026-08-25  
> 代码基线：`113e4bfcd41ce039a49848328b7825fbda55ad1a`  
> 当前分支：`feature/phase2-multi-tenant-runner`

## 结论

本阶段选择 **B：官方 Redis Service + 平台生命周期薄包装**，并已接入生产运行时。官方 `session.Service`、`memory.Service` 的 CRUD、状态、事件、Summary、跨实例可见性和 Runner 注入契约通过验证；平台只负责延迟初始化、健康检查、命名空间、资源所有权和幂等关闭，不重新包装 CRUD，也不保留旧快照 fallback。

Phase 1.5 已关闭，可以生成 Phase 2 详细计划。关闭不代表 SQL 子模块进入生产依赖，也不代表 PostgreSQL Summary 的时区缺陷已被修复；这些限制见“已知限制”。

## 隔离模块与依赖图

所有 Spike 位于被忽略的 `.stage15-spike/`，使用 `GOWORK=off`，没有本地源码 `replace` 或 `go.work`。Go module cache 位于 `.stage15-cache/`，生产 `go.mod` 没有 SQL 子模块。

| 消费者 | 直接模块 | `go list -m all` | `go mod graph` | 模块列表 SHA-256 | 图 SHA-256 |
| --- | --- | ---: | ---: | --- | --- |
| Redis | root `v1.11.2`；`session/redis`、`memory/redis`、`storage/redis v1.11.0`；`go-redis/v9 v9.11.0` | 115 | 488 | `17a51ce063e7bd109ff5daa54f195994a700927e654a8bbe77378aa243ff0975` | `6aa87d9342b104251cd7e8a9c9b0a2eeebcac6f375ecf125126a9d3af13bc285` |
| MySQL | root `v1.11.2`；`session/mysql v1.11.2`；`memory/storage/mysql v1.11.0` | 109 | 402 | `e94f7d52b55067061f5bbc996f4810ffef6cc9a8fdbe0e0953e2a2062bd1d866` | `fad254a1aa735144ec78f364a62c6184f49b11fde88cc6d125dff436f5dda3b5` |
| PostgreSQL | root `v1.11.2`；`session/memory/storage/postgres v1.11.0` | 111 | 452 | `c3d974dbd4516ef9a5973a3e6a947dca8968f91bd7642f75db650930687ebd40` | `981e7af095485641b8447bd667a6ea78f1a80e725cddf35076cc7394bcfc94ca` |

Redis 子模块的 `go.mod` 中存在历史边缘（例如 root/storage 的旧 `v0.x` 要求），但消费者 MVS 最终解析为上表版本；没有意外 `replace`。MySQL Session 发布模块声明 `toolchain go1.24.4`，但远程消费者在 Go `1.21.13 windows/amd64` 下成功 test/build，没有被强制升级。
生产 `go.mod` 直接声明 `session/redis` 与 `memory/redis`；`storage/redis v1.11.0` 由官方 Redis Service 传递引入并以 `// indirect` 固定解析版本，这是模块依赖关系而非遗漏。

复现命令（Windows PowerShell）：

```powershell
$env:GOWORK = 'off'
$env:GOPATH = 'D:\project\OpensourceTencent\trpc-agent-service\.stage15-cache\gopath'
$env:GOMODCACHE = "$env:GOPATH\pkg\mod"
$env:GOSUMDB = 'sum.golang.org'
go list -m all
go mod graph
go test ./...
go test -race ./...
go build ./...
go vet ./...
```

## Redis 契约

`.stage15-spike/redis` 的 miniredis 契约和真实 Redis 7 契约均通过，覆盖：

- Session Create/Get/List/Delete、App/User/Session State、状态删除、Event 顺序、`EventNum`/`EventTime` 过滤、非法 Key、TTL、Summary 跨实例读取和前缀隔离。
- 两套独立 Service 并发写同一 Session；唯一 Event 和不同 State Key 全部保留。同 Key 的覆盖语义不超出官方定义。
- Memory Add/Read/Search/Update/Delete/Clear、重复 Add 幂等、结果限制、关键词搜索、跨实例可见和前缀隔离。
- `Tools()` 为空；生产明确关闭 Add/Update/Search/Load/Delete/Clear，Extractor 为 nil，Memory limit 为 0（无限）。
- context 取消、Redis 停止/恢复、重复/并发 Close、Runner 不关闭借用 Service、部分构造失败时所有已返回 Service 的释放；Runtime 关闭会等待活跃请求排空。
- 固定 Summarizer 可跨实例读取 Summary；生产 Runtime 不配置 Summarizer 或异步持久化。

真实 Redis 7 证据使用独立 `trpc-stage15-redis` 容器，强制 `SCRIPT FLUSH` 后观察到 `EVAL` 回退和后续 `EVALSHA`，并完成 24 路并发 Event/State 写入及两套连接的 Memory 读取。生产 Runtime 还验证了：Redis 停止时 `/healthz=200`、`/readyz=503`、消息请求 `503`；原端口恢复后无需重启 `/readyz=200`。

生产实现位于 `trpcservice/storage/redis_backend.go`：

- `New` 只解析 URI 和前缀，不网络连接。
- `Ready` 使用独立 health client，成功后才创建官方 Session/Memory；Session、Memory、health 各自拥有 client。
- Session 使用 HashIdx-only、同步持久化、无 TTL、用户 Session 索引；Memory 无限容量、无 Extractor、全部工具关闭。
- 官方键统一进入 `<REDIS_KEY_PREFIX>:official-v1`；Phase 1 快照键不迁移、不双读、不混合写。
- 关闭顺序为 RunnerCache -> Session -> Memory -> health client，Backend `Close` 幂等且并发安全。

## SQL 契约

### MySQL

隔离消费者完成 root/interface/option/schema 兼容性核验；`memory/mysql`、`storage/mysql` 和 `session/mysql` 的 `-vet=off` sqlmock 测试通过，覆盖事务、初始化和关闭路径，不启动 MySQL。默认 `go test` 的 Session 包被发布源码中的两处 `log.ErrorfContext(..., "%w", ...)` 触发 vet 报错；这是上游发布包的静态检查缺陷，不是消费者 API 或运行时编译失败。

### PostgreSQL

`postgres:16-alpine` 独立容器中完成真实测试数据库、专用 `stage15_` 表前缀和 `stage15_memories` 表：

- 自动建表、列和索引存在性校验。
- Session 创建、读取、Event、App/User/Session State、List、删除、Summary、两实例跨连接可见性。
- Memory CRUD、关键词搜索、Update/Delete/Clear、两实例可见性、关闭后重建。
- 两个 Service 实例并发 Event/State 写入，所有唯一 Event/State 均保留。

测试可稳定复现官方 `session/postgres v1.11.0` 的时间边界问题：Session `created_at` 以本地墙钟写入 `TIMESTAMP`，Summary `updated_at` 经过 UTC 序列化后早约 8 小时，默认 `GetSessionSummaryText` 的 freshness 条件会过滤掉已存在的 Summary 行。测试同时直接读取 Summary 行并用零时间边界确认跨连接数据确实存在，因此该结果记录为发布模块限制，而非环境失败。SQL 子模块不进入生产 `go.mod`。

## A/B/C 评估

| 方案 | 结果 | 依据 |
| --- | --- | --- |
| A 直接使用官方实现 | 不选 | 官方 Memory 构造会同步 Ping，Service 各自拥有 client，默认暴露工具；无法单独满足延迟启动、健康恢复和统一关闭。 |
| **B 官方实现 + 平台薄包装** | **选定** | 数据契约、Runner 注入、真实 Redis Lua、并发和跨实例可见性通过；薄包装可协调 Ready、命名空间、工具策略和生命周期。 |
| C 保留自研 | 不选 | 没有发现必须重写 CRUD 的官方契约缺陷；旧快照适配器的非原子读改写和 `ErrUnsupported` 已删除。 |

## 回归结果

当前工具链（Go 1.26.4）完整通过：

```text
go test ./...
go test -race ./...
go build ./...
go vet ./...
```

Go 1.21.13 另外通过生产 `go test ./...`、`go build ./...`，以及 Redis/MySQL/PostgreSQL 隔离消费者 test/build；真实 Redis、真实 PostgreSQL、Mock 模型端到端和 HTTP 错误映射均通过。DeepSeek smoke 仍可按环境变量复验，外部模型服务故障不作为存储选型阻塞。

## 已知限制

1. 官方 Redis Memory 的容量检查是 `HLen` 后再写入的非原子检查；生产将 limit 设为无限，不把它当作强配额机制。
2. 官方 Redis Event 顺序按时间戳/ZSet 排序；相同时间戳不是通用的插入顺序保证。契约测试使用单调时间，后续多 Worker 阶段需继续定义并发事件的时间/序列语义。
3. 官方 Memory 构造器在“创建 client 后 Ping 失败”的极窄窗口不会自行 Close；平台无法访问其未返回的 client，只能通过外层 Ready 和恢复重试缩小窗口。这是已记录的上游生命周期风险，不影响已验证的正常构造、统一 Close 和数据契约。其内部 Ping 使用固定的 `context.Background()` 超时；平台会在构造前后检查调用方取消并释放已返回资源，但无法中断该上游调用本身。
4. PostgreSQL `v1.11.0` Summary 时间戳/时区问题未在本阶段修复；SQL 仍是兼容性结论，未进入生产。
5. Phase 1 演示快照数据按非生产数据处理；没有迁移、双读或新旧进程混写兼容。
6. tRPC-Agent-Go Runner 会记录部分 Session 持久化错误但仍可能返回模型回复；平台对 Runner 显式返回的存储错误会再次健康探测并映射为 `503`，而被上游吞掉的中途故障不能从结果通道可靠区分，后续可靠性阶段需补持久化确认/任务幂等。
7. 本阶段不实现 Tenant/AgentApp/ChannelBinding/StorageProfile、RunnerRegistry、IM、Streams、Inbox/Outbox、双 Worker 或 SQL 平台仓储。
