# Spec：数据层切片 —— Redis Session 替换 inmemory（9/4–9/5）

> 目标：把 runner 的会话存储从进程内 `session/inmemory` 换成 Redis 共享后端，
> 使「服务重启后对话上下文不丢」和「未来多节点共享会话」成立。
> 这是第三方依赖四批次原则中的**第 3 批（存储后端客户端）**引入点。

## 1. 事实核查（2026-09-04，锁定版本前必读）

| # | 结论 | 证据 |
| --- | --- | --- |
| 1 | 核心框架最新 release **v1.11.2 不含** redis session 后端，`session/` 下只有 inmemory / noop / externalization / summary | 本地 module cache 目录清单 |
| 2 | Redis 后端是**独立嵌套 Go module**：`trpc.group/trpc-go/trpc-agent-go/session/redis`，版本线与核心对齐，最新 **v1.11.0**（适配核心 v1.11.x） | `go list -m -versions .../session/redis@latest` |
| 3 | **无需升级核心框架**：单独 require 该子 module 即可；其 go.mod 里核心基线是 v0.2.0，MVS 会自动取我们的 v1.11.2 | 子 module go.mod |
| 4 | 构造 API：`redis.NewService(options ...ServiceOpt) (*Service, error)`，实现 `session.Service`；关键选项 `WithRedisClientURL("redis://host:6379")`、`WithKeyPrefix(p)`、`WithSessionTTL(d)`、`WithSessionEventLimit(n)` | v1.11.0 源码 service.go / options.go |
| 5 | 官方测试即用 **miniredis**（纯 Go 进程内 Redis 模拟器）跑通全部 Lua 脚本路径 → 我们的单测可以不依赖本地 Redis | v1.11.0 service_test.go |
| 6 | 传递依赖：`github.com/redis/go-redis/v9 v9.11.0`、嵌套 module `storage/redis`、otel 系列 | 子 module go.mod |
| 7 | `session.Service` 接口共 17 个方法（CreateSession/GetSession/ListSessions/DeleteSession/4×State/UpdateSessionState/AppendEvent/4×Summary/Close）——**自己实现不现实，必须用官方后端** | v1.11.2 session.go |

风险与门禁：子 module 按 replace ../../ 开发，对我们锁定的核心 v1.11.2 的编译兼容性
必须以 `go build ./... && go test ./...` 通过为**硬门禁**；不通过则本切片回退
（保持 inmemory），并把结论记录进方案文档，不带病冻结 go.mod（9/9）。

## 2. 设计

### 2.1 配置 schema（config.yaml 新增顶层 `storage` 段）

```yaml
storage:
  session:
    backend: redis                     # memory（默认，缺省即现状）| redis
    redis_url: redis://127.0.0.1:6379  # backend=redis 时必填
    key_prefix: "trpc-agent-service:"  # 可选，默认 trpc-agent-service:
    session_ttl: 72h                   # 可选，默认 0=不过期；Go duration 字符串
```

- **平台级统一后端**，首期不做租户级 override（租户隔离已由
  `session_id = {tenant}:{channel}:{user}` 前缀 + key_prefix 双重保证；
  租户级后端选择属二期 Storage Adapter 范畴，方案文档口径不变）。
- 校验规则（进 `Config.Validate()`）：backend 只允许 memory/redis；
  backend=redis 时 redis_url 必填且以 `redis://` 或 `rediss://` 开头；
  session_ttl 可被 `time.ParseDuration` 解析。
- 环境变量兜底：`STORAGE_SESSION_BACKEND` / `STORAGE_SESSION_REDIS_URL`
  覆盖文件值（与 MODEL_* 兜底同一套 applyEnvOverride 机制），方便容器部署。

### 2.2 新包 `trpcservice/storage`（工厂，唯一 import redis 子 module 的地方）

```go
// storage.SessionConfig 是 config 包的纯数据（storage 不 import config，避免环）。
func NewSessionService(sc SessionConfig) (session.Service, error)
```

- backend=memory → `inmemory.NewSessionService()`；
- backend=redis → `redis.NewService(WithRedisClientURL, WithKeyPrefix, WithSessionTTL)`；
- **启动烟测**：构造后立即用接口做 `CreateSession → AppendEvent → GetSession → DeleteSession`
  探针（key 固定 `probe/probe/probe`），失败即返回错误 → main fail-fast。
  用接口烟测而非直接 import go-redis 发 PING，保持我们只依赖框架公开接口。

### 2.3 agent 接线改造

- `agent.NewRunner(t *tenant.Context, sess session.Service)`：新增第二参数，
  替换现在函数体内写死的 `inmemory.NewSessionService()`；
- `agent.NewRegistry(cfg *config.Config, sess session.Service)`：Registry 持有
  `sess` 字段；`Apply(cfg)` 重建 runner 时**复用同一个 sess**（会话后端不随
  租户 CRUD 热切换；改 backend = 改配置 + 重启，首期明确此限制并在
  Admin API 文档注释里说明）；
- `main.go`：`sess, err := storage.NewSessionService(cfg.Storage.Session)` →
  `defer sess.Close()` → `agent.NewRegistry(cfg, sess)`。

### 2.4 依赖引入（一次到位）

```
go get trpc.group/trpc-go/trpc-agent-go/session/redis@v1.11.0
go get github.com/alicebob/miniredis/v2@v2.35.0   # 仅测试使用
go mod tidy
```

预期 go.mod 直接依赖新增 2 项（session/redis、miniredis），间接新增
go-redis/v9、storage/redis、cenkalti/backoff 等。9/9 冻结前不再变化。

## 3. 测试策略（三层，前两层完全不需要本地 Redis）

1. **单测（miniredis）** `storage/storage_test.go`、`config` 校验用例：
   - 工厂：memory/redis 两 backend 构造成功；非法 backend / 缺 URL / 坏 TTL 报错；
   - 探针烟测在 miniredis 上通过；miniredis 关闭后 NewSessionService 返回错误（fail-fast 验证）；
   - config：storage 段解析、默认值（backend 缺省=memory）、env 覆盖、Validate 拒绝非法值。
2. **集成测试（条件跑）** `storage/redis_integration_test.go`：
   - `url := os.Getenv("REDIS_TEST_URL")`，为空即 `t.Skip`（CI/无环境不红）；
   - 有真 Redis 时：CreateSession → AppendEvent（一条 user + 一条 model 消息）→
     新 GetSession 拉回事件与消息内容一致 → DeleteSession 后 Get 为空；
   - runner 级：用真 Redis 的 sess 建 Registry，跑一轮对话（模型可用
     MOCK 或跳过，仅验证 session 写入 Redis key 存在）。
3. **E2E 手工冒烟（需本地 Redis + 模型 key，验收标准）**：
   - 起服务（backend=redis）→ webchat 说「我叫小宇」→ 收到回复；
   - **重启进程** → 同一浏览器会话再问「我叫什么」→ 能答出「小宇」
     （inmemory 时代此处必失忆，这是本切片的核心卖点）；
   - `redis-cli keys 'trpc-agent-service:*'` 能看到 session 相关 key；
   - `redis-cli ttl <某key>` 与配置 session_ttl 一致（配了 TTL 时）。

## 4. 用户环境清单（二选一，A 推荐）

### A. Docker（推荐，隔离干净、好清理）

1. 安装 [OrbStack](https://orbstack.dev)（macOS 上比 Docker Desktop 轻）或 Docker Desktop；
2. 起 Redis：
   ```bash
   docker run -d --name redis -p 6379:6379 redis:7-alpine
   ```
3. 验证：`docker exec -it redis redis-cli ping` → 应输出 `PONG`；
4. 观察用（可选）：`docker exec -it redis redis-cli keys '*'`；
5. 用完清理：`docker rm -f redis`。

### B. Homebrew（不想装 Docker 时）

1. `brew install redis`
2. 启动：`brew services start redis`（后台常驻）或 `redis-server`（前台，占一个终端）
3. 验证：`redis-cli ping` → `PONG`
4. 用完：`brew services stop redis`

### 无论哪种

- 默认地址端口 `127.0.0.1:6379` 即可，无需密码（本地开发）；
- 环境就绪后告诉我，我执行 §2 的代码改造 + §3 第 1、2 层测试；
  §3 第 3 层 E2E 需要你的模型 key 已在 config.yaml（已具备）；
- **如果两种都装不了**：我仍可用 miniredis 完成全部代码与单测（§3 第 1 层），
  E2E 冒烟后置到有环境的机器上，切片不算阻塞。

## 5. 排期与验收（实施结果：9/4 全部完成 ✅）

| 时间 | 内容 | 产出 |
| --- | --- | --- |
| 9/4 ✅ | 环境就绪（docker redis:7-alpine，PONG）；go get 子 module + 编译门禁 | session/redis v1.11.0 与核心 v1.11.2 兼容；go.mod 新增直接依赖 2 项，`go` 指令 1.21→1.24.1（依赖要求） |
| 9/4 ✅ | storage 包 + config schema + agent/main/admin 接线 + 单测（miniredis v2.39.0）+ 集成测试 | `go test ./... -race` 全绿；`REDIS_TEST_URL` 集成测试对 docker redis 通过 |
| 9/4 ✅ | E2E 冒烟 | 见下方实施记录 |
| 9/9 | go.mod 冻结（含本切片依赖） | 不再新增第三方依赖 |

验收标准与实测结果：
1. ✅ `backend: memory`（及缺省）行为与现状一致：全部既有测试零修改通过（仅 NewRegistry 签名适配）；
2. ✅ `backend: redis` 重启进程对话上下文保留：E2E 实测「我叫小宇」→ pkill → 重启 → 「我叫什么名字？」→ 答「你叫小宇呀」；
3. ✅ Redis 不可达时启动 fail-fast：miniredis 关闭后 NewSessionService 报 probe 错误（单测覆盖）；
4. ✅ 无本地 Redis 的机器 `go test ./...` 全绿：miniredis 单测 + 集成测试 env 缺省 skip。

### 实施记录（与设计的差异）

- E2E 冒烟另验证：进程死亡后 `smoke:hashidx:*` 3 个 key 仍在 Redis，TTL≈3553s 与 `session_ttl: 1h` 一致；冒烟用临时 smoke-config.yaml + 独立 key_prefix `smoke:`，测完已清理（Redis 清空、临时文件删除），未触碰真实 config.yaml；
- miniredis 实际解析到 v2.39.0（tidy 取最新，test-only）；
- 探针烟测按设计走 CreateSession → AppendEvent → GetSession → DeleteSession 全链路（AppendEvent 是 runner 生产热路径）；
- admin 测试增加 STORAGE_SESSION_* 环境变量防御性清空，防止宿主 env 把测试翻到 redis 后端。
