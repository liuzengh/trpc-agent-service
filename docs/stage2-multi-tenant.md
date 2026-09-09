# Phase 2 多租户配置与 RunnerRegistry 验收结论

> 验证日期：2026-08-27；收口复验：2026-08-28
> 代码基线：`feature/phase1.5-storage-spike@fb3cfa3`
> 实施分支：`codex/phase2-multi-tenant-runner`
> 审阅状态：2026-08-28 已通过

## 结论

Phase 2 已将固定 `demo-binding -> tenant-demo -> assistant -> v1 -> Redis` Runtime 升级为可信绑定驱动的多租户执行内核：

```text
Demo HTTP -> InboundMessage -> ChannelBinding
  -> Tenant -> AgentApp.active_config_version -> ConfigVersion
  -> StorageProfile -> BackendProvider -> RunnerRegistry
  -> tRPC-Agent-Go Runner -> OutboundMessage
```

现有 `/healthz`、`/readyz`、`/api/v1/demo/messages` 和 legacy 环境变量入口保持兼容。`start.sh` 已修正为实际执行 `trpc-service serve` 并透传监听参数。客户端仍不能提交 `tenant_id`、`agent_app_id`、`config_version` 或 `storage_profile_id`。

## 已实现能力

### 不可变目录与可信绑定

- `Tenant`、`AgentApp`、`ChannelBinding`、`ConfigVersion`、`StorageProfile` 和 `ModelConfig` 已形成只读 Catalog。
- `PresetRepository` 在启动时建立索引，校验组合键唯一、Binding ID 全局唯一、启用状态、活动版本以及所有跨对象引用。
- `PLATFORM_CONFIG_FILE` 加载最大 1 MiB 的严格 JSON；拒绝未知字段、多余 JSON 和非 `schema_version=1`。
- JSON 凭据只允许 `env:<ENV_NAME>`；启用 AgentApp 的所有版本所需模型/Redis 凭据在启动时检查。
- 模型 `base_url` 只允许不含 userinfo、query 和 fragment 的绝对 HTTP(S) URL，避免绕过凭据引用保存秘密。
- 未设置 `PLATFORM_CONFIG_FILE` 时，原 Phase 1 环境变量会转换成等价 legacy Catalog，Redis 前缀保持 `<REDIS_KEY_PREFIX>:official-v1`。

示例目录位于 `configs/phase2.example.json`，包含一个 Redis 租户和一个仅供单进程开发验证的 InMemory 租户。

### BackendProvider

- 按 `tenant_id + storage_profile_id` 并发安全地缓存 Backend。
- InMemory 复用官方 `session/inmemory`、`memory/inmemory`；Redis 复用 Phase 1.5 官方 Service 和 `RedisBackend`。
- JSON Redis 命名空间固定为 `<base>:tenant:<tenant_id>:profile:<profile_id>:official-v1`。
- `/readyz` 严格检查启用 AgentApp 的所有声明版本引用的 StorageProfile；任一 Redis 不可用即返回 `503`。
- 单条消息只检查路由选中的 Profile；其他健康租户不会被错误路由或回退到故障租户的后端。
- Provider 关闭所有 Runner 后再关闭 Session/Memory；Backend 和 Provider Close 均幂等。

### RunnerRegistry 与 Runtime

- Runner key 固定为 `tenant_id + agent_app_id + config_version`。
- Registry 精确读取不可变 ConfigVersion、解析模型凭据并选择对应 Backend。
- RunnerCache 继续提供 singleflight、Lease、TTL、排空和幂等关闭，并新增容量满时仅淘汰 `refs == 0` 的最久未使用项。
- 不同租户、Agent 或版本使用不同 Runner；AppName 保持 `tenant/<tenant_id>/app/<agent_app_id>`，跨版本延续同一 App 的 Session 命名空间。
- 配置/后端变化必须发布新版本；StorageProfile 切换不迁移、不双读、不 fallback。
- Runtime 关闭持有生命周期写锁，等待活跃 Handle 结束后按 RunnerRegistry -> BackendProvider 顺序关闭。

## 验收证据

自动化测试覆盖：

- JSON 正常加载、示例目录、最大长度、未知字段、多 JSON、schema、ID、重复键、悬空/跨租户引用。
- 启用 AgentApp 的活动和非活动版本凭据检查、模型 URL 防泄漏和 Config 格式化脱敏。
- 未知/禁用 Binding、HTTP 伪造 `tenant_id`、Repository/Storage/Runner 分类错误。
- Redis/InMemory 选择、租户化 Redis 前缀、严格 readiness、故障无 fallback 和恢复。
- 两租户在相同外部用户/会话 ID 下的配置、Runner、Session 和 Memory 隔离。
- Runner singleflight、版本隔离、LRU/TTL、排空、Close 幂等和 active request drain。
- Mock 模型端到端下 `v1 -> v2` 新请求使用新 Runner，旧活跃请求完成后可安全排空旧 Runner；InMemory -> Redis 新版本从空 Session 冷切换。

真实 Redis 7 使用独立 `trpc-phase2-redis` 容器和 `redis:7-alpine`：

- Phase 2 租户化前缀与官方 `official-v1` 子命名空间正确。
- 两个 Runtime 连接同一 Profile 时 Session 历史跨实例可见。
- Redis `CLIENT PAUSE 500 ALL` 期间同一 Runtime `/readyz` 依赖检查失败；恢复后无需重启重新就绪。
- legacy 真实 Redis Smoke 同时回归通过；验收后专用容器已停止并自动删除，其他容器未修改。

关闭检查结果：

```text
go test ./...             PASS
go test -race ./...       PASS
go build ./...            PASS
go vet ./...              PASS
Go 1.21.13 test ./...     PASS
Go 1.21.13 build ./...    PASS
真实 Redis 7 Phase 2      PASS
真实模型多租户 Smoke      PASS
```

2026-08-28 使用宿主用户环境中的模型凭据完成真实目录 Smoke：`deepseek-v4-flash`、`https://api.deepseek.com`、临时 Redis 7 下，`/healthz`、`/readyz`、`demo-memory` 和 `demo-redis` 均成功。两个租户响应的 request/session/text 均非空，相同外部用户和会话得到不同 Session ID；Redis 专用前缀实际产生 4 个键。凭据未写入 JSON、日志、文档或 Git。

`start.sh` 已静态核对为执行 `trpc-service serve "$@"`。当前 Windows 的 `bash` 是未安装 WSL 的占位程序，且没有 Git for Windows Bash，因此本机未执行 `.sh` 启停闭环；Go CLI/HTTP 行为由命令入口和 Handler 自动化测试覆盖。

## 已知边界与后续问题

- Demo `binding_id` 仍是本地模拟渠道选择器，不等于生产渠道验签；真实 Telegram/企业微信后置。
- JSON 不热加载。版本切换行为通过可切换测试 Repository 验证；生产预置目录更新仍需重启。
- 平台没有配置发布历史，无法跨部署自动识别“同版本号改内容”；必须通过 Git/评审保证版本不可变。
- 严格 readiness 会被任一启用版本的 Redis Profile 拉低，这是本阶段确认的保守语义。
- InMemory 只用于单进程开发与测试，不能作为后续多 Worker 的唯一共享状态。
- `message_id` 仍不幂等，Redis Streams、Inbox/Outbox、租约、重试和双 Worker 尚未实现。
- 后续阶段编号已按逻辑施工 Plan 统一：Phase 3 为 Gateway/Worker 消息可靠性，Phase 4 为双 Worker 与共享状态，Phase 5 为 Telegram、企业微信与 Web UI。
