# Phase 1 Spike 结论

> 核对日期：2026-08-24

## 依赖结论

| 依赖 | 结论 |
| --- | --- |
| `trpc.group/trpc-go/trpc-agent-go v1.11.2` | module path、Go 1.21 声明、Runner/OpenAI/Session/Memory API 已编译并在真实调用链使用 |
| `github.com/go-telegram/bot v1.23.0` | 临时模块编译通过；保留为 Telegram 首选 SDK，不写入生产 `go.mod` |
| `github.com/silenceper/wechat/v2 v2.1.14` | 临时模块编译通过；应用消息发送可复用，内部应用普通回调需平台薄适配 |
| `github.com/redis/go-redis/v9 v9.7.0` | Phase 1 快照适配器使用的历史版本；Phase 1.5 已随官方子模块升级到 `v9.11.0` |

## 模型与 Redis

- tRPC-Agent-Go `model/openai` 会基于配置的 Base URL 拼接 chat completions 路径。
- `deepseek-v4-flash` 已分别使用 `https://api.deepseek.com` 和 `https://api.deepseek.com/v1` 真实调用成功；生产示例采用较短的 `https://api.deepseek.com`。
- OpenAI SDK 默认会重试部分 5xx；Phase 1 显式设置 `MaxRetries=0`，确保上游 4xx/5xx 在模型超时前确定映射为 502。平台重试留到具备 Inbox/Outbox 与幂等后实现。
- API Key 只从宿主用户环境变量读取；测试和输出没有打印 Key。
- Docker Desktop 27.2.0 与 `trpc-redis` 容器已核验，`redis-cli ping` 返回 `PONG`。
- 默认 Codex 沙箱不能访问 Docker 配置与 named pipe；宿主执行正常，该差异不属于 Windows 或 Docker 故障。
- 真实 Redis Smoke Test 已完成：Mock OpenAI-compatible 模型、真实 Runner、Redis Session 写入和回复链路通过。

## 框架 Redis 能力差异（Phase 1 当时结论，Phase 1.5 已纠正）

Phase 1 当时只检查了根模块 `trpc-agent-go@v1.11.2` 的下载目录，因此误写为远程没有 Redis 包。准确事实是：根模块目录不包含这些包，但同一多模块仓库已独立发布 `session/redis`、`memory/redis`、`storage/redis v1.11.0`。Phase 1.5 已在隔离消费者中验证它们可与根模块 `v1.11.2`、Go 1.21.13 和 Runner 一起使用，完整证据见 `docs/stage1.5-storage-spike.md`。

Phase 1 当时采用平台自研快照适配器：

- 以框架 `session.Service` 和 `memory.Service` 为接口契约。
- Redis 保存 Session/Event 和 Memory 的 JSON 快照，禁止故障时回退 InMemory。
- 已验证 Session 跨 runtime 可见、Memory 跨实例可见。
- 当前只承诺 Phase 1 使用到的 Session 创建/读取/事件追加/Session State 和 Memory CRUD/关键词搜索。
- App/User State、Summary 持久化、ListSessions、分页、跨进程原子更新和生产迁移语义留到共享后端阶段完善。

Phase 1.5 已选择“官方 Redis Service + 平台生命周期薄包装”，删除上述快照适配器和 `ErrUnsupported`；历史段落仅用于解释 Phase 1 的完成边界，不再描述当前生产运行时。

## Telegram 覆盖矩阵

| 能力 | SDK 覆盖 | 后续 Adapter 责任 |
| --- | --- | --- |
| 文字、群聊、reply、topic、平台消息 ID | `models.Update` / `models.Message` 覆盖 | 转换为平台统一消息 |
| Polling / Webhook | `Start`、`StartWebhook`、`WebhookHandler` | 生命周期和入口鉴权 |
| 自定义 HTTP | `WithHTTPClient`、`WithServerURL` | Mock、超时、代理和日志脱敏 |
| 429 | `TooManyRequestsError.RetryAfter` | 统一重试分类和退避 |
| Shutdown | `Start(ctx)` 随 context 取消 | 进程优雅退出和 goroutine 验收 |

## 企业微信覆盖矩阵

| 能力 | `silenceper/wechat/v2` 覆盖 | 结论 |
| --- | --- | --- |
| 内部应用 access token / 应用消息发送 | `work.NewWork`、`GetMessage().SendText` | 可复用 |
| AES 算法 | `util.EncryptMsg` / `util.DecryptMsg` | 可复用底层算法，但需平台校验 CorpID |
| SHA1 签名算法 | `util.Signature` | 可复用算法 |
| 内部应用普通回调 URL 验证 | 没有对应的统一 Work API；现有实现主要位于 `work/kf` | 平台补 `VerifyInternalAppURL` |
| 普通回调签名 + 解密 + XML 文本解析 | 没有完整入口 | 平台补 `DecodeInternalAppCallback` 和 `ParseInternalAppText` |
| 可替换 endpoint / HTTP Client | 多数 Work API 使用固定官方 URL 和包级 HTTP 工具 | 正式 Adapter 外包 client 接口，用 `httptest` 验证错误分类 |
| errcode、429/5xx/超时分类 | SDK 提供部分公共错误解码，但没有平台统一分类 | 平台补 `ClassifyWeComError` |

明确缺口函数：

```text
VerifyInternalAppURL(token, corpID, aesKey, signature, timestamp, nonce, echoStr)
DecodeInternalAppCallback(token, corpID, aesKey, signature, timestamp, nonce, body)
ParseInternalAppText(decryptedXML)
ClassifyWeComError(httpStatus, errCode, err)
```

这些函数属于后续企业微信 Adapter，不在 Phase 1 生产入口中实现。

## RunnerCache 结论

生产参数锁定为 Phase 1 保守值：最大 32 项、空闲 TTL 30 分钟、创建超时 10 秒、排空超时 30 秒、关闭超时 5 秒。

- 同一 key 100 并发获取仅创建一次。
- `tenant + app + config_version` 不同版本创建独立 Runner。
- 排空后 Runner 只关闭一次，Cache 重复关闭幂等。
- 共享 Session/Memory Service 由 runtime 持有，Runner 关闭不关闭外部传入 Service。
- Phase 1 只有一个 Agent App，容量参数没有实际压满；本阶段验证的是机制，容量需在后续真实租户规模和指标下重新校准。
- 实际 Runner 构造只组装 Model/Agent/Runner，不进行网络 I/O；端到端 Mock 首次请求约几十毫秒。100 并发测试人为加入 10ms 构造延迟后仍只创建一次。
- 排空测试使用缩短后的 20ms 超时验证强制关闭和错误返回；生产值 30 秒未通过长请求样本重新标定。容量 32 与资源占用仍是 Phase 1 保守锁定值，不是生产规模结论。

## 尚未扩大为 Phase 1 承诺的事项

- 两种 IM 只完成 API/边界 Spike，正式 Adapter 后置。
- `message_id` 不提供 Inbox 幂等。
- 当前单进程 `executor` 不是后续独立 Worker 进程。
- Phase 1 的 Redis 快照适配不是完整生产后端；它已在 Phase 1.5 被官方 Redis Service 替换。
