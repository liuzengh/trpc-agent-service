# Session Run Lease

本文描述 `trpcservice/sessionlease` 的运行契约：同一 Session 的入口互斥、TTL 接管、取消与资源释放。

当前实现是一把合作型 Run 租约：同一 Session 的其他 Worker 在入口收到 `409 session_busy`，持有者失效后可按 TTL 接管。它不提供存储写入 fencing，无法原子拒绝已在写入的旧 Worker；具体边界见第 4 节。

## 1. 模块组成

| 组件 | 路径 | 说明 |
| --- | --- | --- |
| 核心包 | `trpcservice/sessionlease/sessionlease.go`、`lease.go`、`digest.go` | `Coordinator`/`Lease`/`Holder` 接口、共享续约循环、Key 摘要 |
| 内存参考实现 | `trpcservice/sessionlease/memory.go` | 进程内默认实现，也是契约基线 |
| 一致性套件 | `trpcservice/sessionlease/sessionleasetest/` | 每个实现都必须通过的同一组断言 |
| Redis 实现 | `trpcservice/sessionlease/redis/redis.go` | 跨进程实现，Lua 脚本原子获取/续约/释放 |
| 进程接线 | `cmd/trpc-service/storage.go`、`main.go` | `TRPC_SERVICE_SESSION_COORDINATION` 与组合规则、关闭顺序 |
| HTTP 接入 | `trpcservice/web/platform.go` | 入口获取租约、409/503、派生 Run Context、释放规则 |
| 双 Worker 证据 | `cmd/trpc-service/dualworker_test.go` | 两个独立构建的 Worker 共用一个 PostgreSQL schema 和一个 Redis |

本模块不包含 PostgreSQL 租约后端、等待队列、Inbox/Outbox 或 `MaxRunDuration`。IM 的持久受理和执行预算由公共文本消费者负责；fencing token 不进入对外 API。

## 2. 租约是什么

租约的作用域是完整的 `sessiondir.Key`——`{tenant, app, principal, session, epoch}`，与 Session Directory 的 Pin 同一把键。任何一个字段不同就是另一把锁。

```text
<prefix>:{<sha256 digest>}:lock    owner token，带 PX 过期
<prefix>:{<sha256 digest>}:fence   只增不减的计数器
```

Key 里只有版本化的定长 SHA-256 摘要。租户、主体、Session ID 都不进入 keyspace，因此 `SCAN`、`MONITOR` 和慢日志里读不到 PII。摘要按字段长度前缀计算，`tenant-ab` + `c` 和 `tenant-a` + `bc` 不会撞成同一把锁。大括号是 Redis Cluster 的 hash tag，保证一次 Lua 调用里的两个 key 落在同一 slot。

三个脚本都在 Redis 内部原子完成：获取时"没有活锁才写入并 `INCR` fence"，续约和释放都先比对 owner token。**释放从不删除 fence**——一个可以被删掉、从 1 重新开始的计数器不是单调的。代价是 fence key 会随历史 Session 数量累积且不回收，这是一条已知限制。

### 2.1 续约绑定在获取它的 Context 上

`Acquire(ctx, key)` 里的 `ctx` 就是这次 Run 的生命周期。它结束（客户端断开、进程关闭、Run 正常走完）时，续约立即停止，**锁被留给 TTL 自然过期，而不是被删掉**。协调器 `Close()` 同理。

上游 Runner 在 Context 被取消之后，仍然会通过 `context.WithoutCancel` 继续写大约一秒的终态 Event。如果关闭时就把锁删掉，另一个 Worker 会在这一秒里拿到租约并和它并发写同一个 Session。把锁留给 TTL，等于给这段收尾留出它仍然独占的窗口。

只有一种情况会真正删除锁：Run 干净跑完后显式 `Release`。

### 2.2 续约的"未知"一律按失败处理

`Holder.Renew` 有三种结果，含义不能混：

| 返回 | 含义 | 循环的反应 |
| --- | --- | --- |
| `(true, nil)` | 后端确认续约成功 | 继续持有 |
| `(false, nil)` | 后端确认"你已经不是持有者" | 立刻判定失去租约，不重试 |
| `(false, err)` | 超时、连接断开、看不懂的回复 | 视为**未知**，重试到安全边界 |

未知会一直重试，直到 `expires - SafetyMargin`；到点仍未确认就宣告租约丢失。这条时间线的意义是：本进程**先于**其他 Worker 被允许接管的时刻停止认为自己持有租约。默认 TTL 15s、续约 5s、安全边界 2s，`Config.Validate` 会拒绝掉连一次重试都放不下的组合。

Redis 脚本返回本版本不认识的值时，也归到未知一类（`ErrUnavailable`），而不是猜。

### 2.3 迟到的"成功"不是成功

获取用的 Context 带一个上界：`TTL - SafetyMargin - RenewInterval`。锁是从命令**发出之前**开始计时的，所以确认晚到 d，续约循环第一次醒来时剩下的预算就是 `上界 - d`；d 一旦到达这个上界，循环会在第一个 tick 上不做任何续约就宣告租约丢失。

问题在于这个上界能不能真的作用到调用上。go-redis v9 只有在 `ContextTimeoutEnabled` 打开时才把 Context 的 deadline 变成 socket 读超时；关着的时候，命令可以在 deadline 过去很久之后带着"成功"回来。所以 deadline 不是唯一的防线：`Lifetime.HandOut` 在后端**已经回答之后**再查一次实际耗时。这一层是共享的，任何后端（包括社区自己写的）都绕不开，不会因为某个 `Acquire` 忘了写检查而漏掉。

三种情况拿到锁也不交付，各自返回不同的错误，因为对调用方含义不同：

| 情况 | 返回 | 理由 |
| --- | --- | --- |
| 协调器已关闭 | `ErrClosed` | 这次获取从没变成一次 Run |
| 调用方自己的 Context 已结束 | 该 Context 自身的错误，不包装 | "客户端走了"不是"协调坏了" |
| 确认晚于 `acquiredAt + (TTL - SafetyMargin - RenewInterval)` | `ErrUnavailable` | 没有续约预算的租约，调用方在 Run 被取消之前分辨不出它和一把活租约的区别 |

三种情况都会**把锁归还**，而不是留给 TTL：它们都没有变成 Run，没有需要 TTL 覆盖的收尾写入，留着只会白挡下一个 Worker 一个 TTL。归还是 owner-matched 且尽力而为的——`Holder.Release` 只在本持有者仍然拥有锁时删除，所以晚到的归还按契约就是空操作，不靠时序侥幸；删不掉就等 TTL。归还用的是一个**独立的**短超时 Context，否则"调用方已取消"这一路的释放会全部失败，正好把它要避免的锁留在那里。

## 3. HTTP 层的行为

`PlatformServer` 在解析 Pin 和 Runtime **之前**就取租约，所以被拒的请求不会留下任何副作用：不建 Pin、不进 Runtime 缓存。

| 情况 | 响应 | 说明 |
| --- | --- | --- |
| 另一个 Worker 正在跑这个 Session | `409` + `session_busy` + `Retry-After: 2` | 系统正常，稍后再来 |
| 协调后端不可达/回复无法解释/确认迟到（见 2.3）/已关闭 | `503` + `coordination_unavailable`，**不带** `Retry-After` | fail closed：宁可不跑 |
| 客户端自己取消 | 沿用 Context 自身的错误 | "客户端走了"不是"协调坏了" |

拿到租约后，请求 Context 会派生出一个 `runCtx`：请求本身结束、或者 `Lease.Done()` 关闭（租约丢失），都会取消它。**取消是尽力而为且最终一致的**，见第 4 节。

释放规则只有一条：**只有干净跑完才 `Release`。**

- 客户端断开、租约已丢失、进程关闭——都不释放，把锁留给 TTL。
- 干净结束时用 `context.WithoutCancel` 派生的 2 秒超时上下文释放，**释放失败不改动已经写出去的 HTTP 响应**。响应此时通常已经发完（SSE 尤其如此），把一个清理错误改写成用户可见的失败，是拿一个不影响正确性的问题去破坏一个已经成功的请求。
- fence **不进响应头，也不进响应体**。它不是给客户端的东西，暴露出去只会让人以为它有准入含义。

## 4. 这把租约不保证什么（准确表述）

这一节是本文最重要的部分。

**租约是合作型的（cooperative），不是强制型的（enforcement）。** 所有尊重它的参与者不会去跑别人正在跑的 Session，但存储层不强制任何东西。

`Lease.Fence()` 返回每把键单调递增的 token，它**只是一个观测句柄**：

> **fence 目前不参与 Session 写入准入。** 上游 `session.Service.AppendEvent` 没有 fence 或 CAS 参数；PostgreSQL/Redis Session 模块的 `WithAppendEventHook` 在写入之前执行，两步之间没有屏障，不是原子的。因此**不能**说"过期 writer 被原子拒绝"，也不能把这套机制叫做 enforcement fencing。

具体地说，下面这些写入仍然会发生，当前实现不阻止：

- **被暂停或被网络分区的持有者，在 TTL 内恢复后照样写。** 它的租约还没过期，它自己也不知道发生过什么。
- **Context 被取消后的 Runner 仍然写终态 Event**，通过 `context.WithoutCancel` 持续约一秒。这是上游有意的行为，租约靠"锁留给 TTL 过期"来覆盖这段尾巴，而不是靠拦住它。
- **TTL 接管之后的旧持有者。** 它会在下一次续约时得知自己失去了租约（`Done()` 关闭 → 取消 Run），但"得知"发生在它已经停止写之后还是之前，取决于它当时卡在哪里。

所以取消是**尽力而为且最终一致**的：失去租约会关闭 `Done()`、取消 Run Context，剩下的靠 TTL 窗口和上游自己的收尾。

### 4.1 部署形态

**当前只验证了单实例 Redis 部署。** 主从切换可能丢失尚未复制的锁 key，并使 fence 计数器回退，因此不保证 failover 下的互斥和 fence 单调性。需要这些保证的部署必须采用具备相应一致性语义的协调存储；平台租约层无法弥补后端已确认状态的丢失。

### 4.2 已知限制

- fence key 永不回收，一个历史 Session 一个小整数。要回收就得设计一个"在任何 Worker 还持有 token 时 fence 不会后退"的方案，目前没有。
- 没有等待队列。第二个 Worker 被拒绝，不排队。
- 没有 `MaxRunDuration`：一个一直在续约的健康 Worker 可以无限持有租约。

## 5. 进程配置

存储和协调是两根**独立**的轴，因为它们回答的是不同问题：

```bash
TRPC_SERVICE_STORAGE_PROFILE=inmemory|postgres      # 数据放在哪
TRPC_SERVICE_SESSION_COORDINATION=inmemory|redis    # 谁来仲裁 Run
TRPC_SERVICE_REDIS_URL=redis://...                  # 仅 coordination=redis 时需要
```

未设置等于 `inmemory`，进程不建立任何连接。合法组合与它们的含义：

| profile | coordination | 结果 |
| --- | --- | --- |
| `inmemory` | `inmemory` | 默认。单进程，零依赖，`go test ./...` 不触网 |
| `postgres` | `inmemory` | **合法**：持久化存储上的单 Worker 部署 |
| `postgres` | `redis` | 多 Worker 部署，共享持久化部署 |
| `inmemory` | `redis` | **启动即拒绝** |

最后一行是刻意的：Session 存在各自进程的内存里，此时用一把共享锁去仲裁它们，锁保护的是它根本管不到的状态。**一把共享锁配不共享的 Session 是假安全**，它只会让人以为并发问题解决了。未知的 coordination 取值同样是拒绝而不是回退到默认值。

连接 URL 只在启动日志里显示 `set`/`absent`，从不打印内容。URL 解析失败的错误经 `sessionbackend.Scrub` 脱敏后才带上配置错误哨兵——`Scrub` 有意切断 unwrap 链，所以顺序必须是先脱敏再包哨兵，否则调用方就分不出"配置写错了"和"服务器连不上"。

关闭严格逆序：先关协调器（停止续约、不删锁），再关它借用的 Redis 客户端和连接池。协调器借用客户端而不拥有它，`Close()` 从不关闭传进来的客户端。

这个顺序之所以成立，是因为 `Close()` 会**等**：它返回时，这个协调器发出的续约和获取都已经不在路上，客户端在这一刻关闭才是安全的。取消只能拦下还没上线的调用（在连接池里排队、在重试间隙里 sleep）；已经发出去的命令拦不住，`Close()` 会等它回来。

**等多久是客户端的属性，不是本包的。** go-redis v9 默认把 `context.Background()` 传给 socket 读写，本包设的 deadline 到客户端边界就停了。所以分两种情况说，不能混成一句"都有界"：

- **本进程自己建的客户端。** `openLeaseRedisClient` 在 `ParseURL` 之后显式设置 `opts.ContextTimeoutEnabled = true`，因此 `Ping`、`Acquire`、`Renew`、`Release` 的上界各自由本包给的 Context 决定——获取是 `TTL - SafetyMargin - RenewInterval`，续约是它自己的调用超时。`ParseURL` 不会打开这个开关，URL 里也没有对应参数（go-redis 会拒绝不认识的 query key），所以这一行是唯一能打开它的地方。它设在这里是安全的：这个客户端只给协调器用，`redislease.New` 是唯一拿到它的东西，当前代码里**没有** Redis Session store 与它共用（存储 profile 里根本没有 Redis 这一档）。哪天真出现共用，这就不再是租约的决定，而是那个共享客户端的决定。
- **调用方自己传进来的客户端。** `redislease.New` 接受任意 `goredis.UniversalClient`，这正是它作为社区扩展点的含义，也是它能承诺的上限。如果传进来的客户端既不让 Context 变成 socket deadline，又把 `ReadTimeout` 关掉（而不只是设得很长），那么在途命令就没有任何本层能给的上界，而 `Close()` 又要等在途命令——这种客户端可以把关闭一直挂在那里。这是调用方选择的客户端的性质，本包代替不了，也**不宣称**任意客户端下 `Close()` 都有界、都不会挂死。

互斥不依赖上面任何一条：确认迟到的锁一律拒绝交付并归还，见 2.3——那道检查在后端回答**之后**量实际耗时，不靠 deadline 有没有被遵守。

"关闭不删锁"有一类例外，也是 2.3 里那三种：一次获取已经被放行、却在 `Close()` 之后（或调用方已取消之后、或超出预算之后）才拿到锁，它会被拒绝，并且**锁被归还而不是留给 TTL**——这次获取从没变成一次 Run，没有需要 TTL 去覆盖的收尾写入，留着只会白白挡住下一个 Worker 一个 TTL。

## 6. 测试

### 6.1 默认不触网

`go test ./...` 在没有 Redis、没有 PostgreSQL、没有网络的机器上必须通过。内存实现覆盖全部契约；需要真实后端的测试默认 `t.Skipf`。

时序相关的性质也不靠"跑够多次总会撞上"来验。2.3 那三条在 `lifetime_internal_test.go` 里是确定性的：迟到的确认写成一个**往前推**的 `acquiredAt` 时间戳（迟到的回复本来就等价于此，因为锁从命令发出前开始计时），不 sleep、不连接、不等超时；被拒的两条各自断言锁确实回到了 store，正常的一条断言仍然交付租约、`Done()` 未关闭。`ContextTimeoutEnabled` 同理，`storage_test.go` 只读回工厂建出来的 `*redis.Client` 的 options，`NewClient` 是惰性的，不发包。

### 6.2 一致性套件

`sessionleasetest.RunCoordinatorSuite` 是一份被两个实现共用的契约，不是被复制的两份断言。每个子测试拿到一个隔离的后端，并可以在它上面建多个协调器——两个协调器一个后端，就是"两个 Worker 一个 Session"在单进程里的表达。套件只断言接口承诺的东西，不碰 key 命名和 Lua 脚本；尤其**不**断言"失去租约的持有者已经停止写入"，因为没有实现能做到这一点。

内存实现没有"后端不可达"这种失败模式，相关子测试是 `t.Skip`，不是伪造通过。

### 6.3 集成测试的运行方式

需要显式开关加上对应后端的变量，每次运行用独立的 key 前缀和独立 schema，结束时清理。

```bash
# 起依赖（仅监听 127.0.0.1）
docker compose -f deploy/docker-compose.session.yml up -d --wait

# Redis 实现的契约与 key 布局
TRPC_SERVICE_SESSION_INTEGRATION=1 \
TRPC_SERVICE_REDIS_URL='redis://:trpc-local-dev@127.0.0.1:56379/0' \
go test -race -count=1 -timeout 300s ./trpcservice/sessionlease/...

# 双 Worker：两个独立 Worker，一个 PostgreSQL schema，一个 Redis
TRPC_SERVICE_SESSION_INTEGRATION=1 \
TRPC_SERVICE_POSTGRES_DSN='postgres://trpc:trpc-local-dev@127.0.0.1:55432/trpc_session?sslmode=disable' \
TRPC_SERVICE_REDIS_URL='redis://:trpc-local-dev@127.0.0.1:56379/0' \
go test -race -count=1 -timeout 300s -run Integration ./cmd/trpc-service/...
```

上面的口令是 compose 文件的开发默认值，只监听本机。

### 6.4 双 Worker 证据

`cmd/trpc-service/dualworker_test.go` 里的两个 Worker 是各自独立构建的：各自的存储栈、各自的 Resolver、各自的协调器、各自的 HTTP server，共享一个 PostgreSQL schema 和一个 Redis。断言全部经由 HTTP：

- 第一个 Worker 停在会话中间时，第二个 Worker 对**同一** Session 返回 `409 session_busy`，对**其他** Session 正常放行；
- 第一个 Worker 结束后，第二个 Worker 接着同一段会话继续对话，读到完整历史（无 sticky session）；
- 持有者停止续约后，TTL 到期，另一个 Worker 接管，fence 前进；
- `TestIntegrationBootstrapWiresRedisCoordination` 走完整的 `openStorage` 路径，验证 `coordination=redis` 真的接到了默认 key 前缀上——而不是只验证测试自己手工搭的协调器。

同一份文件的注释里写明了它**不**证明什么：被 SIGSTOP 的 Worker、被分区的 Worker、以及取消之后仍在写终态 Event 的 Runner，都不受这套机制阻止。
