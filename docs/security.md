# 安全设计说明

本文集中说明平台的安全机制，每条都给出关键代码位置，评审可直接对照源码验证。
阅读顺序建议：先 [docs/design.md](./design.md) 第 2 节「决策三」了解密钥管理的取舍背景。

| 层面 | 机制 | 关键落点 |
|---|---|---|
| 密钥管理 | 不落库只存引用，运行时 Secret Resolver 解析 | `trpcservice/config/secret*.go` |
| Admin API | 内网监听、token 鉴权 fail-closed、可选 mTLS、错误响应分层 | `trpcservice/web/admin.go`、`cmd/trpc-service/main.go` |
| 多租户隔离 | 配置/数据/工具/审计四层隔离，身份解析 fail-closed | `trpcservice/tenant/`、`trpcservice/web/binding.go` |
| 治理 | 白名单/脱敏/敏感词/token 预算/危险工具带内审批 | `trpcservice/agent/guardrail.go`、`approval.go` |
| 日志与审计 | zap 字段脱敏、URL 密钥打码、审计两路写入 | `trpcservice/log/log.go`、`trpcservice/storage/audit.go` |
| 输入防护 | 回调验签先于解析、配置反序列化严格模式 | 各通道适配器、`web/admin.go` |

---

## 1. 密钥管理：不落库，只存引用

**原则**（design 决策三）：密钥不出现在配置、数据库、日志和 trace 中。数据库里只有
**引用名**——`channel_binding.token_ref` / `aeskey_ref`、`config.secret_ref`、
`tenant.storage_config` 的 `dsn_ref`，以及 env 侧的 `TRPC_MODEL_APIKEY_REF` 等。

运行时由 `config.SecretResolver` 接口（`trpcservice/config/secret.go`）按引用解析：

- **FileResolver（本地默认，`TRPC_SECRET_RESOLVER=file`）**：ref 即 `TRPC_SECRETS_DIR`
  （默认 `data/secrets/`）下的文件名，文件内容去空白后就是明文。ref 必须是**裸文件名**，
  `filepath.Base(ref) != ref`（如 `../x`）直接拒绝——防路径穿越。
- **KMSResolver（生产，`TRPC_SECRET_RESOLVER=kms`）**：`GET {TRPC_KMS_ENDPOINT}/v1/secrets/{ref}`，
  Bearer token 鉴权，5s 超时，非 200 即错误。ref 必须匹配
  `^[A-Za-z0-9][A-Za-z0-9._-]*(/…)*$`（`secretRefPattern`）——ref 会被拼进请求路径，
  不校验的话，能写绑定配置的人就能让平台去请求任意 KMS 路径。
  KMS 自己的 token 也是密钥：经 file resolver 从 `TRPC_KMS_TOKEN_REF`
  （默认 `kms-bootstrap-token`）指向的文件读取——KMS 凭据不能来自 KMS。
- **CachedResolver（两种后端都套）**：进程内短 TTL 缓存（`TRPC_SECRET_CACHE_TTL`，默认 1m）。
  后端短暂故障时**服务陈旧值**（轮换用新 ref，TTL 内的陈旧是安全的）；密钥值只进缓存、
  不进日志。

**K8s 环境的注入约定**（`deploy/k8s/README.md`）：pods 唯一直接持有的明文是 KMS
bootstrap token，由 Secret 挂载到 `TRPC_SECRETS_DIR`（`/etc/trpc/secrets`）；其余密钥全在
KMS、以 `*_REF` 引用。漏挂这个 Secret，resolver fail-closed，三个角色一起 CrashLoop。

**轮换**：换新 ref 名（如 `wecom-secret` → `wecom-secret-20260908`）并存入新值，改绑定/配置
指向新 ref，旧值留到缓存 TTL 过期后下线。wecomws 的 bot secret 每次重连时经 resolver 现取，
轮换后下次重连即生效；wecom/wxkf 的 `access_token` 缓存以 `(corp_id, secret_ref)` 为键，
平台报 40014/42001 时只失效对应身份的条目。

## 2. Admin API 安全

Admin API 能改租户的模型端点、重写策略，是攻击面价值最高的入口（`web/admin.go`）：

- **仅内网监听**：`TRPC_ADMIN_ADDR` 默认 `127.0.0.1:8081`；K8s 里是 ClusterIP、不进 Ingress。
  管理面与公网回调口（`TRPC_HTTP_ADDR :8080`）物理分离——回调口靠验签，Admin 口靠 token。
- **token 鉴权 fail-closed**：`TRPC_ADMIN_TOKEN` 未设置时**进程拒绝启动**
  （`cmd/trpc-service/main.go` 的启动门），且运行时还有一道兜底——token 为空则所有
  `/admin/*` 路由回 503（`AdminAPI.auth`）。比对用 `subtle.ConstantTimeCompare`。
  哨兵值 `dev-insecure` 只在监听 loopback 时被接受，绑到非回环地址会被启动门拒掉。
- **可选 mTLS**：`TRPC_ADMIN_TLS_CERT` / `TRPC_ADMIN_TLS_KEY` / `TRPC_ADMIN_TLS_CLIENT_CA`
  三件套齐备即启用，客户端证书须由给定 CA 签发。操作人归属优先取证书 CN（持有共享 token
  的人谁都能伪造 `X-Admin-User` 头，所以它只是纯 token 部署下的兜底，
  `web/admin.go` 的 `OperatorID`）。
- **错误响应分层**（`writeOpError`）：本服务自己判断的失败（找不到、冲突、配置被拒）
  原样返回可读消息；**数据库/向量库层的失败只回 `"<操作名> failed"`（500）**——pgx 错误
  会带上表名、列名、约束名、SQLSTATE，连接失败时还有 DSN 的 host/port/user/db，这些只进
  服务端日志。响应体是最容易被原样贴进工单和群聊的东西，而 Admin 口正是拿着泄露 token
  的人最想探的地方。
- **列表接口的 `rows.Err()` 陷阱**：pgx 的 `Query` 对 bind/execute 阶段失败返回 nil 错误，
  错误挂在 `rows.Err()` 上；漏查就会把「读失败」答成 200 空列表——事故期间租户、绑定、
  审计查询显示「什么都没有」。所有列表接口都过了 `listFailed`。
- **请求体上限**：admin 端点 1MB（`adminBodyLimit`），防有效 token 持有者用大 body 钉住
  内存并灌爆审计明细列。
- **变更审计**：所有写操作记审计（操作人 + 变更前后内容，`audit_log.detail` 的
  `{"before":…,"after":…}`），并广播配置失效。

另外两处与 Admin 相邻的默认：

- `/metrics` 既不在 Admin 口也不在回调口：每个角色另起内网 metrics 监听
  （`TRPC_METRICS_ADDR`，默认 `127.0.0.1:8082`）——导出的序列带租户维度流量、token 消耗
  和队列积压，挂到公网可达的 mux 上等于白送侦察材料。
- mock 通道默认关闭（`TRPC_MOCK_CHANNEL=false`）：它的回调是**无鉴权消息注入器**，
  开启必须显式声明，且绝不能暴露到 Ingress。

## 3. 多租户隔离

四层隔离，每层都有独立的身份依据：

- **配置**：租户/应用/绑定配置在 PG，经 `tenant.Resolver` 全量快照（TTL 30s + Redis
  pub/sub `tenant:invalidate` 失效广播）下发到所有进程；`tenant_id` 从回调路径解析后
  随消息全链路透传。
- **数据**：会话唯一性是 `(app_id, session_key)`，app 隶属租户，跨租户**天然隔离**——
  同一微信用户出现在两个租户的渠道里，会落在两个不同 app 命名空间下。记忆按
  `(tenant_id, user_id, app_id)` 检索（`memory_item`），审计按 `tenant_id` 归档查询。
  Redis 侧的幂等键（`dedup:`/`done:`/`sent:`）、会话锁（`lock:sess:{app_id}:…`）、
  审批记录（`approval:{app_id}:…`）都带 app/binding 维度——不同租户携带相同
  `channel:user` 会话键也互不可见。
- **工具**：两级白名单收窄（`agent/assemble.go`）：`tenant.tool_policy.allow` 先收窄，
  `agent_app.config.tools.allow` 再收窄，非空即白名单。
- **审计**：每条审计记录带 `tenant_id`，查询接口按它过滤。

**身份解析 fail-closed**（防跨租户冒用的关键，不是锦上添花）：

- 回调侧：绑定级回调（`/callback/{channel}/{binding_id}`）必须用**绑定自己的**
  token/AESKey 验签；引用为空即错误，dispatcher 回 503（`web/binding.go`）——
  回退到 env 全局密钥会让持有全局密钥的人伪造任意租户的回调。
- 出站侧：回复只携带 `binding_id`，适配器据此解析自己的发送身份；绑定查不到或
  `config` 解析失败时**发送直接失败**，不回退全局身份——否则一个租户的消息会以
  另一个企业的机器人发出（`channels/wecom/wecom.go`、`channels/wxkf/wxkf.go` 的
  `outboundIDFor`）。
- 路由侧：`EnqueueHandler` 的 `Routes` 为 nil（PG 启动期不可用）时拒绝一切回调，
  fail closed——未路由的消息绝不能绕过隔离、限流和治理链。
- wecomws 侧：绑定的 `webhook_path` 后缀必须等于 `config.bot_id`，否则每条消息都被
  解析到错误的绑定上静默黑洞。

## 4. 治理（Guardrail 链）

`agent.Guarded`（`trpcservice/agent/guardrail.go`）包装 Worker 的内层处理，顺序固定：
撤回事件处理 → 租户策略加载 → **用户白名单** → **审批应答匹配** → **输入敏感词** →
**token 预算** → 内层 Runner → **输出脱敏与敏感词** → 审批确认消息组装 → 审计落库。

- **输入白名单**：`guardrail_policy.input_allow_users` 非空即白名单，名单外直接 deny。
  它排在审批应答**之前**——被移出白名单的用户不能去确认自己还在白名单时发起的危险操作。
- **输入敏感词**：`guardrail_policy.input_deny_words` 非空时**覆盖**平台基线
  （`DefaultBlockedWords`）；命中 deny，回复固定文案，不点名哪个词。
- **输出脱敏**：`RedactOutput` 用正则把回复中的手机号、身份证号、邮箱打码为 `***`；
  之后过租户的 `output_deny_words`，命中则整段替换为固定文案并记 deny。
  注意审批确认消息也走同一道输出检查——它内嵌工具原始参数，没经过模型侧检查。
- **token 预算**：`guardrail_policy.max_tokens_per_day` > 0 时启用，超预算直接 deny；
  实际消耗在每次运行结束后记账（模型中途失败也计——否则会话可以靠持续故障绕过预算）。
  已知限制（见 `docs/README.md` §10）：预算窗口是「首次使用后 48h 滑动窗」而非自然日，
  Allow/Record 非原子，并发超支可达 N 倍。
- **危险工具带内审批**（`agent/approval.go`）：工具标 `Dangerous` 后被拦截，向用户发
  确认消息、写 `approval:{app_id}:{session_key}`（带原生 TTL）、审计记 `review`，
  **释放会话锁**挂起（Worker 保持无状态）。答复精确匹配「确认/拒绝/取消」
  （`strings.TrimSpace` 后全等）；其他任何内容**不消费也不作废**审批，按普通消息处理；
  超时默认 5 分钟（`DefaultApprovalTimeout`）按拒绝记 `review_timeout`；同会话同时只允许
  一个待审批，冲突直接 deny（`approval_conflict`）；群聊仅消息发起人或租户配置的审批人
  的答复有效。

**模型端点白名单**：租户/应用配置里的 `model.base_url` 必须 https 且 host 精确命中平台
白名单（`TRPC_MODEL_BASE_URL_ALLOW`，默认即平台自身端点的 host），在 Admin 的五条写路径
与 Worker 装配时**双重校验**（`agent.ValidateModelConfig` / `ValidateAppConfig`）。
会话原文全部流向该端点，端点选择权必须留在平台层。

## 5. 日志与审计

**日志脱敏**（`trpcservice/log/log.go`）：zap Core 层（`redactCore`）按**字段名精确匹配**
把敏感字段值替换为 `***`（token / access_token / secret / password / aeskey / api_key /
authorization / cookie 等），`Write` 和 `With` 两条路径都覆盖。两个已知盲区靠 review 守：
不要用 `zap.Any` 打整个结构体（嵌套密钥不匹配字段名），不要把密钥拼进消息字符串
（脱敏只看字段 key，不看消息内容）。URL 内嵌的密钥由 `channels.ScrubError` 在源头打码
（`access_token`、`corpsecret` 在 query string 里，`net/http` 的 `url.Error` 会带完整
URL）。wecomws 的 bot secret 走 subscribe 帧的 body 而非 URL，不会进任何访问日志。

**审计两路写入**（`trpcservice/storage/audit.go`）：

- 常规 `allow` 事件：内存缓冲**异步批量写**（满 100 条或满 1 秒触发，pgx 批量一次写入），
  不在请求关键路径上；缓冲队列满（持续过载）时丢弃**计数并记 error 日志**
  （`audit_dropped_total` 指标 + `Auditor.Dropped()`）——审计出现缺口必须能被运维发现，
  而不是悄悄漏掉。
- `deny` / `review` / `review_timeout` / 危险工具执行等关键决策：**同步写入**
  （3s 超时），宁可增加毫秒级延迟也不接受丢失（合规红线）。
- 批量写失败重试 3 次后降级为逐行恢复——批次是原子的，一行毒数据不该埋掉同批的
  99 条合法事件；逐行仍失败的行记 error 并计数。
- Admin 写操作的变更前后内容进 `audit_log.detail`。

**审计注入防护**：`channel_binding.config` 会**原样**写进审计明细，所以三个通道的
`ValidateBindingConfig` 都用 `json.Decoder.DisallowUnknownFields()`——一个未被通道识别的
键（比如明文 `secret`）会被 400 拒掉，而不是「既进审计又不生效」。发布/回滚等状态切换
也在写路径上重新校验配置（一个策略变更前存的 draft 不能绕过新白名单上线）。

## 6. 回调入口的输入防护

- **验签先于解析**：wecom/wxkf 的 POST 回调先过 `wxbizmsgcrypt` 验签解密再解 XML；
  签名错误或报文非 XML 的回调按互联网垃圾 ack 丢弃，签名通过但解密失败回 5xx 等平台
  重推（通常是 AESKey 配错，修好能救回消息）。回调体上限 1MB。
- **时间戳防重放**：签名不过期，防重放靠消息自带 `CreateTime`——偏离当前 ±5 分钟
  （`channels.CallbackTimestampWindow`）的回调直接丢弃。这是唯一比 24h 幂等键活得久的
  重放边界。
- **入口幂等**：`dedup:{channel}:{binding_id}:{msg_id}`（SET NX EX，24h；wxkf 因 sync_msg
  3 天重拉窗口加宽到 4 天）挡 IM 重推；去重在限流之后、入队之前，入队失败回滚去重键，
  让平台重推不被吞掉。
- **准入控制**：租户级令牌桶（`TRPC_GATEWAY_RATE_QPS`/`_BURST` 默认 50/100，租户
  `rate_policy` 可覆盖）防单租户灌量；`XLEN` 背压（队列达上限 80% 拒收）界定「不丢」
  承诺的边界。
