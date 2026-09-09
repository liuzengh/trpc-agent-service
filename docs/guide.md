# 使用与开发指南

这份文档承接 [`快速开始`](./quickstart.md)——那里只带你跑通第一条消息、看到回复、建一个自己的租户。
跑通之后要做的「深一点」的事都在这里，按需查阅，不必从头读到尾。

| 章节 | 什么时候来看 |
|---|---|
| [1. 危险工具二次确认](#1-危险工具二次确认) | 想让模型在调用危险工具前先问用户 |
| [2. 观测：指标、队列、追踪](#2-观测指标队列追踪) | 消息没回复，要定位原因 |
| [3. 接真实企业微信](#3-接真实企业微信) | 把消息来源从 mock 换成真实 IM |
| [4. 写一个自己的工具](#4-写一个自己的工具) | 给 Agent 挂一个业务工具 |
| [5. 接一个新的 IM 通道](#5-接一个新的-im-通道) | 适配一个平台还没支持的 IM |
| [6. 像生产那样按角色拆进程](#6-像生产那样按角色拆进程) | 从 all-in-one 走向分角色部署 |

**参考手册**：[§7 Admin API 速查](#7-admin-api-速查) ·
[附录 A 配置与密钥机制](#附录-a配置与密钥机制) ·
[附录 B 故障排查](#附录-b故障排查) · [附录 C 重置开发环境](#附录-c重置开发环境) ·
[附录 D 日常命令](#附录-d日常命令)。

---

## 1. 危险工具二次确认

**目标**：完整走一遍「模型想调危险工具 → 平台拦下并问用户 → 用户确认 → 工具放行」。

`delete_user_data` 被标记为 `Dangerous`（它是个 stub，不会删任何真实数据），专门用来演示审批链路。

### 第一轮：触发

```bash
curl -s -X POST 127.0.0.1:8080/callback/mock/$B -H 'Content-Type: application/json' \
  -d '{"msg_id":"appr-1","user_id":"u-appr","text":"请调用工具删除用户 u-999 的全部数据"}'
```

会话事件里实测看到：

```
 1 | user      | 请调用工具删除用户 u-999 的全部数据
 2 | assistant |
 3 | assistant | "blocked: 该操作需要用户在对话中确认后才能执行"
 4 | assistant | 删除用户 u-999 的全部数据属于不可恢复的危险操作。请确认：您确定要永久删除该用户的全部数据吗？
```

同时：

```bash
# 审计记下 review 决策
psql -c "SELECT decision, tool_name FROM audit_log WHERE user_id='u-appr'"
# → review | delete_user_data

# 待审批记录在 Redis，键带 app 维度（跨租户互不可见）
docker compose exec -T redis redis-cli --scan --pattern 'approval:*'
# → approval:f365b692-…:dm:mock:u-appr
```

### 第二轮：答复

```bash
curl -s -X POST 127.0.0.1:8080/callback/mock/$B -H 'Content-Type: application/json' \
  -d '{"msg_id":"appr-2","user_id":"u-appr","text":"确认"}'
```

**验证**：审计多出一行 `allow | delete_user_data`，Redis 里的 `approval:*` 键被消费掉，回复正常下发。

### 答复规则

精确匹配（`strings.TrimSpace` 后全等，`agent/approval.go:308`）：

| 你发 | 效果 |
|---|---|
| `确认` | 放行原工具调用 |
| `拒绝` | 终止并告知用户 |
| `取消` | 同拒绝 |
| 其他任何内容 | **不消费、也不作废审批**，按普通新消息正常处理，审批继续挂起 |

超时默认 **5 分钟**（`agent.DefaultApprovalTimeout`），超时按拒绝处理并记 `review_timeout`。
同一会话同时只允许一个待审批，冲突直接 `deny`（`error_type=approval_conflict`）。
群聊里只有消息发起人（或租户配置的审批人）的答复有效。

> **一个会让人困惑的设计**：答复那一轮**不会出现在 `session_event` 里**。
> 因为审批答复是**控制字，不是内容**——它携带的是模型没有产生过的工具结果，
> 治理链第 3 步直接返回回复、不进 Runner（`agent/guardrail.go:126` 的注释就是这个意思）。
> 副作用是模型下一轮也看不到「确认」这句话。这一轮只在 `audit_log`（`decision=allow`）和日志里可见。

→ 下一步：[§4 写一个自己的工具](#4-写一个自己的工具)：把你自己的危险工具接进来。

---

## 2. 观测：指标、队列、追踪

**目标**：掌握「消息没回复」时第一手该看什么。

```bash
# 指标（注意用你实际设的 metrics 端口）
curl -s 127.0.0.1:8083/metrics | grep -E 'im_inbound_total|llm_tokens_total|stream_length'

# 队列积压——排查「消息没回复」的第一站
docker compose exec -T redis redis-cli XLEN stream:inbound
docker compose exec -T redis redis-cli XLEN stream:outbound
docker compose exec -T redis redis-cli XLEN stream:deadletter

# 服务日志
tail -f data/trpc-service.log
```

指标都带 `channel` / `tenant_id` 维度。常用的几个：`im_inbound_total`、`im_outbound_total`、
`im_dedup_dropped_total`、`im_end_to_end_duration`、`worker_process_duration`、
`worker_process_error_total`、`llm_tokens_total`、`gateway_rejected_total`、
`send_rate_limited_total`、`audit_dropped_total`、`stream_length`、`stream_pending`、
`stream_oldest_pending_seconds`。

**链路追踪**：compose 里的 Jaeger 已就绪，启动时加上

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317   # gRPC exporter 要 host:port，不带 scheme
```

UI 在 <http://localhost:16686>。这个变量**不是必需的**——不设也能正常启动，只是不导出 trace。

Prometheus 在 <http://localhost:9090>，抓取配置是 `deploy/prometheus/prometheus.yml`
（默认抓 `host.docker.internal:8082`；你若改了 metrics 端口，这里也要跟着改）。

→ 下一步：[§3 接真实企业微信](#3-接真实企业微信) 或 [§6 按角色拆进程](#6-像生产那样按角色拆进程)。

---

## 3. 接真实企业微信

**目标**：把消息来源从 mock 换成真实 IM。平台有三个真实通道，各一篇文档：

| 通道 | 文档 | 一句话选型 |
|---|---|---|
| `wecom` 企微自建应用 | [`channels/wecom.md`](./channels/wecom.md) | webhook 回调（需公网 HTTPS），支持群聊/媒体/markdown |
| `wxkf` 微信客服 | [`channels/wxkf.md`](./channels/wxkf.md) | 面向微信外部用户的客服单聊，「事件通知 + sync_msg 拉取」，48h 窗口 |
| `wecomws` 企微智能机器人 | [`channels/wecomws.md`](./channels/wecomws.md) | WS 长连接（免公网入口），每 bot 单连接、Redis leader 锁保证单持有者 |

> **实测状态（2026-09-07）**：**wecomws 已在真实机器人上端到端实测**——订阅、心跳、收发、
> 审批全链路验证通过。**wecom 与 wxkf 未实测**：wecom 的协议逐项对照官方文档无发现，但需要
> 公网 HTTPS 回调地址和真实 corp 才能验证；wxkf 按官方「XML 事件回调 + `kf/sync_msg` 拉取」
> 协议实现（游标持久化、48h 窗口出站），同样需要真实客服账号验证。
>
> 三个通道共有的经验教训（wecomws 实测踩出来的，另两个接入时值得先对照）：
> 平台的协议细节和直觉经常不一致——WS 握手头对大小写敏感、ack 帧没有 `cmd` 字段、`errcode` 在帧顶层、
> 心跳 `req_id` 必须带 `ping_` 前缀否则平台沉默、`aibot_respond_msg` 拒收 `text` 类型（必须
> `stream`，errcode 40008）。接新通道时建议先用探针抓原始帧核对协议，再对照官方 SDK 源码，
> 最后把测试替身改成和真实帧一致的形状。

四类通道的差异（连接方向、应答时限、媒体能力等）见 [`docs/README.md` §6](./README.md)。

→ 下一步：[§5 接一个新的 IM 通道](#5-接一个新的-im-通道)：照着现有适配器写一个自己的通道。

---

## 4. 写一个自己的工具

**目标**：新增一个业务工具，让模型在对话中真实调用它，并受租户白名单约束。

平台工具就是框架的 `tool.Tool` 加一个平台元数据 `Dangerous`。`trpcservice/tool/tool.go` 已经把
模式写好了（`DemoTools` 就是两个 stub），照抄即可：

```go
// trpcservice/tool/biztools.go
package tool

import (
	"context"

	ttool "trpc.group/trpc-go/trpc-agent-go/tool"
	"trpc.group/trpc-go/trpc-agent-go/tool/function"
)

type orderArgs struct {
	OrderID string `json:"order_id" jsonschema:"description=订单号"`
}

type orderResult struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
}

func queryOrder(_ context.Context, in orderArgs) (orderResult, error) {
	return orderResult{OrderID: in.OrderID, Status: "已发货"}, nil // TODO: 换成真实查询
}

// BizTools 是平台对外暴露的工具全集。新增工具在这里登记一次即可。
func BizTools() *Registry {
	return NewRegistry(
		Tool{Tool: function.NewFunctionTool(queryOrder,
			function.WithName("query_order"),
			function.WithDescription("按订单号查询物流状态"))},
		// Dangerous：命中后由治理链拦下，必须用户在对话中确认才真正执行。
		Tool{Tool: function.NewFunctionTool(refundOrder,
			function.WithName("refund_order"),
			function.WithDescription("对指定订单发起退款，不可撤销")), Dangerous: true},
	)
}
```

然后把它接到装配链上——`cmd/trpc-service/main.go:818` 那一行换成你的注册表：

```go
registry := tool.BizTools()   // 原来是 tool.DemoTools()
```

之后是**租户级收窄**，不需要改代码，改配置即可（`agent/assemble.go:360` 两级过滤：
`tenant.tool_policy` 先收窄，`agent_app.config.tools` 再收窄，非空 `allow` 即白名单）：

```bash
curl -s -X POST $A/admin/tenants/$T/apps -H "$H" -H 'Content-Type: application/json' \
  -d '{"name":"support","agent_type":"llm","config":{
         "prompt":"你是 ACME 的客服助手。",
         "tools":{"allow":["query_order","refund_order"]}}}'
```

**验证**：重新构建启动 → 发布应用 → 发一句「查一下订单 A-1001 到哪了」→ 用[快速开始](./quickstart.md)第 2 章的 SQL 看
`session_event`，会出现工具调用与结果事件；再说「给它退款」会先被拦下走[§1 危险工具二次确认](#1-危险工具二次确认)的流程。

> **注意**：工具名一旦被写进租户白名单就成了配置的一部分。改名或删除工具时，老租户的
> `allow` 里会留下一个不存在的名字——`Registry.Allowed` 对未知名字是**忽略**而不是报错，
> 所以表现为「这个工具突然消失了」，而不是报错。

→ 下一步：[§5 接一个新的 IM 通道](#5-接一个新的-im-通道)。

---

## 5. 接一个新的 IM 通道

**目标**：写一个自定义通道，让它出现在绑定列表里并能收发消息。

一个通道只需要做三件事（`trpcservice/channels/channels.go:210`）：

```go
type Channel interface {
	Name() string                                          // 通道标识，对应 channel_binding.channel
	RegisterRoutes(mux *http.ServeMux, h Handler)          // 挂载 IM 回调（入站）
	Send(ctx context.Context, msg OutboundMessage) error   // 调 IM 主动发送接口（出站）
}
```

最小骨架（`channels/mock/mock.go` 就是这个形状的 100 行版本，可以直接抄）：

```go
// trpcservice/channels/feishu/feishu.go
package feishu

const ChannelName = "feishu"
const CallbackPath = "/feishu/callback"

type Channel struct{ client *http.Client }

func (c *Channel) Name() string { return ChannelName }

func (c *Channel) RegisterRoutes(mux *http.ServeMux, h channels.Handler) {
	mux.HandleFunc(http.MethodPost+" "+CallbackPath, c.callback(h))
}

func (c *Channel) callback(h channels.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req callbackRequest
		_ = json.NewDecoder(r.Body).Decode(&req)

		msg := channels.InboundMessage{
			Channel:     c.Name(),
			MsgID:       req.MsgID,     // 平台按它做入口去重
			SessionKey:  channels.SessionKey(c.Name(), req.UserID, req.ChatID),
			UserID:      req.UserID,
			ChatID:      req.ChatID,
			Text:        req.Text,
			WebhookPath: r.URL.Path,    // Gateway 靠它路由到 tenant/app
			ReceivedAt:  time.Now(),
		}
		out, err := h.Handle(r.Context(), msg)
		if errors.Is(err, channels.ErrDuplicate) {
			// 重复投递是成功结果：必须回 200，否则 IM 会一直重推
			return
		}
		_ = err
		_ = out // out.Text 为空 = 回复走异步链路，由 Sender 调 Send 下发
	}
}

func (c *Channel) Send(ctx context.Context, msg channels.OutboundMessage) error {
	for _, seg := range channels.SplitText(msg.Text, 2048) { // 按字节切，不切坏 UTF-8
		// 调飞书 open api 发送 seg
	}
	return nil
}
```

挂到进程里（`cmd/trpc-service/main.go:220` 的 `channelSet`）：

```go
if cfg.FeishuEnabled {                    // 你自己的 env 开关
	ch := feishu.New()
	ch.RegisterRoutes(mux, enqueue)       // 入站
	channelSet[ch.Name()] = ch            // 出站：Sender 按 msg.Channel 找到它
}
```

最后建绑定（`channel` 必须等于 `Name()`）：

```bash
curl -s -X POST $A/admin/apps/$APP/bindings -H "$H" -H 'Content-Type: application/json' \
  -d '{"channel":"feishu"}'
# → webhook_path 自动填充为 /callback/feishu/{binding_id}
```

> **两个一定会踩的坑**：
>
> 1. **没实现 `BindingAware` 就只能用 legacy 路径**。`web/binding.go:50` 对不支持按绑定验签的通道
>    直接回 404（`channel does not support bindings`），`/callback/feishu/{id}` 永远不通。
>    要么实现 `CallbackHandler(h, creds)` 走自动填充路径，要么把 `CallbackPath` 登记进
>    `web/admin.go` 的 `legacyCallbackPaths`（那里只接受这两种形状，其他 `webhook_path` 一律 400）。
> 2. **出站要用 `SplitText` + 串行发送**。平台统一按 2048 字节切分，分段共享同一 trace；
>    出站错误记得过一遍 `channels.ScrubError`，它会把 URL 里的 `access_token` 打码——
>    字段级日志脱敏看不见字符串内部的东西。

→ 下一步：[§6 按角色拆进程](#6-像生产那样按角色拆进程)。

---

## 6. 像生产那样按角色拆进程

**目标**：三进程部署，并亲身体会「worker 无状态」这件事。

```bash
# 三个终端，各自一个角色（共用同一套 PG/Redis）
TRPC_ADMIN_TOKEN=dev-insecure TRPC_MOCK_CHANNEL=true ./bin/trpc-service serve gateway
TRPC_ADMIN_TOKEN=dev-insecure TRPC_MOCK_CHANNEL=true ./bin/trpc-service serve worker
TRPC_ADMIN_TOKEN=dev-insecure                        ./bin/trpc-service serve admin
```

用法：`trpc-service serve [all|gateway|worker|admin]`，不带参数等价于 `all`。

**验证**：起两个 worker 进程，消息会被消费组自动分摊；`kill` 掉其中一个，另一个通过
`XAUTOCLAIM` 接管 pending 消息，不丢。gateway 只做验签/去重/入队（全是 Redis 操作），可以随便加副本。

生产部署（Deployment / HPA / Ingress / db-init Job / Secret 挂载）见
[`deploy/k8s/README.md`](../deploy/k8s/README.md)。

---

## 7. Admin API 速查

建租户 / 应用 / 绑定的请求体字段：

| 接口 | 必填 | 可选 |
|---|---|---|
| `POST /admin/tenants` | `name` | `model_config` `tool_policy` `audit_policy` `guardrail_policy` `rate_policy` `storage_config` |
| `POST /admin/tenants/{id}/apps` | `name` `agent_type` `config` | — |
| `POST /admin/apps/{id}/publish` | `version` | — |
| `POST /admin/apps/{id}/bindings` | `channel` | `webhook_path` `token_ref` `aeskey_ref` `config` |
| `POST /admin/tenants/{id}/storage-migrations` | `resource` `to_backend` | — |
| `POST /admin/apps/{id}/knowledge/documents` | `name` `content` | — |

### 会被 400 拒掉的写法（都是有意的，不是 bug）

- `webhook_path` 填一个平台没挂载的路径 → 400。否则绑定建成功、列表里也正常，但 IM 每次回调都在
  mux 上 404，**是个静默黑洞**。留空让系统自动填充最安全。
- `channel=mock` 却填 `webhook_path=/mock/callback` → 400。legacy 路径是启动时用 env 全局凭据挂载的，
  绑定行自带凭据却挂在那条路径上会「看起来权威、实际验签从不读它」。
- `config` 里出现未知字段 → 400。`config` 会原样写进审计明细，一个未被通道识别的键（比如明文
  `secret`）会**既进审计又不生效**。
- `channel=wecomws` 但 `config` 缺 `bot_id` 或 `secret_ref`，或 `webhook_path` 不匹配
  `^/wecomws/[A-Za-z0-9_-]+$` → 400。
- 密钥字段只收**引用名**（如 `wecom-secret`），不要填明文。

### 其他常用调用

```bash
H='Authorization: Bearer dev-insecure'
A=127.0.0.1:8081

curl -s -H "$H" $A/admin/tenants                        # 列租户
curl -s -H "$H" $A/admin/tenants/$T                     # 租户详情
curl -s -H "$H" $A/admin/tenants/$T/apps                # 列应用
curl -s -X POST $A/admin/apps/$APP/rollback -H "$H" -H 'Content-Type: application/json' -d '{"version":1}'
curl -s -H "$H" "$A/admin/audit?tenant_id=$T&decision=deny"
curl -s -X DELETE $A/admin/apps/$APP/bindings/$B -H "$H"
```

不带 token → **401**；未挂载的回调路径 → **404**；同 `msg_id` 重发 → `{"status":"duplicate"}`。

---

## 附录 A：配置与密钥机制

**配置只来自环境变量**（60 个 `TRPC_*`，全量见 [configuration.md](./configuration.md)），仓库里没有配置文件。
全部由 `trpcservice/config/config.go` 的 `Load()` 集中读取，未设则取默认值。

**密钥永远不出现在配置和数据库里**，只存**引用名**：

```
channel_binding.token_ref = "wecom-token"      ← 这是引用，不是密钥
                                  ↓  运行时
                    SecretResolver 解析（TRPC_SECRET_RESOLVER）
                                  ↓
       file（默认）：读 $TRPC_SECRETS_DIR/wecom-token，即 data/secrets/wecom-token
       kms（生产）：向 TRPC_KMS_ENDPOINT 取值，用 TRPC_SECRETS_DIR 下的 bootstrap token 鉴权
                                  ↓
                    CachedResolver 缓存 1 分钟（TRPC_SECRET_CACHE_TTL）
```

所以本地开发只要把密钥写成 `data/secrets/<引用名>` 即可，权限建议 600。
`data/` 已被 `.gitignore` 忽略（只保留 `data/README.md`），**不要把它提交上去**。

### 常用变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `TRPC_HTTP_ADDR` | `:8080` | 回调口 |
| `TRPC_ADMIN_ADDR` | `127.0.0.1:8081` | Admin 口 |
| `TRPC_METRICS_ADDR` | `127.0.0.1:8082` | 指标口 |
| `TRPC_ADMIN_TOKEN` | `""`（**拒绝启动**） | Admin Bearer token；本地哨兵值 `dev-insecure` |
| `TRPC_PG_DSN` | `postgres://trpc:trpc-dev-only@localhost:5432/trpc?sslmode=disable` | 与 compose 一致 |
| `TRPC_REDIS_ADDR` | `localhost:6380` | compose 把容器 6379 映射到宿主 **6380** |
| `TRPC_SESSION_BACKEND` | `redis` | `redis` 或 `postgres` |
| `TRPC_SECRET_RESOLVER` | `file` | 生产设 `kms` |
| `TRPC_SECRETS_DIR` | `data/secrets` | file resolver 的目录 |
| `TRPC_MODEL_BASE_URL` | `https://api.deepseek.com` | 受 `TRPC_MODEL_BASE_URL_ALLOW` 白名单约束，必须 https |
| `TRPC_MODEL_NAME` | `deepseek-v4-flash` | |
| `TRPC_MODEL_APIKEY_REF` | `deepseek-apikey` | 引用名 |
| `TRPC_MODEL_TIMEOUT` | `60s` | 超时后取消并重试 1 次 |
| `TRPC_MODEL_PRICES` | `""` | 配了才会算 `audit_log.cost` |
| `TRPC_S3_ENDPOINT` / `TRPC_S3_BUCKET` | `localhost:9000` / `artifacts` | Artifact 存储 |
| `TRPC_MOCK_CHANNEL` | `false` | 本地演示才开 |
| `TRPC_LOG_LEVEL` / `TRPC_LOG_FORMAT` | `info` / `console` | 生产建议 `json` |
| `TRPC_GATEWAY_RATE_QPS` / `_BURST` | `50` / `100` | 平台默认入口限流，租户 `rate_policy` 可覆盖 |
| `TRPC_SEND_RATE_QPS` / `_BURST` | `20` / `40` | 发送侧限流 |
| `TRPC_EMBEDDER_MODEL` | `""`（禁用） | 设了才启用向量检索；默认聊天端点没有 embeddings API |
| `TRPC_WECOM_CORP_ID` / `_AGENT_ID` | `""` | 设了才挂载企微通道 |
| `TRPC_WXKF_CORP_ID` / `_KF_ACCOUNT` | `""` | 设了才挂载微信客服通道 |
| `TRPC_WECOMWS_ADDR` | `""` | 设了才启用 WS 通道 |

---

## 附录 B：故障排查

已搬到运维手册：按「消息没回复」的标准排查路径、按症状速查表、日志关键字见
[`docs/operations.md` §5 故障排查 runbook](./operations.md#5-故障排查-runbook)；
告警的逐条处置见同篇 [§4 告警手册](./operations.md#4-告警手册)。

---

## 附录 C：重置开发环境

数据乱了（测试残留、旧 seed、想重来）最干净的办法是**连卷一起删**：

```bash
./stop.sh
docker compose down -v      # 删除 pgdata / redisdata / miniodata 三个卷
docker compose up -d        # 空卷启动 → 自动重跑 init.sql + seed.sql
```

`init.sql` 会建 13 张表（12 业务表 + `schema_migrations`）并把自己标为迁移版本 1；
`seed.sql` 会灌 demo 租户、已发布应用和 4 条绑定（含多租户演示用的 `/callback/mock/{binding_id}`）。

`data/secrets/` 在宿主机上，**不受影响**，密钥不用重配。

只想清 Redis 队列、保留 PG 数据：

```bash
for s in stream:inbound stream:outbound stream:deadletter; do
  docker compose exec -T redis redis-cli XTRIM $s MAXLEN 0
done
docker compose exec -T redis redis-cli --scan --pattern 'dedup:*'  | xargs -r -n50 docker compose exec -T redis redis-cli DEL
docker compose exec -T redis redis-cli --scan --pattern 'done:*'   | xargs -r -n50 docker compose exec -T redis redis-cli DEL
docker compose exec -T redis redis-cli --scan --pattern 'sent:*'   | xargs -r -n50 docker compose exec -T redis redis-cli DEL
```

想保留现有 PG 数据、只补 seed 里缺的绑定：直接 `INSERT INTO channel_binding …`，
参考 `deploy/db/seed.sql` 的第 4 条。

---

## 附录 D：日常命令

```bash
make deps      # docker compose up -d
make build     # ./build.sh  → bin/trpc-service
make start     # ./start.sh（环境变量要在命令前缀里给）
make stop      # ./stop.sh
make test      # go test ./...（不带 -race）
make cover     # ./coverage.sh → 带 -race 和覆盖率
make fmt lint  # gofmt / go vet + golangci-lint
make migrate   # ./deploy/db/migrate.sh up（增量 schema 迁移）
./clean.sh     # 清 bin/、根目录游离二进制、coverage 产物
```

跑测试需要 PG / Redis / MinIO 在线（`make deps`）。**依赖不在线时集成测试会 skip，
覆盖率会从 87.5% 掉到约 55%**——那不是有效测量，CI 的 zero-skip 门禁也会因此失败。
建议给测试指定独立库，避免污染开发数据（见[运维手册 §5.2](./operations.md#52-按症状查)）。

---

## 下一步读什么

| 想了解 | 去哪 |
|---|---|
| 从头跑一遍：安装、跑通、查回复、建租户 | [`docs/quickstart.md`](./quickstart.md) |
| 架构、数据模型、多后端一致性、幂等与迁移、风险清单 | [`docs/README.md`](./README.md) |
| 完整技术方案：选型对比、容量推算、协议细节、取舍论证 | [`docs/design.md`](./design.md) |
| 数据库 schema 与演示数据 | `deploy/db/init.sql`、`deploy/db/seed.sql` |
| 增量 schema 迁移的约定 | `deploy/db/migrations/README.md` |
| 生产部署（Ingress / HPA / db-init Job / Secret 挂载） | `deploy/k8s/README.md` |
| 告警规则 | `deploy/prometheus/alerts.yml` |
