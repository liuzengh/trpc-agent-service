# 快速开始

10 分钟内在本地跑起这个多租户 IM Agent 平台：发出第一条消息，看到 Agent 的回复，
再建一个属于你自己的租户。

一句话概括消息的一生：**同步应答、异步消费**。IM 回调立刻返回，真正的回复几十秒后由 Worker
算完再下发——所以发完消息会先拿到一个 `reply` 为空的 `accepted`，**这是正常的**。

| 想做 | 跳到 |
|---|---|
| 把平台跑起来，发出第一条消息 | [1. 跑通第一条消息](#1-跑通第一条消息) |
| 把回复的正文挖出来 | [2. 看到 Agent 的回复](#2-看到-agent-的回复) |
| 建自己的租户、应用和回调路径 | [3. 建你自己的租户、应用和绑定](#3-建你自己的租户应用和绑定) |
| 跑通之后还要做什么 | [下一步](#下一步) |

## 前置条件

| 需要 | 版本 / 说明 | 检查命令 |
|---|---|---|
| Go | 1.27（见 `go.mod` 的 `go` 指令） | `go version` |
| Docker + Compose 插件 | 起 5 个依赖容器 | `docker compose version` |
| 一个 OpenAI 兼容的模型 API key | 默认对接 DeepSeek | — |
| 空闲端口 | 8080 / 8081 / 8082 / 5432 / 6380 / 9000 / 9001 | `ss -ltnp \| grep -E ':(8080\|8081\|8082\|8083)\b'` |

---

## 1. 跑通第一条消息

```bash
git clone https://github.com/liuzengh/trpc-agent-service.git
cd trpc-agent-service

# ① 起依赖：pgvector(PG16) / redis(宿主 6380) / minio / jaeger / prometheus
docker compose up -d

# ② 放模型密钥（文件名必须叫这个，见 [指南附录 A](./guide.md#附录-a配置与密钥机制)）
mkdir -p data/secrets
echo -n 'sk-你的模型APIKey' > data/secrets/deepseek-apikey

# ③ 构建 + 启动（all-in-one：单进程兼任 gateway + worker + admin）
./build.sh
TRPC_ADMIN_TOKEN=dev-insecure TRPC_MOCK_CHANNEL=true \
TRPC_METRICS_ADDR=127.0.0.1:8083 TRPC_SESSION_BACKEND=postgres ./start.sh

# ④ 发一条消息
curl -X POST 127.0.0.1:8080/mock/callback -H 'Content-Type: application/json' \
  -d '{"msg_id":"demo-001","user_id":"u-demo","text":"用一句话说明什么是幂等"}'
# → {"reply":"","status":"accepted"}
```

③ 里四个环境变量**都是必需的**——安全默认全部关闭，不设不是走默认值，而是功能被关掉或进程拒绝启动：

| 变量 | 不设会怎样 |
|---|---|
| `TRPC_ADMIN_TOKEN=dev-insecure` | **进程拒绝启动**（fail-closed）。`dev-insecure` 是哨兵值，只在绑 loopback 时被接受 |
| `TRPC_MOCK_CHANNEL=true` | mock 通道不挂载。它默认关，因为它是**无鉴权的消息注入器**，生产绝不能开 |
| `TRPC_METRICS_ADDR=127.0.0.1:8083` | 默认 8082；被占用时服务不崩，只是 metrics 关闭并打一条 WARN |
| `TRPC_SESSION_BACKEND=postgres` | 默认 `redis`，那样 PG 的会话表是空的，你**没法用 SQL 查对话**（见第 2 章） |

**验证**：`curl` 返回 `accepted`，且 `data/trpc-service.log` 里出现 `gateway listening {"addr":":8080"}`。

> **你可能会撞上**：8082 常被桌面应用占用，所以上面直接用了 8083。想看是谁占的：
> `ss -ltnp | grep -E ':(8080|8081|8082|8083)\b'`。

停止服务：`./stop.sh`。

---

## 2. 看到 Agent 的回复

mock 通道的 `Send` **只把回复记在内存里，日志只打长度不打正文**（`channels/mock/mock.go:128`）。
所以用 SQL 查最省事——前提是启动时带了 `TRPC_SESSION_BACKEND=postgres`：

```bash
docker compose exec -T postgres psql -U trpc -d trpc -c "
SELECT e.event_seq,
       e.event->>'author'                                     AS 角色,
       left(e.event->'choices'->0->'message'->>'content', 80) AS 内容,
       e.event->'usage'->>'total_tokens'                      AS tokens
FROM session_event e JOIN session s ON s.id = e.session_id
WHERE s.session_key = 'dm:mock:u-demo'
ORDER BY e.event_seq"
```

```
 event_seq |   角色    |                          内容                           | tokens
-----------+-----------+-------------------------------------------------------+--------
         1 | user      | 用一句话说明什么是幂等                                  |
         2 | assistant | 幂等是指一个操作无论执行一次还是执行多次，产生的结果都相同… | 3016
```

正文路径是 `event->'choices'->0->'message'->>'content'`（**不是** `content[0].text`）。
`session_key` 的规则是单聊 `dm:{channel}:{user_id}`、群聊 `group:{channel}:{chat_id}`，
所以 `user_id=u-demo` 走 mock 单聊就是 `dm:mock:u-demo`。

另外两种看法（看不到正文，适合确认链路走通）：

```bash
# 审计：看得到延迟 / token / 成本 / trace
curl -s -H "Authorization: Bearer dev-insecure" "127.0.0.1:8081/admin/audit?limit=5"

# 日志：只确认回复已下发
grep 'reply sent' data/trpc-service.log | tail
```

> **你可能会撞上**：PG 里查不到会话。原因是 `TRPC_SESSION_BACKEND` 默认 `redis`，会话存在 Redis 里，
> PG 的会话表保持为空——**这不是 bug，是后端选择**。想在 PG 里查就切 `postgres`。

---

## 3. 建你自己的租户、应用和绑定

`seed.sql` 已经灌了一个演示租户，所以第 1 章能直接发消息。下面从零建一套，**不需要重启服务**：

```bash
H='Authorization: Bearer dev-insecure'
A=127.0.0.1:8081

# ① 建租户（策略字段都可省略，省略即走平台默认）
T=$(curl -s -X POST $A/admin/tenants -H "$H" -H 'Content-Type: application/json' \
     -d '{"name":"acme-demo"}' | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])")

# ② 建应用（新建即 draft，未发布不接客）
APP=$(curl -s -X POST $A/admin/tenants/$T/apps -H "$H" -H 'Content-Type: application/json' \
     -d '{"name":"support","agent_type":"llm","config":{
            "prompt":"你是 ACME 的客服助手，回答简洁。",
            "tools":{"allow":["get_weather","delete_user_data"]}}}' \
     | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])")

# ③ 发布（原子切换：同租户同名应用最多一个 published）
curl -s -X POST $A/admin/apps/$APP/publish -H "$H" -H 'Content-Type: application/json' \
     -d '{"version":1}'

# ④ 建渠道绑定（webhook_path 留空 → 自动填充为 /callback/{channel}/{binding_id}）
B=$(curl -s -X POST $A/admin/apps/$APP/bindings -H "$H" -H 'Content-Type: application/json' \
     -d '{"channel":"mock"}' | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])")

# ⑤ 立刻用新路径发消息
curl -s -X POST 127.0.0.1:8080/callback/mock/$B -H 'Content-Type: application/json' \
     -d '{"msg_id":"tut-001","user_id":"u-acme","text":"你们支持哪些渠道？一句话"}'
# → {"reply":"","status":"accepted"}
```

**验证**：新绑定**立即可达**（配置快照 TTL 30s + Redis pub/sub 失效广播），会话落在新租户的 app
命名空间下，审计记录的 `tenant_id` 正是新建的那个。

> ④ 的 `webhook_path` **留空最安全**——自己填一个平台没挂载的路径会被 400 拒掉，因为那种绑定
> 建得成、列表里也正常，但 IM 每次回调都在 mux 上 404，是个静默黑洞。
> 其余会被拒的写法和完整字段见 [指南 §7](./guide.md#7-admin-api-速查)。

---

## 下一步

| 想了解 | 去哪 |
|---|---|
| 危险工具二次确认、观测、接真实企业微信 | [`docs/guide.md`](./guide.md) §1–§3 |
| 写一个自己的工具、接一个新的 IM 通道 | [`docs/guide.md`](./guide.md) §4–§5 |
| 按角色拆进程、配置与密钥机制 | [`docs/guide.md`](./guide.md) §6 + 附录 A |
| 消息没回复 / 环境想重来 | [`docs/guide.md`](./guide.md) 附录 B / C |
| 架构、数据模型、多后端一致性、风险清单 | [`docs/README.md`](./README.md) |
| 完整技术方案：选型对比、容量推算、协议细节 | [`docs/design.md`](./design.md) |
| 生产部署（Ingress / HPA / db-init Job / Secret） | `deploy/k8s/README.md` |
