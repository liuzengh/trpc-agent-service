# 运维手册

面向部署、发布、告警处置和故障排查。开发期日常命令（`make *`、`./start.sh`）见
[guide 附录 D](./guide.md#附录-d日常命令)；通道各自的错误码与日志关键字见
[`channels/`](./channels/wecom.md) 下的三篇通道文档。

| 章节 | 什么时候来看 |
|---|---|
| [1. 部署](#1-部署) | 起依赖、上 K8s、按角色拆进程 |
| [2. 发布与回滚](#2-发布与回滚) | 改应用配置、发新版、回滚 |
| [3. 日常观测](#3-日常观测) | 看指标、队列、追踪 |
| [4. 告警手册](#4-告警手册) | 告警响了，按条处置 |
| [5. 故障排查 runbook](#5-故障排查-runbook) | 按症状定位 |

---

## 1. 部署

### 1.1 本地 / 演示：Docker Compose 起依赖

`docker-compose.yml` 只编排**依赖**（pgvector:pg16、redis:7 映射宿主 6380、MinIO、
Jaeger、Prometheus 五个容器），服务本体在本机跑，方便调试：

```bash
docker compose up -d        # 起依赖（空卷首启自动跑 init.sql + seed.sql）
./build.sh                  # 构建 bin/trpc-service
TRPC_ADMIN_TOKEN=dev-insecure TRPC_MOCK_CHANNEL=true ./start.sh
./stop.sh                   # 停止
docker compose down -v      # 连卷一起删 = 重置全部数据（secrets 在宿主机不受影响）
```

Prometheus 抓的是宿主机上的服务，所以服务要带 `TRPC_METRICS_ADDR=:8082`
（或改 `deploy/prometheus/prometheus.yml` 里的 target）才能被抓到。

### 1.2 Kubernetes 生产部署

清单在 `deploy/k8s/`（详细说明见该目录 README，此处是操作序）：

```bash
# ① 构建镜像（tag 必须与三个 Deployment 里的 image 一致，生产不要用 :latest）
docker build -t trpc-agent-service:v1 .

# ② 平台配置 + 基础设施引用（先改 config.yaml 里的占位符）
kubectl apply -f deploy/k8s/config.yaml
kubectl create secret generic trpc-admin-token --from-literal=token="$(openssl rand -hex 24)"

# ③ KMS bootstrap token：pods 唯一直接持有的明文（其余密钥都在 KMS、以 *_REF 引用）。
#    resolver=kms 时读不到该文件会拒绝启动——三个 Deployment 都把它挂到
#    TRPC_SECRETS_DIR（/etc/trpc/secrets），漏挂就是三角色 CrashLoop。
kubectl create secret generic trpc-kms-bootstrap --from-literal=token="<KMS 签发的 bootstrap token>"

# ④ 初始化数据库 schema（幂等 Job，表已存在则跳过）
kubectl create configmap trpc-db-init --from-file=init.sql=deploy/db/init.sql
kubectl apply -f deploy/k8s/db-init.yaml
kubectl wait --for=condition=complete job/trpc-db-init --timeout=120s

# ⑤ 三个角色
kubectl apply -f deploy/k8s/gateway.yaml
kubectl apply -f deploy/k8s/worker.yaml
kubectl apply -f deploy/k8s/admin.yaml

# ⑥ 唯一公网入口（先改 host / secretName / ingressClassName）
kubectl create secret tls trpc-tls --cert=<你的证书> --key=<你的私钥>
kubectl apply -f deploy/k8s/ingress.yaml
```

要点：

- **Ingress 是唯一公网入口**，TLS 在这里终止（企微/微信只接受可信证书的 HTTPS 回调）。
  gateway Service 是 ClusterIP；Ingress 只放行 `/callback`（多租户分发）和两个 legacy
  路径 `/wecom/callback`、`/wxkf/callback`。**`/mock/callback` 绝不能出现在 Ingress 上**——
  它是无鉴权消息注入器。不要给回调路径加重试注解：重试的 POST 是重复投递，去重层能吸收，
  但延迟预算不能。
- **密钥约定**：ConfigMap/Secret 里只有引用（`TRPC_MODEL_APIKEY_REF`、`TRPC_S3_*_REF` 等），
  明文全在 KMS；唯一例外是 KMS bootstrap token，由 Secret 挂进 `/etc/trpc/secrets`。
- **admin 仅内网**：ClusterIP，只承载逐路由 token 鉴权的 `/admin/*`；要更强管控设
  `TRPC_ADMIN_TLS_CERT` / `TRPC_ADMIN_TLS_KEY` / `TRPC_ADMIN_TLS_CLIENT_CA` 三件套启用 mTLS。
- **worker HPA**（2–10 副本）：清单里的 CPU 指标只是兜底——worker 负载大头是等 LLM 返回的
  IO，CPU 低不代表有余量。真正的扩容信号是 `stream_length` 积压，需要 prometheus-adapter
  把该指标接进 HPA（`worker.yaml` 注释里有示例）。
- **优雅停机**：worker 的 `terminationGracePeriodSeconds: 130`（模型超时 60s × 重试 + 余量），
  滚动更新时在途会话由 Stream pending + XAUTOCLAIM 接管，不丢消息。
- 启用 wecomws 时 **leader 循环内嵌在 gateway 副本里**，不用加副本；滚动更新时旧 Pod 关连接，
  新 leader 在 leader TTL（15s）内接管。

### 1.3 按角色拆进程

同一二进制按参数扮不同角色（`trpc-service serve [all|gateway|worker|admin]`，不带参数 = all）：

```bash
# 三个终端，共用同一套 PG/Redis（本地演示三角色分离）
TRPC_ADMIN_TOKEN=dev-insecure TRPC_MOCK_CHANNEL=true ./bin/trpc-service serve gateway
TRPC_ADMIN_TOKEN=dev-insecure TRPC_MOCK_CHANNEL=true ./bin/trpc-service serve worker
TRPC_ADMIN_TOKEN=dev-insecure                        ./bin/trpc-service serve admin
```

角色职责：gateway 只做验签/路由/限流/去重/入队（全是 Redis 操作，随便加副本）；
worker 无状态消费 `stream:inbound` 跑 Runner（起两个进程消息自动分摊，kill 一个另一个
经 XAUTOCLAIM 接管，不丢）；admin 承载 `/admin/*` 和归档任务。

---

## 2. 发布与回滚

### 2.1 应用配置发布与回滚（原子切换）

- `agent_app` 新版本先存 `draft`（`POST /admin/tenants/{id}/apps`），发布后
  `status=published`；**同租户同名应用最多一个 published**（部分唯一索引保证）。
- 发布是一条事务：`publish` 同时「下架旧版本 + 上架新版本 + 把绑定改指新版本」
  （`web/admin.go` 的 `publish`），是原子切换，没有半新半旧的中间态。
- 发布/回滚后经 Redis pub/sub（`tenant:invalidate`）广播配置失效，worker 秒级丢缓存重载
  （快照 TTL 自然过期兜底）；重载失败继续服务旧快照（stale beats down）。
- 已发布版本是**不可变快照**（`updateApp` 只改 draft），所以回滚是纯状态切换：

```bash
# 回滚到指定版本；空 body = 回滚到当前版本的上一版
curl -s -X POST $A/admin/apps/$APP/rollback -H "$H" -H 'Content-Type: application/json' -d '{"version":1}'
```

### 2.2 平台自身发布与回滚

- Gateway/Worker 均无状态：滚动更新（K8s `kubectl rollout`）即可；worker 摘流量后
  在途消息由 Stream pending 机制交接。
- 回滚 = `kubectl rollout undo deployment/trpc-gateway`（worker 同），或把镜像 tag 指回上一版
  重新 apply。
- **schema 只做增量变更**（加列/加表/加索引），`deploy/db/init.sql` 是冻结的 000001 基线，
  之后落在 `deploy/db/migrations/` 由 `make migrate`（golang-migrate）应用——因为不做
  破坏性变更，**回滚镜像不需要回滚 schema**。

---

## 3. 日常观测

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

**链路追踪**：compose 里的 Jaeger 已就绪，启动时加
`OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317`（gRPC exporter 要 host:port，不带 scheme），
UI 在 <http://localhost:16686>。不设也能正常启动，只是不导出 trace。

Prometheus 在 <http://localhost:9090>，抓取配置是 `deploy/prometheus/prometheus.yml`
（默认抓 `host.docker.internal:8082`；改了 metrics 端口这里跟着改）。

---

## 4. 告警手册

规则文件：`deploy/prometheus/alerts.yml`（由 `prometheus.yml` 的 `rule_files` 加载，
接入 Alertmanager 后路由到你的接收渠道）。逐条的含义与处置：

### StreamBacklogHigh（critical）

`stream:inbound` 或 `stream:outbound` 积压 >5 万条持续 5 分钟。
**含义**：消费端远远跟不上，且逼近 MAXLEN（10 万）截断边界——过了边界「不丢」承诺失效。
**处置**：

1. `XLEN stream:inbound` / `stream:outbound` 确认是哪条队列；
2. inbound 积压 → 看 worker 是否在线、是否在刷 `process ... failed`（下游 LLM/PG 故障），
   横向扩 worker；
3. outbound 积压 → 看 sender 日志的 `send rejected`（IM 凭据/限频），查 `send_rate_limited_total`；
4. 若已截断：按 `dedup:` key 与 `session_event` 对账定位丢失范围（有 dedup 记录而无对应
   事件的即为被截断消息）。

### StreamPendingStuck（warning）

最老 pending 消息停留 >5 分钟。**含义**：有消费者崩溃或卡死，reaper 本该早就接管。
**处置**：确认 worker/sender 进程在线；用 `XPENDING stream:inbound workers` 看 pending 挂在
哪个 consumer 名下；若消息反复接管反复失败（毒消息），取出来人工分析后进死信。

### SendersWsBacklog（warning）

`stream_pending{group="senders-ws"}` >50 持续 5 分钟。
**含义**：wecomws 无 leader（没有副本持有租约）或 WS 发送卡死——连接和该消费组都由
leader 持有。**处置**：查 gateway 日志 `wecomws leadership acquired/lost`，确认有副本持有
`lock:leader:wecomws`；查 `wecomws bot ... disconnected` 重连循环；必要时滚动重启
gateway 副本强制重新竞选。

### DeadLetterPresent（warning）

`stream:deadletter` 非空。**含义**：出站消息连续失败超过 5 次（`Sender.MaxAttempts`）
被移入死信，需要人工介入，不会自愈。**处置**：

```bash
docker compose exec -T redis redis-cli XRANGE stream:deadletter - + | head
```

按消息里的 channel/binding 找失败原因（多为 IM 凭据失效或 45009 限频，见通道文档错误码表），
修复后人工重放或联系用户补发。

### WorkerProcessSlow（warning）

worker 处理 P95 >15s 持续 5 分钟。**含义**：逼近端到端 15s 预算。
**处置**：拆延迟——`llm` 慢（模型侧超时/负载，`TRPC_MODEL_TIMEOUT` 默认 60s 兜底）、
工具慢（外部 API）、还是会话锁竞争（同会话消息排队）。

### EndToEndSlow（critical）

端到端 P95（回调 → 回复落 IM）>15s，突破设计目标。**处置**：按链路分段定位——
gateway 入队耗时（`gateway_rejected_total` 是否上涨）、队列停留（`stream_oldest_pending_seconds`）、
worker 处理（`worker_process_duration`）、出站发送（IM API 是否限频/超时）。

### ProcessErrorRateHigh（warning）

处理错误率 >1%。**处置**：`grep -a 'process .* failed' data/trpc-service.log` 看错误面；
常见原因是模型 key 失效（`ModelError`）、PG/Redis 抖动、配置快照加载失败。

### IMDeliveryDegraded（warning）

IM 投递成功率 <99%（分母只计真正发送的结果，跳过其他消费组的消息不算）。
**处置**：`grep -a 'send rejected' data/trpc-service.log` 看 errcode——40014/42001
是 token 问题（核对 secret 文件），45009 是限频，95020 是 wxkf 48h 窗口；
再查 `send_rate_limited_total` 确认是否被本地令牌桶压着。

### SendRateLimited（info）

发送限速把消息留在 pending 等待 reaper 接管。**含义**：IM 平台的频率限制在被触发，
本地令牌桶在兜底排队（不丢）。**处置**：业务量确实超过配额就向 IM 平台申请提额；
临时收紧用租户 `rate_policy.send_qps`/`send_burst`，平台默认改 `TRPC_SEND_RATE_QPS`/`_BURST`。

### GatewayRejecting（warning）

入口准入在拒收回调，`reason` 标签区分 `rate_limited`（单租户令牌桶）与 `backpressure`
（队列接近上限）。**处置**：`rate_limited` → 找出灌量租户，调它的 `rate_policy` 或
平台默认 `TRPC_GATEWAY_RATE_QPS`/`_BURST`（默认 50/100）；`backpressure` → 按
StreamBacklogHigh 的处置走。被拒的消息由 IM 重推兜底，不是丢失。

### ModelCallSlow（warning）

模型调用 P95 >20s 持续 5 分钟，按 tenant_id/model 分组。**含义**：模型端在吃端到端
15s 预算，worker 超时成了唯一兜底。**处置**：先看是该模型普遍慢还是单租户异常——
模型端慢（上游负载/超时）就切换备用模型后端或降模型档位；单租户慢检查其提示词长度
与工具调用轮次；确属正常长任务再调 `TRPC_MODEL_TIMEOUT`（默认 60s），但不要为了
压告警而下调阈值。

### SessionStoreErrorRateHigh（warning）

Session 后端读写错误率 >1%，按 backend 分组（PG/Redis 各自计算，低流量后端不被
全局稀释）。**含义**：会话历史读写在失败，表现为丢上下文或 run 失败。**处置**：按
`backend` 标签定位是 PG 还是 Redis——PG 查连接池是否耗尽、实例是否故障切换；
Redis 查连接与内存（OOM 会拒绝写）；错误会以 infra 错误的形式出现在
`process .* failed` 日志里（`grep -a 'process .* failed' data/trpc-service.log`），
对端恢复后告警自愈，已失败的请求由调用侧重试兜底。

---

## 5. 故障排查 runbook

### 5.1 「消息没回复」的标准排查路径

回调返回 `accepted` 但用户一直没收到回复，按顺序查：

```bash
# ① 队列积压：消息是卡在队头还是已经被消费
docker compose exec -T redis redis-cli XLEN stream:inbound
docker compose exec -T redis redis-cli XLEN stream:outbound
docker compose exec -T redis redis-cli XLEN stream:deadletter

# ② worker 处理失败
grep -a 'process .* failed' data/trpc-service.log | tail

# ③ 出站发送被拒 / token 失效
grep -a 'send rejected' data/trpc-service.log | tail
```

对应结论：

- `stream:inbound` 积压上涨 → worker 没起、打满或下游故障（看 §4 StreamBacklogHigh）；
  all-in-one 模式下先确认日志里有 worker 相关行。
- 队列不长但没有 `reply sent` → 消息进了死信（`stream:deadletter` 非空，人工介入）或
  被治理链拦下（查审计：`GET /admin/audit?decision=deny`）。
- `send rejected` 带 errcode → 按通道文档的错误码表处理（token 失效会自动重取重试一次，
  持续出现是凭据本身错了）。

### 5.2 按症状查

| 症状 | 原因 | 解法 |
|---|---|---|
| 启动日志 `metrics listener failed … address already in use` | 8082 被别的程序占用 | 加 `TRPC_METRICS_ADDR=127.0.0.1:8083`。服务不会崩，只是没指标 |
| 一启动就刷 `worker … process … failed: unknown agent app: a1` / `tenant route inactive: tenant t1` | **Redis 里有集成测试残留消息**。Worker 串行消费，你的消息排在它们后面 | 见 [guide 附录 C](./guide.md#附录-c重置开发环境) 重置，或 `XTRIM stream:inbound MAXLEN 0` |
| `/callback/mock/{binding_id}` 返回 `unknown binding` | 库是用**旧版 `seed.sql`** 灌的（initdb.d 只在空卷首次启动时跑），后来新增的绑定行从没进过库 | 见 guide 附录 C 重置，或手工 INSERT 那条 binding |
| `make test` 之后开发库多出一堆 `pgstore-…` 之类的租户、Redis 里多出队列消息 | `testenv.go` 的默认值就指向开发依赖：`TRPC_TEST_PG_DSN` 默认 = `TRPC_PG_DSN`（同一个 `trpc` 库），`TRPC_TEST_REDIS_ADDR` 默认 = `localhost:6380`（同一个 Redis）。CI 用全新 service container，所以只有本地会这样 | 给测试单独建库再跑：<br>`docker compose exec -T postgres psql -U trpc -d postgres -c 'CREATE DATABASE trpc_test'`<br>`docker compose exec -T postgres psql -U trpc -d trpc_test -v ON_ERROR_STOP=1 < deploy/db/init.sql`<br>`TRPC_TEST_PG_DSN='postgres://trpc:trpc-dev-only@localhost:5432/trpc_test?sslmode=disable' make test`<br>Redis 侧**没有等价开关**（配置只有 host:port，不支持 db index），队列残留只能按 guide 附录 C 清理或另起一个 Redis 实例 |
| 进程启动即退出，日志说 admin token 相关 | `TRPC_ADMIN_TOKEN` 未设（fail-closed） | 本地用 `dev-insecure`，生产用真 token |
| `resolve … no such file` 类错误 | `data/secrets/` 下缺对应引用名的文件 | 按 [guide 附录 A](./guide.md#附录-a配置与密钥机制) 补齐，文件名必须与 `*_REF` 一致 |
| 回复变成「服务繁忙请稍后再试」 | 模型超时（默认 60s）或报错，重试 1 次后降级 | 查日志里的 `ModelError`；确认 `data/secrets/deepseek-apikey` 有效、`TRPC_MODEL_NAME` 正确 |
| 消息进了 `stream:deadletter` | 出站发送连续失败超过 5 次 | 见 §4 DeadLetterPresent |
| PG 里查不到会话 | `TRPC_SESSION_BACKEND=redis`（默认） | 切 `postgres`，或按[快速开始](./quickstart.md)第 2 章末尾去 Redis 查 |
| 企微回调一直 404 | 通道没挂载（`TRPC_WECOM_CORP_ID` 未设）或 `webhook_path` 与后台填的 URL 不一致 | 查启动日志的通道挂载行；核对 `channel_binding.webhook_path` |
| wecomws 消息全无，gateway 某副本「没动静」 | 该副本不是 leader（每 bot 单连接，全局单持有者） | 正常现象；查 `wecomws leadership acquired` 确认 leader 存在；全无 leader 时按 §4 SendersWsBacklog 处置 |

### 5.3 看日志的几个常用姿势

```bash
grep -a 'reply sent'          data/trpc-service.log | tail    # 回复下发成功
grep -a 'process .* failed'   data/trpc-service.log | tail    # worker 处理失败
grep -a 'duplicate message'   data/trpc-service.log | tail    # 去重命中
grep -ac ERROR                data/trpc-service.log           # 错误总数
```

通道各自的日志关键字清单见 [channels/wecom.md](./channels/wecom.md) §6、
[channels/wxkf.md](./channels/wxkf.md) §6、[channels/wecomws.md](./channels/wecomws.md) §6。
