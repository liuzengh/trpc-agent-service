# 部署

这个目录装着把平台跑起来所需的全部产物，以及一份**诚实的验证清单**（见文末）——
哪些性质是真跑出来的，哪些只是离线推导的，哪些根本没验过。

所有产物都按「零拉取可跑」设计：本机 Docker Hub 不可达（spec 事实 #1），所以运行时
镜像是 `scratch`，builder 用本地已有的 `golang:1.24`，假模型和平台打进同一个镜像，
不需要 python。

## 目录

| 路径 | 作用 |
| --- | --- |
| `../Dockerfile` | 两阶段构建：`golang:1.24` builder → `scratch` 运行时，两个静态二进制，UID 65534 |
| `../.dockerignore` | 把 `config.yaml` 与 `.git` 挡在构建上下文外（两者都可能含真密钥） |
| `../docker-compose.yml` | 本地全栈：app + redis + fake-model；collector 挂在 `observability` profile 上 |
| `compose/config/config.yaml` | compose 默认挂载的配置：两个租户，都指向镜像内的假模型 |
| `compose/config-observability/config.yaml` | 同上，只多一个 `telemetry` 段（exporter 没有 env 钩子，只能换文件） |
| `otel-collector.yaml` | OTLP/HTTP 4318 → debug exporter，把 span 打到 collector 自己的日志 |
| `k8s/kustomization.yaml` | 清单入口；ConfigMap 由 `configMapGenerator` 生成，**必须用 `-k` 而不是 `-f`** |
| `k8s/config/config.yaml` | Kubernetes 形状的配置：单租户、无 `audit.file`（走 stdout） |
| `k8s/10-secret.example.yaml` | Secret 的两种形状：A = 只放 `MODEL_API_KEY`；B = 整份配置进 Secret |
| `k8s/20-deployment.yaml` | 探针 / 资源 / 安全上下文 / initContainer 把配置拷进可写 emptyDir |
| `k8s/30-service.yaml` | ClusterIP |
| `k8s/40-hpa.yaml` | `maxReplicas: 1`，**故意是惰性的**（见「已知限制」） |
| `k8s/50-pdb.yaml` | `minAvailable: 1`，会挡住 `kubectl drain`（见「已知限制」） |
| `k8s/60-ingress.yaml` | nginx ingress，含 SSE 所需的 buffering/timeout 注解 |
| `../scripts/check_deploy.sh` | 离线结构门禁，58 条断言 |

## 构建

一次构建打两个 tag：compose 用 `:local`，K8s 清单默认写 `:0.1.0`（与
`trpcservice.Version` 一致）。

```bash
docker build -t trpc-agent-service:local -t trpc-agent-service:0.1.0 .
```

镜像约 11.6 MB。构建期唯一联网的动作是 `go mod download`，走
`ARG GOPROXY=https://goproxy.cn,direct`（默认 proxy.golang.org 在这里不可达，事实 #2）。
如果日后提交了 `vendor/`，删掉 Dockerfile 里 `COPY go.mod go.sum ./` + `RUN go mod download`
两行即可全离线构建 —— Go 会自动切到 `-mod=vendor`。

## Compose

```bash
docker compose up -d --build
curl -s localhost:8080/readyz          # {"status":"ready"}
curl -s localhost:8080/healthz         # {"status":"ok"}
docker compose ps                      # 三个容器都应是 healthy / running
```

两个探针语义不同，故意如此：`/healthz` 不碰任何依赖（能响应 HTTP 即存活），
`/readyz` 要同时满足**租户数 > 0** 且 `storage.Ping` 通过，否则 503 并**一次列出全部**
未满足的原因。依赖故障该摘流量而不该重启进程 —— 重启从来没修好过一次后端宕机。

默认栈里**包含假模型**，这是一个刻意的偏离：仓库不带真密钥（`config.yaml` 正因为
存密钥才被 gitignore），指向真实供应商的栈开箱答不出一条消息，联调和演练就没有
断言对象。假模型是唯一既可达又可控的上游。

发一条消息（假模型回固定脚本，含一处跨 chunk 切分的敏感词，供输出 tripwire 演练）：

```bash
# 先挂上 SSE
curl -sN 'localhost:8080/webchat/stream?tenant=demo&user=alice' &
sleep 0.5
curl -s -X POST localhost:8080/callback/webchat/demo \
     -d '{"user":"alice","msg_id":"m1","text":"hello"}'
```

审计轨迹落在命名卷 `trpc-agent-service_audit` 里，`down` 之后仍在。看它用
`docker cp`（运行时是 `scratch`，里面没有 shell，`docker compose exec` 什么也跑不了；
而 `docker volume inspect` 给的 Mountpoint 在 macOS 上是 VM 内部路径，宿主机上读不到）：

```bash
docker cp tas-app:/data/audit.jsonl /tmp/audit.jsonl && tail -3 /tmp/audit.jsonl
```

改运行时包络不用重启（热更新走的是与租户变更同一条 commit 路径）：

```bash
$ curl -s localhost:8080/admin/settings
{"message_timeout":"2m0s","max_concurrency_per_tenant":0,"max_llm_calls":8}

$ curl -s -X PUT localhost:8080/admin/settings \
       -H 'Content-Type: application/json' \
       --data-binary '{"message_timeout":"3s","max_concurrency_per_tenant":1}'
{"message_timeout":"3s","max_concurrency_per_tenant":1,"max_llm_calls":8}
```

GET 渲染的是**生效值**，所以响应可以原样 PUT 回来。PUT 是**整体替换**，但零值语义与
配置文件一致：上面省略了 `max_llm_calls`，回来的是默认值 8 而不是 0 ——
`max_concurrency_per_tenant: 0` 意思是「不限」，`max_llm_calls: 0` 意思是「取默认」，
因为「不限」正是后者要堵的洞（框架把非正值读作无上限，spec 事实 #16）。
非法值（解析不了的 duration、负数）一律 400，且活配置与磁盘文件都不动。

> **挂载形态是实测出来的，不是猜的**（事实 #19）：
>
> | 形态 | `PUT /admin/settings` | 原因 |
> | --- | --- | --- |
> | `./config.yaml:/config/config.yaml` | **400** | `config.Save` 要写 `<path>.tmp`，而 Docker 建的 `/config` 属 root，UID 65534 建不出来 |
> | `./config:/config` | **200**，已持久化，无 tmp 残留 | ✓ |
> | `./config:/config:ro` | **400** | read-only file system |
>
> 副作用要知道：`Save` 会重新 marshal 整个文件，**注释全部丢失**（实测：上面那次 PUT
> 之后，挂载的配置从 54 行缩到 31 行，多出一个 `agent:` 段）。所以做实验时用
> 一次性副本，别直接改仓库里那份：
>
> ```bash
> mkdir -p .smoke/cfg && cp deploy/compose/config/config.yaml .smoke/cfg/
> CONFIG_DIR=./.smoke/cfg docker compose up -d
> ```
>
> Linux 宿主机上还得让这个目录对 UID 65534 可写（macOS Docker Desktop 会自动映射，
> Linux 不会）。这一点**没有在 Linux 上验证过**，本机是 macOS。

## 指向真实模型

假模型只是默认值。换成真供应商有两种做法，都不需要改镜像：

```bash
# 1. 环境变量：只覆盖 default_tenant（config.Load 调的是
#    applyEnvOverride(cfg.Tenants[cfg.DefaultTenant])，不是逐租户查表）
MODEL_API_KEY=sk-... MODEL_NAME=deepseek-chat \
MODEL_BASE_URL=https://api.deepseek.com \
  docker compose up -d

# 2. 挂自己的配置目录：多租户各有各的 key 只能走这条
cp -r deploy/compose/config /tmp/mine && $EDITOR /tmp/mine/config.yaml
CONFIG_DIR=/tmp/mine docker compose up -d
```

## 可观测变体

collector 镜像要从 Docker Hub 拉，本机拉不到（事实 #1），所以它挂在 profile 上，
默认栈不含它 —— 否则 `up` 会在拉取阶段整体失败。

```bash
CONFIG_DIR=./deploy/compose/config-observability \
  docker compose --profile observability up -d
docker compose logs -f otel-collector        # span 以 debug exporter 打在这里
```

只想看 span、不想拉镜像的话，别用这个目录：在 `compose/config/config.yaml` 里把
`telemetry.traces.exporter` 设成 `stdout`，然后 `docker compose logs app`。这条路径
不需要任何镜像，在本机可用。

## Kubernetes

**两个前置条件，清单里都不包含：**

1. **一个 Redis。** `k8s/config/config.yaml` 里写的是 `redis://redis:6379`，改成你
   集群里真实的地址。没有它进程会在启动探针处 fail-fast 退出（这是设计，不是半死
   状态），表现为 CrashLoopBackOff，日志里是 `init session storage: ...`。
2. **一个集群能拉到的镜像。** 本机构建的镜像不在任何 registry 里。推上去之后改
   `kustomization.yaml` 末尾的 `images:` 块。

```bash
# Secret 里的占位值必须先换掉（10-secret.example.yaml 的形状 A 可以原样 apply，
# 但它带的是 REPLACE_WITH_YOUR_KEY，只在第一次模型调用时 401）
kubectl create secret generic trpc-agent-model \
    --from-literal=MODEL_API_KEY=sk-... \
    --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -k deploy/k8s/          # 必须 -k：ConfigMap 是生成的
kubectl rollout status deploy/trpc-agent-service
```

用 `-k` 而不是 `-f`：`-f deploy/k8s/` 会创建一个卷指向不存在名字的 Deployment，因为
ConfigMap 由 `configMapGenerator` 生成、名字带内容哈希。同一个哈希也让改配置自动
触发 rollout，不需要记得去 bump 一个 checksum 注解。

`kubectl get configmap trpc-agent-config` 查不到东西（名字带哈希），用标签查：
`kubectl get configmap -l app=trpc-agent-service`。

`60-ingress.yaml` 里的 `host: agent.example.com` 和 TLS 段都要替换。两个 IM 平台实际
上只接受 HTTPS 回调，所以 TLS 对真实部署不是可选项 —— 它在这里可省，只是因为这份
清单本来也没法对着真集群 apply。

## 验证：什么验过，什么没验过

| 性质 | 手段 | 状态 |
| --- | --- | --- |
| 镜像构建、11.6 MB、UID 65534、`/data` 可写、TLS 根证书在位 | `docker build` + `docker run` | ✅ 真跑 |
| `-healthcheck` 自探 exit 0 / exit 1（含 503 原因体） | `docker run` | ✅ 真跑 |
| 优雅停机：SIGTERM 后 5s 退出，不等在飞 dispatch | `docker stop` 计时 | ✅ 真跑 |
| 三种挂载形态下 `PUT /admin/settings` 的成败 | 决定性实验 | ✅ 真跑（事实 #19） |
| compose 起栈 → readyz 200 → SSE 四个 chunk + done → 审计落卷（属主 `nobody nogroup`） | 真起栈发消息 | ✅ 真跑 |
| compose / k8s 的 58 条结构属性（交叉引用、探针、副本数一致性、密钥不入上下文…） | `scripts/check_deploy.sh` | ✅ 离线门禁，15/15 变异被该拦的断言拦住 |
| 三份部署配置能过 `config.Load`（含 `Validate` 与 env 覆盖） | `trpcservice/config/deploy_test.go`，7 个测试 | ✅ 真跑 |
| `audit.file` 省略 = log-only 而不是静默丢弃 | `audit.TestEmptyPathIsLogOnlyNotSilent` | ✅ 真跑（修复见事实 #20） |
| **K8s 清单在集群里的实际行为** | — | ❌ **没验过**：本机没有 kind/minikube/k3d/helm（事实 #3）。只有 `kubectl kustomize` 离线解码 + 逐字段对照官方 schema；下面「探针计时」三条是**读 kubelet v1.30.2 源码**得出的，不是在集群里量出来的 |
| **otel-collector 真收到 span** | — | ❌ **没验过**：镜像拉不到（事实 #1） |
| **故障演练矩阵 D1–D7** | `scripts/fault_drill.sh` | ⏳ 尚未执行（spec §2.6） |

`kubectl apply --dry-run=client --validate=false` **不能**当离线校验用：它仍然要做
API discovery 把 kind 映射成 resource，没有集群时对正确的文件也一律报
`the server could not find the requested resource`。`kubectl kustomize` 是纯本地的，
这才是可用的那个工具（事实 #3 在此更正）。

跑门禁：

```bash
scripts/check_deploy.sh              # 缺工具的段落明确 SKIP，不静默通过
scripts/check_deploy.sh --strict     # SKIP 也算失败，CI 用这个
```

它需要 `docker` CLI（**不需要 daemon**，`docker compose config` 是纯客户端的）、
`kubectl`、`ruby`、`jq`、`go`。本机 python3 没有 pyyaml、也没装 yq，所以 YAML→JSON
走 ruby。ruby 有个坑值得记着：Psych 把不带引号、以 `:` 开头的标量读成 Symbol，
转 JSON 时冒号会被吃掉 —— `kubectl kustomize` 恰好会把源码里 `":8080"` 的引号去掉，
于是同一份渲染结果在 Go 和 Ruby 眼里是两个值（事实 #21）。门禁里有一条专门扫这个
形状，Deployment 的 `-addr` 也因此写成 `0.0.0.0:8080`。

## 已知限制（运维必须知道的）

### 超时梯队

四层超时层层包住，改其中一层前先读这里。每一层都**必须严格大于**它里面那层，
否则外层先到期，内层辛苦算出来的诊断信息就白丢了 —— 探针那一层已经因为这个
卡过一次（`timeoutSeconds: 3` 与 `web.readyTimeout` 的 3s 刚好平局）：

| 层 | 值 | 在哪里 |
| --- | --- | --- |
| Docker HEALTHCHECK `--timeout` | 9s | `Dockerfile` |
| 二进制自探的客户端预算 `healthcheckTimeout` | 8s | `cmd/trpc-service/main.go` |
| `/readyz` handler 对外层兜底 `readyTimeout` | 3s | `trpcservice/web/web.go` |
| `storage.Ping` 自己的预算 `pingTimeout` | 2s | `trpcservice/storage/storage.go` |

K8s 探针打的是 `/readyz`，所以它的 `timeoutSeconds` 要跟 **3s** 比而不是跟 2s 比（清单里
是 4s）。`scripts/check_deploy.sh` 把这两个不等式钉住了。

#### 探针计时：三条查过源码才知道的事

摘流量的延迟人人都按 `failureThreshold × periodSeconds` 算（这里是 2×5s ≈ 10s）。这个
公式在本清单下成立，但它成立的理由并不显然 —— 有两个流传很广的乘数会把它变成 24s。
两条都查过 kubelet v1.30.2 源码，都不存在：

| 传闻 | 源码实际 | 后果 |
| --- | --- | --- |
| 探针失败会在一个周期内重试 3 次（`maxProbeRetries = 3`） | 重试的门是 `if err == nil { return }`，而 `DoHTTPProbe` 把传输错误**和超时**一律转成 `(probe.Failure, nil)` | **httpGet 探针从不重试**；那条只对会返回非 nil error 的 exec 类探针有效 |
| `timeoutSeconds` 大于 `periodSeconds` 会让探针重叠 | worker 是串行循环：`doProbe` 跑完才等下一个 tick | 不重叠，但**丢 tick**，有效间隔变成 `max(period, timeout)` |

第二条正是 startup 探针的预算不能写成 `30 × 2s = 60s` 的原因：它的 timeout(4s) 故意大于
period(2s)，一次失败的 `/readyz` 最多耗 `web.readyTimeout` 3s，所以真实预算在 60s（Redis
快速拒绝）到 90s（handler 跑满兜底）之间。readiness 那条 `timeout < period` 的断言就是
为了让 10s 这个数继续为真。

还有一条会改变排障习惯：**kubelet 不把响应体放进事件** —— `DoHTTPProbe` 只用状态码拼
消息，因为体里可能含密钥（k8s issue 99425）。所以 `kubectl describe pod` 里能看到的只有
`HTTP probe failed with statuscode: 503` 或 `context deadline exceeded (Client.Timeout…)`，
`/readyz` 一次列全的那些未满足原因一个字也到不了那里。要看它：

```bash
kubectl port-forward svc/trpc-agent-service 8080 &
curl -i localhost:8080/readyz        # 503 + 原因体在这里，不在事件里
```

这也正是探针 timeout 必须**严格大于** 3s 的真实收益：平局时事件会从「statuscode: 503 =
我活着，依赖没活」变成「Client.Timeout = 进程卡死」，而这两者的处置完全不同（D4 摘流量
等恢复 vs D5 进程已经没救）。体既然到不了事件，状态码就是事件里全部的诊断信息。

单条消息的 `agent.message_timeout`（默认 2m）不在这条链上，它是另一回事 —— 而它比
`terminationGracePeriodSeconds` 大也没有任何意义，见下面第 5 条。

### 其余六条

1. **副本数固定 1。** msg_id 去重表和同会话串行锁都是**进程内**结构（事实 #7），
   第二个副本会静默破坏幂等，并让两个节点同时驱动一个会话。Deployment 因此同时写了
   `replicas: 1` 和 `strategy: Recreate` —— 滚动更新在 1 副本下算出
   `maxUnavailable = 0`，新旧 Pod 会重叠一整个就绪窗口，恰好是最容易收到 IM 重投的
   时候。HPA 的 `maxReplicas: 1` 是同一件事的第三处钉子。解锁需要两步：去重搬到
   Redis SETNX，会话按 key 做一致性哈希路由。
2. **K8s 里的热更新不跨调度。** initContainer 把配置拷进 emptyDir，所以
   `PUT /admin/settings` 能成功（ConfigMap 卷是只读的，直接挂会让每次 PUT 变成
   `error_type=commit`），但改动只活到 Pod 被重新调度为止，ConfigMap 仍是持久真相。
   要真正持久的活配置得有个存储（Redis 或 PVC），属二期。
3. **PDB 会挡住 drain。** `replicas: 1` + `minAvailable: 1` 意味着驱逐 API 拒绝一切
   自愿中断，节点维护需要显式绕过：
   `kubectl drain <node> --ignore-daemonsets --disable-eviction`。这是「永不归零」在
   容量为一时的必然结果，不是预算写错了。
4. **`/admin` 没有鉴权。** 它是改租户和改运行时包络的面，而 Ingress 为了方便把它
   暴露出去了。放到带认证的代理后面、用 source-range 注解限制来源，或者干脆从
   Ingress 里去掉、改用 `kubectl port-forward`。
5. **停机不等在飞消息。** `srv.Shutdown` 拿 5s 预算，之后 main 直接返回，不等持有
   `WithoutCancel` ctx 的 dispatch goroutine（事实 #8）。所以 `stop_grace_period` /
   `terminationGracePeriodSeconds` 都是 10s —— 比 `message_timeout` 大没有任何意义，
   那些消息横竖会被截断。把 grace 调到 135s「覆盖 2m 默认值」是直觉性的错误，只会
   让 rollout 变慢。
6. **GOMAXPROCS 要跟着 `limits.cpu` 走。** Go 运行时按**节点**核数开线程，而这个仓库
   没有 vendor `automaxprocs`。16 核节点上 `limits.cpu: 2` 的进程会拿 16 个线程去挤
   2 CPU 的配额，时间都花在 CFS 限流上 —— 在指标里长得和模型变慢一模一样。清单里
   两者是同步的，改一个记得改另一个。
