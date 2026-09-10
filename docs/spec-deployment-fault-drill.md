# Spec：部署与韧性切片 —— go.mod 冻结、Compose/K8s 部署、端到端联调与故障演练（9/9–9/10）

> 目标：把方案文档 3.6「部署形态 = Compose 最小部署 / K8s 生产部署」和 2.3「无状态节点水平扩展」
> 从设计变成能一键起、能真演练的产物：镜像可构建、栈可拉起、健康探针可打、故障可注入可观测，
> 并按 9/9 排期冻结 go.mod。演练中发现的两个真实缺口（消息超时硬编码、无租户并发配额）在本切片一并补齐。

## 1. 事实核查（2026-09-09，动手前必读）

| # | 结论 | 证据 |
| --- | --- | --- |
| 1 | **Docker Hub 不可达**：`registry-1.docker.io` 的 manifest HEAD 请求 i/o timeout；本机可用镜像只有 `golang:1.24`、`redis:7-alpine`、`ubuntu:22.04`（另有 `docker.m.daocloud.io/library/*` 前缀副本）→ 部署产物必须**零拉取可跑**：builder 用本地 `golang:1.24`，runtime 用 `scratch`，假模型不能指望 `python:*-alpine` | `docker pull python:3.12-alpine` / `alpine:3.20` 双双失败 |
| 2 | 容器内默认 `GOPROXY=https://proxy.golang.org` **不可达**（i/o timeout），改 `goproxy.cn` 可正常列版本 → Dockerfile 用 `ARG GOPROXY=https://goproxy.cn,direct`（可 `--build-arg` 覆盖）；仓库若存在 `vendor/`，Go 自动走 `-mod=vendor`，同一份 Dockerfile 也能全离线构建 | `docker run golang:1.24 go list -m -versions trpc.group/...` |
| 3 | 本机只有 `kubectl`，**没有 kind / minikube / k3d / helm**；且 `kubectl apply --dry-run=client` 仍会去 `localhost:8080/openapi/v2` 拉 schema，离线必须加 `--validate=false` → K8s 清单的验收口径只能是「离线解码通过 + 字段逐项对照官方文档」，**不假装在集群里跑过** | `which kind minikube k3d helm` 全 not found |
| 4 | 现有 HTTP 面**没有任何健康检查端点**（`web.NewServer` 只挂 `/`、`/api/tenants`、`/admin/`、`/callback/`、`/webchat/`）→ Compose healthcheck 与 K8s 探针无处可打，必须新增 | `trpcservice/web/web.go:17-40` |
| 5 | 单条消息的超时是**硬编码 2 分钟**，且 ctx 已被 `handleCallback` 的 `WithoutCancel` 从 HTTP 请求 deadline 上摘下 → 模型挂死时唯一兜底就是这个 2 分钟；超时最终落进 `error_type=agent`，与真实模型报错混为一谈，运维分不清「慢」和「坏」 | `channels.go:212`、`channels.go:233` |
| 6 | **没有任何并发配额**：一条消息一个 goroutine，租户级洪峰只能靠 Go 调度器硬扛 → 方案文档 2.3「租户级配额限制单租户最大并发」与风险清单 #3 的缓解措施尚未落地 | `channels.go:216-220` 无节流 |
| 7 | `dedup`（msg_id 幂等）与 `sessionSerializer`（同会话串行）**都是进程内结构**，代码注释里就写着「待 Redis SETNX / 一致性哈希路由」→ **副本数 > 1 时这两条保证只在单节点内成立**，必须写进部署文档的显式限制，而不是装作已解决 | `channels.go:371-375`、`channels.go:415-416` |
| 8 | `main` 已有 SIGTERM/SIGINT 优雅停机（`srv.Shutdown` 5s），但 dispatch goroutine 持的是 `WithoutCancel` 的 ctx，Shutdown **不等它们** → 停机时在飞消息由 message_timeout 兜底，K8s `terminationGracePeriodSeconds` 要按这个口径设 | `main.go:94-101` |
| 9 | Redis 真在跑（`redis:7-alpine`，Up 4 days，127.0.0.1:6379），Docker 29.5.2 + Compose v5.1.3 可用 → **故障演练可以真停真起**，不是纸面推演 | `docker ps` |
| 10 | go.mod 直接依赖 13 项（miniredis + 9 个 otel 模块 + yaml.v3 + trpc-agent-go + 其 session/redis 子模块）、`go` 指令 1.24.1，本机工具链 go1.26.5 → 冻结基线要同时锁「go 指令版本 + 直接依赖清单」，间接依赖由 go.sum 兜 | `go.mod`、`go version` |
| 11 | `scripts/fake_model.py` 只会一种回复，**无法注入故障**（超时/5xx/429），而容器里又拉不到 python 镜像（#1）→ 演练需要 Go 版假模型 `cmd/fake-model`，与平台同一构建产物、零第三方依赖 | 事实 #1 + 脚本现状 |
| 12 | **（动手后补测）框架的 redis `GetSession` 会把连接错误吞掉**：内部 `checkSessionExists` 失败时只 `log.Warn`，随后因为 zset/hashidx 两个 exists 均为 false 而直接返回 `(nil, nil)` → 用它做 readiness 探针会在 Redis 宕机时**误报健康**。同一包的 `ListAppStates` 直接映射 `HGETALL`，连接错误如实返回，`redis.Nil` 转空 map，且仍不写入 → 只读探针必须走它 | `session/redis@v1.11.0/service.go:376-390`（`log.WarnfContext` + `getSessionInternal` 返回 `nil, "", nil`）、`internal/hashidx/state.go:106-119` |
| 13 | **（动手后补测）假上游必须先排空请求体再挂住**：`net/http` 只在请求体读完后才启动探测客户端断开的后台读，所以一个「收到请求就挂住、从不读 body」的 handler **永远看不到 `r.Context()` 被取消**，`httptest.Server.Close()` 也会一直阻塞到 delay 结束 | `net/http/server.go:2059-2063`（`registerOnHitEOF(req.Body, startBackgroundRead)`）；实测同一用例 **5.01s → 0.31s**（加上 `io.Copy(io.Discard, r.Body)` 后取消被立即观测到）→ `cmd/fake-model` 的 `timeout` 模式必须照此写 |
| 14 | **（动手后补测）`WebChat.Send` 会掩盖「用已超时 ctx 发送」这个 bug**：它是 `select { case ch <- msg: case <-ctx.Done(): }`，而通道有 64 缓冲且被 SSE 循环持续排空 → 两个 case 同时就绪时 Go **随机选择**，发送仍有约一半概率侥幸成功 | 变异实测：把 `g.reply` 改回用原 ctx，端到端 SSE 断言 5 次中只拦住 **3** 次 → 该不变量必须由确定性镜像适配器守（5/5 拦住），SSE 用例只作端到端佐证 |
| 15 | **（动手后补测，正面结论）预算耗尽后上游请求会被真取消，不是被遗弃**：框架把 ctx 一路透传到 HTTP 层，dispatch 放弃后连接随即关闭 → D1「goroutine 不泄漏」在单测层面已有证据（假模型自己观测到取消） | `model/openai/openai.go:1774`（`m.client.Chat.Completions.NewStreaming(ctx, ...)`）、`openai.go:690-707`（emit 也 select `ctx.Done()`）；`TestDispatchTimeoutReachesTheUser` 的 `waitCancelled` |
| 16 | **（dep7 演练挖出的真实缺陷）框架的 LLM 循环默认无上限，而我们的 `NewRunner` 没有设 `WithMaxLLMCalls`**：`llmflow` 的 `for {}` 拿不到「最终响应」就立即再打一次；一个回 200 但零 chunk 直接 `[DONE]` 的上游**永远满足不了退出条件** → 实测一条用户消息在 2s 预算内打了 **16,588** 次上游（≈8.3k/s），单测同条件 5s 打了 **28,123** 次（≈5.6k/s），且 `surfaced: ""` —— 用户什么错误都看不到，只能等预算耗尽。修复：`agent.max_llm_calls`（默认 8）透到 `llmagent.WithMaxLLMCalls` | `llmflow/llmflow.go:214`（`for {`）、`llmflow.go:2459`（`max LLM calls (N) exceeded`）、`agent/option.go`（`maxLLMCalls <= 0` 即不限）；`IncLLMCallCount` 在真正调用**之前**执行 → 上游恰好收到 N 次 |
| 17 | **（dep7 实测）`openai-go@v1.12.0` 客户端自带重试**：`MaxRetries: 2`（共 3 次尝试），且尊重 `Retry-After`。两个运维后果：① 上游 5xx 时我们的实际放大是 **3 倍**（演练实测一次 `error500` 消息 = 假模型收到 3 次请求）；② `Retry-After` ≥ `message_timeout` 时，客户端还在退避里预算就到期了，于是这条被归类为 `error_type=timeout` 而不是 `agent`（分类没错，ctx 确实到期，但**根因是限流**） | 演练实测：`500：客户端重试到 3 次 = 3`、`429：Retry-After(2s) 吃掉预算，只打一次 = 1` 且审计为 `timeout`（§4.5） |
| 18 | **（dep7 实测，约束 dep9 写脚本）bash 3.2 在「命令替换作为函数实参、且前面还有其他带引号实参」这个位置上会把内层引号解坏**：`chk "a" "b" "$(curl -d "{\"k\":\"v\"}")"` 里的 payload 被拆成多个词，服务端收到的是一个 **JSON 字符串**而不是对象 → `400 decode body: json: cannot unmarshal string into Go value of type admin.tenantDTO`。阴的是它在赋值形态 `out="$(...)"` 下**不发作**，所以隔离测试全绿、只有真脚本会红 | 实测旧写法 **3/3 失败**、新写法 **3/3 通过**；`od -c` 对比两种位置的实参字节 → 演练脚本的 JSON payload 一律用 heredoc 落文件 + `--data-binary @file`，全程不碰转义 |
| 19 | **（dep8 实测）挂载形态决定 `PUT /admin/settings` 能不能成功，三选一只有一种可用**：`config.Save` 写 `<path>.tmp` 再 rename → 单文件挂载 **400**（Docker 建的 `/config` 属 root，UID 65534 建不出 tmp）、目录可写 **200**、目录 `:ro` **400**。副作用：`Save` 重新 marshal 整个文件，**注释全丢**（实测 54 行 → 31 行）→ 实验必须用一次性副本，别改仓库里那份。K8s 侧同源：ConfigMap 卷永远只读，只能 initContainer 拷进可写 emptyDir | 决定性实验：三种挂载各起一次栈，各 PUT 一次；`config.Save` 的 tmp+rename |
| 20 | **（dep8 挖出的真实缺陷）`audit.New("")` 曾返回 nil logger**，而 K8s 形状的配置故意不写 `audit.file`（只读根文件系统下无处可落）→ 整条治理留痕**静默消失**，与 `audit.go:69`／`admin.go:41`／`config.example.yaml:33` 三处「省略即 log-only」的承诺自相矛盾。原回归测试 `TestNilLoggerIsNoop` 只断言 `err == nil`，所以从来没有机会拦住它 | 修为 `&Logger{}`（空路径 = 只走 log）；新增 `audit.TestEmptyPathIsLogOnlyNotSilent`，断言 log 里**真的出现了**记录而不只是不报错 |
| 21 | **（dep8 实测）同一份 k8s 渲染结果在 Go 和 Ruby 眼里是两个值**：`kubectl kustomize` 重发射时会**去掉** `":8080"` 的引号，ruby 的 Psych 把以 `:` 开头的裸标量读成 **Symbol**、转 JSON 时冒号被吃掉（`"8080"`），而 Go 的 YAML 没有 Symbol 类型、如实读成 `":8080"` → 门禁用 ruby 做 YAML→JSON 时，会对着一个不存在的问题报绿或报红。修：`-addr` 写 `0.0.0.0:8080`（语义等价，`probeHost` 已支持且有单测钉住），门禁加两条正则专扫这个形状 | `kubectl kustomize` 输出对比源文件；`ruby -ryaml -rjson` 与 Go 解析结果并排；正则先用 fixture 验证**既抓坏形状又不误伤** `redis://redis:6379`、`http://x:4318`、注释行与带引号的 `":8080"` |
| 22 | **（dep8 查源码，纠正三条流传的探针算术）kubelet 的 probe worker 是「doProbe 跑完再等下一个 tick」的串行循环** → ① `timeoutSeconds > periodSeconds` **不会**让探针重叠，而是丢 tick，把有效间隔拉长成 `max(period, timeout)`；② `maxProbeRetries = 3` 对 **httpGet 探针完全不生效**（重试的门是 `if err == nil { return }`，而 `DoHTTPProbe` 把传输错误**和超时**一律转成 `(probe.Failure, nil)`），所以 `failureThreshold × periodSeconds` 没有隐藏的 3 倍乘数；③ **响应体永不进事件** —— `DoHTTPProbe` 只用状态码拼消息（体内可能含密钥，issue 99425），`/readyz` 一次列全的原因到不了 `kubectl describe`。后果：探针 timeout 必须**严格大于** `web.readyTimeout`(3s)，而收益不是「保住原因体」（保不住），是让事件停在 `statuscode: 503`（依赖没活 → D4 摘流量等恢复）而不是滑成 `Client.Timeout`（进程卡死 → D5）；原因体只能靠 `port-forward` + curl 或应用日志 | kubelet v1.30.2 的 `pkg/kubelet/prober/{prober.go,worker.go}` + `pkg/probe/http/http.go` 源码直读（本机无集群，**未在集群里量过**）。门禁把 bound 从 `web.go` 里 `sed` 出来而不是写死 3，变异 M24 把 `readyTimeout` 改成 10s 后被 `want>'10'` 当场拦下 |
| 23 | **（dep9b 实测，推翻一条假事实）`PUT /admin/settings` 的热更新会落盘，所以跨重启**：`admin.commit()` 第一步就是 `config.Save(s.path, next)`，`/config` 是可写 bind mount（事实 #19）→ 演练脚本原先按「热更新只在内存里、重启即失效」写断言，实测**恰好相反**：PUT `message_timeout: 7s` 后文件立刻是 7s，`docker kill app` + `start` 后新进程读到的仍是 7s。运维后果是「临时调参」不会自愈，误配会被写进磁盘留到下一次重启 | `admin.commit()` 源码；D6 实测三证：重启后 `/admin/settings` 仍回 30s、挂载文件里 grep 到 `message_timeout: 30s`、仓库那份配置的 14 行注释仍在（对照组） |
| 24 | **（dep9b 实测）`config.Save` 会省略「等于默认值」的字段，所以「文件里看不到」≠「没生效」**：`agentToYAML` 在 timeout/calls 均为默认且配额为 0 时整段返回 nil（与 `storageToYAML` 同源，目的是让 Save 输出 diff-clean），而 `parseAgent` 重新 Load 时会把同一批默认值填回 → 无损，但反直觉：D6 热更新后副本文件里 `max_llm_calls` 消失了，第一眼像数据丢失。另两个副作用：注释被抹平（事实 #19）、缩进变 4 空格 | `config.go` 的 `agentToYAML` / `agentYAML` / `parseAgent`；`.smoke/drill/config/config.yaml`（54 行 → 37 行，agent 段只剩 `message_timeout: 7s`） |
| 25 | **（dep9b 挖出的杀伤半径）`dedup` 的键只有 `msg_id` + TTL、且是进程内存 → 重复 msg_id 会被静默吞掉**：返回 **202**、审计里**一行都没有**（连 span 都不开）、用户**收不到回复**。这个症状与「故障期间消息挂死」**逐字相同**，所以 msg_id 不唯一的演练脚本会把自己的 bug 误读成产品故障 —— D4 连败两轮（单轮 80.44s / 89.76s 等不到终态）的真凶就是它，而不是 Redis | `channels.go` 的 dedup 结构（事实 #7）；现场证据：SSE 日志 0 字节、`req_drill-*-d4-*` 只落了 1 个文件、callback 仍 202 |
| 26 | **（dep9b 实测，bash 3.2 陷阱第 6 条）命令替换里的自增不回父 shell**：`el=$(send …)` 是子 shell，`SEQ=$((SEQ+1))` 只改副本 → 连续三次调用各自看到 `SEQ=1`，父 shell 停在 0，于是三条消息拿到**同一个 msg_id**，撞上事实 #25。上一轮的「修复」因此是 **no-op**（与 dep9a 的 no-op 变异同源：改完必须证明改到了）。修法：序号走文件 `$SMOKE/.seq`，并加撞号守卫（`[ -e "$req" ] && return 9`）把「静默吞掉」变成显式失败 | 最小复现 `SEQ=0; f(){ SEQ=$((SEQ+1)); echo $SEQ; }; x=$(f); y=$(f); z=$(f)` → 三次都是 1；修复后 D4 由 7 FAIL 降到 1 FAIL |
| 27 | **（dep9b 实测）「没拿到指标 dump」是 SKIP，「指标是 0」是 FAIL，两者不能混**：otel 周期 reader 的**第一次导出发生在 interval 之后**，起栈后立刻读会得到空；dump 行的顶层键是 `ScopeMetrics`，指标全空时是 `"ScopeMetrics":[]`。脚本因此加了 `wait_metric` 门禁（最多 30 × 0.5s 等出非 NODUMP），并用「过滤后计数为 0」而不是「没等到 dump」来判负。另两条读数经验：trace 的 stdout exporter 是多行美化 JSON，一条消息给日志加 **134 行**；4 条并发消息后 **40/40** dump 行全部解析成功 → 「并发写导致 JSON 交错截断」的假设被实测否掉 | D2/D7 的 `metric` / `wait_metric` / `delta` 实现；M5 变异里 5 条 SKIP 全部是 `--only` 未选中，**0 条**是等不到 dump |
| 28 | **（dep9b 实测）`docker compose up -d` 对立刻退出的容器仍报 0；`up -d --force-recreate app` 不重建依赖容器** → ① fail-fast 断言不能靠 `up` 的退出码，必须落 `State.ExitCode` 与 `State.Error`；② 「假模型没被一起重启」不能用它的 requests 计数证明（演练开头 `reset:true` 已把计数归零，那是**空断言**），要用 `State.StartedAt` 逐字对照 | D5 的 `docker inspect -f '{{.State.ExitCode}}'`；D6 kill 前捕获 `mstart`，重启后与假模型的 `StartedAt` 逐字相同 |
| 29 | **（dep9b 实测）Redis stop 后 `/readyz` 的耗时方差 0.066–2.65s，而 reason 串五次逐字相同 → 方差全在 Docker 内嵌 DNS（`127.0.0.11:53`），不在 handler**。原先手写的 2.0s 上界是一次幸运样本，第四遍量到 2.65s 就红了。修法沿用 M24：上界用 `sed` 从 `web.go` 读 `readyTimeout`（常量漂移会当场报红），并补一条**更强**的证据 —— 503 的响应体里是依赖自己报的错而**不含** `context deadline`，这直接证明「没挂满兜底预算」，比耗时数字更硬 | 五个样本 1.089 / 0.090 / 0.131 / 0.066 / 0.136s，reason 串一致；`check_deploy.sh:448` 的同源做法 |
| 30 | **（dep9b 实测）同一场 Redis 宕机，两条路径的错误串不同源**：`/readyz` 走我们的 `storage.Ping`，报 `session backend read: dial tcp: lookup redis on 127.0.0.11:53: no such host`；消息路径走框架的 redis session 实现，报 `check session exists: check session exists pipeline: dial tcp: …` → 把 readyz 的措辞套到审计 `detail` 上是**没量过的猜测**（先量后钉）。同一行审计实测：`error_type=runner`、`decision=error`、`latency_ms=232`，与 readyz / reply 三行共用**同一个 trace_id**（会话失败也没断链） | D4 实测 `detail` 原文；`ntrace` helper 数出新审计行里的 distinct trace_id = 1 |
| 31 | **（dep9b2 查出）指标属性键是 `tenant`，审计 JSONL 字段是 `tenant_id`，这个分歧是有意的**（结构化日志也拼 `tenant`），但 `metrics.go` 的 doc 注释原先写成 `tenant_id` —— 照注释去对齐不会大声失败，会让**所有按租户过滤的查询静默归零**。注释已改并写明后果。M5 变异实测（8 处 `tenant=` → `tenant_id=`）：恰好 **4 条**指标断言变红且 `got=[0]`，其余 40 条一条不红、0 条 dump SKIP → 证明断言真的在读标签，且**标签写错是 FAIL 不是 SKIP** | `metrics.go:126-132` 注释；`audit.go` 的 JSONL 字段名；`.smoke/mut5.sh` 的预测-验证记录 |
| 32 | **（dep11 验 README 时实测）`start.sh` 对一个已经死掉的进程依旧报成功**：它用 `nohup bin/trpc-service &` + `echo $! > pid` + `echo "started: pid=$!"`，**中间没有任何存活检查**；而 `config.Load` 在既无 `config.yaml` 又无 `MODEL_API_KEY` 时会直接报错退出（实测 **rc=1**，错误文本把两条补救都写出来了）→ 新克隆上 `./start.sh` 会打印 `started: pid=…`，而进程早已不在，真正的错因只在 `data/trpc-service.log` 里。这跟事实 #28（`up -d` 对立刻退出的容器报 0）是**同一个形状**：启动命令的退出码不等于进程活着。已写进 README 快速开始第 1 节；`start.sh` 本身属仓库模板脚本，**本切片未改** | `/tmp/noconf` 下直接跑二进制：rc=1 + `load config: no config at config.yaml and MODEL_API_KEY unset; copy config.example.yaml to config.yaml or export MODEL_API_KEY`；`start.sh` 全文 20 行无存活检查 |

## 2. 设计

### 2.1 go.mod 冻结（排期 9/9 的硬项）

- `docs/deps-baseline.txt`：冻结基线 = `go` 指令版本 + 13 项直接依赖的 `path version`，一行一条，排序稳定；
- `scripts/check_deps.sh`：三种模式，全部离线可跑 ——
  - 默认（门禁）：① `go mod verify`（本地缓存与 go.sum 一致）；② 直接依赖清单与基线 diff，漂移即 exit 1 并打印 diff；
  - `--tidy`（提交前/CI 全量）：先跑 `go mod tidy`，用 tidy 前后的**文件快照**比对（不依赖 `git diff`，因此在非 git 检出里也能跑，也不会因 go.mod 已有无关未提交改动而误报），有漂移则打印 diff 并 exit 1；
  - `--update`：重生成基线（保留注释头）。冻结后若确需新增依赖，必须同时改基线文件 —— 让「破例」在 diff 里显式可见；
- 口径：**本切片零新增第三方 Go 依赖**（假模型走 stdlib，部署产物是 YAML/Dockerfile/shell）。

### 2.2 健康检查与自探（部署前置条件）

| 端点/模式 | 语义 | 用途 |
| --- | --- | --- |
| `GET /healthz` | 恒 200（能响应 HTTP 即存活），不碰任何依赖 | liveness：依赖故障**不该**重启进程 |
| `GET /readyz` | 200 需同时满足：租户数 > 0 **且** `storage.Ping` 通过；否则 503 + JSON 原因 | readiness：Redis 抖动时自动摘流量，恢复后自动回流量 |
| `trpc-service -healthcheck` | 进程自探：GET 自己的 `/readyz`，200 → exit 0，否则 exit 1 | `scratch` 镜像里没有 shell/curl/wget，Compose healthcheck 与 K8s exec 探针都靠它 |

`storage.Ping(ctx, svc)` 是**只读**探测：对固定 app 名 `readyz` 调 `ListAppStates`，空结果（key 不存在）也算健康。
不用启动期那套 `CreateSession → AppendEvent → DeleteSession` 写探针 —— readiness 每几秒打一次，
不能每次都往共享后端写一对垃圾 key。

也不能用 `GetSession(probeKey)`：事实 #12 证明它在 Redis 宕机时返回 `(nil, nil)`，探针会恒绿。
`ListAppStates` 既只读又能如实报错，是两者中唯一可用的选择（实现处已写入注释，防止后人“优化”回 GetSession）。

`web.NewServer(gateway, admin, tenantIDs, ready)` 新增第 4 个参数 `ready func(context.Context) error`，
`nil` 即恒就绪。两个探针都**不启 span**（每几秒一次、永远不停，会把真正承载全链路的 message span 淹没）；
`/readyz` 一次收集**全部**未满足条件再返回，运维不用一次修一个来回。路径以 `web.HealthPath` / `web.ReadyPath`
常量导出，main 的 `-healthcheck` 与部署清单共用同一常量，避开四处字面量漂移。

`trpc-service -healthcheck` 在**加载配置之前**就分流：它的职责是通过 HTTP 问运行中的进程，而不是自己再走一遍
依赖初始化（否则 Redis 宕机时自探会因为启动探针失败而退出，无法区分“进程没起”和“进程起了但依赖坏了”）。

### 2.3 演练暴露的两个缺口，一并补齐

**新增顶层 `agent` 配置段**（全部可选，纯默认整段省略，与 `storage` / `log` 同一套 `*ToYAML` 机制）：

```yaml
agent:
  message_timeout: 2m               # 单条消息端到端上限（模型调用 + 流式排空），默认 2m，须 > 0
  max_concurrency_per_tenant: 0     # 单租户在飞消息上限，0 = 不限（默认）
  max_llm_calls: 8                  # 单条消息可引发的模型调用上限，默认 8
```

环境变量覆盖：`AGENT_MESSAGE_TIMEOUT` / `AGENT_MAX_CONCURRENCY_PER_TENANT` / `AGENT_MAX_LLM_CALLS`
（容器/K8s 里调参不用改挂载的配置文件）。

`max_llm_calls` 是 dep7 演练挖出缺陷后补的（事实 #16、§4.5），它的零值语义**故意与配额不同**：
`max_concurrency_per_tenant: 0` = 不限，`max_llm_calls: 0` = 取默认值 8。因为「不限」正是这个字段要堵的洞，
而框架把非正值读作无上限 —— 任何一个部署都不应能表达「不限」。文件与 env 两条解析路径都做这个替换，
`Validate` 里再留一道 `must be positive` 兜底（只有手建 `Config` 才能走到，但它正是「cloneConfig 漏字段」
这类回归的命名哨兵，治理切片踩过同一个坑）。

**dispatch 行为变化**（`channels.go`）：

| 场景 | 现状 | 切片后 |
| --- | --- | --- |
| 超时 | 硬编码 2min，错误落 `error_type=agent` | 取 `AgentConfig.MessageTimeout`；`ctx.Err()` 为 deadline 时归类 `error_type=timeout`，回复 `TimeoutText` 友好话术 |
| 租户洪峰 | 无节流，全部并发进模型 | 入口 try-acquire 租户配额；拿不到就**立即拒绝**（不排队不阻塞）：审计 `inbound` + `throttled`/block(stage=admission, rule=concurrency) + 指标 `result=throttled` + 回复 `ThrottleText` |
| 热更新 | — | `Governance.LimitsFor func() config.AgentConfig` 由 `admin.Service.Agent` 提供，**每次 dispatch 重读**而不是装配时捕获 → 活配置一变，下一条消息即生效 |

**写入面：`GET/PUT /admin/settings`**（dep6 补齐）。GET 渲染的是**生效值**（`message_timeout` 总是给出
duration 字符串而不是空字段），所以客户端可以把响应原样 PUT 回来；PUT 是**整体替换**，零值语义与配置
文件一致 —— 省略 `message_timeout` 等于文档默认值 2m，`max_concurrency_per_tenant: 0` 等于不限。走的
是与租户变更**同一条 commit 路径**（`cloneConfig → Save（先 Validate）→ Registry.Apply → 换活配置`），
所以非法值（解析不了的 duration、负数）一律 400，且活配置与磁盘文件都不动。`Registry.Apply` 会重建整张
runner 表并原子换掉：在飞的 dispatch 已持有旧 runner 引用，不受影响。

审计口径与租户端保持一致：**提交前的校验失败不落审计**（body 连 duration 都解析不出来，属于客户端错误，
与租户 DTO「缺 secret 直接 400」同形），**commit 失败落 `error_type=commit`**。`tenant_id` 写 `*` 表示
「不是租户级变更」，`detail` 记录变更后的**实际取值**（`settings message_timeout=2s max_concurrency_per_tenant=0`）——
运维把一波 `throttled` 与「谁在什么时候调小了配额」对上，靠的就是这一行。

`LimitsFor` 直接返回 `config.AgentConfig`，**不在 channels 里再造一个 `Limits` 类型**：两处结构体字段一旦漂移，
配置校验过的值到 dispatch 就未必成立，而多一个类型换不来任何隔离（channels 已经通过 `metrics` 间接依赖 `config`，
直接引一条边不成环：`config` 只依赖 `tenant`）。main 的装配因此是一行 `LimitsFor: adm.Agent`。

四条实现口径（都是踩过或推演过才定的，改代码前先读这里）：

1. **配额在 session 锁之前 acquire**。反过来写的话，超额消息会全部堆在同会话的锁上排队，等拿到锁时配额
   早已释放 —— 配额形同虚设，洪峰照旧打进模型；
2. **`WithTimeout` 与 dispatch span 提到函数最前**，session 锁在 deadline 之内。等锁的时间计入
   `message_timeout`，因为对用户的承诺是「一条消息端到端有界」；一个要在无界等待之后才开始计时的上限
   不是上限。这也让被节流/被拦截的消息同样带 trace_id；
3. **失败回复必须走脱离 deadline 的 ctx**（`context.WithoutCancel` + 独立的 `replyTimeout`）。
   `WebChat.Send` 是 `select { case ch <- msg: case <-ctx.Done(): return ctx.Err() }`，用已超时的 ctx 发送
   会立即失败 —— 超时路径唯一的目的就是告诉用户「超时了」，而它恰好是最容易静默失败的那条；
4. **流式排空同时 select `ctx.Done()`**。只靠 `for ev := range events` 的话，超时能否生效取决于框架是否
   及时关闭通道；而 dispatch 持着 session 锁，一个不合作的流会把该会话之后的所有消息一起卡死。
   预算耗尽即停止排空并归类为 `timeout`（框架内部同样收到 ctx，生产端会随之退出，D1 的
   「goroutine 不泄漏」断言就是这条的实测把关）。

配额拒绝走「拒绝」而不是「排队」，是方案文档「队列背压」的最小实现：进程内无界队列在洪峰下只会把
内存和延迟一起拖垮，而 IM 侧本来就有重试与用户重发。审计里留痕 + 指标可告警，运维能看到被拒的量。

被节流的消息**仍写 `inbound` 审计行**，与输入 guardrail 拦截保持同一形状（先记录到达，再记录决策）：
审计行的职责是「每条入站消息都留痕」，而消息量统计走的是 `trpcservice.messages` 计数器，两者不会互相重复计数。

`audit` 新增事件常量 `EventThrottled = "throttled"`，`Record.Stage` 取值扩到 `admission|input|output`、
`Rule` 扩到 `concurrency|length|keyword`（注释同步更新）；`metrics` 不新增仪器，复用
`trpcservice.messages` 的 `result` 标签（取值扩到 `ok/guardrail/error/throttled`）。
`TimeoutText` / `ThrottleText` 放在 `channels` 而不是 `guardrail`：两者都不是租户安全策略的判定结果，
一个是预算、一个是准入控制。

### 2.4 部署产物

> **本节在 dep8 落地后被纠偏过一次**：下面的清单是实际产出，与原设计的三处出入逐条列在
> 「与原清单的差异」里 —— 不悄悄把原设计改成事后看起来一直如此。

```
Dockerfile                     多阶段：golang:1.24 builder → scratch runtime，两个二进制，非 root(65534)
.dockerignore                  排除 config.yaml 与 .git（两者都可能含真密钥）+ bin/ data/ .smoke/
docker-compose.yml             app + redis + fake-model（默认栈）；otel-collector（profile observability）
deploy/compose/config/config.yaml                compose 默认挂载：两个租户，都指向镜像内假模型
deploy/compose/config-observability/config.yaml  同上，只多一个 telemetry 段
deploy/otel-collector.yaml     OTLP/HTTP 4318 接收 → debug exporter（演示 otlp 链路，需联网拉镜像，默认不启）
deploy/k8s/kustomization.yaml  清单入口；ConfigMap 由 configMapGenerator 生成（必须 -k，不能 -f）
deploy/k8s/config/config.yaml  Kubernetes 形状：单租户、无 audit.file（走 stdout）
deploy/k8s/10-secret.example.yaml  MODEL_API_KEY 占位（真 Secret 不入库）
deploy/k8s/20-deployment.yaml  探针 / 资源 / env / grace / 只读根文件系统 / 非 root / initContainer 拷配置
deploy/k8s/30-service.yaml     ClusterIP
deploy/k8s/40-hpa.yaml         CPU 70%（**不是**方案文档写的「按并发 session」，差异见 §5）
deploy/k8s/50-pdb.yaml         minAvailable: 1
deploy/k8s/60-ingress.yaml     可选，webhook 需要公网入口
deploy/README.md               ★新增：目录说明 + 快速开始 + **诚实的验证清单**（真跑／离线门禁／没验过）
scripts/check_deploy.sh        ★新增：58 条离线结构门禁（compose JSON + kustomize 交叉引用 + 跑真测试并计数 + 镜像实物）
trpcservice/config/deploy_test.go ★新增：7 个测试把三份部署配置喂给真 config.Load（含 Validate 与 env 覆盖）
```

**与原清单的差异（三处，都是被实测逼出来的）**：

1. **`fake-model` 从 `demo` profile 挪进默认栈。** 仓库不带真密钥（`config.yaml` 正因为存密钥才被
   gitignore），指向真实供应商的默认栈开箱答不出一条消息，dep9 的联调与 dep10 的演练矩阵就没有断言
   对象。假模型是唯一既可达（零拉取，事实 #1）又可控（六模式故障注入）的上游。
2. **`deploy/compose/config.demo.yaml` 单文件 → `config/` 与 `config-observability/` 两个目录。**
   两个原因：① 单文件挂载让 `PUT /admin/settings` 必然 400（事实 #19），必须挂目录；② telemetry
   exporter 没有 env 钩子，切 otlp 变体只能换整份文件，于是变体自成一个目录。
3. **`deploy/k8s/00-configmap.yaml` → `kustomization.yaml` 的 `configMapGenerator` + `k8s/config/config.yaml`。**
   生成的 ConfigMap 名字带内容哈希，改配置**自动触发 rollout**，不必记得去 bump 一个 checksum 注解；
   代价是 `apply` 必须用 `-k`（用 `-f` 会得到一个卷指向不存在名字的 Deployment，门禁变异 M13 钉住这条），
   且 ConfigMap 卷只读 → 需要 initContainer 拷进可写 emptyDir（事实 #19 的 K8s 侧同源）。

镜像设计要点：

- `FROM scratch` + 静态二进制（`CGO_ENABLED=0 -trimpath -ldflags="-s -w"`）→ **零拉取**（事实 #1）、最小攻击面；
- 从 builder 拷 `ca-certificates.crt`：调真实模型 API 与 IM API 都要 TLS 根证书；
- 同一镜像装两个二进制：`/trpc-service`（平台）与 `/fake-model`（演练用假模型）→ demo profile 复用 app 镜像，不需要 python；
- `USER 65534:65534`（scratch 无 passwd，用数字 UID）；审计目录挂卷并可写；
- Compose `depends_on: {redis: {condition: service_healthy}}`，app 的 healthcheck 用 `-healthcheck` 自探。

副本数**固定 1**：事实 #7 的进程内 dedup 与 session 锁在多副本下失效，一致性哈希路由属二期。
Compose 里显式写 `deploy.replicas: 1` 并在文档标注原因，K8s 清单同样 `replicas: 1` + PDB。

### 2.5 假模型 `cmd/fake-model`（演练的故障注入点）

stdlib 实现的 OpenAI 兼容流式端点，行为对齐 `scripts/fake_model.py`（含「内部资料」跨 chunk 切分，
供输出 tripwire 演练），额外提供**运行时**故障注入：

```
GET  /__mode            查当前模式、delay，以及本进程累计收到的请求数 requests
POST /__mode {"mode":"timeout","delay":30}   切换模式，无需重启容器
POST /__mode {"mode":"ok","reset":true}      reset 将计数器归零
```

`requests` 与 `reset` 是 dep7 实装时超出原锁定口径加的两个字段，因为演练需要一个**能把「一条消息花了
几次上游调用」读成数字**的判据（D3 幂等、事实 #16 的调用风暴、事实 #17 的客户端重试放大，全部靠它）。
计数在模式切换时不自动归零，`reset` 是显式的，否则「上一模式残留的次数」会让断言变成猜谜。

| mode | 行为 | 演练用途 |
| --- | --- | --- |
| `ok` | 4 个 chunk + usage + `[DONE]` | 基线 |
| `timeout` | 睡 `delay` 秒（> message_timeout）再回 | 模型超时 → `error_type=timeout` |
| `error500` | 直接 500 + JSON 错误体 | 模型服务故障 |
| `limit429` | 429 + `Retry-After` | 模型限流（风险清单 #2） |
| `slow` | 每 chunk 间隔 `delay` 秒但总量在超时内 | 慢而不坏：验证流式与延迟直方图 |
| `empty` | 200 但零 chunk 直接 `[DONE]` | 退化流：验证空回复不挂死 |

`scripts/fake_model.py` 保留给「本机不想构建 Go 二进制」的轻量冒烟（governance spec §4 仍指向它），
两者行为一致；二期清理成一个（记入 §5 差异）。

### 2.6 联调与演练脚本

- `scripts/e2e.sh`：对一个已在跑的地址（默认 `http://localhost:8080`）做全链路断言 —— 租户列表、
  `/healthz`、`/readyz`、正常对话（流式 chunk + 空 Done）、输入 guardrail 拦截、输出跨 chunk 截断、
  审计行存在且带 trace_id。全部通过打印 `E2E PASS`，任一失败非零退出；
- `scripts/fault_drill.sh`：驱动 Compose 栈做 7 个演练，每个打印 `PASS/FAIL` 并在末尾汇总；
  需要 Docker，不可用时明确 skip 而不是静默通过。

> 实现时修订（实测结果见 §4.7 / §4.8）：`fault_drill.sh` **自己起栈自己拆栈**（不依赖调用者先 `up`，
> 因为 D5 要「redis 未起时启动 app」、D4 要真停真起，栈的形状必须归脚本管），并加了 `--only D4[,D6]`
> 单跑、`--strict`（任一 FAIL/SKIP 即非零退出）、`--keep`（保留栈与 `.smoke/` 供事后取证）；
> 它在仓库配置副本之外**追加 telemetry**，因为演练的判据里有一半是指标 dump 与 trace_id，
> 而默认配置是关着的。`e2e.sh` 仍只打「已在跑的地址」，不起栈。

演练矩阵（对齐方案文档 3.6 的五个故障 + 风险清单编号）：

| # | 演练 | 注入方式 | 期望（验收断言） | 对应 |
| --- | --- | --- | --- | --- |
| D1 | 模型超时 | fake-model `mode=timeout, delay=30` + `message_timeout=3s` | 用户在 3s 级拿到 `TimeoutText`；审计 `error_type=timeout` 且 `latency_ms` 已记；指标 `result=error`；**goroutine 不泄漏**（回复后进程仍能服务下一条） | 3.6 F1 / 风险 #2 |
| D2 | 模型 5xx 与限流 | `mode=error500` / `limit429` | 错误话术回给用户，审计 `error_type=agent` 带原始 detail，进程不崩 | 3.6 F1 / 风险 #2 |
| D3 | 重复投递幂等 | 同一 `msg_id` 连发两次 | 只执行一次（审计仅一行 `inbound`，假模型只收到一次请求），第二次静默 ACK | 风险 #1 |
| D4 | Redis 宕机（运行中） | `docker stop redis` | readyz 转 503（Compose/K8s 会摘流量）；新消息拿到错误回复而非挂死；`docker start redis` 后 readyz 自动回 200 且对话恢复 | 3.6 F3 / 风险 #4 |
| D5 | Redis 宕机（启动期） | redis 未起时启动 app | **fail-fast**：进程以「session backend probe」错误退出，不进半死状态 | 3.6 F3 / 风险 #4 |
| D6 | 节点宕机后会话恢复 | 对话一轮 → `docker kill app` → 起回来 → 追问上下文 | 新进程答得上来（session 全在 Redis），验证「无状态节点 + 共享后端」 | 3.6 F5 / 预期效果 3 |
| D7 | 单租户洪峰隔离 | `max_concurrency_per_tenant=1` + 慢模型，租户 A 连发 3 条，同时租户 B 发 1 条 | A 的超额消息被 `ThrottleText` 拒绝且审计 `throttled`；**B 正常拿到回复**（不被 A 拖垮） | 2.3 配额 / 风险 #3 / 预期效果 3 |

## 3. 测试策略

1. **包内单测**（无需 Docker、无需模型 key）：
   - `config`：`agent` 段解析、默认值、纯默认省略的 Save 往返、env 覆盖、非法值（timeout ≤ 0、并发 < 0、坏 duration）；
   - `storage`：`Ping` 在 miniredis 上通过、miniredis 关闭后报错；
   - `web`：`/healthz` 恒 200；`/readyz` 在 ready=nil / 通过 / 报错三态下分别 200/200/503 且带原因；
   - `channels`：超时归类 `error_type=timeout` + `TimeoutText`；配额拒绝 `throttled` 审计 + `result=throttled` 指标 +
     `ThrottleText`；配额=0 时行为与切片前完全一致（零回归）；配额释放（连发 N 轮不泄漏计数）；
   - `admin`：`Limits()` 读活配置、PUT 热更新后 Gateway 立即看到新值；
   - `agent`（dep7b 新增）：空上游下调用数被 `max_llm_calls` 封顶且错误点名上限；非正上限被纠正而不是透传；
     `Registry.Apply` 重建 runner 后新上限仍生效；
   - `cmd/fake-model`（dep7 新增）：与 `scripts/fake_model.py` 的 **parity 门禁**（事件数、逐字段、跨 chunk 切分、usage）；
     六个故障模式各自的状态码与报文；`timeout` 模式能观测到取消（事实 #13）；`/__mode` 的非法输入不污染运行中的模式。
2. **构建门禁**：`scripts/check_deps.sh` 绿（verify + 基线一致），`--tidy` 绿（tidy 幂等）；
   并各做一次**负例验证**（门禁必须真能拦住漂移，而不是恒绿）。
3. **真跑验收**：`docker build` 成功且镜像可起；`docker compose up` 起栈；`scripts/e2e.sh` PASS；
   `scripts/fault_drill.sh` 七项全 PASS；K8s 清单 `kubectl apply --dry-run=client --validate=false` 全部解码通过。

## 4. 实测结果

### 4.1 go.mod 冻结门禁（dep2，2026-09-09 实测）

环境：本机 go1.26.5，`go.mod` 声明 `go 1.24.1`，13 项直接依赖；模块缓存已热（此前构建过）。

| # | 用例 | 命令 | 结果 |
| --- | --- | --- | --- |
| 1 | 默认门禁正例 | `./scripts/check_deps.sh` | **PASS** exit 0：`all modules verified` + `ok: 13 direct dependencies match the frozen baseline` |
| 2 | 完全离线跑通 | `GOPROXY=off ./scripts/check_deps.sh --tidy` | **PASS** exit 0：`ok: go mod tidy is idempotent`，且 go.mod/go.sum byte-identical（快照 diff 为空） |
| 3 | `--update` 幂等 | 连跑两次 `--update` 后比对 | **PASS**：与手写基线 byte-identical，注释头保留（证明 awk 解析 go.mod 的结果与人工清点一致） |
| 4 | 负例 A：基线版本被改（verify 仍过，只有 diff 能抓） | 基线里 `yaml.v3 v3.0.1` → `v3.0.0` | **PASS** exit 1，diff 精确指出该行 + 打印冻结策略提示 |
| 5 | 负例 B：go.mod 有依赖但基线未更新 | 从基线删掉 `yaml.v3` 一行 | **PASS** exit 1，diff 显示 `+gopkg.in/yaml.v3 v3.0.1` |
| 6 | 负例 C：`--tidy` 抓到非幂等 | 从 go.mod 删掉一条 indirect（`google/uuid`） | **PASS** exit 1，diff 精确显示 tidy 加回的那一行；门禁失败后 go.mod 已由 tidy 恢复成与备份 byte-identical，go.sum 未动 → 失败不留脏文件 |
| 7 | 负例 D：非法参数 | `./scripts/check_deps.sh --nope` | **PASS** exit 2 + usage |
| 8 | bash 3.2 兼容 | 以上全部在 macOS 自带 `/bin/bash` 3.2 下跑 | **PASS**：无 `;&` case fallthrough、无 GNU-only grep BRE（`\|`）；`sed -i ''` 亦为 BSD 写法 |

结论：**冻结生效**。基线锁住「go 指令版本 + 13 项直接依赖」，任何新增/升级/降级都会让门禁非零退出，
且必须先改 `docs/deps-baseline.txt` 才能通过 —— 破例在 review diff 里显式可见。
门禁三种模式全部离线可跑，可直接挂 CI（无需 GOPROXY 可达）。

### 4.2 健康检查与自探（dep4，2026-09-09 实测）

**单测**（`go test ./...` 全绿）：

| 包 | 用例 | 结果 |
| --- | --- | --- |
| `storage` | `Ping` 在 memory 后端通过；在 miniredis 上通过；**连跑 5 次后 `mr.Keys()` 数量不变**（证明只读，不写垃圾 key）；miniredis 关闭后报错；`svc == nil` 报错；ctx 已取消时报错 | **PASS**（4 个用例） |
| `web` | `/healthz` 在零租户 + ready 报错时仍 200；`/readyz` 在 ready=nil / 通过 / 报错三态下分别 200/200/503；无租户 → 503 且 reason 含 tenant；两个条件同时不满足 → 503 且带**两条** reason；**依赖恢复后不重启即回 200**（atomic 开关模拟 D4）；ready 收到的 ctx 带 deadline；`/` `/api/tenants` `/admin/` `/callback/` `/webchat/` 未被新路由抢走，未知路径仍 404 | **PASS**（7 个用例） |
| `cmd` | `probeHost` 把 `:8080` / `0.0.0.0:8080` / `[::]:8080` 转成 `127.0.0.1:*`，已可拨号的地址原样返回；`-healthcheck` 确实打的是 `web.ReadyPath`（钉住自探与路由的耦合）；200→exit 0、503→exit 1、无人监听→exit 1 | **PASS**（3 个用例） |

**真跑取证**（本机真 Redis 容器 + 真进程，`.smoke/` 下临时 config：`backend: redis`、`message_timeout: 3s`、`max_concurrency_per_tenant: 1`）：

| # | 场景 | 实测 | 结果 |
| --- | --- | --- | --- |
| 1 | 启动 | 进程起在 `:18080`，`session=redis`，启动写探针通过 | **PASS** |
| 2 | `/healthz` | `200 {"status":"ok"}` | **PASS** |
| 3 | `/readyz` | `200 {"status":"ready"}` | **PASS** |
| 4 | `-healthcheck` | exit 0，stdout 无输出 | **PASS** |
| 5 | **`docker stop redis`** 后 `/healthz` | 仍 `200` —— liveness 不碰依赖，依赖故障**不会**让编排器重启一个还能自愈的进程 | **PASS** |
| 6 | **`docker stop redis`** 后 `/readyz` | `503 {"status":"unavailable","reasons":["storage: session backend read: dial tcp 127.0.0.1:6379: connect: connection refused"]}` | **PASS** |
| 7 | **`docker stop redis`** 后 `-healthcheck` | exit 1，stderr 原样打印 503 与 reason（运维看到“为何不就绪”而不只是一个数字） | **PASS** |
| 8 | **`docker start redis`** 后 | `/readyz` 自动回 `200 ready`，`-healthcheck` 回 exit 0，**进程全程未重启** | **PASS** |

第 6–8 行就是演练矩阵 **D4**（Redis 运行中宕机 → 摘流量 → 恢复 → 自动回流量）的核心断言，已在真实
 go-redis + 真 Redis 容器上提前取证；也反向验证了事实 #12：若探针用 `GetSession`，第 6 行会是 `200`。
Redis 容器已恢复为 Up 状态。

### 4.3 韧性两项：可配超时 + 租户配额（dep5，2026-09-09 实测）

**单测**（`go test -race -count=1 ./...` 全绿；channels 新增 8 个、admin 新增 2 个）：

| 包 | 用例 | 结果 |
| --- | --- | --- |
| `channels` | `TestLimitsDefaults`：零值 `Governance` 复现配额前的行为；`LimitsFor` 返回非正 timeout 时被纠正而不是照用（`WithTimeout(<=0)` 会发出**已过期**的 ctx，等于每条消息必失败） | **PASS** |
| `channels` | `TestTenantQuota`：按租户独立计数、到顶拒绝、`release` 幂等（`sync.Once`）、全部归还后 map 无残留（泄漏一次就永久节流该租户） | **PASS** |
| `channels` | `TestDispatchTimeoutReachesTheUser`：真 `WebChat` + SSE + 会挂住的假上游，超时归类 `error_type=timeout`、指标 `result=error`、审计留痕，且**真浏览器连接收到了** `本次回复超时，请稍后重试。`；并观测到上游请求被取消（事实 #15） | **PASS** |
| `channels` | `TestReplySurvivesAnExpiredDeadline`：确定性镜像适配器，在 `Send` 内部快照 ctx —— 终态回复所走的 ctx 必须**不再受 dispatch deadline 约束**（事实 #14 的正解） | **PASS** |
| `channels` | `TestDispatchOverQuotaIsRejected`：槽位已满时**立即拒绝**，不进模型、不排队，落 `throttled`/`stage=admission`/`rule=concurrency` + `result=throttled` + 回复 `当前咨询量较大，请稍后重试。`；且**不重复计 inbound** | **PASS** |
| `channels` | `TestThrottleHappensBeforeTheSessionLock`：准入闸**在会话锁之前** —— 洪峰正是一堆消息挤在同一把锁上，放在锁后配额永远不生效 | **PASS** |
| `channels` | `TestBudgetCoversTheSessionLockWait`：deadline **在会话锁之前**取，等锁时间计入 `message_timeout`（在无界等待之后才开始的上界不是上界） | **PASS** |
| `channels` | `TestLimitsAreRereadPerDispatch`：`LimitsFor` 每条消息现查而非装配时捕获，改配额对**下一条**立即生效 | **PASS** |
| `admin` | `TestAgentDefaults`：配置文件无 `agent` 段时解析出文档默认值（2m / 不限），不是会让每条消息秒失败的零值 | **PASS** |
| `admin` | `TestAgentSurvivesTenantMutations`：租户 PUT 走 `cloneConfig → Save → Validate`，内存访问器与**磁盘文件**都必须保住 `agent` 段 | **PASS** |
| `admin` | `TestSettingsHotUpdate`：GET 渲染生效值且可原样 PUT 回；PUT 后 `Agent()` 与磁盘立即变；不能解析的 duration / `-1s` / 负配额均 400 且**活配置与文件都不动**；`PUT {}` 复位到默认且 `agent:` 整段从文件消失；POST → 405；审计 5 条（3 ok + 2 `error_type=commit`）、`tenant_id="*"`、detail 带实际取值、解析失败的请求**不落审计** | **PASS** |

**变异测试**（改完即还原，`diff` 确认 byte-identical）—— 每个不变量都必须**真能拦住**回归：

| # | 变异 | 期望 | 实测 |
| --- | --- | --- | --- |
| M1 | 把配额 acquire 挪到会话锁**之后** | `TestThrottleHappensBeforeTheSessionLock` 失败 | **拦住** |
| M2 | 把 `WithTimeout` 挪到会话锁**之后** | `TestBudgetCoversTheSessionLockWait` 失败 | **拦住** |
| M3 | 把 `g.reply` 改回用**原 ctx** 发送 | 端到端 SSE 断言失败 | **只拦住 3/5 次** → 不可靠门禁（事实 #14），补 M4 |
| M4 | 同 M3，改由确定性镜像适配器判定 | `TestReplySurvivesAnExpiredDeadline` 失败 | **拦住 5/5** |
| M5 | 从 `cloneConfig` 摘掉 `Agent: c.Agent` | `TestAgentSurvivesTenantMutations` 失败 | **拦住**，且报错精确：`400 {"error":"agent.message_timeout must be positive"}` —— 印证治理切片踩过的同一个坑：漏字段会让**所有**租户变更被 `Validate` 拒绝 |
| M6 | `putSettings` 绕过 `commit`，直接 `s.cfg = next`（只改内存不落盘） | `TestSettingsHotUpdate` 失败 | **拦住**：`envelope on disk = {2m0s 0}, want {3s 2}` |

**真跑取证**（真进程 `:18080` + 真 Redis + `.smoke/config.yaml`：`message_timeout: 3s`、`max_concurrency_per_tenant: 1`；假上游为照事实 #13 写的「排空 body 后挂住」handler，监听 `:9999`）：

| # | 场景 | 实测 | 结果 |
| --- | --- | --- | --- |
| 1 | 装配链 | 启动日志打印 `message_timeout=3s max_concurrency_per_tenant=1` → `config → admin.LimitsFor → gateway` 在**真二进制**里是通的（单测覆盖不到 `main.go`） | **PASS** |
| 2 | **D1** 模型超时 | SSE 收到 `本次回复超时，请稍后重试。`；审计 `inbound 15:44:54.808` → `model_call error_type=timeout 15:44:57.809`，**间隔 3.001s**（= message_timeout）；日志无任何 ERROR/WARN | **PASS** |
| 3 | **D7** 单租户洪峰 | 配额=1、同租户并发 3 条：3 条 `inbound`、**2 条 `throttled`（+12ms / +25ms 立即拒绝，没有排队）**、被放行的那条在 +3.0s 落 `error_type=timeout` | **PASS** |
| 4 | 配额无泄漏 | 洪峰结束后再连发 4 条，**全部被放行未被节流** → 计数正确归还 | **PASS** |
| 5 | 进程未被卡死 | 上述全程后 `/healthz` 200、`/readyz` 200、`-healthcheck` exit 0 | **PASS** |

第 2、3 行即演练矩阵 **D1**（模型响应超时）与 **D7**（单租户洪峰）的核心断言，已在真进程上取证；
dep9 的 `scripts/fault_drill.sh` 只需把这两步脚本化并补齐其余 5 项。

### 4.4 运行时热调参（dep6，2026-09-09 实测）

真进程 `:18080` + 真 Redis + 挂住式假上游，起始 `message_timeout: 30s`（**故意设得比下面的测量值大一个
数量级**：PUT 若不生效，消息会挂满 30s，一眼可辨）。脚本 `.smoke/hot_settings.sh`，**16 条断言全 PASS**：

| # | 场景 | 实测 | 结果 |
| --- | --- | --- | --- |
| 1 | 启动 | `/readyz` 200；启动日志 `message_timeout=30s` | **PASS** |
| 2 | `GET /admin/settings` | `{"message_timeout":"30s","max_concurrency_per_tenant":0}` —— 生效值而非空字段 | **PASS** |
| 3 | `PUT {"message_timeout":"2s",...}` | 200；GET 立即返回 `2s`；磁盘 `message_timeout: 2s` | **PASS** |
| 4 | **调参后发一条消息** | 耗时 **≈2s**（而不是启动值 30s），SSE 收到 `本次回复超时，请稍后重试。` | **PASS** |
| 5 | 再调 `1s` 后发第二条 | 耗时 **≈1s** —— 可重复、双向调 | **PASS** |
| 6 | 全程进程未重启 | PID 始终 `15158`；`/readyz` 仍 200 | **PASS** |
| 7 | `PUT {}` 复位 | GET 回 `2m0s`/`0`，且 `agent:` 整段从配置文件消失（纯默认不落盘） | **PASS** |
| 8 | 审计 | 3 条 `event=admin`（三次成功 PUT）、detail 带 `settings message_timeout=2s max_concurrency_per_tenant=0`；2 条 `error_type=timeout` | **PASS** |

第 4–6 行是「热更新」这个说法的全部证据：`config → admin.commit → Service.Agent() → gateway.LimitsFor`
这条链在真二进制里贯通，**改完不重启**。单测只能证明各段各自成立，证明不了 `main.go` 把它们接对了。

### 4.5 假模型对真平台（dep7 + dep7b，2026-09-09 实测）

三层证据，缺一不可：假模型自己的输出格式（单测）、缺陷修复的门禁（单测 + 变异）、真链路（演练）。
单测只能证明假模型说的 SSE 是对的，**证明不了框架的 openai 客户端认它** —— 后者只有真跑能回答。

**单测**（`cmd/fake-model/main_test.go` 6 个 + `trpcservice/agent/agent_test.go` 5 个，`-race` 全 PASS）：

| 测试 | 钉住的不变量 |
| --- | --- |
| `TestCompletionMatchesThePythonOriginal` | parity 门禁：200 + `text/event-stream`、6 个事件（4 chunk + finish + `[DONE]`）、每 chunk 的 `id/object/model/choices[0].index`、`"finish_reason":null` **必须出现**（不能省）、`内部资料` 在任一 chunk 里都不完整但重组后 == 完整回复、末块 usage 11/7/18 |
| `TestFaultModes`（5 子测试） | 500 带 `"type":"server_error"`；429 带 `Retry-After: 2` + `"type":"rate_limit_error"`；empty 只有 `[DONE]`；slow 首块 <100ms（证明是流式而不是缓冲）且总耗时 ≥ 3×delay；`reset` 后计数器归 1 |
| `TestTimeoutModeObservesCancellation` | 事实 #13 的门禁：delay=5s、客户端 150ms 取消，`srv.Close()`（会等未完成请求）必须 <1s —— 实测 **0.15s** |
| `TestModeEndpointValidation` | 未知 mode / 负 delay / 坏 JSON 一律 400 且**不改变运行中的 mode**；省略 delay 落回模式默认；DELETE/PUT → 405 |
| `TestEmptyUpstreamIsBoundedByTheCallCap` | 事实 #16 的门禁：空上游下调用数 == 上限，且错误点名上限 |
| `TestNonPositiveCapIsCorrectedNotTrusted` | 非正上限被纠正为默认，而不是被透传给框架变成「无上限」 |
| `TestApplyPicksUpANewCap` | `Registry.Apply` 重建 runner 后新上限生效（租户变更不会把它丢回框架默认） |

**变异门禁 M7**：摘掉 `llmagent.WithMaxLLMCalls(maxLLMCalls)` → `TestEmptyUpstreamIsBoundedByTheCallCap`
精确拦住，并把缺陷量化得比 shell 更狠：**一条用户消息 = 5 秒内 28,123 次上游调用（≈5.6k/s），且
`surfaced: ""`** —— 用户端什么错误都看不到，只能等预算耗尽。还原后 `diff` byte-identical。

**真跑演练** `.smoke/fake_model_e2e.sh`（真平台 `:18080` + 真 Redis + 假模型 `:9999`，`message_timeout: 2s`、
`max_llm_calls: 3` —— 故意压到 3，好让「有上限」这个数字一眼可辨）：**51 条断言全 PASS，连跑 3 轮稳定**。

| 模式 | 用户可见 | 留痕 | 上游调用数 | 结论 |
| --- | --- | --- | --- | --- |
| `ok` | 4 个 chunk 逐个到达 + 空 `Done`；耗时 <1s | `prompt_tokens:11, completion_tokens:7` | **1** | 框架的 openai 客户端认这个假模型；一条消息 = 一次调用（D3 的判据） |
| `timeout`（delay 30） | 耗时 **≈2s**（贴着 `message_timeout`，不是 30s）+ `本次回复超时，请稍后重试。` | `error_type=timeout`；假模型日志 `caller gave up` | **1** | D1：预算真的截断了挂死，且上游被真取消（事实 #15） |
| `error500` | 有终态回复，**不是**超时话术 | detail 带原始上游报文 `injected upstream failure`；`error_type=agent` | **3** | D2；3 次是客户端 `MaxRetries:2`（事实 #17），**不是我们的重试** |
| `limit429` | 有终态回复 | 归类 **`timeout`** 而不是 `agent` | **1** | D2；`Retry-After(2s) ≥ message_timeout(2s)`，退避中预算就到期 —— 分类没错（ctx 确实到期）但根因是限流，必须写下来否则读者会以为 429 一定是 `agent` |
| `empty` | 有终态回复 + 错误点名 `max LLM calls (3) exceeded` | `error_type=agent` | **3**（== 上限） | 事实 #16 修复前这里是 **16,588**（≈8.3k/s）；现在被封顶 |
| `slow`（delay 0.3） | ≥0.9s 且 <2.0s，收齐 4 个 chunk，无超时话术 | — | **1** | D7 的延迟面：慢而不坏，流式仍逐块到达 |
| 输出 tripwire | `world, 内部` 已到用户、`资料 leaked.` 被拦、收到 `回复因触发租户安全策略已被截断。` | `"stage":"output","rule":"keyword"` | **1** | 跨 chunk 关键词只有尾窗检查器能抓到 |
| 租户变更后重跑 `empty` | — | — | **3** | `Registry.Apply` 重建 runner 没把上限丢回框架默认 |

四条纪律是被这一轮的失败教出来的，已写进脚本头，dep9 沿用：

1. **两个进程都要过就绪门禁**。上一版只等平台，而新链接的假模型二进制首次执行被 macOS 拖了几秒（实测冷启动
   519ms / 热启动 31ms，但首次链接后首次执行多了 ≈4s），第一条消息打在 connection refused 上，`timeout` 模式根本没被
   设进去 —— 却「通过」了，因为 `ok` 模式的回复也刚好耗时 2s。**没有门禁的断言可以因为完全无关的原因变绿**；
2. **每次注入都带 `reset` 并回读校验**。计数器是模式级的，才能把「一条消息花了几次上游调用」读成数字；
3. **断言 helper 自己先过逐例自检**。这一轮有 4 条红全是 `hasnt` 的两个分支被写反，产品行为完全正确，而它打印的
   还是「文件里出现了 X」这种谎话，现场看着像 WebChat 投递竞态；而自检的第一版又因为「数总数」在 fixture 写不出来时
   凑成 4对/4错 **假绿**，改成逐例签名 `PFFPPFPF` 才真能区分（两个变异 `PPFFPFPF` / `FFPPPFPF` 均被拦下并 exit 1）；
4. **JSON payload 用 heredoc 落文件 + `--data-binary @file`**，不在 `"$( ... )"` 里内联带 `\"` 的 JSON（事实 #18）。
   这一条花了一整轮排查：旧写法 3/3 稳定 400，但隔离测试 20/20 全 200 —— 因为它只在「命令替换作为函数实参」
   这个位置发作。服务端给的错误原文 `cannot unmarshal string into admin.tenantDTO` 是唯一线索，所以响应体必须落盘。

### 4.6 部署产物（dep8，2026-09-09 实测）

三层证据，沿用 §4.5 的纪律（helper 先自检、变异必须证明「改到了」、门禁必须被**该拦的那条**拦住）。

**第一层：真跑取证（Docker daemon 在，Redis 在）**

| 性质 | 手段 | 结果 |
| --- | --- | --- |
| 镜像构建 | `docker build -t trpc-agent-service:local -t :0.1.0 .` | **11.6 MB** scratch，双二进制，UID 65534，`/data` 可写，TLS 根证书在位 |
| `-healthcheck` 自探 | `docker run` | 就绪 exit 0；依赖不可达 exit 1 并打出 503 的原因体 |
| 优雅停机 | `docker stop` 计时 | SIGTERM 后 **5s** 退出，不等在飞 dispatch（事实 #8） |
| 三种挂载形态 | 决定性实验，各起一次栈 | 单文件 **400** / 目录 rw **200** / 目录 ro **400**（事实 #19） |
| 起栈到发消息 | `docker compose up -d` | `/readyz`→`{"status":"ready"}`、`/healthz`→`{"status":"ok"}`、callback **202**、SSE **4 chunk + done**，敏感词「内部资料」跨 chunk 2/3 仍被输出 tripwire 拦下 |
| 审计落卷 | `docker cp tas-app:/data/audit.jsonl` | 命名卷 `trpc-agent-service_audit`，属主 `nobody nogroup`，`down` 之后仍在 |
| 热调参 | `GET/PUT /admin/settings` | PUT **200** 并持久化；落盘后挂载的配置从 **54 行缩到 31 行**（`Save` 抹注释，事实 #19） |

两条写进 README 前被本机证伪的指令：本机**没有 busybox 镜像**（不能拿它窥卷）；macOS 上
`docker volume inspect` 给的 Mountpoint 是 VM 内部路径，宿主机 `ls` 报 No such file。两条都删了，
只保留实测可用的 `docker cp`（scratch 里没有 shell，`compose exec` 什么也跑不了）。

**第二层：离线结构门禁 `scripts/check_deploy.sh`（58 条断言全绿）**

两个意外可用的通道：**`docker compose config --format json` 是纯客户端的**（`DOCKER_HOST=tcp://127.0.0.1:1`
仍 exit 0），所以 daemon 起不来时门禁照样能跑；`kubectl kustomize` 是纯本地的，而
`kubectl apply --dry-run=client --validate=false` **仍要做 API discovery**，无集群时对正确文件也一律报错
（事实 #3 在此更正）。YAML→JSON 只能走 ruby（本机 python3 无 pyyaml、无 yq），于是有了事实 #21。
另一个必需的动作：`docker compose config` **默认省略 profile 服务**，要 `--profile observability` 才带出
collector —— 这本身就是一条值得断言的性质（变异 M10）。

门禁自己的三条纪律：① Section B 一律 `env -u CONFIG_DIR -u APP_PORT …`，因为复用 shell 里导出过的
`CONFIG_DIR` 会让断言打到调用者的环境变量而不是文件的默认值；② Section D 跑 Go 测试并**计数**
（want=7），改名后匹配到 0 个不会静默变绿；③ 两处复合断言加了 **null 前置守卫** —— jq 里 `null.foo`
返回 null 而不报错，`join(" ")` 又把 null 渲染成空串，于是「整个探针块缺失」会伪装成「值差一点」的 FAIL
—— 与 §4.5 纪律 3 里那个「数总数凑成假绿」是同构形状。首跑 2 条 FAIL 全是我自己的错（一条期望值写成
`ok` 而 jq 返回路径，一条 jq 路径漏了 `.securityContext`），第二条正是这个陷阱当场现形。

探针超时那一层还挖出两个真缺陷，都已修：平局（`timeoutSeconds: 3` 与 `web.readyTimeout` 3s 相等，
事实 #22），以及门禁里的 bound 本来是我手打的字面量 3 —— 现在用 `sed` 从 `web.go` 读，常量漂移会当场报红。

**第三层：变异门禁（15/15 被该拦的那条拦住，9/9 文件字节还原）**

每条变异断言三件事：pre-image 出现次数正好等于预期（否则是 no-op，而 no-op「通过」是最坏结果，
因为它看起来像绿门禁）、门禁非零退出、且失败的正是**该拦的那条**（关键字匹配 —— 被无关断言拦住是
运气不是门禁）。首跑 3 条 NO-CHANGE 全是 pre-image 缩进写错，正是这条纪律当场发挥作用（否则会得到
3 个假 CAUGHT）；真实缩进是用 python3 `repr()` 查出来的，macOS 的 `cat -A` 不支持。

| 变异 | 改动 | 被哪条拦住 |
| --- | --- | --- |
| M10 | 删 collector 的 `profiles: [observability]` | 默认栈服务集合 |
| M11 | `/config` 挂载加 `:ro` | 事实 #19 可写目录挂载 |
| M12 | `0.0.0.0:8080` → `:8080` | 事实 #21 裸标量扫描 |
| M13 | 生成器改名 | ConfigMap 悬空引用 |
| M14 | HPA `maxReplicas: 3` | 三处副本数一致（事实 #7） |
| M15 | 删 fake-model 的 `healthcheck: disable: true` | 继承镜像 HEALTHCHECK → 永久 unhealthy |
| M16 | `.dockerignore` 删 `config.yaml` | 密钥不入构建上下文 |
| M17 | `readOnlyRootFilesystem: false` | 安全上下文 |
| M18 | Service `targetPort: httpx` | selector / 端口名交叉引用 |
| M19 / M22 | readiness / startup `timeoutSeconds: 1` | 两条 `> web.readyTimeout` |
| M20 | Ingress backend `name: https` | Ingress 交叉引用 |
| M21 | demo 配置 `backend: memory` | Section D 计数 7→5 |
| M23 | readiness `timeoutSeconds: 5`（= period） | `timeout < period`（事实 #22①） |
| M24 | `web.go` 的 `readyTimeout` 3s→10s | 两条 num_gt 报 `want>'10'`，证明 bound 是运行时从源码读的 |

**没验过的（如实标注，与 `deploy/README.md` 验证清单同源）**：K8s 清单在集群里的实际行为（本机无
kind/minikube/k3d，事实 #3）、otel-collector 真收到 span（镜像拉不到，事实 #1）、事实 #22 的探针计时
（读源码得出，未在集群里量过）、Linux 宿主机上 UID 65534 对挂载目录的写权（本机是 macOS，Docker Desktop
会自动映射，Linux 不会）。故障演练矩阵 D1–D7 见 §4.8（已实测）。

### 4.7 端到端联调（dep9 + dep10 复验，2026-09-10 实测）

`scripts/e2e.sh --strict`：**44 PASS / 0 FAIL / 0 SKIP，`E2E PASS`**。它不动容器，只对一个已在跑的
地址断言；dep10 的复验是**重新建镜像、重新起栈**后再跑一遍（用仓库配置的一次性副本 `.smoke/e2e-cfg/`
+ 追加 telemetry），确认结果不是绑在某个活了四天的旧容器上。

| 面 | 实测 |
| --- | --- |
| A 拓扑与健康 | 租户表含 `demo` / `demo-alt`（D7 的旁观者）、`default_tenant=demo`；`/healthz` → `{"status":"ok"}`、`/readyz` → `{"status":"ready"}`；callback **202** |
| B 正常对话 | 4 个 chunk 逐个到达 + 终态 `"done":true`，耗时 <1s，**一条消息 = 一次上游调用**（D3 的判据） |
| C 输入 guardrail | 用户只收到拦截话术，模型回复没发出，被拦的消息 **0 次上游调用**，审计 `stage=input rule=keyword` |
| D 输出 tripwire | 跳 chunk 关键词：`world, 内部` 已到用户、`资料 leaked.` 被拦、收到 `CutoffText`；还原后第三块重新到达且不再有截断话术 |
| E 全链路 trace_id | 同一 trace_id 串起多行审计（**含不带 `user_id` 的 `model_call` 那行**）；`user_id=tr` 的行只属于 **1** 个 trace_id；32 位十六进制；事件序列 `inbound model_call reply` |

取证的具体 trace_id：`2e7686d609c57823a87d11ca70b375fb`。

**README 的快速开始是逐字跑过的（dep11），不是凭印象写的**：第 3 节那段（`cp` 仓库配置 → 追加
telemetry → `CONFIG_DIR=./.smoke/e2e-cfg docker compose up -d --build` → `e2e.sh --strict`）把副本目录删干净
重来一遍，同样 **44 / 0 / 0**，trace_id `4b31051061447756288f5ffcf8ee5139`（与上表不同 —— 每次运行本就
应该不同）。第 1/2/5 节也逐字验过：`build.sh` rc=0、`start.sh` 后 `/healthz`+`/readyz` 都回 200、`stop.sh`
后端口连不上（curl rc=7）；**不带** `CONFIG_DIR` 的默认栈 `up -d --build` rc=0 且 readyz 就绪；`down`
不带 `-v` 后审计命名卷仍在（实测 **721 行**）—— 这正是「只看本次新增行」那条纪律的承重证据，
没有它，对着跑过多轮的栈再跑会直接拿历史行蒙对。

**证伪（dep9a）**：e2e.sh 先被两个对照实验试过牙齿 —— ① **no-op 变异**：把一条断言的 pre-image
改成文件里不存在的串，验证它真的在改东西而不是恒绿；② **`AUDIT_BASE` 切片承重对照**：把「只看本次
新增行」改成「看全文件」，审计类断言必须变绿得更容易（否则切片根本没承重）。两个实验都按预期变红/变绿。

### 4.8 故障演练矩阵 D1–D7（dep9b / dep9b1a / dep9b2，2026-09-10 实测）

`scripts/fault_drill.sh --strict`：**143 PASS / 0 FAIL / 0 SKIP，`DRILL PASS`，exit 0**，
**连续四遍复现**。最后两遍的断言输出做了逐字 diff：**完全一致**，只有两个 `State.StartedAt`
时间戳不同（它们本来就应该每遍不同，否则「容器没被动过」就是空断言）。
日志里每条 PASS 都带着实测量（`check` 的第四位备注），所以本节引用的数字不是转述，是从日志里取的。

**先量后钉**：每个演练先把可观测量（耗时 / 审计字段 / 指标增量 / 退出码 / 容器状态）逐个量出来，
再写断言（dep9b1），然后修 12 条对不上的（dep9b1a）。其中两条“对不上”是**产品缺陷**，已修：
错误路径根本不留 `reply` 审计行，而 `Send` 失败被记成 `decision=ok`（dep9c，`reply()` 收口）——
现在 D1/D2/D4 各有一条回归断言钉住“失败路径也有 reply 行且如实记 error”。
其余十条是**我把没量过的猜测当成了期望值**，典型的是 D4 的 `detail`：我按 `/readyz` 的措辞写了
`session backend`，而消息路径实际报的是 `check session exists: … pipeline: dial tcp: …`（事实 #30）。

**逐演练实测量（最后一遍）**

| # | 实测量 | 对应验收项 |
| --- | --- | --- |
| D1 | 耗时 **3.12s**（预算 3s，不是上游的 30s）、`latency_ms=2999`、上游 **1** 次（超时不重试）、`error_type=timeout`、`detail=context deadline exceeded`、三行同一 trace_id、`result=error` 指标 +1；**上游自己报了 `caller gave up` +1 而 `nobody cancelled` = 0** | 用户在 3s 级拿到 `TimeoutText`；goroutine 不泄漏用的是因果证据而不是内存曲线：预算耗尽后上游请求**被真取消**（事实 #15） |
| D2 | `error500`：**1.46s** / `latency_ms=1262` / 上游 **3** 次 / `error_type=agent` / detail 保留 `500 Internal Server Error`；`limit429`：**4.18s** / `latency_ms=4018` / 上游 **3** 次 / `error_type=agent` / detail 保留 `429 Too Many Requests` | 3 次是客户端 `MaxRetries:2` 而不是我们的重试（事实 #17）。**429 在这里归 `agent` 而 §4.5 归 `timeout`，两者都对**：归类取决于「`Retry-After` vs `message_timeout`」，这里预算是 30s 所以重试跑完了 |
| D3 | 两次投递都 **202**、上游 **1** 次、用户 **1** 个终态、审计共 **3** 行且只有 **1** 行 `inbound` | 第二次静默 ACK，**连 trace 都不开**（事实 #25 的正面用途） |
| D4 | 宕机前 session 键 **30** 个；`readyTimeout` 从 `web.go` 读出 **3**；`/readyz` **503** 于 **0.23s**（且在兜底预算内、体内**不含** `context deadline`）；宕机期间的消息 **0.43s** 拿到终态（`error_type=runner`、`decision=error`、`latency_ms=232`、三行同一 trace_id）；`/healthz` **不动**；`docker start` 后自愈 **0.09s**、键仍 **30**、上游看到的历史 **4 > 3** | readyz 转 503（编排层据此摘流量）而 liveness 不动 → **不会被无谓重启**；新消息拿错误回复而非挂死；起来后自愈且会话不丢 |
| D5 | `up -d` 退出码 **0**（陷阱，事实 #28）、容器 **`exited 1`**（自己退的不是被杀）、`State.Error` 点名 session 后端且是 **probe** 失败（不是配置解析）、`listening` 日志 **0** 行（端口从未绑定）、启动横幅 **1** 次（进程真跑起来了）、`/healthz` 返回 **`000`**（连不上）、把 URL 改回来就能起 | **fail-fast**，不进半死状态；`up` 的退出码不可用作判据 |
| D6 | 第一轮上游看到 2 条消息 / 只打了 **1** 次；`docker kill` **`exited 137`**；同一容器起回来后 readyz 200、恢复 **0.35s**；启动横幅 **1→2**（确实是新进程）；**假模型的 `StartedAt` 逐字未变**；新进程把宕机前那一轮一起发给上游（`messages=4`）且是本轮的新问题；上游共 **2** 次；用户拿到正常回复 | 「无状态节点 + 共享后端」；热更新**跨了重启**（事实 #23）+ 落盘双证；`Save` 抹注释与省略默认值的代价也钉在这里（事实 #19/#24） |
| D7 | `quota=1` + 慢模型；A 三条都 **202**（限流在异步分发里判）、收到 **2** 条 `ThrottleText` + **3** 个终态、放行的那条拿到正常回复；**B 只有 1 个终态、拿到完整正常回复、一次都没被限流**；上游 `requests=2`（被拒的 2 条一次都没打）；审计 **4** `inbound` / **2** `throttled`（`stage=admission`、`rule=concurrency`、detail 带当时配额）/ **2** `model_call` / **4** `reply`（其中 2 行 `decision=block`）；指标 `demo/throttled` **+2**、`demo/ok` **+1**、**`demo-alt/ok` +1**（旁观租户在指标里也看得见） | 单租户洪峰被配额挡住且**留痕**，旁观租户完全不受影响 |

一个诚实的读数注意：D4 的 session 键数上一遍是 **15**、这一遍是 **30** —— `cleanup()` 用的是
`docker compose down`（不带 `-v`），命名卷留着，所以紧接着的第二遍看到两倍的键。断言故意写 `> 0`
而不钉绝对值，这个数字**不是常量**，不得当常量引用。

**变异证伪（dep9b2，M1–M5：5/5 有牙齿，跑完 sha256 逐字还原）**

纪律同 §4.6：**预测先行**（先写下「哪几条应该红」），再验 pre-image 命中次数（否则是 no-op），
最后对账。5 个变异跑完，`scripts/fault_drill.sh` 的 sha256 与跑前逐字相同（`854ff125…`）。

| 变异 | 改动 | 预测 vs 实测 | 查出什么 |
| --- | --- | --- | --- |
| M1 | `send()` 的 msg_id 退回不唯一 | D4：**26 PASS / 9 FAIL**，撞号守卫打 2 行告警 | **查出脚本自己的一个假绿**：守卫在 `: > "$log"` 之前 return，上一轮的 `Hello` 还躺在 SSE 日志里，「恢复后用户重新拿到正常回复」靠陈旧证据蒙对 → 已把清空移到守卫之前（纪律 4 在 SSE 日志上的同一形状） |
| M2 | D6 两轮之间对 Redis `FLUSHALL`（注入失忆） | **24 / 2**，两条预测全中 | `新进程把宕机前那一轮一起发给上游` want=4 got=2 + 上游日志 `messages=4` 消失 → 会话恢复的断言真在读 Redis |
| M3 | D7 的旁观租户 `BYSTANDER` 改成 `demo`（两个租户折叠） | **21 / 9**，B 相关三条命中 | 隔离断言有牙（B 拿不到完整回复 / B 被限流 1 次 / `requests` 变 1）。**一条落空**：`指标 demo-alt/ok 增 1` 没红 —— 折叠后 `demo/ok` 的 delta 恰好还是 1，蒙对。这不是断言没牙齿而是**变异选错了维度**（它验的是租户隔离，不是标签敏感性），用 M5 补上 |
| M4 | D1 的预算 3s→60s（上游仍挂 30s） | **13 / 9**，四条预测全中 | 耗时 **30.51s**（不再是 3s）、无超时话术、`caller gave up` **+0**、`nobody cancelled` **+1** → 「预算真的截断了挂死」不是恒真断言，而且取消证据会随预算翻转 |
| M5 | 指标过滤键 `tenant=`→`tenant_id=`（8 处） | **44 / 4 / 5**：恰好 4 条红、全是指标断言、0 条「等不到 dump」 | 四条全中且 `got=[0]`，其余 **40** 条一条不红；5 条 SKIP 全是 `--only` 未选中 → 断言真的在读标签，且**标签写错是 FAIL 不是 SKIP**（事实 #27/#31）。顺手查出 `metrics.go` 的 doc 注释写着 `tenant_id` 而代码用 `tenant` —— 注释撒谎，已修 |

**真跑教出来的三个脚本 bug**（均已修，都已进事实清单）：① 上一轮的 msg_id 修复是 **no-op**，因为
`el=$(send …)` 是子 shell（事实 #26）；② `wait_metric()` 的 `for` 少了 `done`，而 `bash -n` 报的是
**下面那个函数的 `}`**，错位两行；③ readyz 的 2.0s 上界是一次幸运样本，第四遍量到 2.65s 就红了
（事实 #29）。另有一条是可读性缺陷：PASS 行本来只打 `yes`，归档时拿不到实测量 → `check` 加了第四位
备注，**日志本身变成证据**，本节的数字就是直接从日志里取的。

**全量门禁（dep11，同一棵树）**：`go build` / `go vet` rc=0；`go test ./...` 全 ok；
`scripts/check_deps.sh` rc=0（**13 项**直接依赖与冻结基线一致）；`scripts/check_deploy.sh`
**58 / 0 / 0**；`format.sh` rc=0（无新改动）；`lint.sh` rc=0 空输出；`coverage.sh` **total 77.6%**。

**没验过的（如实标注）**：① D1 的 goroutine 不泄漏只有**间接**证据（上游观测到取消），部署栈上没有
pprof 可看；② D4 只验到 `/readyz` 转 503，**没验 kubelet 真的摘了 endpoint**（本机无集群，事实 #3）；
③ 演练固定 **1 副本**，所以 `dedup` / `sessionSerializer` 在多副本下仍只在单节点内成立（事实 #7）；
④ 指标是 stdout push，**collector 真收到过 span/metric 未验**（镜像拉不到，事实 #1）。

## 5. 排期与验收

| 时间 | 内容 | 产出 |
| --- | --- | --- |
| 9/9 | go.mod 冻结基线 + 门禁脚本 | 直接依赖 13 项锁定，漂移可检测（§4.1 八条用例全 PASS） |
| 9/9 | 健康检查 + 韧性两项（可配超时、租户配额）+ 运行时热调参 + 单测 | 探针可打（§4.2：单测 14 个 + 真 Redis 停/起 8 条全 PASS）；超时/洪峰有明确行为与留痕（§4.3：单测 10 个 + 6 个变异均被拦 + D1/D7 真进程取证）；`GET/PUT /admin/settings` 改完不重启生效（§4.4：16 条真跑断言全 PASS） |
| 9/9 | Go 版假模型 + 运行时故障注入 + 演练挖出的调用风暴缺陷修复 | `cmd/fake-model` 六模式与 python 原版 parity（§4.5：单测 11 个 + 变异 M7）；真链路 **51 条断言全 PASS ×3 轮**；新增 `agent.max_llm_calls`（默认 8）把「一条消息打 16,588 次上游」封成有界报错（事实 #16） |
| 9/9–9/10 | Dockerfile + Compose + K8s 清单 | 一条命令起栈，零镜像拉取（§4.6：11.6 MB 镜像真构建、compose 真起栈发消息取证、**58 条离线门禁全绿 + 15/15 变异被该拦的那条拦住**）；挖出并修掉四个真缺陷（事实 #19 挂载形态、#20 audit 静默丢、#21 YAML 方言、#22 探针超时平局）；K8s 在集群里的行为与 collector **未验**，已如实标注 |
| 9/10 | 端到端联调 + 7 项故障演练实测 | `e2e.sh --strict` **44 / 0 / 0**（重建镜像重起栈后复验，§4.7，trace_id 取证）；`fault_drill.sh --strict` **143 / 0 / 0 连续四遍**（§4.8，D1–D7 逐演练实测量）；变异 **M1–M5 全有牙齿**并查出 2 个真问题（脚本的陈旧 SSE 日志假绿、`metrics.go` 注释撒谎）；挖出并修掉产品缺陷（错误路径无 reply 审计行、`Send` 失败记成 `decision=ok`） |
| 9/10–9/11 | 全量门禁 + 文档归档 + README 快速开始 | `build`/`vet`/`test` 全绿，`check_deps` 13 项一致，`check_deploy` **58 / 0 / 0**，`format`/`lint` rc=0，`coverage` **77.6%**；事实清单补到 **#32**，新增 §4.7/§4.8 与下表验收对账；README 快速开始 5 节**逐字跑过** |

验收标准（对应方案文档 3.6 / 6.3 / 风险清单）：§2.6 演练矩阵的「期望」列已逐条落到 §4.8 的实测量表。
方案文档 §6「预期效果 1」的验收项对账如下（只记本切片能给的那部分证据，别的切片不冒充）：

| 验收项 | 本切片能给出的证据 | 状态 |
| --- | --- | --- |
| 多租户 | D7 洪峰隔离：配额挡住 A、旁观租户 B 完全不受影响，且指标按 `tenant` 维度可见（§4.8）；e2e A 段租户表含 `demo`/`demo-alt` | 达标（本切片补的是「隔离」这一面） |
| 节点化 | D6：`docker kill`(137) 后同一容器 **0.35s** 恢复、启动横幅 **1→2**、新进程从 Redis 重建对话（`messages=4`）；D4：readyz 503 而 healthz 不动（该摘流量而不是重启） | 达标（**单副本**形态；多副本下 `dedup`/`sessionSerializer` 的限制见事实 #7） |
| 数据同步 | D4：Redis `stop`/`start` 后 session 键仍 **30** 个、上游看到的历史 **4 > 3**；D6：跨进程重建 | 达标（共享后端语义，**不是**多副本双向同步） |
| 多后端（≥3 类） | 仅 Redis（session/event/summary）+ 本地文件（审计 JSONL、配置 YAML）+ 进程内存（memory backend） | **未达标** —— 见下节差异；补齐需新增第三方驱动，与 9/9 冻结直接冲突，**需单独决策** |
| IM 接入（两类差异） | 本切片不涉；e2e/演练走的是 `callback` + `webchat` SSE（确定性镜像适配器） | 由通道切片覆盖（不在本 spec 范围） |
| 全链路 `trace_id` | e2e E 段：同一 trace_id 串起 `inbound model_call reply`（**含不带 `user_id` 的 `model_call` 行**）、32 位十六进制、一个 user 只属一个 trace_id；**D1/D4 的失败路径也是三行同一 trace_id**（断了链就不算全链路） | 达标 |
| ≥8 项风险清单 | 按方案文档 §8 的**原编号**逐条对（不重编、不造映射）：**#1** 重复投递→D3、**#2** 模型超时/限流→D1+D2、**#3** 单租户洪峰→D7、**#4** Session 后端抖动→D4+D5，四条**实测达标**；**#6** 密钥泄漏、**#8** 频控收紧有**部分**证据（D2 实测 `Retry-After` 退避、话术不含内部端点与上游错误串；`.dockerignore` 把 `config.yaml` 挡在构建上下文外，变异 M16 验过）；**#10** token 成本有计量（`trpcservice.model.tokens`）与调用次数上限（事实 #16）但**没有预算上限** | 4 条达标 / 2 条部分 / 1 条有计量无预算；**#5 跨节点并发写同一 session 未验** —— 演练固定 1 副本，而 `sessionSerializer` 是进程内的（事实 #7），多副本下这条缓解不成立；#7 工具滥用、#9 后端迁移不在本切片 |
| 演练新识别的风险（不在原清单里） | 事实 #16：空上游→**无上限调用风暴**（修前一条消息 16,588 次），已用 `agent.max_llm_calls` 封顶并实测；事实 #17：客户端自带重试把上游 5xx **放大 3 倍**，且 `Retry-After` ≥ 预算时归类会变成 `timeout` 而不是 `agent` | 已归档为事实 + 已修（#16）/ 已记录运维后果（#17） |
| 框架复用/平台新增边界 | 事实 #12（框架吞连接错误）/#15（ctx 透传到 HTTP 层）/#16（默认无 `MaxLLMCalls`）/#17（openai-go 自带重试）/#30（两条会话路径不同源）都是这条边界的记录 | 达标 |

### 与方案文档的差异（诚实记录，二期项）

- **Gateway 与 Worker 仍是同一进程**：方案文档 2.3 画的是两个独立部署单元 + RPC 直连，首期是单二进制；
  因此 Compose/K8s 的副本数固定 1，「按 session_id 一致性哈希路由」未实现（进程内锁替代）；
- **HPA 用 CPU 而非「并发 session」自定义指标**：后者要装 prometheus-adapter + 暴露自定义 metric，
  超出本切片；清单里保留 HPA 骨架与注释说明换指标的位置；
- **Session 后端不可用时无补偿队列**：方案文档 3.6 R3 的「降级到只读/本地缓存 + 写操作进补偿队列」
  首期降级为「readyz 摘流量 + 明确错误回复 + 审计留痕」，不静默丢；
- **多后端 ≥3 类未达标**：目前只有 Redis（session/event/summary）+ 本地文件（审计 JSONL、配置 YAML）
  + 进程内存（memory backend）。要按方案文档降级路径补齐 MySQL / 向量库，必须新增第三方驱动，
  **与 9/9 冻结直接冲突** —— 需要单独决策（破例引入第 5 批，或以「Storage Adapter 抽象 + 三实现」论证达标）；
- **`agent` 段无前端页面**：写入面是 `GET/PUT /admin/settings`（§4.4 已真跑取证热调参不重启生效），
  但仓库自带的聊天页只有对话界面，调参仍要靠 curl / 容器里的 `AGENT_*` 环境变量；
  一个极简的设置页属于二期；
- **审计文件无轮转**、dedup 未换 Redis SETNX、微信客服 cursor 仍是内存态（沿用治理切片的已知限制）；
- **`max_llm_calls` 默认 8 是经验值**：依据是「无工具问答恰好 1 次，其余留给工具循环」，**尚未在真实多工具
  场景下标定**；当前平台未接工具，8 相当于 8 倍余量。真接上工具后应按实际循环深度重算（可热调，不必重启）；
- **假模型有两份**（Go + Python），行为一致但需同步维护，二期收敛为 Go 版。
