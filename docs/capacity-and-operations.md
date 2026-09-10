# 企业级系统容量评估、故障恢复与高可用运维规范
# (Enterprise Capacity Sizing, Fault Recovery & Operations Guide)

本文档针对 `README.md` 与 `complete-development-plan.md` 中关于系统稳定性、协程防泄漏、容量评估、多级容灾降级、灰度发布与生产部署的严苛要求，提供企业级落地的计算模型、容灾时序、架构拓扑与应急 Runbook。

---

## 一、 系统容量评估与基准计算模型 (Capacity & Sizing Model)

### 1.1 单节点并发 Session 安全承载模型

在 Agent 服务中，每个长连接或流式会话（Session）都会在内存中占用协程（Goroutine）、上下文缓冲区、SSE 连接以及数据库/Redis 连接池配额。

$$N_{\text{max\_sessions}} = \min \left( \frac{M_{\text{available}} - M_{\text{base}}}{M_{\text{session}}}, \frac{FD_{\text{available}}}{FD_{\text{session}}}, \frac{Pool_{\text{DB}} \times K_{\text{reuse}}}{QPS_{\text{session}}} \right)$$

#### 核心资源开销基线表（基于 4 核 CPU / 8GB 内存单节点 Worker）

| 资源维度 | 单节点上限 / 配置 | 单 Session 平均消耗 | 安全并发会话数 ($N_{\text{safe}}$) | 瓶颈说明 |
| :--- | :--- | :--- | :--- | :--- |
| **内存 (RAM)** | 8 GB（服务预留 6 GB） | ~4.5 MB（含 Prompt 历史缓存、SSE Buffer、Trace） | **800 ~ 1,200 会话** | LLM 上下文（如 32k tokens）较大时增加 GC 压力 |
| **Goroutines** | ~50,000 | 3 个（HTTP/SSE 连接、Worker 监听、Drain 协程） | **10,000+** | Go 轻量协程调度（每个 ~2-4 KB），非主要瓶颈 |
| **文件描述符 (FD)** | 65,535 (ulimit -n) | 2 ~ 3 (TCP socket + DB conn + Redis conn) | **15,000+** | Linux 内核网络句柄 |
| **数据库连接池** | `max_open_conns = 100` | 事务持有时间 ~30ms，时分复用 | **500 ~ 800 并发执行** | 数据库连接池是最严苛的物理瓶颈 |

> **容量评估结论**：
> - 单台 **4C8G** 规格节点安全承载 **500 ~ 800 个活跃并发执行 Session**。
> - 在 10,000 并发在线用户场景下，假设 5% 的用户在同一秒内发起请求（即 500 QPS 活跃），生产环境推荐部署 **3 ~ 5 个 Worker 副本**以实现 N+2 冗余。

---

### 1.2 Token 吞吐换算与外部模型供应商配额瓶颈

Agent 服务的大模型推理通常受限于外部供应商的 **RPM (Requests Per Minute)** 与 **TPM (Tokens Per Minute)** 配额限制。

- **单次交互平均输入 Token** ($T_{\text{in}}$)：2,000 tokens（含系统 Prompt、Few-shots、知识库召回文本、最近历史会话）
- **单次交互平均输出 Token** ($T_{\text{out}}$)：500 tokens
- **单次会话总耗时** ($t_{\text{gen}}$)：模型流式输出 500 tokens 通常耗时 5 ~ 15 秒（首字延迟 TTFT ~800ms，生成速率 ~40-60 tokens/s）。

#### 吞吐换算关系式
$$\text{Required TPM} = QPS_{\text{active}} \times 60 \times (T_{\text{in}} + T_{\text{out}})$$
$$\text{Cost per Million Invocations} = 10^6 \times \left( \frac{T_{\text{in}}}{10^6} \times P_{\text{in}} + \frac{T_{\text{out}}}{10^6} \times P_{\text{out}} \right)$$

*示例*：当系统面临 **100 QPS** 活跃并发时：
- 每分钟需要消耗：$100 \times 60 \times 2,500 = 15,000,000\text{ TPM}$ (15M TPM)。
- 若供应商单一账号 TPM 配额为 2M TPM，则必须在平台层配置**多账号轮询**或在 `ModelConfig` 中配置**跨供应商 Failover 候选模型（如 OpenAI 主模型 + 混元/DeepSeek 备用模型）**，否则将引发 429 Rate Limit。

---

### 1.3 数据库与 Redis QPS 放大模型（1 : 5 放大比）

用户端发送 1 条 IM 消息到达平台后，全链路将产生多级持久化与状态 I/O 操作：

```mermaid
sequenceDiagram
    autonumber
    participant IM as IM 平台 (企微/TG)
    participant GW as Gateway / Connector
    participant KF as Kafka
    participant WK as Worker
    participant RD as Redis (分布式租约)
    participant DB as PostgreSQL (状态/Outbox)
    participant RAG as Vector DB (知识库)

    IM->>GW: 1. Connector 接收并规范化消息
    GW->>KF: 2. Produce Inbound Envelope
    KF->>WK: 3. Consume Envelope
    WK->>RD: I/O #1: Acquire Session Lease (SET NX PX)
    WK->>DB: I/O #2: Read Session State & Inbound Deduplication
    WK->>RAG: I/O #3: Vector Embedding & Similarity Search
    WK->>WK: Execute Model & Tools
    WK->>DB: I/O #4: Write Agent Execution & Outbox Entry (TX)
    WK->>DB: I/O #5: Deliver Outbox Message & Update State
    WK->>RD: Release / Refresh Lease
```

#### QPS 放大放大推导：
$$QPS_{\text{Redis}} = 2 \times QPS_{\text{Inbound}} \quad (\text{Lease Acquire + Heartbeat/Release})$$
$$QPS_{\text{Postgres}} \approx 3 \sim 4 \times QPS_{\text{Inbound}} \quad (\text{Deduplication Check + Commit State & Outbox + Complete Outbox})$$

> **架构含义**：
> 当外部流量达到 **1,000 QPS** 峰值时，PostgreSQL 必须承受 **3,000 ~ 4,000 QPS** 的写入与事务提交。因此，必须保持连接池优化（PgBouncer）、Outbox 表分区归档以及 Redis 分布式租约的轻量化。

---

### 1.4 Kafka 削峰与 Connector 背压

外部 IM 由常驻长连接/长轮询 Connector 接入，不再假设 HTTP Webhook 的固定响应时限或未经实测的单节点 QPS。Gateway 只完成协议标准化、canonical Session 解析和 Kafka 发布；Kafka 按 `session_key` 分区吸收短时突发，Worker 消费速率由真实模型/工具容量决定。Telegram 只有在消息成功持久发布或明确忽略后才推进 `offset`；企微/飞书长连接发布失败时持续退避重试。容量值必须通过压测得到，不能从架构图直接推导生产数字。

---

## 二、 全链路多级容灾与降级矩阵 (Disaster Recovery & Degradation Matrix)

| 故障场景 | 影响范围 | 检测机制 | 自动降级与恢复策略 | 数据一致性保障 |
| :--- | :--- | :--- | :--- | :--- |
| **Worker 节点突发宕机** | 该节点上正在执行的模型调用中断 | Redis 租约心跳超时（默认 10s）；Kafka 消费者心跳丢失触发 Rebalance | 1. 租约过期被释放；<br>2. Kafka 将未 Commit 消息重新派发给健康节点；<br>3. 新节点接管重试。 | PostgreSQL 状态机使用 `claim_owner` 乐观并发控制，未 Commit 事务全量回滚，无残留脏数据。 |
| **IM 平台重复推送 / 网络超时重试** | 相同 `message_id` 的请求被多次发送 | 入口 `inbound.MessageID` 校验与数据库唯一约束判定 | 1. 内存层 `historyInbounds` 快速命中过滤；<br>2. 数据库 `claims` 表主键唯一冲突捕获；<br>3. 识别为重复消息后直接返回 200 OK，丢弃重复事件。 | 严格保证每条消息仅被 Agent 执行一次（Exactly-once processing semantic per message ID）。 |
| **PostgreSQL 数据库短暂不可用 (闪断 / 主从切换)** | 租户配置读取失败、状态无法持久化 | 连接池报网络错误 / `connection refused`；K8s Readiness 探针失败 | 1. K8s Readiness 探针失败，自动从 Service Endpoints 切除 Pod 流量；<br>2. Worker 端执行指数退避重试（Backoff: 1s, 2s, 4s）；<br>3. Kafka 消息不 Commit，待数据库恢复后自动继续消费。 | Kafka 游标未前移，不会造成任何业务消息丢失。 |
| **主模型服务超时 / 429 限流 / 5xx 故障** | Agent 无法生成回复 | HTTP 返回 429/500/504 或 context 超时（如 30s） | 自动触发 `FailoverCandidates` 候选列表转移：<br>`openai-primary` (429) $\rightarrow$ 自动转由 `hunyuan-backup` 接管生成。 | 运行时透明切换，Trace 记录实际生效的 Provider 与 Model，消耗配额计入对应账单。 |
| **外部工具调用超时或持续报错** | Agent 陷入死循环调用失败工具 | 治理拦截器 `governance.max_tool_calls`（默认 8 次） | 1. 单工具设置 5s 上下文超时；<br>2. 若工具执行连续出错且达到 `max_tool_calls`，强行截断工具循环；<br>3. 模型基于已有上下文给出降级兜底解释。 | 避免工具无限消耗 LLM Token 预算。 |
| **Runner 事件流卡顿与超时** | 协程长期挂死未释放 | 上下文 `ctx.Done()` 触发 | `collectRunWithContext` 立即中断主流程，后台独立协程排空剩余通道（Drain），确保通道关闭并销毁协程。 | **杜绝 Goroutine 泄漏**（零泄漏不变量）。 |

---

## 三、 Go 运行时生命周期管理与通道防泄漏机制 (Go Concurrency & Leak Prevention)

在 Go 语言实现的高并发 Agent 系统中，**Goroutine 泄漏**与**未排空 Channel 阻塞**是导致内存耗尽（OOM）的主要根源。

### 3.1 核心泄漏风险分析
底层的 Agent 框架（如 `tRPC-Agent-Go`）通过 Channel 将事件流推给上层：
```go
events, err := tenantRunner.Run(runContext, ...)
```
若主协程在遇到以下情况时直接 `break` 或 `return`：
1. 上游 HTTP 请求超时，`runContext.Done()` 触发；
2. 收到终端错误 `IsTerminalError()`；
3. 收到结束标志 `IsRunnerCompletion()`。

**若此时底层 Producer 协程仍在尝试向 Channel 写入后续事件，而读取方已退出且无人消费，Producer 协程将永久阻塞挂起，引发严重内存泄漏！**

### 3.2 企业级双保险排空实现（已在 `runtime.go` 落地）

```go
func collectRunWithContext(ctx context.Context, events <-chan *event.Event, onDelta func(string)) runOutcome {
	// ... 变量初始化 ...
	drained := false
	defer func() {
		// 无论何种路径（正常结束、异常退出、panic、超时取消），未排空则启动后台排空
		if !drained {
			drainEvents(events)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			// 1. 上下文超时取消：主协程安全退出，defer 触发 drainEvents 释放发送端
			return runOutcome{usage: usage, trace: frameworkTrace, err: ctx.Err()}
		case agentEvent, ok := <-events:
			if !ok {
				drained = true
				goto done
			}
			// ... 业务处理 ...
			if agentEvent.IsRunnerCompletion() {
				frameworkTrace = agentEvent.ExecutionTrace
				goto done // defer 触发 drainEvents，异步吸收尾随事件直到 close
			}
		}
	}
done:
	// ...
}

func drainEvents(events <-chan *event.Event) {
	if events == nil {
		return
	}
	go func() {
		for range events {
			// 静默消耗所有残留事件，确保底层写入协程顺利退出
		}
	}()
}
```

---

## 四、 灰度发布策略与配置版本回滚 (Canary Release & Rollback)

### 4.1 灰度路由架构与 INV-03 不变式

根据工程约束 **INV-03（进队时版本绑定）**：
> 任何在途执行的消息信封必须打上确定性的 `config_version`。执行 Worker 绝不能在消息处理中途去读取动态变更的配置，否则会导致同一个多轮会话前半句用 Prompt A，后半句用 Prompt B 的混乱状态。

```mermaid
flowchart TD
    Inbound[收到渠道消息] --> Resolve[ResolveBinding 获取当前 ActiveVersion]
    Resolve --> CheckCanary{存在 CanaryPolicy?}
    CheckCanary -- 否 --> UseActive[Envelope.ConfigVersion = ActiveVersion]
    CheckCanary -- 是 --> CheckWhitelist{匹配用户白名单?}
    CheckWhitelist -- 是 --> UseCanary[Envelope.ConfigVersion = CanaryVersion]
    CheckWhitelist -- 否 --> CheckChannel{匹配灰度渠道?}
    CheckChannel -- 否 --> UseActive
    CheckChannel -- 是 --> CalcHash[FNV-1a Hash(conversation_id) % 100]
    CalcHash --> CheckWeight{Hash < WeightPercent?}
    CheckWeight -- 是 --> UseCanary
    CheckWeight -- 否 --> UseActive
    UseActive --> Stamp[永久密封入 Kafka Envelope]
    UseCanary --> Stamp
    Stamp --> Kafka[(Kafka Inbound Topic)]
    Kafka --> Worker[Worker 严格按 Envelope.ConfigVersion 加载只读快照执行]
```

### 4.2 确定性哈希与会话粘性保障
- **Hash 算法**：采用 32-bit FNV-1a 算法对 `conversation_id` 进行哈希，取模 100。
- **会话粘性**：同一个会话的所有后续对话其 Hash 值完全固定，只要灰度比例不调小，该用户在整个会话周期内永远稳定路由在 Canary 版本上，彻底杜绝版本抖动。

### 4.3 租户级配置版本回滚 SOP

当新发布的配置（例如修改了 Prompt 导致机器人输出异常、或修改了工具权限导致报错）需要紧急止损时：

1. **一键停止灰度（秒级生效）**：
   在控制台 Bot 配置页面，将「启用灰度发布分流」开关关闭（或调用 API 将 `canary` 置空）。
   - Ingress 入口立即停止分流，新进消息全部走基线稳定版本；
   - 之前已进入 Kafka 队列的在途消息继续以标记的 Canary 跑完，状态完全可溯源。
2. **基线版本物理回滚（版本单调递增激活）**：
   - 打开「版本历史 (Version History)」面板；
   - 选择历史上验证完好的稳定版本（例如 v2），点击「回滚至此版本」；
   - 控制面将以全新版本号（例如 v4，内容与 v2 完全一致）原子写入 `application_configs`，并更新 `applications.active_config_version = 4`；
   - PostgreSQL 事务提交同时向 Outbox 写入 `ConfigCacheInvalidation` 事件，Redis 缓存秒级失效更新。

---

## 五、 部署方案与生产高可用拓扑 (Deployment Topologies)

### 5.1 最小可运行部署（Docker Compose 开发者模式）

适用于本地联调、CI 自动化测试与单机快速演示环境。

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:15-alpine
    environment:
      POSTGRES_DB: agent_platform
      POSTGRES_USER: platform_admin
      POSTGRES_PASSWORD: admin_password
    ports:
      - "5432:5432"
    volumes:
      - pg_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U platform_admin -d agent_platform"]
      interval: 5s
      timeout: 3s
      retries: 5

  redis:
    image: redis:7-alpine
    command: ["redis-server", "--appendonly", "yes"]
    ports:
      - "6379:6379"

  kafka:
    image: bitnami/kafka:3.5
    ports:
      - "9092:9092"
    environment:
      - KAFKA_CFG_NODE_ID=0
      - KAFKA_CFG_PROCESS_ROLES=controller,broker
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093
      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      - KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@kafka:9093
      - KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER

  trpc-service:
    build:
      context: .
      dockerfile: Dockerfile
    ports:
      - "8080:8080"
    environment:
      - CONFIG_PATH=/app/configs/trpc-agent-service.json
      - POSTGRES_DSN=postgres://platform_admin:admin_password@postgres:5432/agent_platform?sslmode=disable
      - REDIS_ADDR=redis:6379
      - KAFKA_BROKERS=kafka:9092
    depends_on:
      postgres:
        condition: service_healthy

volumes:
  pg_data:
```

---

### 5.2 生产推荐高可用拓扑（Multi-AZ Kubernetes Topology）

在企业生产级环境下，系统采用多可用区（AZ1、AZ2、AZ3）对等容灾部署：

```
                              [ Cloud CDN / WAF ]
                                      │
                         [ Multi-AZ ALB / NLB ]
                                      │
              ┌───────────────────────┴───────────────────────┐
              ▼                                               ▼
     [ AZ-1 K8s Ingress ]                           [ AZ-2 K8s Ingress ]
              │                                               │
    ┌─────────┴─────────┐                           ┌─────────┴─────────┐
    ▼                   ▼                           ▼                   ▼
[ Pod: Gateway-1 ]  [ Pod: Worker-1 ]           [ Pod: Gateway-2 ]  [ Pod: Worker-2 ]
 (Connector Leader)  (Agent Runner)              (Gateway Standby)   (Agent Runner)
    │                   │                           │                   │
    └───────────────────┼───────────────────────────┼───────────────────┘
                        ▼                           ▼
          ═════════════════════════════════════════════════
          [ 云托管 Kafka 集群 (3 AZ 对等分布，Replication=3) ]
          ═════════════════════════════════════════════════
                        │                           │
                        ▼                           ▼
          ═════════════════════════════════════════════════
          [ 云托管 Redis Cluster (主备跨 AZ 自动切换) ]
          ═════════════════════════════════════════════════
                        │                           │
                        ▼                           ▼
          ═════════════════════════════════════════════════
          [ 云托管 PostgreSQL HA (Primary-Standby Multi-AZ) ]
          ═════════════════════════════════════════════════
```

#### 关键生产配置规范：
1. **Pod 拓扑反亲和性 (PodAntiAffinity)**：
   确保同一微服务（Gateway 或 Worker）的各 Pod 必须打散调度至不同的物理 Node 与可用区（`topologyKey: topology.kubernetes.io/zone`）。
2. **水平自动扩缩容 (HPA)**：
   - Gateway 基于 CPU 利用率（> 70%）或 HTTP Inbound QPS 扩缩容；
   - Worker 挂载自定义指标（Kafka Topic Lag 延迟消费深度）进行自动扩容。
3. **健康检查探针 (Probes)**：
   - **Liveness**：`/healthz` 检查进程主循环；
   - **Readiness**：`/readyz` 检查数据库连接、Redis Ping 及 Kafka Producer 联通性，任何依赖闪断立刻隔离流量，杜绝产生 502。
4. **优雅终止 (Graceful Shutdown)**：
   - Kubernetes 调度 `preStop` 钩子停止接受新请求；
   - 服务内部触发 `context.Context` 取消，等待当前执行中的 Agent 完成 Outbox 提交，最长优雅等待时间设为 30s。
