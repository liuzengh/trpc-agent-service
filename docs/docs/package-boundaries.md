# app、agent 与 runtime 包边界

本页是 `trpcservice/app`、`trpcservice/agent` 和
`trpcservice/runtime` 的实现契约。它把当前已经落地的职责和演进中必须遵守的依赖方向写清楚，
避免“包名正确但职责继续漂移”。

本页不新增 `control-plane` domain，也不要求把控制面代码搬到新的顶层
包。控制面是系统语义；应用与版本领域仍由 `app` 负责，组合装配和测试
辅助代码可以继续留在内部实现区域。

## 总体方向

```text
外部协议 / HTTP
        |
        v
Gateway -------> runtime: Plan、执行协调、租约和调度
                   |
                   v
                 agent: tRPC-Agent-Go Agent / Runner 组装
                   |
                   v
             tRPC-Agent-Go

bootstrap 负责把 app、agent、runtime、storage 和 Gateway 的具体实现组合起来。
```

这里的箭头表示调用和依赖方向，不表示所有实现必须位于不同进程。单进程
部署仍然必须保留相同的生命周期和所有权边界。

## 包职责

| 包 | 拥有的概念 | 不应拥有的概念 |
| --- | --- | --- |
| `trpcservice/app` | Agent App、不可变 Revision、发布/回滚生命周期、领域校验和 Repository 契约 | Runner、Agent 组装、执行调度、队列、Outbox、运行时存储实现 |
| `trpcservice/agent` | tRPC-Agent-Go 的 Agent/Runner/Session 适配、Agent execution snapshot、Runner 构造和租户能力绑定 | ExecutionPlan 解析、租约/队列调度、回复投递、数据迁移、App/Revision 生命周期变更 |
| `trpcservice/runtime` | ExecutionPlan、配置快照组合、PlanResolver、Runner Registry、执行协调和内部调度 | App/Revision 领域生命周期、具体 `llmagent`/Runner 组装、协议适配和渠道回复 |
| `trpcservice/channels` | Channel Binding、候选验证、协议中立的 ingress/adapter 契约 | Outbox Provider、回复物化、Gateway 事件渲染 |
| `trpcservice/channels/provider` | 组合时的租户渠道 Provider 注册和 Outbox Provider 适配 | Binding 领域生命周期和执行调度 |
| `trpcservice/gateway/replies` | 将 Gateway 事件流渲染成安全的协议中立回复 | Channel Binding 生命周期和 Provider 投递 |
| `trpcservice/agent/runnerfactory` | 把完整 ExecutionPlan 的工厂输入接到 Agent Runner 组装，并注入 runtime-owned Model/Storage materializer | Runner 缓存、租约、领域 Profile 持久化 |
| `trpcservice/runtime/model` | SecretResolver、ModelProviderRegistry 和 ModelFactory 的运行时物化 | Model Profile 持久化、配置生命周期和带凭据的计划状态 |
| `trpcservice/runtime/storage` | 租户范围内的 Session、Event、Memory、Artifact 等能力契约及其后端适配 | 选择执行租户、解析 Plan、驱动 Runner、回复发送策略 |
| `trpcservice/runtime/storage/factory` | Backend ProviderRegistry、StorageFactory、CapabilitySet 及 capability 生命周期 | Backend Profile 领域校验和持久化 |
| `trpcservice/outbox` | 回复物化、发送、重试和死信边界 | Agent 编排、控制面配置和执行调度 |
| `trpcservice/internal/migration` | 后端迁移、双写、校验和切换工具 | 在线执行、Runner 生命周期和请求路由 |
| `trpcservice/gateway` | 可信身份建立、协议中立的请求/事件转换、入口幂等和调用 runtime | Agent/Runner 的具体构造、控制面领域变更 |
| `trpcservice/bootstrap` | 具体 Repository、Provider、Registry、Dispatcher 和 Worker 的组合装配 | 业务领域规则和新的跨层抽象 |

`app` 是最稳定的领域底座；`agent` 是平台与上游 Agent 框架之间的适配
边界；`runtime` 使用这些能力完成一次服务内部执行，但不反向拥有上游
Agent 的实现。

`runtime/model` 与 `runtime/storage/factory` 虽然位于 runtime 目录下，属于
运行时物化的叶子适配器，不属于调度核心：它们只接收无密钥的 Profile 输入，
在 `agent/runnerfactory` 组装时创建 Model/Storage capability。这样保留了
现有导出路径和兼容调用，同时让 `runtime/runner`、`runtime/execution` 不再
直接依赖上游 Agent-Go 的实现；物化实现保持在这两个叶子包和 runnerfactory 的所有权边界内。

`runtime/storage` 的基础持久化契约已经按能力拆成
`SessionStateStore`、`EventHistoryStore`、`MessageStore` 和 `ReplyStore`。
存储后端可以在私有的环境 composition 中同时持有多项能力，用于创建后端 provider，
但 `runtime/storage` 不再导出聚合存储接口。Bootstrap 公共 Config、Gateway 和
Outbox 都只接受自己需要的最窄接口，不再从聚合存储隐式探测能力。

`trpcservice/outbox` 的 Worker 现在分别接收 `ReplyStore` 和
`MessageStore`：前者拥有回复分片的 claim/transition，后者只负责所有分片
投递完成后的 inbound message 状态推进。Worker 不再从一个聚合存储中探测
隐式能力；组合根必须显式注入两个能力。

事件渲染所有权在 `trpcservice/gateway/replies`。同样，Channel Provider
Registry 的组合实现位于 `trpcservice/channels/provider`，Binding 根包不再承载
投递注册表。

## 允许的依赖

当前实现和目标演进遵守以下规则：

1. `app` 不依赖 `agent`、`runtime`、Gateway 或运行时存储。App/Revision
   的领域不变量不能由执行路径反向定义。
2. `agent` 可以消费 `app` 的不可变快照，以及 Backend、Model、Tool 和
   Tenant 的能力；上游 `trpc-agent-go` 的 Agent、Runner、Session 类型和
   组装逻辑由 `agent` 持有。`agent/runnerfactory` 作为组合适配层接入
   runtime-owned 的 Model/Storage 物化实现。
3. `runtime` 可以消费 `app` 和 `agent` 提供的快照/工厂输入契约，并持有
   Plan、调度和执行协调；具体 Agent/Runner 的构造必须通过 `agent` 的
   边界完成。
4. `runtime/runner` 只持有通用 Runner Factory、Registry、lease、失效和关闭
   逻辑，不依赖具体 Agent、Model 或 Storage 实现。`runtime/execution` 只消费
   `agent.RunnerEvent` 这样的服务内事件契约；上游事件类型、错误脱敏和 source
   channel 的 bounded drain 全部由 `agent` 适配边界拥有。这里不得组装
   `llmagent`、模型 Provider 或 Session 实现。
5. `runtime/model` 和 `runtime/storage/factory` 负责运行时物化；它们只消费
   `model`/`backend` 的无密钥契约，不把物化实现放回领域包。
6. `runtime/storage` 是 runtime 的持久化子边界；`trpcservice/outbox` 和
   `trpcservice/internal/migration` 是服务级的交付/迁移边界。它们可以提供
   能力给调用方，但不能把调度、认证或控制面生命周期带回存储实现。
7. `bootstrap` 是具体实现的组合根。新的跨包依赖优先在组合根注入，
   不通过全局变量、隐式 Context 值或跨层反向调用建立。
8. 新增接口应放在实际消费者所属的包；只有同一契约确实被多个独立
   消费者共享时，才考虑建立中立的能力包。

## 当前边界说明

`trpcservice/agent/sessionstore` 消费
`trpcservice/storage/session` 提供的中立 Session 持久化契约，把上游
Session 行为接到租户范围的持久化能力。具体存储实现直接实现该契约；Session
适配器不反向依赖 runtime 调度或 runtime 存储包。

同样，`runtime` 读取 `agent` 的 execution snapshot 和 factory input，以便为完整 Plan
建立 Runner 缓存键。这是 runtime 消费 agent 契约，不是 runtime 重新实现 Agent；中立
capability contract 只在存在多个独立消费者时作为共享边界。

## 一次执行的所有权

```text
Gateway
  - 验证外部身份并建立可信请求
  - 调用 runtime 解析固定 ExecutionPlan
  - 把已接受的请求交给 runtime execution

runtime
  - 按固定 Plan 获取 Runner lease
  - 驱动一次执行并负责取消、lease、协议中立的执行事件流和 bounded drain
  - 通过 agent 的中立事件契约消费 Runner，不接触上游事件对象

agent/runnerfactory
  - 从 Plan 投影 Model/Storage factory input
  - 调用 runtime-owned materializer 后交给 agent 组装

agent
  - 根据固定输入构造或复用上游 Agent / Runner
  - 绑定租户范围的 Model、Tool、Session 和 Storage 能力
  - 调用上游 Runner，归一化事件和错误，并在取消/终止时有界排空 upstream source

runtime/storage 与 trpcservice/outbox
  - 分别负责持久化能力和回复交付
  - 不拥有 Gateway 的协议输出或 Runner 的生命周期
```

异步执行中，创建资源的一方必须明确关闭责任：runtime execution 关闭
自己的执行事件流和 Runner lease，Gateway 关闭对外的 Dispatch 事件流，
Outbox 负责回复投递资源。Context 始终由调用链显式传递，不存储在长期
对象中。

## 包边界维护规则

- queue 是 runtime 的独立、显式组合边界：Bootstrap 可选地接管
  `runtime/queue.Worker` 的生命周期。`trpcservice/internal/migration` 的阶段状态
  通过 `StateStore` 注入，不再由 Tool 自己保存进程内 map；它不会被默认塞进同步
  Gateway 请求路径，也不属于 runtime 调度包。
- 移动代码时优先移动所有权和测试，不为了包名创建重复类型或兼容层。
- 任何跨租户存储能力都必须携带显式 `tenant_id`；字符串前缀不能替代
  授权和数据隔离。
- 任何影响导出 API、持久化格式、事件顺序、取消或关闭语义的变化，都
  必须单独说明兼容性，不作为目录整理的附带结果。
- 每个包边界变更都包含与边界相关的契约测试，并验证
  `go test ./...`、Race（如果涉及并发）和 `git diff --check`。

本页描述的是包的责任；目录移动保持小步、可独立验证和可回滚，并在 tracker #130 中链接对应实现。
