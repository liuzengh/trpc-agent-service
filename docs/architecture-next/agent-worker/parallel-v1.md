# Parallel：SDK 并发分支与显式后继汇总

## 既有契约与装配

沿用 AgentSpec 的 `kind: parallel` / `children`，以及外层 `sequence`。Profile
仍绑定已有模型、工具和数据资源，Control 发布不可变 Manifest；不引入新 DSL、
队列、调度平台、累计 Token 预算或隐式汇总节点。

最小结构为 `sequence[parallel[research_a,research_b],aggregator]`。Worker Reader
保留原 children 顺序；Factory 一次解析全树最小凭据闭包，各 LLM 分别装配显式模型、
MCP、Knowledge、Memory、Artifact 与 Summary 消费选项。引用共享资源不授予兄弟节点
工具权限。共享静态门禁保留单父树、全可达、无环、最多 128 节点、深度 16、组合节点
1–64 个唯一 child 等 AgentSpec 既有约束。

实际执行使用 SDK v1.11.2 `parallelagent.New`，外层仍使用 `chainagent.New`。
整条执行只有一个平台 Run、一个根 Runner 和一个正式 Session overlay。SDK 决定
分支调度与事件上下文；Worker 不复制一套并发执行循环。

## Final 与上下文

静态终端路径单独检查：LLM 即终端，Sequence 沿最后 child 继续；若到达 Parallel，
必须拒绝发布与运行，提示用户显式增加后继 LLM。不能把“最后完成的分支”作为 Final，
也不能以 DFS 最后一个 LLM 冒充最终结果。Control 沿既有公开诊断返回：

```text
terminal parallel node "fanout" requires an explicit successor llm in sequence
```

现有 Web 诊断面板显示该原始原因，不改写用户的树。显式 aggregator 的实际模型请求
消费前序分支输出；中间分支输出保存为 Session 事件，但只有终端 LLM 的完整非工具
assistant 输出可以成为 Final。整个 Runner 成功结束才导出候选并走原 Stage → Complete。

SDK 分支上下文隔离不等于数据资源隔离：并行模型历史不读取兄弟的新增输出，后继节点
可以读取全部前序结果；显式共享的 MemoryAttempt 和 Artifact 服务仍为整 Run 共享。
本包不新增跨后端事务、回滚或确定性的并发写入顺序。Memory 的锁保护与 Artifact 的
同名文件版本行锁沿用原实现；本包模型/MCP 并发验收不等于 Memory/Artifact/Knowledge
并行后端验收。Memory 沿原正式接受协议应用；
Artifact/MCP 外部副作用沿原服务边界，失败 Run 不代表外部操作自动撤销。

## Invocation 与取消生命周期

组合节点沿用公开 `Invocation.Clone` 接缝：使用 SDK 新 ID 与 parent 关系，保留原
Session、Branch、通知服务、取消/deadline 与 trace context，不浅拷贝锁或伪造身份。

实测分支错误可能让 Runner 根流先结束，而兄弟 SDK 工具已收到取消但还未返回。
薄包装器因此跟踪、排空真实 SDK composite event stream；Executor 返回前取消 Run，
并在既有 `DrainTimeout` 内等待这些流结束，然后释放共享 Attempt 服务。超时清空结果、
返回 `ErrDrain`，不产生成功 Final。忽略取消的 Go 工具不会被强制终止；这不是新调度器。

## 发布合同与验证入口

固定 `worker_plan_contract=llm-sequence-parallel-tree-v1`，历史 platform-v1 digest
不变；Control 与 Worker 按实际固定 endpoint hosts 重算一致 release pin，不动态信任
旧 Sequence digest，也不改写已发布 Manifest。

```sh
GOCACHE=/tmp/trpc-worker-go-cache go test -race \
  ./api/schemas/deployment/v1 \
  ./services/control-api/internal/deployment/domain \
  ./services/agent-worker/internal/execution/adapter/outbound/manifestadapter \
  ./services/agent-worker/internal/execution/adapter/outbound/runtimeadapter \
  ./services/agent-worker/internal/execution/adapter/outbound/trpcagent

python3 scripts/test-worker-parallel-joint.py --race --artifacts /tmp/parallel-fixture
python3 scripts/test-worker-parallel-joint.py --race --live \
  --env-file /absolute/path/to/.env --artifacts /tmp/parallel-live
python3 scripts/test-worker-memory-web.py --scenario parallel \
  --artifacts /tmp/parallel-gui --coordination /tmp/parallel-gui-coordination
```

SDK 定向测试已观察实际双 HTTP 请求 barrier 重叠、两种相反完成顺序、慢分支请求排除
先完成兄弟输出、汇总请求消费全部四份前序叶子输出、usage 精确合计 50。HTTP 503、
deadline、工具取消后延迟返回和 drain 超时分别阻止成功 Final。Clone 的 trace/父子关系
沿已完成的 Sequence 接缝测试验证；不将单 TraceID 等同于完整父链验证。

新增真实 SDK Parallel Memory 定向测试：两个分支首请求 barrier 重叠，各自实际调用
`memory_add`；后继实际 `memory_load` 读取两条独立内容，Seal 候选保留两个唯一 ID、
完整内容、原 scope 和 BaseRevision 7。整 Run 恰好三工具、六模型 round、usage 60，
`-race -count=3` 通过。此项覆盖共享 Attempt，无 PG/Redis 并行后端验收含义。

正式 HTTP → Channel Lab fixture 已完成三个同 Session Run：A→B 与 B→A 两种实际
分支完成顺序均成功，每轮五次模型请求、两次选定 MCP、一个正式 candidate/Completion/
Final；第三轮一支实际 HTTP 401，兄弟实际 socket EOF/RESET，失败 0 candidate，
accepted ref/digest 不变。全部模型 HTTP handler 退出后再次读取，未出现晚到候选或
终态变化，原 accepted candidate 字节不变。

真实 DeepSeek 正常 Run 已完成：五次 HTTP 200、两个实际 MCP 调用，aggregator 请求
消费两份真实分支结果，正式 Session 与唯一 Final/Channel Lab 对应，实际返回 usage 合计 20,429 tokens。fixture/live 使用
相同 race Worker 二进制，均清理成功。live HTTP 时间区间只代表透明代理观察到的
请求重叠，不代表 Provider 内部推理时长；live 不注入 barrier、不修改请求/响应正文。

既有 GUI 已实际发布 Agent v2 / Profile r2 / Deployment r2，并通过同一 Manifest
执行一轮：两个并行分支、两个 MCP 与显式后继汇总，五次真实 HTTP fixture 模型请求，
正式候选保留三份输出而用户只收到 aggregator Final。两个 Bearer 字段发布后重新登录、
重开页面均为空且不回显原值。一次 locator 超时后复用同一服务栈和已发布版本恢复浏览器，
没有重新发布 Agent/Profile。该 GUI 使用确定性模型与真实 MCP，不声称 GUI 调用外部模型。
helper 单测只证明断言及脚手架，不替代上述公开发布与正式执行。
证据文件名为 `parallel-joint.json` / `parallel-web.json`，代码包报告给出实际绝对路径。

## 后续

下一包沿已有 `loop.body/max_iterations` 接入 SDK `cycleagent`，不扩展用户契约。
真实 Telegram 和实时外部模型故障注入不由本 Parallel 正常 live 验收代替。外部
Embedding 可用配置仍是独立遗留项，与本包普通工具和并发编排解耦。

## 后续已完成的真实数据后端回归

本包最初的共享 Memory SDK 测试之后，已另行完成 PG/Redis 正式 Memory、MinIO/PG
Artifact 并行保存及失败副作用、Qdrant 双 resource/tenant scope 回归。组合 Knowledge
导入旧单叶字段接缝已最小修复。具体实际后端、生产修复和未验边界见
[最终矩阵](orchestration-acceptance-v1.md)，不将有限矩阵扩大为任意组合通过。
