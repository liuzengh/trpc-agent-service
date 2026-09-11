# Loop：按已发布次数执行 SDK Cycle

## 不新增用户契约

复用既有 AgentSpec `kind: loop`、单一 `body` 节点引用和 `max_iterations: 1..32`。
Profile 的模型/工具/数据资源绑定、Control 编译与 immutable Manifest wire 不增加字段。
Worker Plan 与 SDK Request 传递已发布 Body/MaxIterations，不展开复制节点或为每轮
新增 Run/Runner。固定 release contract 为 `llm-sequence-parallel-loop-tree-v1`，
历史 platform-v1 digest 不变，实际部署仍按固定 hosts 重算一致 pin。

Reader 按 body 递归，Factory 保留静态树与一次性最小凭据批次。嵌套 Sequence/Parallel
仍按已有树、深度、节点数量和独立叶子权限约束验证；不按迭代展开重新发放工具额度。
Loop 节点不接收 LLM 或数据能力选项，每个叶子保留原显式声明。

## 执行与结束

调用 SDK v1.11.2：

```go
cycleagent.New(nodeID,
    cycleagent.WithSubAgents([]agent.Agent{body}),
    cycleagent.WithMaxIterations(int(publishedMaxIterations)),
)
```

必须显式传入已发布次数。SDK 未指定 max 时可无限循环，Worker 不使用这个缺省值；
也不另加隐式次数、累计 Token 预占或新的总输出限制。正常执行在次数用尽时结束。

SDK 现有默认 escalation 针对错误事件；普通 assistant Done 不是整 Loop 完成，
可纠正工具业务错误也不被擅自变成自然提前退出。当前 AgentSpec 没有表达自定义退出
谓词，Worker 因此不添加文本约定、额外模型判断或退出 DSL。主模型/工具错误与取消
继续走现有失败分类，并阻止后续迭代。

前轮事件留在同一个 Session overlay，后轮实际模型请求消费前轮结果。终端路径按
Sequence 的最后 child、Loop 的 body 递归；整体末端若到达 Parallel，仍要求显式
后继 LLM，不取某个并行分支。每次相同终端叶的新 SDK InvocationID 开始时清空旧
Final 候选，避免最后轮无有效文本却误用早一轮结果。

只有整根 Runner 正常结束且最后有效终端执行提供非空业务文本，才能导出正式候选、
Stage → Complete 并回复 Final。失败、取消或最后轮空输出不接受前轮成功文本。
Iteration 不是正式 Session commit；只产生整 Run 的一份候选与一个终态回复。
外部工具/Artifact 副作用依旧遵守原服务边界，不因 Loop 失败自动回滚外部系统。

## 验证入口与覆盖

```sh
GOCACHE=/tmp/trpc-worker-go-cache go test -race \
  ./api/schemas/deployment/v1 \
  ./services/control-api/internal/deployment/domain \
  ./services/agent-worker/internal/execution/adapter/outbound/manifestadapter \
  ./services/agent-worker/internal/execution/adapter/outbound/runtimeadapter \
  ./services/agent-worker/internal/execution/adapter/outbound/trpcagent

python3 scripts/test-worker-loop-joint.py --race --artifacts /tmp/loop-fixture
python3 scripts/test-worker-loop-joint.py --race --live \
  --env-file /absolute/path/to/.env --artifacts /tmp/loop-live
```

SDK 定向测试实际执行 1/2/3/32 轮 HTTP，精确检查请求数、前轮上下文与 usage；
还包括 body 为嵌套 Sequence、第二轮错误阻止第三轮、最后轮空输出、第二轮阻塞取消、
同一 body MCP 工具状态和整 Run 额度不重置。取消 fixture 最初未消费请求 body 导致
httptest.Close 等待，修正测试服务的读流行为后通过，不修改产品失败语义。

正式 fixture 三个同 Session Run 已通过：正常两轮、第二轮 HTTP 401 失败、后继正常
恢复。每 Run 两次实际模型 HTTP、一个 Attempt，候选数量分别 1/0/1；成功 Final
只等于第二轮不同文本，失败不推进 accepted ref/digest，全部请求退出后再读取无迟到
候选，后继 actual request 与正式候选排除失败轮输入/输出。

真实 DeepSeek 的两次场景尝试均保留严格 FAIL，不再重跑：第一次第二响应改写了要求
逐字引用的第一响应；只澄清公开指令后，第二次两响应正文相同，未遵循第二轮标记要求。
逐字引用/第二标记是本测试额外的模型指令遵循条件，不是 Loop 通用协议。

第二次 live 的产品运行事实已完整读取：恰好两次 HTTP 200，第二实际请求包含第一
响应的完整 assistant 消息，一个 SUCCEEDED Run/Attempt/正式候选/Completion/Final，
Gateway receipt 与 Delivery part ACCEPTED，Channel Lab 单条回复等于实际第二响应。
两次响应相同，因此该 live 单例不能独立区分首轮或末轮 Final 选择；该区别由上面的
确定性不同输出测试证明。此次 usage 为 Provider 返回 511+612=1,123 tokens。
第一次 live 的引用断言在 Delivery 读取之前失败，未收集的交付不倒填；第二次已把
事实采集移到模型措辞断言之前，报告分离运行语义与场景 FAIL，没有放宽 quote 断言。
所有 live 服务均回收，未修改 SDK、模型参数、迭代次数或透明 HTTP 请求/响应字节。

既有 GUI 已通过：实际编辑 max_iterations 1→2 与 body instruction，发布 Agent v2、
保留原 Profile r1、发布 Deployment r2；同一 Manifest 恰好两轮确定性 HTTP，两个
输出不同，正式候选保留两份原文，仅第二份成为 Final，Gateway/Channel Lab 对应。
模型凭据原值未回显。GUI PASS 不覆盖或改写上面的真实模型场景 FAIL。

## 后续组合回归

普通工具与三种编排逐包完成后运行相关统一回归，并补已开放 Parallel 数据能力的
必要真实后端组合；现有共享 Memory SDK 无丢写测试不是 PG/Redis/Artifact 并行存储
验收。外部 Embedding 的明确可用配置仍单列，不以确定性 embedding fixture 宣称
真实外部语义检索验收。真实 Telegram 与 live 故障注入不由本包正常 live 代替。

后续 Parallel 的必要真实后端回归现已完成，含正式 Memory 接受/读回/失败不应用、
Artifact 版本/即时副作用及 Knowledge 双 resource/tenant scope，详见
[最终矩阵](orchestration-acceptance-v1.md)。本页的两次 live 场景 FAIL 保持不变。
