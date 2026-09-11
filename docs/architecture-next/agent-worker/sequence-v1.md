# Sequence：单 Run 的 SDK 有序 Agent 树

## 契约与装配

复用已有 AgentSpec `kind: sequence` / `children`、Profile model/tool/knowledge slots，
Control 原有编译结构和不可变 Manifest。没有新增 DSL、节点数据库、任务队列、调度器
或累计 Token 预算。本包只开放 LLM 与有序 Sequence；Parallel 和 Loop 分包交付。

Worker 门禁独立检查有序单父树：root 可达、无环、无孤立节点、子节点不可复用、
最多 128 节点、root 深度为 1 且最大 16、每个 Sequence 为 1–64 个不重复 children。
这些均沿用 AgentSpec 既有约束。节点 map 顺序不参与执行，children 原序保留。
Sequence 不接受 LLM 的模型、指令或数据能力选项。

每个 LLM 的模型、工具、Knowledge、Memory、Artifact、Summary 消费开关分别校验；
然后仅对全树资源 union 校验一次精确闭包。Summary 专用模型也属于全局模型依赖。
多个节点可以引用同一资源；每个节点最多一个 Knowledge 引用，全树可有不同 namespace。
Memory / Artifact 的存储 role 仍各为一份，选中存储不意味着所有节点获得工具。

Reader 保留 `Plan.Nodes` 和叶子依赖选择。Factory 一次解析完整且去重的 credential
use 批次，按节点保存独立模型凭据，只初始化全树选中的 MCP / Knowledge 依赖。
单个 MemoryAttempt、Artifact 服务、正式 Session overlay 为整 Run 共享；节点分别
装配显式工具/预加载/消费选项。`max_tool_calls` 是现有整 Run 计数，不因叶子数量增加
而重新发放额度；每次模型输出仍使用发布值及节点显式 override。

## 执行与 Final

实际使用 SDK v1.11.2 `chainagent.New(...WithSubAgents(...))` 递归构建，只有一个
Runner，不手写节点执行循环、不为叶子创建独立 Runner。

前序事件通过同一个 Session 进入后序请求；SDK Chain 继承 FilterKey，兄弟叶子的
Branch 不同。SDK 默认把前序兄弟的回答/工具记录转换为带作者说明的 user 上下文，
不是直接替换 Invocation.Message，也不保证保留 assistant/tool role。Worker 不为此
启用 PreserveForeignMessages；验收比较实际请求中的解码内容。

本包只对已支持的有序 Chain 静态确定最后一个 LLM 叶子：

1. 该叶子的完整、无 tool call 的 assistant 文本成为 Final 候选。
2. 继续消费整个 Runner，所有节点正常结束后才导出 Snapshot / Final。
3. 中间叶子 Done 不是 Run 完成，Runner completion 控制事件也不是必含正文的业务 Final。
4. 任一模型/基础设施错误、终端空文本或取消都阻止成功 Final；正式接受仍沿原 Stage →
   Complete。失败回复不表示接受失败轮正文，accepted Session head 不推进。

此终端叶子规则不自动套用于 Parallel 或 Loop。Parallel 后续必须有显式汇总节点。
Memory / Artifact 等副作用仍遵守各自既有存储边界，不把整条 Chain 宣称为跨后端事务。

## SDK Invocation 隔离接缝

race 测试观察到 SDK Chain 的 goroutine 写 Invocation.AgentName，与 Runner 的
诊断读取竞争。`isolatedChain` 仅使用公开 Invocation.Clone 后调用真实 Chain.Run，
避免二者共享可变 invocation 对象；保留父 Branch、Session、append notices 和上下文，
采用 SDK 自己生成的新 InvocationID，不伪造 ID、不复制 mutex、不修改 SDK。

修复前的 race 日志保留。修复后的真实 SDK 顺序、工具、取消与三轮 Summary 测试
确认 FilterKey 保持 Session appKey、Branch 保持 root/first 与 root/last，只有显式
开启摘要消费的节点读 accepted Summary。

## 发布与验证

Worker release digest 固定纳入 `worker_plan_contract=llm-sequence-tree-v1`，仅对
worker-v1 生效；历史 platform-v1 digest 不变。这是进程固定实现版本，不是新增用户
字段。Control / Worker 使用实际固定 hosts 重算同一 pin，历史发布保持不可变。

```sh
GOCACHE=/tmp/trpc-worker-go-cache go test -race \
  ./api/schemas/deployment/v1 \
  ./services/control-api/internal/deployment/domain \
  ./services/agent-worker/internal/execution/adapter/outbound/manifestadapter \
  ./services/agent-worker/internal/execution/adapter/outbound/runtimeadapter \
  ./services/agent-worker/internal/execution/adapter/outbound/trpcagent

python3 -B scripts/test-worker-sequence-joint.py --race --artifacts /tmp/sequence-joint
python3 -B scripts/test-worker-sequence-joint.py --race --live \
  --env-file /absolute/path/to/.env --artifacts /tmp/sequence-live
```

确定性联合按成功 → 终端 401 → 新鲜恢复的 3 个正式 Run 验证顺序、末叶 Final、
失败 head 不推进、后继不消费失败轮输入；live 为一个真实 DeepSeek 正常 Run，不把
fixture 故障注入等同于真实 provider 故障验收。IM 仍为现有 Channel Lab，真实 Telegram
未新增验收。GUI 复用已有结构编辑页，不新增第二种树编辑协议。

## 已观察验收（2026-09-09）

- 六个相关 Go 包 race 门禁通过；Executor 整包 race count=3 通过。
- 独立 Clone probe 验证新 InvocationID 的父关联、event.ParentInvocationID、共享
  Session/services/append notice、原 deadline/cancel、相同 TraceID 及精确 ParentSpanID。
  两叶 Knowledge namespace 选择和真实 tool result 隔离也通过；probe 不替代实际 Chain。
- 正式 fixture 首次执行即通过 3 Run：SUCCEEDED → FAILED（终端 HTTP 401）→ SUCCEEDED，
  各 1 Attempt；共 9 次模型 HTTP、3 次实际 selected MCP。失败轮 0 candidate，
  accepted ref/digest/原 candidate bytes 不变，恢复轮实际排除失败输入及首节点输出。
- 真实 DeepSeek 首次执行即通过 1 Run / 1 Attempt，3 次完整 HTTP 200、1 次实际 MCP。
  末叶真实请求读入前叶输出，最终只有末叶文本进入 Completion/outbox/Gateway/Lab。
  响应 usage 合计 2173，仅作 Provider 返回值记录，不推断计费。
- 两轮相同 race Worker 二进制，独立 JSON 与进程日志读回 165 项通过；内部依赖清理与
  凭据扫描通过。真实 provider 的终端失败和恢复未另行注入，属于 fixture 覆盖。
- 实际 GUI 使用现有结构编辑页完成 Agent v2 / Profile r2 / Deployment r2，保留嵌套
  Sequence，并实际修改叶子 instruction 和双侧 MCP capability。该页面发布的同一
  Manifest 完成 1 Run / 1 Attempt、三次模型 HTTP、末叶消费前叶输出、正式 Session
  和唯一末叶 Final 到 Lab。Bearer 保存后重开不回显；GUI 模型为确定性 fixture，
  MCP / Control / Gateway / Worker / 数据库和 Lab 为实际进程，清理及凭据扫描通过。
