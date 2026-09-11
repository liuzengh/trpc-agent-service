# Agent Worker / Execution 术语

本文统一 Worker V1 的领域语言；具体实现证据见 [实现状态](implementation-status.md)。技术实现、状态转换和
验收规则见 [Worker V1](README.md)。

## 接纳与执行

**Admission**：Gateway 已接受一条输入并固定运行目标的事实，不是 Agent 已执行的事实。
_Avoid_：执行完成、Run 成功

**Run**：一次逻辑 Agent 执行，具有固定输入和固定运行版本；执行重试保持同一 Run 身份。
_Avoid_：消息、Attempt

**ExecutionAttempt / Attempt**：Execution 对一个 Run 的一次执行尝试，包含准备、执行与结束，
不要求模型已经被调用；本文中的 Attempt 均指 ExecutionAttempt。
_Avoid_：DeliveryAttempt、Run

**ExecutionGeneration**：同一 Run 内 Attempt 的递增执行代次。
_Avoid_：RouteGeneration、SessionGeneration、ConnectionGeneration

**ExecutionGrant**：某 Worker 在某个 Attempt 的有限有效期内具有执行资格的事实。
_Avoid_：租户管理权限、永久授权

**LeaseEpoch**：ExecutionGrant 的隔离代次，用于区分当前资格与已经被替代的资格。
_Avoid_：版本号、路由代次

**Completion**：Execution 已接受的一次 Run 终态及其不可变结果，不等于模型返回或渠道发送成功。
_Avoid_：Done 事件、ReplyIntent、Delivery Receipt

## 运行配置与会话

**RuntimeManifest**：确定 AgentVersion、ProfileRevision 和平台执行契约的不可变执行快照。
_Avoid_：Draft、最新 Profile、公开 Manifest View

**SessionScope**：决定哪些运行可以共享对话历史的身份范围。
_Avoid_：单独的 sender_id、渠道 ReplyContext

**Session**：某个 SessionScope 中的一条有顺序的运行历史。
_Avoid_：登录 Session、模型连接、外部 Chat

**SessionGeneration**：同一会话范围内明确隔离历史的代次，不随进程重启或执行重试变化。
_Avoid_：Attempt 序号、RouteGeneration

**SessionSequence**：一个 Run 在所属 Session 内的稳定接纳位置。
_Avoid_：模型 Token 序号、Reply sequence、Provider 原始发送顺序

**Attempt Transcript**：一次尚未被正式接受的执行尝试产生的会话内容。
_Avoid_：已提交历史、最终对话

**SessionCandidate**：某次 Attempt 产生、尚未被正式接受的一份候选会话内容。
_Avoid_：正式历史、Completion

**Accepted Session Head**：当前可以供后续运行读取的最新已接受会话内容身份。
_Avoid_：最后到达的候选、正在执行的临时状态

**SessionCommit**：Execution 正式接受一份候选会话内容的事实；失败 Run 不改变已接受历史。
_Avoid_：SDK 事件回调、临时 Transcript、另一份完整会话正文

**CredentialBatch**：为一个确定 Attempt 授权的完整运行凭据集合；同一次尝试不混合多个批次。
_Avoid_：Profile 配置、永久凭据快照

## 回复

**Final**：一个 Run 的完整逻辑最终回复，而不是某一段渠道消息。
_Avoid_：Progress、发送成功

**ReplyIntent**：Execution 对一个已提交 Final 产生的回复意图，不携带重新选择渠道目标的权力。
_Avoid_：Provider ACK、用户已看到

**CommittedFinalAuthorization**：一个特定 Final 已被 Execution 正式接受的不可变证明。
_Avoid_：活跃 Lease、Worker 自报成功

**DeliveryAttempt**：Gateway Delivery 对某个投递单元的一次外部发送尝试，不重新执行 Agent。
_Avoid_：ExecutionAttempt、Run 重试
