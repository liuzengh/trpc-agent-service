# 从一个可运行 Agent 开始

这份指南不讨论多租户、消息队列、分布式锁或 Kubernetes。目标只有一个：亲手跑通下面这条链路。

```text
HTTP 请求
  → model.Message
  → runner.Runner
  → LLMAgent
  → TutorialModel
  → Event channel
  → HTTP JSON 响应
```

默认配置不需要模型 API Key，也不需要 Redis。它使用确定性的本地 Mock Model 和 InMemory Session，适合先观察 tRPC-Agent-Go 的基本工作方式。完成基础链路后，可以按照第 10 节切换到 Redis，验证重启恢复和多节点共享 Session。

## 1. 启动服务

环境要求：

- Go 1.24.0 或更高版本；
- `curl`；
- 端口 8080 未被占用。

直接运行：

```bash
go run ./cmd/trpc-service
```

也可以明确强制使用 Mock Model：

```bash
./start-mock.sh
```

看到下面的输出表示服务已经启动：

```text
trpc-agent-service 0.1.0
model provider=mock name=tutorial-mock-model stream=false
session backend=inmemory ttl=0s
coordinator backend=local lease_ttl=30s renew_interval=10s
idempotency backend=local processing_ttl=2m0s completed_ttl=24h0m0s
tutorial chat server listening on :8080
```

也可以使用仓库脚本：

```bash
./build.sh
./start.sh
```

服务启动时会自动读取仓库根目录的 `.env`。首次 clone 后先复制模板：

```bash
cp .env.example .env
```

`.env` 已加入 `.gitignore`，不会被提交；`.env.example` 只保存字段和安全的默认值。

检查服务：

```bash
curl -sS http://127.0.0.1:8080/healthz
```

返回：

```json
{"status":"ok"}
```

端口可以在 `.env` 中配置，也可以通过命令行覆盖：

```bash
TRPC_AGENT_ADDR=127.0.0.1:18080
```

```bash
go run ./cmd/trpc-service -addr 127.0.0.1:18080
```

需要使用其他配置文件时，可以执行：

```bash
go run ./cmd/trpc-service -env-file ./config/dev.env
```

## 2. 切换到真实模型

默认使用 Mock Model，因此第一次运行不需要网络和 API Key。要连接 OpenAI 或兼容 OpenAI Chat Completions API 的服务，编辑 `.env`：

```dotenv
TRPC_AGENT_MODEL_PROVIDER=openai
TRPC_AGENT_MODEL_NAME="你的模型 ID"
OPENAI_API_KEY="你的 API Key"

# 使用兼容服务时设置；直连 OpenAI 时可以不设置。
OPENAI_BASE_URL="https://your-provider.example/v1"

# 当前 /chat 返回完整 JSON，先保持关闭最容易观察。
TRPC_AGENT_MODEL_STREAM=false
```

保存后重新运行 `go run ./cmd/trpc-service` 或 `./start.sh`。

推荐先用独立脚本检查真实模型。它通过项目实际使用的 tRPC-Agent-Go OpenAI Model 发出一次最小请求，不启动其他平台依赖，也不会打印 API Key：

```bash
./check-model.sh
```

检查成功后后台启动真实模型服务：

```bash
./start-real.sh
tail -f data/trpc-service.log
```

两个脚本都会先移除当前终端中可能覆盖 `.env` 的模型环境变量，再从 `.env` 重新加载配置。使用其他文件时设置 `TRPC_AGENT_ENV_FILE=/path/to/dev.env`。

不要把真实 API Key 写进仓库、README 或启动参数。开发环境从环境变量读取，生产环境应由 Secret Manager 注入。OpenAI 官方文档也建议在服务端从环境变量或密钥管理服务加载 API Key。

真实模型和 Mock Model 使用同一个 `model.Model` 接口。切换模型不会改变 HTTP、Runner、Session 和 Event 处理代码：

```text
TRPC_AGENT_MODEL_PROVIDER=mock
  → TutorialModel

TRPC_AGENT_MODEL_PROVIDER=openai
  → tRPC-Agent-Go model/openai
  → OpenAI 或兼容服务
```

当前 `/chat` 返回一次完整 JSON，真实模型默认使用非流式请求。要实验框架内部的流式 Event，可以额外设置：

```bash
TRPC_AGENT_MODEL_STREAM=true
```

如果配置不完整，服务会在启动时直接报错。例如 `provider=openai` 时必须设置模型 ID 和 API Key，不会静默回退到 Mock。

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `TRPC_AGENT_MODEL_PROVIDER` | `mock` | `mock` 或 `openai` |
| `TRPC_AGENT_MODEL_NAME` | 空 | 真实模型的模型 ID，`openai` 模式必填 |
| `OPENAI_API_KEY` | 空 | 服务端模型凭据，`openai` 模式必填 |
| `OPENAI_BASE_URL` | SDK 默认地址 | OpenAI-compatible 服务地址，可选 |
| `TRPC_AGENT_MODEL_STREAM` | `false` | 是否让模型以流式方式向 Runner 返回事件 |

进程中已经存在的环境变量优先于 `.env`。这允许 Kubernetes、Docker 或 CI 使用 Secret 注入覆盖本地文件，而不需要修改 `.env`。

## 3. 第一轮对话

发送一条自我介绍：

```bash
curl -sS -X POST http://127.0.0.1:8080/chat \
  -H 'Content-Type: application/json' \
  -d '{
    "binding_key": "tutorial-http",
    "message_id": "tutorial-message-1",
    "user_id": "alice",
    "session_id": "demo",
    "message": "我叫小明。"
  }'
```

返回类似：

```json
{
  "reply": "你好，小明。我已经把这句话保存在当前 Session 中。",
  "request_id": "...",
  "message_id": "tutorial-message-1",
  "user_id": "alice",
  "session_id": "demo",
  "event_count": 7,
  "replayed": false,
  "tenant_id": "tutorial-tenant",
  "app_id": "tutorial-app",
  "revision_id": "tutorial-revision-1",
  "agent_name": "tutorial-agent"
}
```

这里有五个重要概念：

- `message_id` 标识这一条外部消息，客户端重试时必须复用；
- `message` 被转换成 `model.Message`；
- `request_id` 表示本次 Runner 执行；
- Runner 把用户消息和 Agent 回复写入 Session；
- 一次 `Run` 会产生多个 Event，因此 `event_count` 往往大于 1。

## 4. 第二轮验证 Session

保持相同的 `user_id` 和 `session_id`：

```bash
curl -sS -X POST http://127.0.0.1:8080/chat \
  -H 'Content-Type: application/json' \
  -d '{
    "binding_key": "tutorial-http",
    "message_id": "tutorial-message-2",
    "user_id": "alice",
    "session_id": "demo",
    "message": "我叫什么？"
  }'
```

返回：

```json
{
  "reply": "你叫小明。这个名字来自当前 Session 的历史消息。",
  "request_id": "...",
  "message_id": "tutorial-message-2",
  "user_id": "alice",
  "session_id": "demo",
  "event_count": 7,
  "replayed": false
}
```

TutorialModel 本身没有保存用户资料。它只检查 tRPC-Agent-Go 传给模型的 `request.Messages`。第二轮能回答名字，是因为 Runner 根据 `user_id + session_id` 找回了第一轮 Session，并将历史消息加入模型请求。

## 5. 换一个 Session

只修改 `session_id`：

```bash
curl -sS -X POST http://127.0.0.1:8080/chat \
  -H 'Content-Type: application/json' \
  -d '{
    "binding_key": "tutorial-http",
    "message_id": "tutorial-message-3",
    "user_id": "alice",
    "session_id": "another-session",
    "message": "我叫什么？"
  }'
```

这次返回：

```json
{
  "reply": "我还不知道你的名字。你可以告诉我：我叫小明。",
  "message_id": "tutorial-message-3",
  "user_id": "alice",
  "session_id": "another-session",
  "replayed": false
}
```

框架中的完整 Session Key 是：

```text
AppName + UserID + SessionID
```

本示例固定使用：

```text
AppName = t/tutorial-tenant/a/tutorial-app
UserID = HTTP 请求中的 user_id
SessionID = HTTP 请求中的 session_id
```

只要其中一个值变化，就会进入另一份会话历史。

## 6. 已经实现了什么：从启动到返回答案

这一节按真实执行顺序解释当前代码。读完后，你应该能回答下面几个问题：

- `.env` 是在哪里读取的？
- Mock Model 和真实模型在哪里切换？
- `LLMAgent`、`Runner` 和 `Session` 是在哪里创建的？
- 重复的 `message_id` 在哪里被拦截？
- 同一个 Session 的并发请求在哪里协调？
- `/chat` 收到 JSON 后，怎样进入 `runner.Run`？
- 为什么 Runner 返回 Event channel，而不是直接返回字符串？
- 第二轮对话为什么能够看到第一轮内容？

### 6.1 先看整体分层

当前实现分成七层：

```text
cmd/trpc-service
  负责启动、装配和关闭进程

trpcservice/config
  负责读取 .env 并校验模型、Session、Coordinator 和 Idempotency 配置

trpcservice/storage
  负责创建和探测 InMemory / Redis Session Service

trpcservice/coordination
  负责本地 Session 锁和 Redis 分布式租约

trpcservice/idempotency
  负责消息去重、执行中等待和结果复用

trpcservice/web
  负责 HTTP、JSON 和参数校验

trpcservice/agent
  负责 Model、LLMAgent、Runner、Session 和 Event
```

一次 `/chat` 请求的代码路径如下：

```text
POST /chat
  ↓
web.Handler.handleChat
  ↓
routing.Resolver.Resolve(binding_key)
  ↓
agent.Runtime.ChatWithScope
  ↓
IdempotencyStore.Begin
  ↓
SessionCoordinator.Acquire
  ↓
model.NewUserMessage
  ↓
runner.Run
  ↓
LLMAgent
  ↓
TutorialModel 或 model/openai
  ↓
Runner Event channel
  ↓
collectChatResult
  ↓
SessionLease.Release
  ↓
IdempotencyAttempt.Complete
  ↓
chatResponse JSON
```

这里有意让 HTTP 层和 Agent 层分开。以后把 `/chat` 换成企业微信、Telegram 或消息队列时，Runner 部分仍然可以复用。

### 6.2 进程从哪里开始

入口是 [`cmd/trpc-service/main.go`](../cmd/trpc-service/main.go)。`main` 本身只做一件事：调用 `run`，如果失败就打印错误并用非零状态退出。

```go
func main() {
    if err := run(); err != nil {
        log.Printf("trpc-agent-service stopped: %v", err)
        os.Exit(1)
    }
}
```

把主要逻辑放进 `run()`，是为了让错误沿调用链返回，而不是在中间到处 `log.Fatal`。后续增加数据库、Redis 和消息队列时，也可以按相同方式初始化，任何一个关键组件失败都阻止服务带病启动。

`run()` 的装配顺序是：

```text
解析命令行参数
→ 加载 .env
→ 确定监听地址
→ 读取模型配置
→ 创建 Model
→ 读取 Session 配置
→ 创建并探测 Session Service
→ 读取 Coordinator 配置
→ 创建并探测 Session Coordinator
→ 读取 Idempotency 配置
→ 创建并探测 Idempotency Store
→ 创建 Control Plane Route Resolver
→ 创建 Runtime
→ 创建 HTTP Handler
→ 启动 HTTP Server
→ 等待退出信号
→ 优雅关闭
```

命令行参数 `-addr` 的优先级高于 `TRPC_AGENT_ADDR`。如果两者都没有设置，服务监听 `:8080`。`-env-file` 可以指定其他配置文件，默认读取根目录 `.env`。

### 6.3 `.env` 是怎样加载的

[`trpcservice/config/env.go`](../trpcservice/config/env.go) 使用 `godotenv.Load` 读取 `.env`：

```go
func LoadDotEnv(path string) (bool, error) {
    if err := godotenv.Load(path); err != nil {
        // 默认 .env 不存在时允许继续启动；文件损坏则返回错误。
    }
    return true, nil
}
```

这里有两个重要行为：

1. 根目录 `.env` 是可选的，所以 CI 或容器可以完全依赖进程环境变量；
2. 已经存在的进程环境变量不会被 `.env` 覆盖。

第二点使配置优先级变成：

```text
命令行参数
  > 进程环境变量 / Kubernetes Secret
  > .env
  > 代码默认值
```

`.env` 已加入 `.gitignore`，可提交的是 `.env.example`。真实 API Key 不应该进入 Git。

### 6.4 模型配置如何校验

[`trpcservice/config/model.go`](../trpcservice/config/model.go) 把零散环境变量转换成一个明确的 `ModelConfig`：

```go
type ModelConfig struct {
    Provider string
    Name     string
    BaseURL  string
    APIKey   string
    Stream   bool
}
```

当前支持两个 provider：

```text
mock
openai
```

未配置 provider 时使用 `mock`。选择 `openai` 后，模型名和 API Key 必填；`OPENAI_BASE_URL` 如果存在，还会检查 scheme、host、query 和 user info，避免把明显错误或包含凭据的 URL 传给模型客户端。

这一步发生在 HTTP Server 启动前。配置缺失时服务直接退出，例如：

```text
load model config: OPENAI_API_KEY is required when model provider is openai
```

服务不会因为真实模型配置错误而静默回退到 Mock。否则你可能以为正在测试真实模型，实际收到的却是固定回复。

`ModelConfig.APIKey` 只用于创建模型，不会写入启动日志、HTTP 响应或 Session。

### 6.5 Mock 和真实模型如何共用一个接口

模型创建入口是 [`trpcservice/agent/model_factory.go`](../trpcservice/agent/model_factory.go)：

```go
func BuildModel(cfg config.ModelConfig) (model.Model, error) {
    switch cfg.Provider {
    case "mock":
        return NewTutorialModel(), nil
    case "openai":
        options := []openai.Option{
            openai.WithAPIKey(cfg.APIKey),
        }
        if cfg.BaseURL != "" {
            options = append(options, openai.WithBaseURL(cfg.BaseURL))
        }
        return openai.New(cfg.Name, options...), nil
    }
}
```

这里的 `openai.New` 来自：

```text
trpc.group/trpc-go/trpc-agent-go/model/openai
```

也就是说，当前实现没有自己拼接 `/chat/completions` HTTP 请求。鉴权、请求转换、流式响应和 OpenAI-compatible 协议适配由 tRPC-Agent-Go 的 Model 实现负责。

`BuildModel` 返回的类型统一为 `model.Model`。这个接口只要求两个方法：

```go
type Model interface {
    GenerateContent(
        ctx context.Context,
        request *Request,
    ) (<-chan *Response, error)

    Info() Info
}
```

因此 Runtime 不需要知道底层是 Mock、OpenAI 还是兼容服务。将来增加 DeepSeek、Qwen 或租户自己的网关时，扩展 Model Factory 即可。

### 6.6 TutorialModel 实际做了什么

Mock 实现在 [`trpcservice/agent/mock_model.go`](../trpcservice/agent/mock_model.go)。它没有真正的语言理解能力，只处理几种确定的规则：

```text
“我叫小明”
  → 回复已记住小明

“我叫什么”
  → 向前搜索历史 user message 中的“我叫……”

其他文本
  → 回复“我收到了：……”
```

关键点是：`TutorialModel` 没有自己的 Session map，也没有把姓名保存到全局变量。它只读取：

```go
request.Messages
```

第二轮能找到“小明”，说明历史消息确实经历了下面的路径：

```text
第一轮用户消息
→ Runner 写入 Session
→ 第二轮 Runner 读取 Session
→ Runner 构造 model.Request.Messages
→ TutorialModel 读取历史
```

这也是保留 Mock 的主要价值：它用稳定规则验证 Session 和 Runner 管道，不受网络、余额、temperature 或模型版本影响。

### 6.7 Session Factory 如何选择 InMemory 或 Redis

Session 创建入口是 [`trpcservice/storage/session.go`](../trpcservice/storage/session.go)。它读取已经校验过的 `SessionConfig`，返回统一的 tRPC-Agent-Go `session.Service`：

```go
switch cfg.Backend {
case "inmemory":
    service = inmemory.NewSessionService()
case "redis":
    service, err = redissession.NewService(
        redissession.WithRedisClientURL(cfg.RedisURL),
        redissession.WithKeyPrefix(cfg.RedisKeyPrefix),
        redissession.WithSessionTTL(cfg.TTL),
        redissession.WithEnableAsyncPersist(false),
        redissession.WithEnableUserSessionIndex(true),
        redissession.WithCompatMode(redissession.CompatModeNone),
    )
}
```

这里的 Redis Session 实现来自独立子模块：

```text
trpc.group/trpc-go/trpc-agent-go/session/redis
```

Factory 创建服务后会执行一次只读探测。Redis 地址错误或服务不可用时，进程在启动阶段失败，而不是先监听 HTTP、等第一条聊天请求到来才报错。

当前 Redis 配置使用同步写入路径：

```go
WithEnableAsyncPersist(false)
```

这意味着 `AppendEvent` 成功返回时，Redis 写命令已经完成，其他节点随后可以读到新 Event。框架的异步持久化使用进程内队列，不适合当前“重启后仍应保留会话”的验收目标。

这里的“同步写入”不等于每次请求都完成磁盘 `fsync`。示例 Compose 使用 AOF `everysec`，宿主机异常断电时理论上仍可能丢失最近约一秒的数据。生产环境应根据恢复点目标选择托管 Redis、高可用复制以及 `appendfsync always` 等更强策略，并接受相应的写入延迟和成本。

新项目没有旧版 Redis Session 数据，因此使用 `CompatModeNone`，只采用新的 HashIdx 存储结构，避免每次请求额外检查旧 ZSet 格式。

### 6.8 Runtime 如何创建 Agent 和 Runner

正式启动使用 [`NewRuntimeWithCompilerServices`](../trpcservice/agent/runtime.go)，Runner 保留一个 fallback Agent，但每次请求会由 Revision Compiler 选择实际 Agent：

```go
agentInstance := llmagent.New(
    tutorialAgentName,
    llmagent.WithModel(selectedModel),
    llmagent.WithInstruction("..."),
    llmagent.WithGenerationConfig(
        model.GenerationConfig{Stream: stream},
    ),
)

runnerInstance := runner.NewRunner(
    "trpc-agent-service",
    fallbackAgent,
    runner.WithSessionService(sessionService),
)
```

这三个对象的职责不同：

| 对象 | 当前职责 |
| --- | --- |
| `session.Service` | 保存用户消息、assistant 消息和会话状态 |
| `coordination.Coordinator` | 保证同一个 Session 的完整 Agent turn 不会并发推进 |
| `idempotency.Store` | 保证同一个外部 message ID 只执行一次并缓存结果 |
| `agent.Compiler` | 把不可变 revision 编译成当前请求使用的 Agent |
| `LLMAgent` | 组织提示词、模型调用和将来的 Tool 循环 |
| `runner.Runner` | 管理一次运行、Session 读写、request ID 和 Event 流 |

`session.Service`、`LLMAgent` 和 `runner.Runner` 来自 tRPC-Agent-Go；Coordinator 和 Idempotency Store 是本项目新增的平台层。我们写的 `Runtime` 把这些组件组合成 HTTP 和未来 IM Channel 可以调用的应用服务。

`sessionService`、`coordinator` 和 `idempotencyStore` 都由外部 Factory 注入。三者分别负责会话存储、同 Session 串行和同消息去重，不能互相替代。

Runtime 不再使用固定 AppName。Route Resolver 根据 Channel Binding 构造内部 Storage Scope。完整 Session Key 是：

```text
t/{tenant_id}/a/{app_id} + user_id + session_id
```

`NewRuntime` 是默认 InMemory 的便捷函数，单元测试可以继续使用。正式启动路径使用：

```text
BuildModel
+ NewSessionService
+ NewCoordinator
+ idempotency.New
+ NewRevisionCompiler
+ NewRuntimeWithCompilerServices
```

因此 `.env` 可以分别选择真实模型、Session 后端、协调后端和幂等后端。

### 6.9 一次 `ChatWithMessageID` 调用发生了什么

HTTP 层先通过 `binding_key` 得到可信 Scope，再调用 `Runtime.ChatWithScope`。它先进入幂等层；只有首次出现的 `message_id` 才会继续获取 Session Lease：

```go
scope, err := routeResolver.Resolve(ctx, bindingKey)
begin, err := r.idempotency.Begin(ctx, idempotencyKey, fingerprint)
compiledAgent, err := r.compiler.Compile(ctx, scope)

lease, err := r.coordinator.Acquire(ctx, coordination.Key{
    AppName:   scope.StorageScope,
    UserID:    userID,
    SessionID: sessionID,
})
leaseCtx := coordination.ContextWithFencingToken(
    lease.Context(),
    lease.FencingToken(),
)

events, err := r.runner.Run(
    leaseCtx,
    userID,
    sessionID,
    model.NewUserMessage(text),
    agent.WithAgent(compiledAgent),
)
```

`model.NewUserMessage(text)` 把普通字符串转换成框架消息：

```go
model.Message{
    Role:    model.RoleUser,
    Content: text,
}
```

随后 Runner 大致执行：

```text
1. 用 AppName + UserID + SessionID 查找 Session
2. Session 不存在时创建 Session
3. 把本轮 user message 持久化为 Event
4. 把历史 Event 投影成 model.Request.Messages
5. 调用 LLMAgent
6. LLMAgent 调用选中的 model.Model
7. 接收模型 Response
8. 把完整 assistant Response 写入 Session
9. 对调用方发送 Runner completion Event
10. 关闭 Event channel
```

这些步骤主要由 tRPC-Agent-Go 完成。我们的 Runtime 没有手工查询 Session，也没有自己拼历史 messages，但会在外层完成消息幂等和 Session 协调。Runner 成功结束并释放 Lease 后，幂等层才把最终结果标记为 `completed`。

锁的 Key 与 Session Key 一致：

```text
t/{tenant_id}/a/{app_id} + user_id + session_id
```

同一 Session 必须串行，不同 Session 可以并行。Coordinator 的保护范围覆盖 `runner.Run` 和整个 Event 消费过程，不会在 `runner.Run` 刚返回 channel 时就提前释放。第 11 节会继续拆解这条链路。

### 6.10 为什么返回 Event channel

模型执行不是只有一个最终字符串。一次运行中可能出现：

```text
用户消息已保存
模型开始执行
partial 文本
tool_call
tool_result
最终 assistant 消息
错误事件
Runner completion
```

所以框架返回：

```go
<-chan *event.Event
```

当前 `/chat` 是非流式 HTTP JSON 接口，因此 [`collectChatResult`](../trpcservice/agent/runtime.go) 会一直消费 Event channel，并把多个事件聚合成一个 `ChatResult`。

它处理三类内容：

- `evt.RequestID`：保存本次运行 ID；
- `choice.Delta.Content`：累积流式文本；
- `choice.Message.Content`：保存完整 assistant 回复。

遇到 `Response.Error` 时记录运行错误，但仍继续排空 channel。这里“继续读取到关闭”很重要；如果调用方收到错误就立即停止读取，生产中的 Agent 或 Tool goroutine 可能因为无法继续发送 Event 而阻塞。

HTTP 返回中的 `event_count` 只是为了让学习者看到“一次 Run 不止一个 Event”。当前常见值是 7，但这不是稳定协议。更换模型、开启流式、加入 Tool 或升级框架后，Event 数量都可能变化，业务代码不能依赖固定数字。

### 6.11 HTTP Handler 做了什么

HTTP 入口在 [`trpcservice/web/handler.go`](../trpcservice/web/handler.go)。它注册三个路由：

```text
GET  /healthz
GET  /readyz
POST /chat
```

`/chat` 接受：

```json
{
  "binding_key": "tutorial-http",
  "message_id": "message-001",
  "user_id": "alice",
  "session_id": "demo",
  "message": "你好"
}
```

处理步骤是：

```text
检查必须使用 POST
→ 限制 request body 大小
→ 严格解析 JSON
→ 拒绝未知字段和多个 JSON 对象
→ 去除字符串首尾空格
→ 检查 message_id、user_id、session_id、message
→ 根据 binding_key 反查 tenant/app/revision
→ 调用 ChatService.ChatWithScope
→ 返回 chatResponse
```

HTTP 层只依赖一个小接口：

```go
type ChatService interface {
    ChatWithScope(ctx context.Context, input agent.ChatInput) (agent.ChatResult, error)

    Ready(ctx context.Context) error
}
```

它不知道 Runtime 内部使用的是 InMemory 还是 Redis，也不知道模型来自 Mock 还是 OpenAI。这个边界以后可以保留：企业微信 Adapter、Telegram Adapter 或队列 Worker 都调用同一类应用服务。

`r.Context()` 会传入 Runtime 和 Runner。客户端断开、HTTP 超时或服务关闭时，context cancellation 能沿调用链传递到模型调用。

### 6.12 真实模型路径和 Mock 路径有什么不同

两条路径只在 Model Factory 处分叉：

```text
Mock:
  BuildModel
  → TutorialModel.GenerateContent
  → 本地生成 model.Response

OpenAI-compatible:
  BuildModel
  → trpc-agent-go/model/openai.New
  → OpenAI-compatible /v1/chat/completions
  → 适配为 model.Response
```

从 `NewRuntime` 往上看，两者完全一样：

```text
相同 LLMAgent
相同 Runner
相同 Session
相同 Event 聚合
相同 /chat 响应
```

测试中还创建了一个本地 `httptest.Server` 模拟 OpenAI-compatible API，用来验证：

- 请求路径是 `/v1/chat/completions`；
- Authorization header 正确；
- 模型名和 messages 被发送；
- 返回的 Chat Completion 能经过 tRPC-Agent-Go、Runner 和 Runtime 变成最终 reply。

这个测试不会访问公网，也不会消耗真实 token。

### 6.13 `/healthz` 和 `/readyz` 的区别

当前服务提供两个健康端点：

```text
GET /healthz
  只说明 HTTP 进程仍然存活

GET /readyz
  会检查 Session Service、Session Coordinator 和 Idempotency Store
```

使用 Redis Session、Redis Coordinator 或 Redis Idempotency Store 时，`/healthz` 可能仍返回正常，但 Redis 故障会让 `/readyz` 返回 `503`。部署平台应该根据 `/readyz` 决定是否继续把新请求发给该节点。

readiness 检查使用两秒超时：Session Service 执行只读的 `ListAppStates`，Coordinator 和 Idempotency Store 执行本地状态检查或 Redis `PING`。它不会创建聊天 Session、消息记录或业务 Session Lease。

### 6.14 服务如何关闭

`main.go` 使用 `signal.NotifyContext` 监听 SIGINT 和 SIGTERM。按下 `Ctrl+C` 或执行 `./stop.sh` 后：

```text
收到退出信号
→ http.Server.Shutdown
→ 等待正在处理的 HTTP 请求
→ ListenAndServe 返回
→ Runtime.Close
→ Runner.Close
→ Session Coordinator.Close
→ Idempotency Store.Close
→ Session Service.Close
```

HTTP Shutdown 最多等待 10 秒。`Runtime.Close` 使用 `sync.Once`，重复调用不会重复关闭资源。

正式启动路径由 `main.go` 创建 Session Service、Coordinator 和 Idempotency Store；`NewRuntimeWithServices` 成功后，Runtime 接管它们的生命周期并负责关闭。默认的 `NewRuntime` 便捷函数会创建 InMemory Session、Local Coordinator 和 Local Idempotency Store。

### 6.15 哪些代码是我们写的，哪些是框架提供的

| 能力 | 当前来源 |
| --- | --- |
| HTTP `/chat`、参数校验、JSON 响应 | 本项目 |
| `.env`、模型配置校验 | 本项目 |
| Model Factory | 本项目 |
| TutorialModel | 本项目，仅用于教学和测试 |
| `model.Model`、`model.Message`、`model.Response` | tRPC-Agent-Go |
| OpenAI-compatible Model Adapter | tRPC-Agent-Go `model/openai` |
| `LLMAgent` | tRPC-Agent-Go |
| `runner.Runner` | tRPC-Agent-Go |
| Event channel | tRPC-Agent-Go |
| InMemory Session | tRPC-Agent-Go |
| Redis Session | tRPC-Agent-Go `session/redis` 子模块 |
| Session 配置、Factory 和 readiness | 本项目 |
| Local / Redis Session Coordinator | 本项目 |
| Redis 租约 Lua、续租、安全释放和 fencing token | 本项目 |
| Local / Redis Idempotency Store | 本项目 |
| message ID 冲突检测、执行中等待和结果复用 | 本项目 |
| Runtime 和 Event 聚合 | 本项目对框架的应用封装 |

这就是目前阶段的主要成果：我们没有重写 Agent 框架，而是把 tRPC-Agent-Go 的 Model、LLMAgent、Runner、Session 和 Event 组织成了一条能通过 HTTP 实际调用、能切换真实模型、能验证多轮会话的最小链路。

### 6.16 推荐阅读顺序

如果是第一次看代码，按下面顺序阅读最容易：

1. [`trpcservice/web/handler.go`](../trpcservice/web/handler.go)：先看请求从哪里进入；
2. [`trpcservice/agent/runtime.go`](../trpcservice/agent/runtime.go)：看 Runner 怎样被调用；
3. [`trpcservice/coordination/coordinator.go`](../trpcservice/coordination/coordinator.go)：看 Coordinator 和 Lease 接口；
4. [`trpcservice/coordination/local.go`](../trpcservice/coordination/local.go) 和 [`redis.go`](../trpcservice/coordination/redis.go)：看本地锁和 Redis 租约；
5. [`trpcservice/idempotency/store.go`](../trpcservice/idempotency/store.go)：看消息幂等接口和状态；
6. [`trpcservice/idempotency/local.go`](../trpcservice/idempotency/local.go) 和 [`redis.go`](../trpcservice/idempotency/redis.go)：看本地与跨节点去重；
7. [`trpcservice/storage/session.go`](../trpcservice/storage/session.go)：看 Session 后端怎样切换；
8. [`trpcservice/agent/mock_model.go`](../trpcservice/agent/mock_model.go)：看 Model 接口如何实现；
9. [`trpcservice/agent/model_factory.go`](../trpcservice/agent/model_factory.go)：看真实模型怎样替换 Mock；
10. [`trpcservice/config`](../trpcservice/config)：看环境变量如何变成配置；
11. [`cmd/trpc-service/main.go`](../cmd/trpc-service/main.go)：最后看所有组件如何装配和关闭。

不要一开始深入 tRPC-Agent-Go 的所有内部实现。先沿着上面的文件跟完一条请求，再根据兴趣进入框架源码。

## 7. 运行测试

```bash
go test ./...
```

两轮会话测试在 [`trpcservice/agent/runtime_test.go`](../trpcservice/agent/runtime_test.go)，它验证：

1. 第一轮告诉 Agent“我叫小明”；
2. 第二轮使用同一 Session 询问名字；
3. Agent 能从 Session 历史中找到“小明”；
4. 换一个 Session 后不能读取之前的名字。

HTTP 参数和响应测试在 [`trpcservice/web/handler_test.go`](../trpcservice/web/handler_test.go)。

Session 协调测试在 [`trpcservice/coordination`](../trpcservice/coordination) 和 [`runtime_coordinator_test.go`](../trpcservice/agent/runtime_coordinator_test.go)，覆盖同 Session 串行、不同 Session 并行、跨 Runtime 互斥、租约续期、租约丢失、TTL 恢复和请求取消后的锁释放。

消息幂等测试在 [`trpcservice/idempotency`](../trpcservice/idempotency) 和 [`runtime_idempotency_test.go`](../trpcservice/agent/runtime_idempotency_test.go)，覆盖并发重复消息、跨 Runtime 去重、完成结果复用、内容冲突、失败重试、processing TTL 和续期。

也可以运行 race test：

```bash
go test -race ./...
```

## 8. 当前示例故意没有实现的部分

这不是生产服务。当前限制包括：

- InMemory Session 重启后仍会丢失；Redis Session 已支持重启持久化和跨实例共享；
- 默认 Mock Model 只识别几种固定句式，真实模型需要自行配置凭据；
- 已支持本地按 Session 并行和 Redis 分布式租约，但 fencing token 还没有在 Session 存储写入边界强制校验；
- 已支持基于 `message_id` 的本地和 Redis 幂等，但 completed 结果过期后再次收到同一消息仍会重新执行；
- 已有 Redis Session，但没有 PostgreSQL、消息队列或运行 journal；
- 没有租户和 Agent revision；
- 没有企业微信或 Telegram 回调；
- 没有 Tool、MCP、Memory 和 Knowledge；
- 没有生产级鉴权、幂等和审计。

这些限制是刻意保留的。当前阶段只需要看清 Runner 怎样连接 Message、Agent、Model、Session 和 Event。

## 9. 可以自己做的三个实验

### 实验一：修改回复

修改 `tutorialReply`，让普通消息返回不同内容。观察 HTTP 和 Session 流程是否需要变化。答案应该是不需要。

### 实验二：修改 user_id

在同一个 `session_id` 下，把第二轮 `user_id` 改成 `bob`。Agent 不应该知道 alice 的名字。这能帮助理解 Session Key 不只有 `session_id`。

### 实验三：比较 InMemory 和 Redis 重启行为

使用 InMemory 时，告诉 Agent 名字后重启进程，再问名字，Agent 会忘记。切换到 Redis 后重复实验，Agent 应该仍能找回历史。这能直观看到共享 Session 后端解决了什么问题。

## 10. Redis Session 接入后的运行链路

最开始的运行链路是：

```text
HTTP 请求
  → model.Message
  → runner.Runner
  → LLMAgent
  → Model
  → Event channel
  → HTTP JSON 响应
```

接入 Redis 后，这条主链没有被替换。变化发生在 Runner 的前后：Runner 执行 Agent 之前要从 Redis 恢复 Session 并保存本轮用户消息，执行过程中产生的完整 Event 还要写回 Redis。

```text
HTTP 请求
  → model.Message
  → runner.Runner
      → Redis：读取或创建 Session
      → Redis：写入本轮 user Event
      → LLMAgent
          → 从 Session.Events 组装模型历史
          → Model
          → Agent Event
      → Redis：写入完整 assistant Event
      → Runner Event channel
  → HTTP JSON 响应
```

下面按实际代码执行顺序拆开这条链路。

### 10.1 Redis 替换的是 `session.Service`

InMemory 和 Redis 都实现了 tRPC-Agent-Go 的 `session.Service` 接口。Runner 依赖的是这个接口，而不是某个具体数据库：

```text
runner.Runner
    ↓ 依赖 session.Service
    ├── session/inmemory
    └── session/redis
```

因此切换 Session 后端不会改变下面这些代码：

- HTTP Handler 仍然调用 Runtime 应用服务；
- 首次消息经过幂等和 Coordinator 后仍然调用 `runner.Run`；
- LLMAgent 和 Model 不需要知道 Redis 地址；
- Event channel 的消费方式不变；
- HTTP 响应结构不变。

变化的只是 `runner.WithSessionService(...)` 收到的具体实现。

### 10.2 启动时怎样把 Redis 交给 Runner

启动链路从 `.env` 开始：

```text
.env
  ↓
config.LoadSessionConfigFromEnv
  ↓
storage.NewSessionService
  ↓
trpc-agent-go/session/redis.NewService
  ↓
ProbeSessionService
  ↓
agent.NewRuntimeWithCompilerServices
  ↓
runner.NewRunner(..., runner.WithSessionService(redisService))
```

第一步，[`config.LoadSessionConfigFromEnv`](../trpcservice/config/session.go) 把环境变量转换成 `SessionConfig`：

```go
type SessionConfig struct {
    Backend        string
    RedisURL       string
    RedisKeyPrefix string
    TTL            time.Duration
}
```

第二步，[`storage.NewSessionService`](../trpcservice/storage/session.go) 根据 `Backend` 创建框架提供的实现：

```go
case config.SessionBackendRedis:
    service, err = redissession.NewService(
        redissession.WithRedisClientURL(cfg.RedisURL),
        redissession.WithKeyPrefix(cfg.RedisKeyPrefix),
        redissession.WithSessionTTL(cfg.TTL),
        redissession.WithEnableAsyncPersist(false),
        redissession.WithEnableUserSessionIndex(true),
        redissession.WithCompatMode(redissession.CompatModeNone),
    )
```

这里真正实现 `GetSession`、`CreateSession` 和 `AppendEvent` 的是 tRPC-Agent-Go `session/redis` 子模块。平台层 Factory 只负责选择实现、传入配置和探测连接。

第三步，Factory 调用一次只读的 `ListAppStates`。这会强制发生 Redis 网络访问。如果 Redis 不可达，错误在 HTTP Server 启动前返回。

第四步，[`NewRuntimeWithCompilerServices`](../trpcservice/agent/runtime.go) 把 Redis Service、Coordinator、Idempotency Store 和 Revision Compiler 交给 Runtime，其中 Session Service 会注入 Runner：

```go
runner.NewRunner(
    tutorialAppName,
    agentInstance,
    runner.WithSessionService(sessionService),
)
```

完成这一步后，Runner 并不知道这个 Service 是在 `main.go` 中怎样创建的，只通过统一接口读写 Session。这就是依赖注入在当前项目里的实际作用。

### 10.3 第一条消息进入时，Runner 先查 Session

用户发送：

```json
{
  "binding_key": "tutorial-http",
  "message_id": "redis-message-1",
  "user_id": "alice",
  "session_id": "redis-demo",
  "message": "我叫小明。"
}
```

HTTP Handler 完成 JSON 校验后调用 Runtime；Runtime 的幂等检查确认这是首次消息，随后进入实际 Agent turn：

```go
Runtime.ChatWithMessageID(
    ctx,
    "redis-message-1",
    "alice",
    "redis-demo",
    "我叫小明。",
)
```

Runtime 把文本转换成 `model.Message`，然后进入：

```go
r.runner.Run(
    ctx,
    userID,
    sessionID,
    model.NewUserMessage(text),
)
```

Runner 首先生成 `request_id`，然后构造 Session Key：

```text
AppName   = tutorial-app
UserID    = alice
SessionID = redis-demo
```

接下来调用统一接口：

```text
sessionService.GetSession(key)
```

因为这是第一次对话，Redis 中还没有对应 Session，所以返回 `nil`。Runner 随后调用：

```text
sessionService.CreateSession(key, emptyState)
```

Redis Service 创建 Session 元数据，并在启用用户 Session 索引时同时写入索引。创建完成后，Runner 拿到一个内存中的 `*session.Session` 对象；后面的 LLMAgent 操作的是这个对象，不会在每个处理步骤中直接查询 Redis。

第一轮的读取路径可以概括为：

```text
runner.Run
  → GetSession(tutorial-app, alice, redis-demo)
  → Redis 未命中
  → CreateSession
  → 得到空的 session.Session
```

### 10.4 用户消息在模型调用前写入 Redis

Session 准备好后，Runner 不会立即调用模型，而是先把当前用户消息包装成一个 Event：

```text
model.Message(role=user, content="我叫小明。")
  ↓
event.Event(author=user, response.choices[0].message=用户消息)
```

然后调用：

```text
sessionService.AppendEvent(session, userEvent)
```

当前配置使用：

```go
redissession.WithEnableAsyncPersist(false)
```

因此 `AppendEvent` 会等待 Redis 写命令完成后再返回。Redis 的 HashIdx 实现使用 Lua 脚本更新 Session 数据：Event 数据写入事件结构，Event 携带的 `StateDelta` 则合并到 Session 元数据中。单次脚本执行是原子的。

顺序非常重要：

```text
读取或创建 Session
→ 写入当前 user Event
→ 调用 LLMAgent
```

这样 LLMAgent 构造本轮模型请求时，当前用户消息和此前历史都已经出现在 `Session.Events` 中。

同步 Redis 写入不代表每条消息都已经执行磁盘 `fsync`。示例 Compose 使用 AOF `everysec`，磁盘恢复能力仍由 Redis 的持久化策略决定。

### 10.5 Redis 历史怎样变成 `model.Request.Messages`

模型本身不会访问 Redis。Redis Service 先把数据恢复为框架的 Session 对象：

```go
session.Session{
    AppName: "tutorial-app",
    UserID:  "alice",
    ID:      "redis-demo",
    Events:  []event.Event{...},
    State:   session.StateMap{...},
}
```

Runner 再把这个 Session 放进 `agent.Invocation`，传给 LLMAgent：

```text
Redis 数据
  → session.Session
  → agent.Invocation.Session
  → LLMAgent ContentRequestProcessor
  → model.Request.Messages
```

LLMAgent 的 Content Request Processor 会从 `Session.Events` 中投影出适合发送给模型的历史消息，同时加入 Agent instruction 等上下文。概念上，本轮请求会得到类似内容：

```text
system: 你是 tutorial-agent，使用会话历史回答
user:   我叫小明。
```

到了第二轮，则可能是：

```text
system: 你是 tutorial-agent，使用会话历史回答
user:   我叫小明。
assistant: 你好，小明……
user:   我叫什么？
```

最后才调用统一模型接口：

```go
selectedModel.GenerateContent(ctx, request)
```

所以“真实模型为什么能记住名字”的完整答案是：Redis 保存 Event，Runner 恢复 Session，LLMAgent 把 Event 投影成模型消息；不是模型自己在服务端保存了这个用户。

### 10.6 模型响应怎样写回 Redis

Model 返回的是 `<-chan *model.Response>`，LLMAgent 再把 Response 转换成 Agent Event。Runner 启动自己的 Event Loop，对 Agent Event 做两件事：

```text
Agent Event
  ├── 符合条件的完整 Event → sessionService.AppendEvent → Redis
  └── 对调用方可见的 Event → processedEventCh → Runtime
```

流式 partial Event 可以立即向调用方传播，但不会把每一个文本碎片都作为完整会话消息保存。最终完整 assistant Event 才会进入 Session。这样下一轮恢复历史时，得到的是完整回复，而不是大量零散 token。

当前 `/chat` 没有直接把 Event 流转成 SSE，而是由 `collectChatResult` 一直读取到 channel 关闭：

```text
Runner processedEventCh
  → collectChatResult
      → 累积 partial content
      → 保存 final message
      → 记录 request_id
      → 统计 event_count
  → chatResponse JSON
```

因此在正常写入路径中，一次请求结束时发生了两件不同的事：

1. Session Service 已经保存了本轮可持久化 Event；
2. HTTP 调用方拿到了聚合后的最终 JSON。

还要注意两类写入失败的处理位置不同：本轮 user Event 在 Agent 启动前写入，失败会让 `runner.Run` 直接返回错误；Agent 产生的 Event 在异步 Event Loop 中写入，当前框架会记录持久化错误，并可能继续向调用方发送 Event。生产平台后续需要为这种情况补充持久化失败指标、告警和运行 journal，不能仅凭 HTTP `200` 判断整轮数据一定完整落库。

### 10.7 第二轮或另一个节点为什么能恢复历史

下一次请求只要使用同一个：

```text
t/{tenant_id}/a/{app_id} + user_id + session_id
```

Runner 就会再次调用 Redis `GetSession`。这一次不再返回 `nil`，而是读取 Session 元数据、Event、State 和已有 Summary 等数据，恢复成新的 `session.Session` 对象。

```text
节点 B 收到第二轮请求
  → Runner 构造相同 Session Key
  → Redis GetSession 命中
  → 恢复第一轮 user/assistant Events
  → 追加第二轮 user Event
  → LLMAgent 组装完整历史
  → Model 回答“小明”
```

这解释了两个现象：

- Agent 进程重启后仍能恢复会话，因为新进程会重新从 Redis 加载 Session；
- 节点 B 能继续节点 A 的会话，因为两个节点使用相同的 Redis 和 Key 规则。

因此当前读取链路不要求 sticky session。负载均衡器可以把下一轮请求发给另一个 Worker，只要两个 Worker 的模型/Agent 配置兼容，并共享相同的 Session 后端。

### 10.8 Redis 中保存的不是模型对象

Redis 中保存的是 Session 数据，不是运行中的 Go 对象，也不是完整的 LLMAgent 或 Model：

| 数据 | 用途 |
| --- | --- |
| Session metadata | 标识 App、User、Session、创建时间和 Session State |
| Event data | 保存用户消息、完整 assistant 回复以及将来的 Tool Event |
| Event time index | 按时间恢复和过滤 Event |
| User session index | 列出某个 App/User 下的 Session |
| State | 保存 Agent/Graph 在会话范围内的状态变化 |
| Summary | 历史过长时保存会话摘要；当前示例尚未主动配置摘要策略 |

每个请求开始后，框架把所需数据加载成当前进程里的 `session.Session`；请求结束后，再把新增 Event 写回 Redis。这种“共享持久化状态 + 请求内内存对象”的模式，才是 Worker 可以水平扩展的基础。

### 10.9 `/readyz` 走的是另一条短链路

readiness 不会执行 Agent 或调用模型：

```text
GET /readyz
  → web.Handler.handleReady
  → Runtime.Ready
  → sessionService.ListAppStates
  → Redis
  → 200 ready / 503 not ready
```

它验证的是“当前节点能否访问 Session 后端”。所以 Redis 故障时，Go 进程可能仍然存活，`/healthz` 仍然是 `200`，但 `/readyz` 会变成 `503`。

### 10.10 当前 Redis 链路还没有解决什么

单独使用 Redis Session 可以解决：

```text
会话不再绑定单个进程
多节点可以读取同一份历史
Agent 重启后可以恢复 Session
```

但 Redis 的单次命令或 Lua 脚本原子性，不等于整个 Agent Run 是一个分布式事务。两个节点仍可能同时执行：

```text
节点 A 读取 Session 版本 N
节点 B 也读取 Session 版本 N
节点 A 调用模型并追加 Event
节点 B 调用模型并追加 Event
```

如果没有额外协调，这仍可能造成同一个 Session 的两轮消息交错。Redis Session 的职责是保存数据，不负责持有一次完整 Agent Run 的执行权。

当前项目已经增加 Session Coordinator。它保护的范围不是某一次 Redis `AppendEvent`，而是完整的一轮执行：

```text
读取 Session
→ 写入用户消息
→ Agent / Model / Tool 执行
→ 写入最终 Event
```

理解这个边界后，就能看清为什么 Redis Session 和 Redis Coordinator 是两个不同组件：前者保存会话，后者决定当前由哪个 Worker 推进会话。

## 11. Session Coordinator 接入后的运行链路

加入 Coordinator 后，一次请求的最外层链路变成：

```text
HTTP / IM 请求
  → 计算 Session Key
  → SessionCoordinator.Acquire
  → 获得 Lease 和 fencing token
  → runner.Run
  → 持续消费 Event channel
  → SessionLease.Release
  → 返回响应
```

关键点是 Lease 必须覆盖整个 Runner 生命周期，而不只是 `runner.Run` 这个函数调用。`runner.Run` 返回的是 channel，此时 Agent、Model 或 Tool 可能仍在后台执行。

### 11.1 Coordinator 接口表达什么

接口位于 [`trpcservice/coordination/coordinator.go`](../trpcservice/coordination/coordinator.go)：

```go
type Coordinator interface {
    Acquire(ctx context.Context, key Key) (Lease, error)
    Ready(ctx context.Context) error
    Close() error
}

type Lease interface {
    Context() context.Context
    FencingToken() int64
    Release(ctx context.Context) error
}
```

`Coordinator` 负责等待和授予执行权，`Lease` 表示本轮请求当前拥有的执行权。

`Lease.Context()` 不只是原始 HTTP context 的别名。Redis Coordinator 无法继续证明租约所有权时，会主动取消这个 context，使取消信号沿下面的方向传播：

```text
Lease Context
  → runner.Run
  → LLMAgent
  → Model
  → Tool
  → Event Loop
```

`FencingToken()` 是每次成功获取租约时递增的编号。Token 已经放入 Runner 使用的 context，后续 Session 写入包装器、Tool 或 Plugin 可以读取它。

### 11.2 Local Coordinator 怎样做到同 Session 串行

默认配置是：

```dotenv
TRPC_AGENT_COORDINATOR_BACKEND=local
```

Local Coordinator 在进程内维护一个按 Session Key 分组的 semaphore：

```text
tutorial-app/alice/session-a → semaphore A
tutorial-app/alice/session-b → semaphore B
```

如果两个请求属于同一个 Session：

```text
请求 1 → 获得 semaphore A → 执行
请求 2 → 等待 semaphore A
请求 1 → Release
请求 2 → 获得 semaphore A → 执行
```

如果两个请求属于不同 Session：

```text
请求 1 → semaphore A → 并行执行
请求 2 → semaphore B → 并行执行
```

每个条目记录持有者和等待者的引用数。最后一个引用释放后，条目从 map 删除，避免服务运行时间越长，已经结束的 Session 锁对象越积越多。

Local Coordinator 解决了原来全局 `sync.Mutex` 的吞吐问题，但它只能看到当前进程，不能协调另一个 Worker。

### 11.3 Redis Coordinator 怎样抢占租约

多 Worker 部署时配置：

```dotenv
TRPC_AGENT_COORDINATOR_BACKEND=redis
TRPC_AGENT_COORDINATOR_LEASE_TTL=30s
TRPC_AGENT_COORDINATOR_RENEW_INTERVAL=10s
TRPC_AGENT_COORDINATOR_RETRY_INTERVAL=50ms
```

Coordinator 复用 `REDIS_URL` 和 `REDIS_KEY_PREFIX`，但使用独立的 `coord:session` Key 命名空间。

Session Key 不会以明文直接拼进 Redis Key。代码先对 `AppName + UserID + SessionID` 做 SHA-256，再生成带 Redis Cluster hash tag 的两个 Key：

```text
<prefix>:coord:session:{hash}:lock
<prefix>:coord:session:{hash}:fence
```

`lock` 保存随机 owner ID，`fence` 保存单调递增 token。两个 Key 使用相同 `{hash}`，为将来放入 Redis Cluster 同一 slot 做准备。

获取租约通过一个 Lua 脚本原子完成，逻辑相当于：

```text
如果 lock 不存在：
  token = INCR fence
  PSETEX lock lease_ttl owner_id
  返回 token
否则：
  返回 0
```

返回 `0` 表示其他节点仍持有租约，当前请求按 `RetryInterval` 等待后重试。等待过程中会同时监听请求 context 和 Coordinator close context，所以客户端取消或服务关闭时不会永久阻塞。

### 11.4 为什么需要随机 owner 和 fencing token

随机 owner 用于安全续租和释放。假设节点 A 的租约已经过期，节点 B 获得了同一个 lock；这时节点 A 的延迟释放请求不能删除节点 B 的锁。

因此续租和释放都必须比较 owner：

```text
GET lock == my_owner
  → 可以 PEXPIRE 或 DEL

GET lock != my_owner
  → 当前节点已经不是持有者
```

fencing token 处理的是更深一层的问题：旧节点可能在租约过期后仍短暂运行。新节点拿到更大的 token 后，下游存储如果只接受比已记录 token 更大的写入，就能拒绝旧节点的迟到写入。

当前实现已经做到：

- Redis 原子生成递增 token；
- `Lease.FencingToken()` 暴露 token；
- token 注入 Runner context。

当前还没有做到：

- tRPC-Agent-Go Redis Session 的 `AppendEvent` 尚未校验 token；
- 所以 token 目前是“已经传播但尚未在存储边界强制执行”。

这也是为什么文档不能直接宣称已经获得严格 fencing 保证。

### 11.5 续租怎样覆盖长时间模型调用

Agent 调用可能超过初始 Lease TTL，例如模型响应较慢或 Tool 执行时间较长。Redis Lease 创建后会启动一个续租 goroutine：

```text
每隔 RenewInterval
  → Lua 检查 lock owner
  → owner 相同：PEXPIRE 刷新 TTL
  → owner 不同：取消 Lease Context
  → Redis 请求失败：取消 Lease Context
```

当前策略偏向一致性：一次续租错误就认为无法继续证明所有权，立即取消本轮 Agent 执行。这样会牺牲短暂 Redis 抖动时的可用性，但能降低旧 Worker 在未知租约状态下继续写入的风险。

续租 goroutine 会在下面任一条件发生时退出：

- Lease 正常释放；
- HTTP 请求 context 取消；
- Coordinator 关闭；
- 续租失败或 owner 不匹配。

`Release` 会先停止续租并等待 goroutine 退出，再执行删除脚本，避免出现“刚删除 lock，续租 goroutine 又把它延长”的竞态。

### 11.6 Runtime 为什么要排空 Event 后再释放

[`Runtime.runChatTurn`](../trpcservice/agent/runtime.go) 中 Coordinator 包裹 Runner 的实际结构是：

```text
Acquire Lease
  → runner.Run(lease.Context())
  → collectChatResult
      → 一直读取 Event channel
      → 直到 Runner 关闭 channel
  → Release Lease
```

如果写成下面这样就是错误的：

```text
Acquire
→ runner.Run 返回 channel
→ 立即 Release
→ 后台 Agent 仍在产生和写入 Event
```

第二个请求会在第一轮真正结束前进入同一个 Session，锁就失去了意义。

当前 Runtime 使用 `defer` 保证成功、模型错误、context 取消等路径都会尝试释放 Lease。释放时使用一个脱离原请求取消信号的两秒 context：即使客户端已经断开，服务仍有机会执行 compare-and-delete；如果 Redis 仍不可达，lock 最终由 TTL 回收。

### 11.7 节点崩溃后为什么不会永久死锁

正常路径会主动 `Release`，节点崩溃时则无法执行清理。Redis lock 自带 Lease TTL，因此：

```text
节点 A 获得 lock
→ 节点 A 崩溃，续租停止
→ TTL 到期，Redis 自动删除 lock
→ 节点 B 下一次重试成功
```

TTL 太短会增加正常请求中途失租的风险；TTL 太长会延长故障节点退出后的恢复时间。当前默认值是：

```text
Lease TTL      = 30s
Renew Interval = 10s
Retry Interval = 50ms
```

配置校验要求续租间隔不超过 TTL 的一半，至少留出一次额外续租机会。

### 11.8 readiness 和关闭链路

使用 Local Coordinator 时，`Ready` 检查组件是否已经关闭；使用 Redis Coordinator 时，`Ready` 会执行 Redis `PING`。

```text
GET /readyz
  → Session Service readiness
  → Coordinator readiness
  → Idempotency Store readiness
  → 三者都成功才返回 200
```

服务关闭顺序是：

```text
HTTP Server 停止接收请求
→ 等待 Handler 返回
→ Runtime.Close
→ Runner.Close
→ Coordinator.Close
→ Idempotency Store.Close
→ Session Service.Close
```

Coordinator Close 会拒绝新的 Acquire，并取消仍存活的 Lease Context。正常的 HTTP shutdown 会先等待请求结束，因此关闭 Coordinator 时通常已经没有活跃 turn。

### 11.9 当前阶段获得了什么

当前并发语义已经从：

```text
所有 Session 全局串行
```

推进到：

```text
同一 Session 串行
不同 Session 并行
多个 Runtime 对同一 Session 互斥
节点崩溃后依靠 TTL 恢复
租约丢失时取消 Runner 链路
```

这使 Worker 水平扩展具备了基本的会话执行边界。但在 fencing token 真正接入 Session 写入校验之前，极端暂停恢复场景仍不能视为严格解决。

## 12. 消息幂等接入后的运行链路

外部系统不能假设一次 HTTP 投递只会到达一次。客户端可能因为响应超时主动重试，IM 平台也可能在没有及时收到确认时重复推送同一条消息。如果没有幂等层，两次投递会分别调用模型并向 Session 追加两轮 Event。

加入 Idempotency Store 后，完整链路变成：

```text
HTTP / IM 消息
  → message_id + 消息内容指纹
  → IdempotencyStore.Begin
      ├── started：本请求成为执行者
      ├── processing：等待当前执行者
      └── completed：直接复用历史结果
  → SessionCoordinator.Acquire
  → Runner.Run
  → 排空 Event channel
  → SessionLease.Release
  → IdempotencyAttempt.Complete
  → 返回 reply + request_id + replayed
```

### 12.1 `message_id` 和 `request_id` 不是一回事

现在 `/chat` 要求调用方提供 `message_id`：

```json
{
  "binding_key": "tutorial-http",
  "message_id": "wecom-msg-10001",
  "user_id": "alice",
  "session_id": "demo",
  "message": "你好"
}
```

两个 ID 的来源和作用不同：

| 字段 | 产生方 | 作用 |
| --- | --- | --- |
| `message_id` | HTTP 客户端或 IM 平台 | 标识同一条外部消息，重试时必须保持不变 |
| `request_id` | tRPC-Agent-Go Runner | 标识实际发生的一次 Agent 执行 |

首次执行时会产生新的 `request_id`。重复提交相同 `message_id` 时不会创建新 Runner 执行，而是返回第一次保存的 `request_id`。

幂等 Key 当前由四部分组成：

```text
StorageScope + ChannelBindingID + UserID + SessionID + MessageID
```

因此同一个外部 ID 可以安全地出现在另一个 Session。进入多租户阶段后，还会在最外层加入 `tenant_id` 和 `channel`。

### 12.2 为什么还要保存消息指纹

只有 `message_id` 不足以判断请求是否真的相同。调用方可能错误地复用 ID：

```text
message_id = 100，message = "查询订单 A"
message_id = 100，message = "删除订单 B"
```

如果直接返回第一条消息的缓存，错误会被隐藏。因此 Runtime 会对规范化后的消息正文计算 SHA-256：

```text
message
  → trim spaces
  → SHA-256
  → fingerprint
```

同一幂等 Key 对应的 fingerprint 不一致时，Store 返回 `ErrKeyConflict`，HTTP 层映射为 `409 Conflict`。Redis Key 本身也使用作用域字段的 SHA-256，不把用户 ID、Session ID 和消息 ID 以明文写入 Key 名。

### 12.3 启动时怎样装配 Idempotency Store

启动链路新增一段：

```text
.env
  → config.LoadIdempotencyConfigFromEnv
  → idempotency.New
      ├── LocalStore
      └── RedisStore
  → Store.Ready
  → agent.NewRuntimeWithCompilerServices
```

本地默认配置是：

```dotenv
TRPC_AGENT_IDEMPOTENCY_BACKEND=local
```

多 Worker 使用 Redis：

```dotenv
TRPC_AGENT_IDEMPOTENCY_BACKEND=redis
TRPC_AGENT_IDEMPOTENCY_PROCESSING_TTL=2m
TRPC_AGENT_IDEMPOTENCY_COMPLETED_TTL=24h
TRPC_AGENT_IDEMPOTENCY_RENEW_INTERVAL=30s
TRPC_AGENT_IDEMPOTENCY_POLL_INTERVAL=50ms
```

Redis Store 复用 `REDIS_URL` 和 `REDIS_KEY_PREFIX`，使用独立的命名空间：

```text
<prefix>:idempotency:message:<sha256>
```

Local Store 适合单进程开发；多个 Worker 必须使用 Redis，否则每个进程只能看到自己的幂等记录。

### 12.4 `Begin` 的三个分支

Runtime 首先调用：

```go
begin, err := r.idempotency.Begin(ctx, key, fingerprint)
```

Store 会返回三个状态之一。

`BeginStarted` 表示记录不存在，当前请求原子地创建了 `processing` 记录，并获得一个 `Attempt`：

```text
不存在
  → processing
  → 当前请求获得 Attempt
```

只有这个分支可以继续获取 Session Lease 和调用 Runner。

`BeginProcessing` 表示另一个请求正在处理相同消息。当前请求不会调用模型，而是进入 `Store.Wait`。

`BeginCompleted` 表示第一次执行已经成功，Store 直接返回缓存的 `reply`、`request_id` 和 `event_count`。

Redis 的“检查不存在并创建 processing”在一个 Lua 脚本中完成，两个 Worker 同时收到同一消息时，只有一个能得到 `BeginStarted`。

### 12.5 为什么幂等检查必须在 Session Coordinator 前面

当前顺序是：

```text
Idempotency Begin
→ Session Coordinator Acquire
→ Runner
```

假如反过来：

```text
Session Coordinator Acquire
→ Idempotency Begin
```

重复 webhook 虽然最终可能不会执行模型，但会先进入同一个 Session 的锁等待队列，占用连接和等待时间。更糟糕的实现甚至可能让它们获得锁后依次重复执行。

先做消息幂等，可以在最外层把重复请求分流：

```text
首次消息 → Coordinator → Runner
重复消息 → Wait 或 Replay
```

Coordinator 解决“一个 Session 同一时刻只能推进一轮”，Idempotency Store 解决“一条外部消息最多产生一轮”。

### 12.6 processing Attempt 怎样覆盖长时间执行

首次请求拿到的 `Attempt` 包含自己的 context。Runtime 使用这个 context 继续获取 Session Lease：

```text
Idempotency Attempt Context
  → Session Coordinator
  → Runner
  → LLMAgent
  → Model / Tool
```

Redis processing 记录带 TTL，防止 Worker 崩溃后永远停留在处理中。同时 Attempt 会启动续期 goroutine：

```text
每隔 RenewInterval
  → 比较 Redis 中的完整 processing 记录
  → 仍由当前 owner 持有：刷新 ProcessingTTL
  → 记录消失或 owner 改变：取消 Attempt Context
  → Redis 访问失败：取消 Attempt Context
```

续期采用“无法证明所有权就停止”的策略。如果幂等记录丢失，本轮 Runner 会收到 context cancellation，避免两个 Worker 在未知状态下继续执行同一消息。

Worker 直接崩溃时续期停止，`ProcessingTTL` 到期后记录自动消失。后续重复投递再次执行 `Begin`，可以成为新的执行者。

### 12.7 成功时为什么保存完整结果

Runner 完成、Event channel 排空并释放 Session Lease 后，Runtime 调用：

```go
attempt.Complete(ctx, idempotency.Result{
    Reply:      result.Reply,
    RequestID:  result.RequestID,
    EventCount: result.EventCount,
})
```

Redis 使用 compare-and-replace Lua 脚本：只有 Redis 中的 processing JSON 仍与当前 Attempt 完全相同，才能替换成 completed。

```text
processing(owner=A, fingerprint=F)
  → completed(fingerprint=F, result=R)
```

completed 记录保存 `CompletedTTL`，当前默认 24 小时。在这段时间内，重复请求直接返回完全相同的业务结果，其中 `replayed=true`。

保存结果而不是只保存“处理过”有两个原因：

- 同步 HTTP 重试仍然需要拿到答案；
- IM Adapter 可以判断第一次发送失败后是否复用原回答，而不必重新调用模型。

### 12.8 失败时为什么删除 processing

如果 Coordinator、Runner、Model 或 Tool 返回错误，Runtime 调用：

```go
attempt.Fail(ctx)
```

Redis 使用 compare-and-delete，只有原 owner 才能删除 processing。删除后，正在等待的重复请求会发现记录不存在，重新执行 `Begin`，其中一个请求成为新的执行者。

```text
首次执行失败
  → 删除 processing
  → 等待者重新 Begin
  → 允许重新执行
```

这表示当前语义是“成功结果去重，失败允许重试”。如果外部工具包含不可逆副作用，未来还需要工具级幂等键，不能只依赖消息层删除失败记录。

### 12.9 正在处理的重复请求怎样等待

Local Store 使用每条记录的 `done` channel 唤醒等待者；Redis Store 按 `PollInterval` 读取共享记录：

```text
processing → 继续等待
completed  → 返回缓存结果
不存在     → 返回 ErrRetry，Runtime 重新 Begin
fingerprint 不同 → ErrKeyConflict
```

等待使用重复请求自己的 HTTP context。客户端断开时只有这个等待者退出，不会取消真正拥有 Attempt 的首次请求。

如果两个节点几乎同时收到同一消息：

```text
节点 A：BeginStarted → 调用一次模型 → Complete
节点 B：BeginProcessing → Wait → 读取 A 的 completed 结果
```

节点 B 返回的 `request_id` 与节点 A 相同，并标记 `replayed=true`。

### 12.10 HTTP 响应怎样表示重放

首次执行返回：

```json
{
  "reply": "...",
  "request_id": "runner-request-1",
  "message_id": "wecom-msg-10001",
  "user_id": "alice",
  "session_id": "demo",
  "event_count": 7,
  "replayed": false
}
```

使用相同内容和相同 `message_id` 再次请求，响应中的 `reply`、`request_id` 和 `event_count` 保持不变，只是：

```json
{"replayed": true}
```

如果同一个 `message_id` 携带不同内容，返回：

```text
HTTP 409 Conflict
```

这比静默返回旧结果更容易发现上游 ID 生成错误。

### 12.11 readiness 和关闭顺序

`/readyz` 现在检查三个有状态组件：

```text
Session Service
→ Session Coordinator
→ Idempotency Store
→ 全部正常才返回 200
```

Redis Idempotency Store 的 readiness 使用 `PING`。如果幂等 Redis 不可用，节点不会绕过幂等继续调用模型，而是返回不可用。

Runtime 关闭时会关闭 Idempotency Store，Store 会取消仍在执行的 Attempt Context 和等待请求，续期 goroutine 随之退出。

### 12.12 当前幂等边界

当前实现已经保证：

```text
同一进程重复消息只执行一次
多个 Worker 重复消息只执行一次
执行中的重复请求等待首次结果
完成后的重复请求复用原结果
失败或 processing 超时后允许重新执行
```

仍需注意：

- completed 记录超过 TTL 后，同一消息再次到达会被视为新消息；
- 消息正文指纹暂时只覆盖文本，未来图片、文件和卡片需要加入规范化 payload；
- 模型成功但 completed 写入失败时，Session 可能已经存在回复，而幂等记录最终会过期；需要运行 journal 或事务型 outbox 进一步收敛；
- Tool 已产生外部副作用后再失败，必须由 Tool 自己支持业务幂等。

## 13. PostgreSQL 控制面接入后的启动链路

Session 保存聊天历史，Idempotency Store 保存消息执行结果；租户、Agent App、Revision 和 Channel Binding 属于另一类长期配置，因此新增 Control Plane Repository。

当前启动链路增加为：

```text
.env
→ LoadControlPlaneConfigFromEnv
→ controlplane.New
    ├── inmemory：加载 tutorial bootstrap snapshot
    └── postgres：打开连接池
          → PostgreSQL Ping
          → embedded migration
          → 可选 tutorial bootstrap
→ Repository.Ready
→ HTTP Server
```

默认仍使用内存控制面，原有教程不需要 PostgreSQL：

```dotenv
TRPC_AGENT_CONTROL_PLANE_BACKEND=inmemory
```

使用 PostgreSQL：

```bash
docker compose up -d postgres
```

```dotenv
TRPC_AGENT_CONTROL_PLANE_BACKEND=postgres
TRPC_AGENT_POSTGRES_URL=postgres://trpc_agent:trpc_agent_dev@127.0.0.1:5432/trpc_agent?sslmode=disable
TRPC_AGENT_POSTGRES_AUTO_MIGRATE=true
TRPC_AGENT_POSTGRES_BOOTSTRAP_TUTORIAL=true
```

Migration 使用内嵌 SQL、事务和 PostgreSQL advisory lock。`schema_migration` 保存版本和 SHA-256 checksum；已经执行的 migration 文件如果被修改，启动会失败，而不是静默接受不同 schema。

第一版 schema 已包含：

```text
tenant
agent_app
agent_revision
backend_binding
channel_binding
external_identity
conversation
inbound_message
agent_run
outbound_message
queue_outbox
audit_log
```

Control Plane Repository 提供带 tenant scope 的读取接口：

```text
GetTenant
GetAgentApp(tenant_id, app_id)
GetRevision(tenant_id, revision_id)
GetStableRevision(tenant_id, app_id)
GetChannelBindingByCallbackKey
ListBackendBindings(tenant_id, app_id)
```

PostgreSQL URL 不会写入启动日志。`/readyz` 除了检查 Runtime，还会调用 Repository `PingContext`；数据库断开后节点会退出就绪状态。

## 14. Channel Binding 到租户 Runtime 的路由链路

HTTP Test Channel 现在要求 `binding_key`，不接受调用方直接指定 `tenant_id` 或 `app_id`：

```text
POST /chat(binding_key)
→ ControlPlaneRepository.GetChannelBindingByCallbackKey
→ 检查 binding.status
→ GetTenant(binding.tenant_id)
→ 检查 tenant.status
→ GetAgentApp(binding.tenant_id, binding.app_id)
→ GetStableRevision
→ runtimecontext.NewScope
→ Runtime.ChatWithScope
```

可信 Scope 包含：

```text
tenant_id
app_id
revision_id
channel_type
channel_binding_id
storage_scope = t/{tenant_id}/a/{app_id}
```

`Scope.Validate` 会重新计算 Storage Scope。即使内部调用方构造：

```text
tenant_id = tenant-a
storage_scope = t/tenant-b/a/app-b
```

Runtime 也会在接触 Idempotency、Coordinator 和 Session 之前拒绝请求。

三个有状态边界现在分别使用：

```text
Session:
  storage_scope + runtime_user_id + session_id

Coordinator:
  storage_scope + runtime_user_id + session_id

Idempotency:
  storage_scope + channel_binding_id + runtime_user_id + session_id + message_id
```

Runner 构造时仍然可以共享，但每次调用通过 tRPC-Agent-Go `agent.WithAppName(scope.StorageScope)` 覆盖 AppName。因此两个租户即使使用相同的 `user_id` 和 `session_id`，也会进入不同 Session。

HTTP 响应会返回解析后的：

```json
{
  "tenant_id": "tutorial-tenant",
  "app_id": "tutorial-app",
  "revision_id": "tutorial-revision-1"
}
```

这些字段用于 Test Channel 调试。真实 IM Channel 不会允许回调请求覆盖它们。

## 15. Agent Revision Compiler 运行链路

Route Resolver 已经得到稳定 `revision_id`。Runtime 在获得 Idempotency Attempt 后、获取 Session Lease 前调用 Revision Compiler：

```text
Runtime Scope(revision_id)
→ ControlPlaneRepository.GetRevision(tenant_id, revision_id)
→ 校验 revision.tenant_id/app_id 与 Scope 一致
→ cache key = tenant_id + revision_id + checksum
→ cache hit：返回不可变 Agent
→ cache miss：singleflight 编译
    → 严格解析 agent_config
    → 解析 model_config
    → llmagent.New
    → 写入并发安全 cache
→ runner.Run(..., agent.WithAgent(compiledAgent))
```

编译发生在 Session Lease 之前，避免配置读取和模型客户端创建占用会话执行锁。Idempotency Attempt 在外层，因此同一条重复消息也不会触发多次实际 Runner 执行。

当前支持 `agent_type=llm`，Agent 配置包括：

```json
{
  "name": "tenant-specific-agent",
  "description": "Agent description",
  "instruction": "Tenant instruction",
  "stream": false
}
```

Model 有两种来源：

```text
source=startup_env
  → 复用进程启动时配置的 Model

source=revision
  → 根据 provider/name/base_url 创建 tRPC-Agent-Go Model
  → API Key 只保存环境变量名 api_key_env
  → 实际 Secret 从进程环境读取
```

Revision JSON 使用 `DisallowUnknownFields`。错误字段、缺少 name/instruction、未知 Agent 类型、Scope 不一致或 Secret 不存在都会在调用模型前失败。

同一 revision 的并发 cache miss 使用 `singleflight` 合并，避免多个 Worker goroutine 在一个进程里重复构造同一模型客户端。不同进程各自拥有本地不可变缓存，控制面仍是配置真相。

HTTP 响应新增实际执行的 `agent_name`。幂等 completed 结果也保存该字段，因此重放结果不会依赖当前最新 revision。

## 16. 持久化 Inbox 和 Transactional Outbox

同步 `/chat` 仍用于开发调试。新的 `/inbound` 模拟真实 IM callback：它不等待模型，而是先完成可靠入站事务。

```text
POST /inbound
→ binding_key 解析 tenant/app/revision
→ 生成稳定 conversation_id/inbound_id/request_id
→ BEGIN PostgreSQL transaction
→ INSERT conversation（首次会话固定 revision）
→ INSERT inbound_message（binding + external message 唯一）
→ conversation.last_turn_seq + 1
→ INSERT agent_run(status=queued)
→ INSERT queue_outbox(topic=agent.run)
→ COMMIT
→ HTTP 202 ACK
```

请求示例：

```json
{
  "binding_key": "tutorial-http",
  "message_id": "external-message-001",
  "user_id": "alice",
  "session_id": "durable-session",
  "chat_type": "direct",
  "message": "hello"
}
```

同一个 `channel_binding_id + message_id` 再次到达时，PostgreSQL 唯一索引保证只保留一个 inbound。Payload hash 相同则返回原来的 request；内容不同则返回冲突。

一次成功接收必须同时存在：

```text
1 conversation
1 inbound_message
1 agent_run
1 queue_outbox
```

任何一步失败都会回滚整个事务，Gateway 不返回成功 ACK。conversation 在第一次创建时写入 `pinned_revision_id`，后续即使 stable revision 改变，同一会话仍使用原 revision。

内存控制面对应 `MemoryJournal`，方便无 PostgreSQL 测试；PostgreSQL 控制面自动复用同一个连接池创建 `PostgresJournal`。

## 17. Outbox Relay、Redis Streams 和 Agent Worker

服务现在会启动两个后台循环：Outbox Relay 和 Agent Worker。

```text
queue_outbox(pending)
→ Relay 使用 FOR UPDATE SKIP LOCKED 领取一批
→ 标记 publishing + locked_by + locked_until
→ XADD Redis Stream
→ 标记 queue_outbox=published
→ Worker XREADGROUP
→ agent_run=running
→ Runtime.ChatWithScope(request_id)
→ Idempotency + Coordinator + Revision Compiler + Runner
→ agent_run=completed
→ inbound_message=processed
→ outbound_message=pending
→ XACK
```

Redis Stream 使用 Consumer Group。Worker 每次接收新消息前执行 `XAUTOCLAIM`，可以接管超过 `ClaimMinIdle` 的 pending delivery。Worker 在完成 PostgreSQL 事务后才 `XACK`。

Relay 的发布顺序是：

```text
先 XADD
后把 PostgreSQL outbox 标成 published
```

因此 relay 在两步之间崩溃可能导致重复 Stream 消息，但不会丢消息。重复 delivery 会被 Runtime 的 Redis `message_id` 幂等层转换成结果重放。

Worker 任务重试也采用 at-least-once：Retry 先重新 `XADD` 增加 attempt，再 ACK 旧 delivery。超过最大次数后保留 failed agent_run 并 ACK poison message。

Queue 配置：

```dotenv
TRPC_AGENT_QUEUE_BACKEND=redis
TRPC_AGENT_QUEUE_STREAM=agent-runs
TRPC_AGENT_QUEUE_GROUP=agent-workers
TRPC_AGENT_QUEUE_CONSUMER=worker-node-1
TRPC_AGENT_QUEUE_BLOCK_TIMEOUT=1s
TRPC_AGENT_QUEUE_CLAIM_MIN_IDLE=30s
TRPC_AGENT_QUEUE_MAX_LEN=100000
```

开发默认使用 Memory Queue；Gateway、Relay 和 Worker 分进程部署时必须使用 Redis Queue。

AgentTask 携带 Gateway 事务生成的稳定 `request_id`，Runtime 通过 tRPC-Agent-Go `agent.WithRequestID` 注入 Runner。因此数据库、Stream、Runner Event、outbound 和后续 trace 使用同一个请求 ID。

`agent_run` 和 `outbound_message` 更新检查 fencing token。Worker 即使在完成后、ACK 前崩溃，接管 Worker 只会重放幂等结果并幂等写入同一个 outbound。

## 18. Reply Sender 和 Channel Adapter

Worker 不直接调用外部 IM API。它只在完成事务中写入 `outbound_message(status=pending)`，Reply Sender 独立领取：

```text
outbound_message(pending)
→ FOR UPDATE SKIP LOCKED
→ sending + locked_by + locked_until + attempt_count
→ ControlPlane.GetChannelBinding
→ ChannelRegistry.Get(channel_type)
→ Adapter.Send
→ sent + provider_message_id + sent_at
```

当前提供 HTTP Test Adapter，它不会发起网络请求，只记录 Delivery Receipt，用于验证完整链路。统一 Adapter 接口包含通道类型、发送方法和能力描述；后续企业微信与 Telegram 实现相同接口。

Reply Sender 根据 `Capabilities.MaxTextRunes` 按 Unicode rune 切分长文本，避免按字节切坏中文或 emoji。Provider 错误可以通过 `DeliveryError` 标记 `Retryable` 和 `RetryAfter`。

发送失败时：

```text
可重试 + 未超过 MaxAttempts
→ outbound 退回 pending
→ 设置 next_attempt_at

不可重试或超过 MaxAttempts
→ outbound=dead
→ 保留错误类型和脱敏错误信息
```

最终异步闭环已经是：

```text
/inbound
→ PostgreSQL Inbox/Outbox
→ Redis Streams
→ Agent Worker
→ tRPC-Agent-Go Runner
→ outbound_message
→ Reply Sender
→ Channel Adapter
→ provider receipt
```

## 19. 进程角色拆分

同一个二进制现在支持六类运行角色：

```text
all      Gateway + Relay + Worker + Sender + Jobs + Admin
gateway  只启动 HTTP Gateway
relay    只转发 PostgreSQL queue_outbox
worker   只消费 Agent task
sender   只发送 outbound_message
jobs     只处理 Summary / Memory / Knowledge 后台任务
admin    只提供控制面 HTTP API
```

本地仍可使用：

```bash
go run ./cmd/trpc-service -role all
```

多进程运行：

```bash
go run ./cmd/trpc-service -role gateway -addr :8080
go run ./cmd/trpc-service -role relay
go run ./cmd/trpc-service -role worker
go run ./cmd/trpc-service -role sender
go run ./cmd/trpc-service -role jobs
```

也可以设置 `TRPC_AGENT_ROLE`。Relay 和 Worker 之间必须使用 Redis Queue；所有角色共享 PostgreSQL Control Plane/Journal，Worker 共享 Redis Session、Coordinator 和 Idempotency。

服务收到 SIGINT/SIGTERM 后，errgroup 会取消对应角色循环；Gateway 先执行 HTTP Shutdown，Redis blocking read、续租 goroutine、Relay 和 Sender 随 context 退出，最后按所有权关闭 Queue、Runtime 和数据库连接。

当前不同角色仍由同一装配函数创建依赖，后续 Kubernetes 阶段会进一步按角色最小化 Secret 和连接权限。

## 20. 企业微信和 Telegram Channel Adapter

本节描述已经落地的 Adapter 代码路径。Telegram 已使用真实 Bot、固定 Cloudflare Tunnel 完成私聊、群聊、Topic、去重和 Webhook 恢复联调；日常手动操作见 [`telegram-manual-runbook.md`](telegram-manual-runbook.md)。企业微信仍是本地模拟协议测试，尚未使用真实企业账号完成端到端联调。具体状态见 [`feature-status.md`](feature-status.md)。当前两个 Adapter 的出站能力都是文本消息，媒体 ID 解析不等于支持媒体发送。

统一回调地址：

```text
/callbacks/{channel_type}/{callback_key}
```

`callback_key` 先查询 Control Plane Channel Binding，URL 不暴露 tenant ID。Callback Gateway 根据 `channel_type` 从 Registry 找到 Adapter，Adapter 完成协议验签和解码后，Gateway 将每条消息写入持久化 Inbox，最后才返回协议 ACK。

企业微信实现包括：

```text
GET URL 验证
POST msg_signature 校验
timestamp 最大偏移校验
EncodingAESKey AES-CBC 解密
PKCS#7 padding 校验
CorpID receiver 校验
XML 文本消息解析
MsgId → external_message_id
Access Token 单飞缓存
应用消息 send API
无效 Token 刷新重试
45009 Retry-After 分类
```

企业微信 Binding Config 保存 Secret 引用而不是明文：

```json
{
  "corp_id": "ww123",
  "agent_id": 1000002,
  "callback_token_ref": "env://WECOM_CALLBACK_TOKEN",
  "encoding_aes_key_ref": "env://WECOM_ENCODING_AES_KEY",
  "app_secret_ref": "env://WECOM_APP_SECRET"
}
```

Telegram 实现包括：

```text
X-Telegram-Bot-Api-Secret-Token 校验
update_id → external_message_id
private/group/supergroup 识别
message_thread_id topic 隔离
chat/user ID 规范化
sendMessage
4096 限制前的 4000 rune 切分
429 retry_after
5xx 可重试分类
```

Telegram Binding Config：

```json
{
  "bot_token_ref": "env://TELEGRAM_BOT_TOKEN",
  "webhook_secret_ref": "env://TELEGRAM_WEBHOOK_SECRET"
}
```

原始 provider user/chat ID 不进入 Session Key。Callback Gateway 使用 binding-scoped SHA-256 生成 `runtime_user_id` 和 `session_id`；Reply Target 单独保存在受访问控制的消息流水中，用于 Sender 回送。当前应用层没有对该字段单独加密，生产部署需要依赖数据库静态加密、最小权限和保留期策略。

开发环境使用 `env://VARIABLE` Secret Store。Adapter、日志和 HTTP 错误不会打印 Secret 值；KMS、Vault 或云 Secret Manager Adapter 尚未实现，是生产接入前的后续工作。

## 21. Admin API 和 Revision 发布

Admin API 默认关闭：

```dotenv
TRPC_AGENT_ADMIN_ENABLED=true
TRPC_AGENT_ADMIN_TOKEN="至少 24 字符的随机 Token"
```

独立启动：

```bash
./bin/trpc-service -role admin -addr :8081
```

所有 `/admin/*` 请求要求 Bearer Token，比较使用常量时间函数。Token 只从环境或 Secret 注入，不写数据库和日志。

当前写接口：

```text
POST /admin/tenants
POST /admin/apps
POST /admin/revisions
POST /admin/revisions/publish
POST /admin/channel-bindings
POST /admin/backend-bindings
```

Admin Service 负责：

- 标识符、状态和 JSON 配置校验；
- JSON canonicalization；
- Revision checksum；
- 自动生成 revision/binding ID；
- Secret 只保存引用；
- 创建时间和 version；
- 发布时使用 expected app version 乐观锁。

Revision 发布不会修改旧 Revision，只更新 `agent_app.stable_revision_id` 并把 app version 加一。使用旧 version 再次发布返回 `409 Conflict`，因此两个管理员不会静默覆盖彼此的发布。

同一接口可以把 stable revision 指回历史 revision，实现配置回滚。后续审计阶段会为每次写操作记录操作者、旧值、新值、request ID 和 trace ID。

## 22. 租户级 Tool 治理

平台现在提供一个进程级 Tool Catalog：

```text
echo
current_time
dangerous_demo
```

Revision 不能注册任意 Go 代码，只能通过 `tool_policy.allowed_tools` 从 Catalog 中选择：

```json
{
  "allowed_tools": ["echo", "current_time", "dangerous_demo"],
  "dangerous_tools": ["dangerous_demo"],
  "denied_users": ["blocked-user"],
  "max_tool_calls": 4,
  "max_run_duration": "60s"
}
```

Revision Compiler 将 allowed tools 注入 `llmagent.WithTools`。每次 Run 又生成请求级治理选项：

```text
agent.WithToolFilter
agent.WithToolPermissionPolicy
agent.WithMaxRunDuration
```

执行顺序是：

```text
Catalog 中存在
→ Revision allowlist 可见
→ 用户没有被 deny
→ tool call 次数没有超预算
→ dangerous tool 已获得批准
→ 执行 Tool
```

危险工具未批准时返回 tRPC-Agent-Go `PermissionActionAsk`，模型收到结构化 `approval_required`；未授权和超预算返回 `PermissionActionDeny`。批准列表只存在可信 `ChatInput.ApprovedTools` 中，不接受普通用户消息直接声明“我已批准”。

Admin 创建 Revision 时会严格解析 Tool Policy，并拒绝 Catalog 中不存在的工具。这样错误配置不会等到真实用户触发 Tool 时才暴露。

`dangerous_demo` 本身没有外部副作用，只用于验证审批链路。后续接入真实业务 Tool/MCP 时，每个危险 Tool 还必须实施自己的不可绕过授权和业务幂等。

## 23. OpenTelemetry、审计和成本链路

现在一条消息不再只有业务结果，还会同时产生 trace、指标和审计事件。OpenTelemetry 默认关闭导出，但 W3C `traceparent` 传播始终启用；开发环境不启动 Collector 也不会影响聊天。

启用 OTLP/gRPC 导出：

```dotenv
TRPC_AGENT_OTEL_ENABLED=true
TRPC_AGENT_OTEL_SERVICE_NAME=trpc-agent-service
TRPC_AGENT_OTEL_ENDPOINT=127.0.0.1:4317
TRPC_AGENT_OTEL_INSECURE=true
TRPC_AGENT_OTEL_SAMPLE_RATIO=1
```

HTTP 中间件先提取调用方的 `traceparent`，再创建入口 span，并通过响应头返回 `X-Trace-ID`。Gateway 把 trace context 写入持久化 `AgentTask`，因此 Outbox Relay 和 Redis Streams 不会切断调用链。Worker 从任务恢复父上下文，创建 `worker.agent.run` span；审计记录使用同一个 trace ID。进程退出时会在五秒超时内 flush trace 和 metric provider。

```text
IM callback / HTTP
  → 提取或创建 trace_id
  → Gateway 持久化 AgentTask(traceparent)
  → Outbox Relay / Redis Streams
  → Worker 恢复父上下文
  → Runner / Model / Tool
  → Reply Sender
```

关键运行事件会写入统一 `audit.Writer`。InMemory 控制面使用内存 Writer，PostgreSQL 控制面写 `audit_log`：

```text
inbound_accepted / inbound_duplicate
run_completed / run_failed
tool_allow / tool_deny / tool_ask
reply_sent / reply_failed
admin_tenant_created / admin_app_created
admin_revision_created / admin_revision_published
admin_channel_binding_created / admin_backend_binding_created
```

审计字段包括 tenant、channel、user、session、message、request、trace、Agent、Revision、Tool、decision、latency、error type 和 cost。`details` 会递归屏蔽名称含 `secret`、`token`、`password`、`api_key`、`authorization`、`cookie` 的字段；工具参数本身不会进入权限审计。审计 Writer 不拥有共享 PostgreSQL 连接，关闭顺序不会提前断开控制面。

Runner Event 中的 `model.Usage` 会按“invocation + response ID”去重后汇总，避免同一个流式响应被重复计费。Revision 可在 `model_config` 中声明单价：

```json
{
  "source": "startup_env",
  "prompt_cost_per_million": 2.5,
  "completion_cost_per_million": 10
}
```

价格单位为每百万 token 的美元成本。结果写入 `agent_run.prompt_tokens`、`completion_tokens`、`cost`，同时写审计并输出以下租户维度指标：

```text
agent.inbound.messages
agent.idempotency.replays
agent.runs
agent.run.duration
agent.reply.deliveries
agent.reply.duration
agent.model.prompt_tokens
agent.model.completion_tokens
agent.model.cost
```

当前实现已经把 callback、Gateway、队列和 Worker 串成一个 trace。后续接入 Memory、Knowledge、Artifact 时，会在各 Storage Adapter 上继续创建读写 span；Reply Sender 的跨进程父上下文也会随 outbound 记录继续完善。

## 24. 可恢复的危险工具审批链路

`PermissionActionAsk` 现在不再只是模型看到的一段错误。平台会创建持久化 `tool_approval`，把审批编号附加到 IM 回复，并把后续“批准/拒绝”消息路由到审批状态机。

完整链路如下：

```text
模型请求 dangerous tool
  → Tool PermissionPolicy 判断需要审批
  → 写 tool_approval(status=pending)
  → 本轮 Runner 正常结束
  → Worker 查询本 request 的 pending approval
  → 回复中附加 approval_id 和操作命令
  → 用户在原 IM 会话回复“批准 apr_xxx”或“拒绝 apr_xxx”
  → Channel Adapter 完成验签和身份标准化
  → Approval Service 校验 tenant + binding + user
  → approved/denied 状态原子更新
  → 生成新的持久化 AgentTask
  → approved 任务携带不可伪造的批准凭证
  → Runner 继续执行或确认取消
```

支持的严格命令是：

```text
批准 apr_0123456789abcdef0123456789abcdef
同意 apr_0123456789abcdef0123456789abcdef
approve apr_0123456789abcdef0123456789abcdef
拒绝 apr_0123456789abcdef0123456789abcdef
deny apr_0123456789abcdef0123456789abcdef
reject apr_0123456789abcdef0123456789abcdef
```

只有整条消息符合命令格式才会进入审批逻辑。普通聊天中出现“帮我 approve 一下”不会被误判。批准者必须是发起请求的同一个标准化 IM 用户，而且必须来自同一 Channel Binding；跨租户、跨企业微信账号或另一个 Telegram Bot 的审批都会返回身份不匹配。

批准凭证不是只绑定工具名，而是绑定：

```text
tool_name + SHA-256(canonical tool arguments)
```

模型在恢复执行时如果更换参数，PermissionPolicy 会再次返回 `ask`，不会复用旧批准。数据库只保存参数哈希，不把可能含密钥和个人信息的完整工具参数写入审批表或审计日志。

`tool_approval` 的主要状态为：

```text
pending → approved
pending → denied
pending → expired
```

默认十五分钟过期。`decision_message_id` 在 Channel Binding 内唯一，用来处理 IM 重复投递；第一次决策成功后，同方向重复消息会复用第一次决策消息创建的 continuation，反向决策则冲突。`resumed_at` 表示 continuation 已经可靠写入 Inbox/Outbox 链路。即使进程在“更新审批状态”后崩溃，IM 平台重投同一消息时也会用稳定 ID 再次执行幂等入站，不会产生两个有效 Tool 调用。

当前文本确认流程已经通过 Adapter 级自动测试，且不依赖平台特有卡片；真实 IM 账号上的审批仍待联调。后续实现按钮卡片发送后才能启用 `SupportsCard`，按钮回调最终仍必须进入同一审批状态机，不能绕过身份、过期时间和参数哈希校验。

离线 `TutorialModel` 不会主动生成 Tool Call；要端到端观察审批，需要使用支持 function calling 的真实模型，并在 Revision 中把 `dangerous_demo` 同时放入 `allowed_tools` 和 `dangerous_tools`。`dangerous_demo` 没有真实副作用，只用于安全验证。

## 25. 租户级 Memory Router

Runner 现在接入了 tRPC-Agent-Go `memory.Service`，但实际对象是平台的 `MemoryRouter`。它不固定指向一个 Redis 或一张 SQL 表，而是从框架传入的 `AppName` 解析可信 Storage Scope：

```text
t/{tenant_id}/a/{app_id}
  → 严格解析 tenant_id / app_id
  → 查询 backend_binding(resource_type=memory)
  → app 级配置优先于 tenant 默认配置
  → 按 binding_id + version 构建并缓存 Memory Service
  → 转发 tRPC-Agent-Go memory.Service 调用
```

伪造的 `AppName`（例如只传 `tutorial-app`、路径穿越或另一个租户）会在访问后端前被拒绝。后端即使共享同一 Redis/SQL，Memory Key 中仍包含完整 tenant-scoped AppName。

当前支持三种 Memory 后端：

```json
{
  "resource_type": "memory",
  "backend_type": "inmemory",
  "config": {"memory_limit": 1000}
}
```

```json
{
  "resource_type": "memory",
  "backend_type": "redis",
  "config": {"key_prefix": "tenant-memory"},
  "secret_ref": "env://TENANT_MEMORY_REDIS_URL"
}
```

```json
{
  "resource_type": "memory",
  "backend_type": "postgres",
  "config": {
    "schema": "public",
    "table_name": "tenant_memories",
    "memory_limit": 10000
  },
  "secret_ref": "env://TENANT_MEMORY_POSTGRES_DSN"
}
```

Redis URL 和 PostgreSQL DSN 优先从 Secret Store 解析，控制面只保存引用。服务首次访问一个 binding 时才建立连接并探测后端；同一版本被所有并发请求复用。发布新 binding version 后会创建新实例，旧实例保留到进程退出，避免关闭仍在执行中的请求。

平台 Tool Catalog 已直接注册 tRPC-Agent-Go 提供的六个 Memory Tool：

```text
memory_add
memory_update
memory_delete
memory_clear
memory_search
memory_load
```

Revision 仍需在 `tool_policy.allowed_tools` 中显式开放。建议把 `memory_delete` 和 `memory_clear` 同时加入 `dangerous_tools`，从而复用上一节的持久化审批。Memory Tool 从 tRPC-Agent-Go Invocation Context 获取当前 Memory Service、AppName 和 UserID，调用最终仍经过 `MemoryRouter`，普通模型参数无法指定另一个租户。

如果希望每次模型调用自动带上最近 Memory，可以在 Agent Config 设置：

```json
{
  "name": "support-agent",
  "instruction": "Use durable memory when useful.",
  "preload_memory": 10
}
```

`preload_memory=0` 表示不自动注入，但 Agent 仍可显式调用 `memory_search` / `memory_load`。Memory 在 `AddMemory` 返回成功后已对其他 Worker 可见：InMemory 只保证单进程，Redis 和 PostgreSQL 提供跨节点可见性。真实 Redis 和 PostgreSQL adapter 均来自 tRPC-Agent-Go v1.11 系列，平台层只负责租户路由、Secret 注入和生命周期。

测试覆盖 InMemory、miniredis、真实 PostgreSQL，以及伪造 Storage Scope 拒绝。PostgreSQL 集成测试可手工运行：

```bash
TEST_POSTGRES_URL='postgres://...' \
  go test ./trpcservice/storage -run TestMemoryRouterPostgresIntegration -v
```

## 26. Artifact Router 与 S3 / MinIO

Runner 同时接入了 tRPC-Agent-Go `artifact.Service`。Artifact Router 和 Memory Router 使用相同的租户解析、Backend Binding 优先级、binding version cache 与 Secret Store，但路由键增加到：

```text
AppName + UserID + SessionID + Filename + Version
```

当前后端：

```json
{
  "resource_type": "artifact",
  "backend_type": "inmemory",
  "config": {}
}
```

```json
{
  "resource_type": "artifact",
  "backend_type": "s3",
  "config": {
    "bucket": "trpc-agent-artifacts",
    "endpoint": "http://minio:9000",
    "region": "us-east-1",
    "path_style": true,
    "retries": 3
  },
  "secret_ref": "env://TENANT_ARTIFACT_S3_CREDENTIALS"
}
```

本地 Secret 的值是 JSON，不进入控制面和日志：

```dotenv
TENANT_ARTIFACT_S3_CREDENTIALS='{"access_key_id":"...","secret_access_key":"...","session_token":""}'
```

S3 adapter 使用 tRPC-Agent-Go 官方实现，兼容 AWS S3、MinIO、R2 和其他 S3-compatible 服务。对象键以 tenant-scoped AppName 开头，例如：

```text
t/tenant-a/a/app-a/user-a/session-a/report.pdf/0
t/tenant-a/a/app-a/user-a/session-a/report.pdf/1
```

因此共享 bucket 中的租户对象天然分前缀，Router 又会拒绝非法 AppName。版本从 0 开始；Load 不指定版本时取最新版本，Delete 删除一个逻辑文件的全部版本。

官方 S3 Artifact Service 使用“列举版本 → 计算下一个版本 → 上传”的方式。平台在进程内为同一逻辑文件加 mutex；当控制面使用 PostgreSQL 时，还会在专用数据库连接上获取 `pg_advisory_lock(hash(artifact key))`，覆盖多个 Worker 节点。锁一直持有到上传结束，取消后使用独立两秒 Context 释放，避免连接返回池时仍携带 session-level lock。正常 Agent Tool 路径还受到 Session Coordinator 的会话租约保护。

本地 Compose 已加入 MinIO 和一次性 `minio-init`，会创建测试 bucket：

```bash
docker compose up -d minio minio-init

TEST_S3_ENDPOINT=http://127.0.0.1:9000 \
  go test ./trpcservice/storage -run TestArtifactRouterS3Integration -v
```

官方 S3 adapter 本身要求 Go 1.22.2；后续 Qdrant adapter 要求 Go 1.24.0，因此项目统一使用 Go 1.24.0。生产环境应开启 bucket versioning、服务端加密、生命周期清理、恶意文件扫描和最小权限 IAM；公开下载应通过短期签名 URL 或受鉴权代理，不能直接暴露 bucket。

## 27. Knowledge Router 与 Qdrant

Agent Revision 的 `knowledge_config` 现在会在编译时生成真正的 tRPC-Agent-Go `knowledge.Knowledge`，并通过 `llmagent.WithKnowledge` 注入。框架自动增加 `knowledge_search` Tool；平台的请求级 ToolFilter/PermissionPolicy 会把这个框架 Tool 加入当前 Run 的允许集合，但不会把它放进用户可注册的全局 Tool Catalog。

Revision 示例：

```json
{
  "enabled": true,
  "max_results": 5,
  "min_score": 0.2,
  "chunk_size": 800,
  "chunk_overlap": 100,
  "embedding": {
    "provider": "openai",
    "model": "text-embedding-3-small",
    "base_url": "https://api.openai.com/v1",
    "dimensions": 1536,
    "secret_ref": "env://TENANT_EMBEDDING_API_KEY"
  }
}
```

离线学习可以把 provider 改为 `hash`。Hash Embedder 是确定性的归一化哈希向量，不访问网络，只适合测试路由、切块和权限，不代表生产检索质量。

Backend Binding 可选择进程内向量库或 Qdrant：

```json
{
  "resource_type": "knowledge",
  "backend_type": "qdrant",
  "config": {
    "host": "qdrant",
    "port": 6334,
    "tls": false,
    "collection_name": "tenant_documents",
    "dimensions": 1536,
    "max_results": 20
  },
  "secret_ref": "env://TENANT_QDRANT_API_KEY"
}
```

向量维度必须和 Embedder 完全一致。Router 以 `binding_id + binding_version + revision_checksum` 缓存 Vector Store/Embedder，Revision 配置或后端 binding 变化会得到新实例。

文档写入时平台执行：

```text
验证 tenant/app/revision
→ 删除同 source_document_id 的旧 chunks
→ 按 rune 切块并保留 overlap
→ 计算 embedding
→ 生成不可猜测的 tenant/app scoped chunk ID
→ 强制写 metadata.tenant_id / app_id / source_document_id / chunk_index
→ VectorStore.Add
```

搜索时，即使模型或调用方在 SearchFilter 中伪造另一个 `tenant_id` / `app_id`，Scoped Knowledge 也会丢弃这两个外部值并写回当前 Scope。共享 Qdrant collection 因此仍有强制租户过滤；文档 ID 本身也包含 tenant/app 的 SHA-256 派生值。

当前 Admin 写入口：

```text
POST /admin/knowledge/documents
POST /admin/knowledge/documents/delete
```

写入请求示例：

```json
{
  "tenant_id": "tenant-a",
  "app_id": "support-agent",
  "revision_id": "revision-3",
  "document_id": "refund-policy",
  "name": "退款政策",
  "content": "完整文档文本",
  "metadata": {"category": "policy"}
}
```

Compose 已加入 Qdrant。真实集成测试：

```bash
docker compose up -d qdrant

TEST_QDRANT_HOST=127.0.0.1 TEST_QDRANT_PORT=6334 \
  go test ./trpcservice/storage -run TestKnowledgeRouterQdrantIntegration -v
```

官方 Qdrant adapter 要求 Go 1.24，因此项目最低版本提升为 Go 1.24.0。Admin 文档写入在 InMemory Job Repository 下可同步验证；生产 PostgreSQL 模式会返回 durable job。

## 28. Durable Background Job

平台现在有独立于 Agent Run Queue 的 `background_job`。它用于耗时较长、允许最终一致、但不能因进程退出而丢失的任务：

```text
session_summary
memory_extract
knowledge_upsert
knowledge_delete
```

PostgreSQL Job Queue 使用 `FOR UPDATE SKIP LOCKED` 抢占任务，字段包括 job ID、tenant/app/revision、type、dedupe key、JSON payload、attempt/max attempts、next attempt、lock owner/lease、last error、traceparent 和完成时间。Worker 崩溃后，`locked_until` 到期即可被其他节点重新领取；失败按指数退避，超过上限进入 `dead`。

Agent Worker 在 `agent_run + outbound_message` 完成后，为同一个 conversation turn 写入 Summary 和 Memory Job：

```text
Agent 完成 Session Event 持久化
→ agent_run completed / outbound pending
→ Enqueue session_summary(dedupe=conversation_id:turn_seq)
→ Enqueue memory_extract(dedupe=conversation_id:turn_seq)
→ ACK Agent Queue
```

如果 Job 入队失败，Agent Task 不会 ACK。重试时 Runtime 命中消息幂等结果，`CompleteRun` 与 Background Job dedupe 都不会重复产生业务副作用。

Revision 的 Agent Config 控制 Summary 周期：

```json
{
  "name": "support-agent",
  "instruction": "Help the user.",
  "summary_every_turns": 10
}
```

到达水位后，Job Processor 重新从共享 Session 后端读取完整 Session，调用配置在 tRPC-Agent-Go Session Service 上的 `SessionSummarizer`，再由 Session backend 原子保存 Summary 与 cutoff boundary。未到水位的 Job 直接完成，不调用模型。

Memory Config 控制自动抽取：

```json
{
  "auto_extract": true,
  "every_turns": 5
}
```

Memory Job 只读取 `memory:last_extract_at` 之后的 Session Event，使用 tRPC-Agent-Go `memory/extractor` 生成操作。自动路径只允许幂等的 `memory_add`，不会让后台模型自行执行 clear/delete；写入全部成功后才推进 Session State 水位。若在写入后、水位前崩溃，重试的 Add 仍由 Memory backend 的 canonical ID 保证幂等。

Knowledge Admin API 现在默认返回 `202 Accepted`：

```json
{
  "job_id": "job_...",
  "document_id": "refund-policy",
  "queued": true,
  "duplicate": false
}
```

Upsert 在未提供 `operation_id` 时使用完整 payload SHA-256 去重；Delete 必须提供 `operation_id`，避免一次旧删除永久阻止未来重新上传后的删除。

管理接口：

```text
POST /admin/jobs/get
POST /admin/jobs/retry
```

`retry` 只允许把 `dead` 任务恢复到 pending，且 attempt 重置；运行中或已完成任务返回 `409 Conflict`。Job 查询按 tenant ID 强制过滤。

生产中建议独立启动：

```bash
./bin/trpc-service -role jobs
```

Job payload 保存 W3C `traceparent`，Processor 恢复父上下文后创建 `background.{job_type}` span，审计记录完成/失败、attempt、latency 和 error type。`context.Context` 取消会终止模型/Embedding 调用；状态 finalize 使用独立三秒 Context，尽量在进程退出前释放 claim 或记录失败。

## 29. Backend Migration 状态机

平台不通过直接修改一个 Backend Binding 来切库，而是创建独立、带版本的 `backend_migration`：

```text
planned
  → dual_write
  → backfill
  → verify
  → cutover
  → completed

dual_write/backfill/verify/cutover
  → rollback
  → rolled_back

任意运行阶段 → failed
```

Migration 固定 source/target binding。Source 必须是当前 `active`，Target 必须使用 `migration_target`；migration 期间数据库允许同一 app/resource 同时存在两条 binding，但 partial unique index 仍保证最多一个 `active`。

管理接口：

```text
POST /admin/backend-migrations
POST /admin/backend-migrations/get
POST /admin/backend-migrations/transition
POST /admin/backend-migrations/backfill-memory
POST /admin/backend-migrations/verify-memory
```

所有 transition 都要求 `expected_version`。合法路径由 Admin Service 检查，跳过 backfill/verify 或两个管理员并发更新都会失败。进入 cutover 前，Migration 的 verification 必须包含 `passed=true`。

Memory Router 根据状态实时改变读写策略：

| 状态 | 读取 | 写入 |
| --- | --- | --- |
| `planned` | source | source |
| `dual_write` / `backfill` / `verify` | source | source → target |
| `cutover` | target | target → source |
| `completed` | target（成为 active） | target |
| `rollback` / `rolled_back` | source | source |

切读后仍反向双写 source，给快速回滚留出窗口。`completed` transition 在同一 PostgreSQL 事务中把 source 标成 `retired`、target 标成 `active`；`rolled_back` 则恢复 source active。Router 使用 binding version 作为 cache key，因此状态完成后的新请求不会继续使用旧实例。

Memory backfill 请求示例：

```json
{
  "tenant_id": "tenant-a",
  "migration_id": "migration-memory-01",
  "operation_id": "batch-0001",
  "user_ids": ["user-a", "user-b"]
}
```

Backfill Job 从 source 读取用户全部有效 Memory，保留 fact/episode metadata 后用 canonical Add 写 target，再比较 source/target 的 ID、内容和数量。Verify Job 对指定用户集合再次比较；全部通过后由 Job Processor 自己把状态从 `backfill` 更新到 `verify` 并写 verification，操作者不需要手工伪造结果。

Memory 和 Knowledge 的在线写都会在迁移阶段双写。Secondary 失败时当前调用返回可重试错误，同时 `repair_backlog` 原子加一；Background Job 或 Agent 消息重试依靠 canonical ID / document replacement 保持幂等。Verify 全部通过后 repair backlog 清零。`backend_migration` 查询会显示 checkpoint、verification、repair backlog、state 和 version。

Knowledge 迁移对新 Upsert/Delete 实施同样的源读双写和目标读反向双写。历史 Knowledge 回填应从原始文档源重新提交 Knowledge Job，而不是从向量反推文本；这能同时重算新 Embedder/新维度。InMemory → Qdrant 或 Qdrant collection 迁移因此可以使用相同状态机。

生产切换顺序：

```text
创建 migration_target
→ planned 健康检查
→ dual_write 观察 secondary 错误
→ backfill 分批提交 user/document job
→ verify，repair_backlog 必须为 0
→ cutover，观察读延迟与命中率
→ 保留反向双写回滚窗口
→ completed
```

状态机、active binding swap、Memory 双写/切读、repair backlog 和 PostgreSQL 索引均有单测与真实 PostgreSQL 集成测试。

## 30. Tenant Session Router

Runner 现在不再直接持有启动时选择的单一 Session Service，而是持有实现完整 `session.Service` 接口的 `SessionRouter`。启动 Session 仍作为 `startup_config` 后端保留，已有 `.env` 配置完全兼容；每个 tenant/app 可以用 Backend Binding 覆盖：

```json
{
  "resource_type": "session",
  "backend_type": "redis",
  "config": {
    "key_prefix": "tenant-a-session",
    "ttl": "720h"
  },
  "secret_ref": "env://TENANT_A_REDIS_URL"
}
```

```json
{
  "resource_type": "session",
  "backend_type": "postgres",
  "config": {
    "table_prefix": "tenant_a_runtime",
    "ttl": "0s"
  },
  "secret_ref": "env://TENANT_A_POSTGRES_DSN"
}
```

Router 覆盖框架完整接口：Create/Get/List/Delete Session、App/User/Session State、AppendEvent、Summary 创建/读取。每次操作先从 AppName 解析 tenant/app，再选择 Backend Binding；非法 AppName 不会落到默认库。`/readyz` 的内部探测 AppName 是唯一例外，只用于探测 startup backend。

这使 Worker 真正无状态：

```text
Worker A 收到 turn 1
→ SessionRouter → tenant Redis/PostgreSQL
→ Event/State 同步持久化

Worker B 收到 turn 2
→ 相同 Storage Scope
→ 从共享后端恢复历史
```

不需要 HTTP sticky session。Session Coordinator 只负责同一 Session 的执行顺序，Session Router 负责状态存储；二者都使用 `t/{tenant}/a/{app} + user + session`。

Session migration 使用与 Memory 相同的状态：

```text
dual_write/backfill/verify：source 读，source → target 写
cutover：target 读，target → source 写
completed：target active
rollback：source 读写
```

双写覆盖 Session/Event、App State、User State、Session State 和 Summary。向一个尚不存在的 target Session AppendEvent 时，Router 会先用当前 State 创建 target Session，再追加 Event；secondary 失败会增加 migration repair backlog 并把本次操作作为可重试错误返回。

历史回填接口：

```text
POST /admin/backend-migrations/backfill-session
POST /admin/backend-migrations/verify-session
```

请求明确列出批次中的 Session：

```json
{
  "tenant_id": "tenant-a",
  "migration_id": "migration-session-01",
  "operation_id": "batch-0001",
  "sessions": [
    {"user_id": "user-a", "session_id": "chat-01"},
    {"user_id": "user-b", "session_id": "chat-02"}
  ]
}
```

Backfill Job 复制 Session State 和完整 Event 顺序，同时复制 App/User State；源存在 Summary 时在 target 上重新生成 Summary。Verify 比较 Event 数量、State 字节值和 Summary 是否同时存在。所有 Session 通过后，Job Processor 写入 `verification.passed=true` 并推进到 verify，才允许 cutover。

当前正式支持 `startup_config`、InMemory、Redis 和 PostgreSQL 四种 Session binding。Redis 与 PostgreSQL 均关闭异步 Event persist，`AppendEvent` 成功意味着下一节点可见；Redis 的磁盘持久性仍取决于 AOF/RDB 策略，PostgreSQL 提供更强耐久性但写延迟和成本更高。

## 31. RBAC、限流、预算与日志脱敏

Admin 不再只有一个全局共享 Token。兼容字段 `TRPC_AGENT_ADMIN_TOKEN` 仍会创建 legacy superadmin；生产推荐一次注入多个 Principal：

```dotenv
TRPC_AGENT_ADMIN_PRINCIPALS_JSON='[
  {
    "name":"platform-admin",
    "token":"replace-with-secret-token-1",
    "role":"superadmin",
    "tenant_ids":["*"]
  },
  {
    "name":"tenant-a-operator",
    "token":"replace-with-secret-token-2",
    "role":"operator",
    "tenant_ids":["tenant-a"]
  }
]'
```

角色：

| 角色 | 权限 |
| --- | --- |
| `superadmin` | 创建租户和所有跨租户操作 |
| `tenant_admin` | 指定租户内配置写、运维和读 |
| `operator` | 指定租户内 migration/job 运维和读 |
| `auditor` | 指定租户只读、审计查询 |

Token 使用常量时间比较。每个路由在 JSON 解码后、Repository 调用前检查 tenant scope；知道另一个租户的 app/revision/job ID 也会先得到 `403`。Principal 名称写入 Admin audit 的 `user_id`。

只读管理接口包括：

```text
POST /admin/tenants/get
POST /admin/apps/get
POST /admin/revisions/get
POST /admin/channel-bindings/get
POST /admin/backend-bindings/list
POST /admin/backend-migrations/get
POST /admin/jobs/get
POST /admin/audit/query
```

Audit Query 必须带 tenant ID，可按 decision、trace ID 和 limit 过滤；PostgreSQL 使用 tenant 条件查询，Memory Writer 也执行同样隔离。

租户 Quota 保存在 `tenant.quota_config`：

```json
{
  "requests_per_minute": 120,
  "concurrent_runs": 20,
  "daily_prompt_tokens": 5000000,
  "daily_completion_tokens": 1000000,
  "daily_cost_usd": 100
}
```

`TRPC_AGENT_QUOTA_BACKEND=local` 用于单进程教程；生产设置为 `redis`，复用 `REDIS_URL` 和 `REDIS_KEY_PREFIX`。Gateway 在持久化新入站前按 tenant/user/minute 限流；Worker 在 Runner 前检查 tenant 并发和当日已用 token/cost。同步 `/chat` Test Channel 同样执行两层检查。

并发计数使用 Redis Lua 原子 INCR/回滚并设置五分钟兜底 TTL，进程崩溃不会永久占槽。Usage 用 `tenant + UTC date + request_id` SET NX 幂等记账，因此 Agent Queue 重试不会重复增加成本。一次调用可以把余额推过阈值，但下一次 Runner 会被拒绝；这是“已发生调用必须记账、未来调用 fail closed”的边界。

运行日志经过统一 Redacting Writer，当前会屏蔽：

```text
Authorization / Bearer
api_key / access_token / password / secret
postgres://user:password@host
redis://user:password@host
```

Writer 向标准 `log` 保持原始写入长度语义，避免破坏调用方；Audit details 仍使用独立的结构化 key 递归脱敏。生产还应在 Collector 和日志平台再做一层字段过滤，并禁止记录完整用户消息和 Tool 参数。

## 32. 生产部署与可观测栈

仓库现在可以构建一个非 root 多阶段镜像，镜像同时包含服务进程和 migration 命令：

```bash
docker build -t trpc-agent-service:local .
docker run --rm trpc-agent-service:local -role worker
```

本地完整依赖：

```bash
docker compose up -d postgres redis minio minio-init qdrant
```

可观测栈使用 Compose profile，包含 OTel Collector、Prometheus、Tempo 和 Grafana：

```bash
docker compose --profile observability up -d
```

应用配置 `TRPC_AGENT_OTEL_ENDPOINT=127.0.0.1:4317`。Collector 将 metric 暴露到 9464 给 Prometheus，把 trace 发送到 Tempo；Grafana 已自动配置两个数据源。

不需要手工拼请求时，可以直接运行端到端验收脚本：

```bash
./scripts/e2e-observability.sh
```

它会验证 Trace ID 传播、Tempo 中的 HTTP/Session spans、租户级平台指标以及 Prometheus/Grafana 健康状态，并在结束后停止测试进程和可观测容器。

Kubernetes 清单位于 `deploy/kubernetes`：

```text
platform.yaml          Namespace / ConfigMap / 6 Deployments / Services / HPA / PDB / NetworkPolicy
secret.example.yaml    Secret 字段模板，不能直接用于生产
migration-job.yaml     独立 trpc-migrate Job
```

生产必须先由 Secret Manager 生成 `trpc-agent-secrets`，再运行 migration Job，最后发布 Gateway/Admin/Relay/Worker/Sender/Jobs。Pod 全部 non-root、只读根文件系统、drop capabilities，并设置 requests/limits；Gateway/Worker 配有 PDB，Gateway/Worker/Jobs 配有 HPA。

仓库还提供标准库实现的压测器：

```bash
./bin/trpc-loadgen -requests 10000 -concurrency 200 -sessions 1000
```

它输出 throughput、p50/p95/p99/max。入站 ACK 压测后还需等待 Redis Stream lag、background pending、outbound pending 回到 0，才能得到完整处理能力。

故障演练脚本默认拒绝运行，必须显式确认：

```bash
TRPC_AGENT_DRILL_CONFIRM=yes ./scripts/fault-drill.sh
```

脚本只 stop/start Compose 的 Redis/PostgreSQL，不删除 volume。应观察 `/readyz` 退出就绪、依赖恢复后任务 reclaim、幂等结果、repair backlog 和审计记录。

完整说明见 [deployment.md](deployment.md) 和 [capacity.md](capacity.md)。

## 33. 最终链路加固

最终验收补齐了四个容易被忽略的边界。

第一，Reply Sender 现在继续原 trace。Worker 把当前 W3C `traceparent` 写入 outbound payload，Sender claim 后恢复父上下文并创建 `reply.send` span。因此一条 trace 可以覆盖：

```text
HTTP/IM callback
→ Gateway persist
→ Redis Agent Queue
→ Worker / Runner / Model / Tool
→ Session / Memory / Knowledge / Artifact spans
→ outbound_message
→ Reply Sender / IM API
```

第二，Agent 灰度策略真正参与路由：

```json
{
  "canary_revision_id": "revision-2026-09-03",
  "canary_percent": 10,
  "salt": "rollout-wave-1"
}
```

Admin 使用 `POST /admin/apps/rollout` 和 expected version 发布。Resolver 以 tenant/app/salt/user/session 做稳定哈希；同步 Test Channel 每轮稳定命中同一 bucket，异步 IM 的 conversation 在首轮写入 `pinned_revision_id`，即使随后把 canary 调回 0，已有会话也不跳版本。

第三，Revision Guardrail 使用 tRPC-Agent-Go Model Callbacks：

```json
{
  "max_input_chars": 8000,
  "blocked_input_patterns": ["(?i)drop\\s+database"],
  "redact_output_patterns": ["sk-[A-Za-z0-9_-]+"]
}
```

BeforeModel 在调用供应商前阻断输入，AfterModel 克隆并脱敏 Response，不修改共享对象。Go regexp 使用 RE2。Tool 另有 `tool_execution` journal：BeforeTool 记录 request/tool_call/arguments hash，AfterTool 保存 succeeded/failed 和 result hash；相同 request/tool_call 再次出现时 fail closed，要求查询下游或人工对账，不盲目重放副作用。

第四，企业微信和 Telegram 会识别图片/文件消息。默认只把受信任 provider media/file ID 与文件名/MIME/caption 转成占位文本，不自动访问 URL。真正下载必须经过独立 Artifact downloader、大小/MIME/病毒扫描；这避免 callback 直接触发 SSRF。

## 34. 验收方式

快速代码验收：

```bash
go test -race ./...
go vet ./...
./lint.sh
./build.sh
docker compose --profile observability config -q
docker build -t trpc-agent-service:local .
```

真实多进程验收：

```bash
./scripts/e2e-multiprocess.sh
```

脚本启动 PostgreSQL/Redis 和 Gateway/Relay/Worker/Sender/Jobs。第一轮由 Worker A 保存“我叫小明”，随后终止 A，第二轮由 Worker B 从共享 Redis Session 恢复姓名；最终检查 PostgreSQL outbound sent 和 Background Job 无 dead/failed。脚本只 stop 容器，不删除 volume。

主要交付物入口：

```text
docs/architecture.md             系统架构图与组件边界
docs/sequence.md                 企业微信核心时序图
docs/data-model.md               核心表和实际 migration
docs/data-consistency.md         同步、幂等、双写和恢复
docs/backend-adapters.md         Redis/SQL/Qdrant/S3 选型
docs/im-channels.md              企业微信/Telegram 差异
docs/governance-operations.md    Guardrail、审计、指标、安全
docs/risks.md                    20 项生产风险与门禁
docs/deployment.md               Compose/Kubernetes/灰度/备份
docs/capacity.md                 容量公式和压测器
```
