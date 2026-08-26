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

当前示例不需要模型 API Key，也不需要 Redis。它使用确定性的本地 Mock Model 和 InMemory Session，适合用来观察 tRPC-Agent-Go 的基本工作方式。

## 1. 启动服务

环境要求：

- Go 1.21 或更高版本；
- `curl`；
- 端口 8080 未被占用。

直接运行：

```bash
go run ./cmd/trpc-service
```

看到下面的输出表示服务已经启动：

```text
trpc-agent-service 0.1.0
model provider=mock name=tutorial-mock-model stream=false
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
  "user_id": "alice",
  "session_id": "demo",
  "event_count": 7
}
```

这里有四个重要概念：

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
  "user_id": "alice",
  "session_id": "demo",
  "event_count": 7
}
```

TutorialModel 本身没有保存用户资料。它只检查 tRPC-Agent-Go 传给模型的 `request.Messages`。第二轮能回答名字，是因为 Runner 根据 `user_id + session_id` 找回了第一轮 Session，并将历史消息加入模型请求。

## 5. 换一个 Session

只修改 `session_id`：

```bash
curl -sS -X POST http://127.0.0.1:8080/chat \
  -H 'Content-Type: application/json' \
  -d '{
    "user_id": "alice",
    "session_id": "another-session",
    "message": "我叫什么？"
  }'
```

这次返回：

```json
{
  "reply": "我还不知道你的名字。你可以告诉我：我叫小明。",
  "user_id": "alice",
  "session_id": "another-session"
}
```

框架中的完整 Session Key 是：

```text
AppName + UserID + SessionID
```

本示例固定使用：

```text
AppName = tutorial-app
UserID = HTTP 请求中的 user_id
SessionID = HTTP 请求中的 session_id
```

只要其中一个值变化，就会进入另一份会话历史。

## 6. 已经实现了什么：从启动到返回答案

这一节按真实执行顺序解释当前代码。读完后，你应该能回答下面几个问题：

- `.env` 是在哪里读取的？
- Mock Model 和真实模型在哪里切换？
- `LLMAgent`、`Runner` 和 `Session` 是在哪里创建的？
- `/chat` 收到 JSON 后，怎样进入 `runner.Run`？
- 为什么 Runner 返回 Event channel，而不是直接返回字符串？
- 第二轮对话为什么能够看到第一轮内容？

### 6.1 先看整体分层

当前实现分成四层：

```text
cmd/trpc-service
  负责启动、装配和关闭进程

trpcservice/config
  负责读取 .env 并校验模型配置

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
agent.Runtime.Chat
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

### 6.7 Runtime 如何创建 Agent、Runner 和 Session

核心装配在 [`trpcservice/agent/runtime.go`](../trpcservice/agent/runtime.go) 的 `NewRuntime`：

```go
sessionService := inmemory.NewSessionService()

agentInstance := llmagent.New(
    tutorialAgentName,
    llmagent.WithModel(selectedModel),
    llmagent.WithInstruction("..."),
    llmagent.WithGenerationConfig(
        model.GenerationConfig{Stream: stream},
    ),
)

runnerInstance := runner.NewRunner(
    tutorialAppName,
    agentInstance,
    runner.WithSessionService(sessionService),
)
```

这三个对象的职责不同：

| 对象 | 当前职责 |
| --- | --- |
| `session.Service` | 保存用户消息、assistant 消息和会话状态 |
| `LLMAgent` | 组织提示词、模型调用和将来的 Tool 循环 |
| `runner.Runner` | 管理一次运行、Session 读写、request ID 和 Event 流 |

这些类型都来自 tRPC-Agent-Go。我们写的 `Runtime` 是一层很薄的应用封装，把框架对象组合成 HTTP 层容易调用的 `Chat` 方法。

当前 Session 使用：

```go
inmemory.NewSessionService()
```

所以数据只存在本进程内。后续换成 Redis 时，主要变化发生在 `sessionService` 的创建处，LLMAgent、Runner 调用和 HTTP Handler 都可以保持不变。

`tutorialAppName` 固定为 `tutorial-app`。完整 Session Key 是：

```text
tutorial-app + user_id + session_id
```

`NewDemoRuntime` 是测试和本地教学使用的便捷函数，它固定选择 TutorialModel。正式启动路径使用 `BuildModel + NewRuntime`，因此可以由 `.env` 选择真实模型。

### 6.8 一次 `Chat` 调用发生了什么

HTTP 层最终调用 `Runtime.Chat`：

```go
events, err := r.runner.Run(
    ctx,
    userID,
    sessionID,
    model.NewUserMessage(text),
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

这些步骤主要由 tRPC-Agent-Go 完成。我们的 `Runtime.Chat` 没有手工查询 Session，也没有自己拼历史 messages。

当前 Runtime 用一个全局 `sync.Mutex` 包住 `runner.Run` 和 Event 消费。它是教学阶段的保护措施，避免两个请求同时更新 InMemory Session。它也意味着所有会话暂时串行执行，所以不适合生产。后续会先改成按 Session 加锁，再升级为跨节点租约。

### 6.9 为什么返回 Event channel

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

### 6.10 HTTP Handler 做了什么

HTTP 入口在 [`trpcservice/web/handler.go`](../trpcservice/web/handler.go)。它注册两个路由：

```text
GET  /healthz
POST /chat
```

`/chat` 接受：

```json
{
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
→ 检查 user_id、session_id、message
→ 调用 ChatService.Chat
→ 返回 chatResponse
```

HTTP 层只依赖一个小接口：

```go
type ChatService interface {
    Chat(
        ctx context.Context,
        userID string,
        sessionID string,
        text string,
    ) (agent.ChatResult, error)
}
```

它不知道 Runtime 内部使用的是 InMemory 还是 Redis，也不知道模型来自 Mock 还是 OpenAI。这个边界以后可以保留：企业微信 Adapter、Telegram Adapter 或队列 Worker 都调用同一类应用服务。

`r.Context()` 会传入 Runtime 和 Runner。客户端断开、HTTP 超时或服务关闭时，context cancellation 能沿调用链传递到模型调用。

### 6.11 真实模型路径和 Mock 路径有什么不同

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

### 6.12 服务如何关闭

`main.go` 使用 `signal.NotifyContext` 监听 SIGINT 和 SIGTERM。按下 `Ctrl+C` 或执行 `./stop.sh` 后：

```text
收到退出信号
→ http.Server.Shutdown
→ 等待正在处理的 HTTP 请求
→ ListenAndServe 返回
→ Runtime.Close
→ Runner.Close
→ Session Service.Close
```

HTTP Shutdown 最多等待 10 秒。`Runtime.Close` 使用 `sync.Once`，重复调用不会重复关闭资源。

当前 Session Service 是由 Runtime 创建并持有的，所以 Runtime 要负责关闭它。后续引入 Redis、数据库和消息队列时，也应明确每个连接由谁创建、由谁关闭。

### 6.13 哪些代码是我们写的，哪些是框架提供的

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
| Runtime 和 Event 聚合 | 本项目对框架的应用封装 |

这就是目前阶段的主要成果：我们没有重写 Agent 框架，而是把 tRPC-Agent-Go 的 Model、LLMAgent、Runner、Session 和 Event 组织成了一条能通过 HTTP 实际调用、能切换真实模型、能验证多轮会话的最小链路。

### 6.14 推荐阅读顺序

如果是第一次看代码，按下面顺序阅读最容易：

1. [`trpcservice/web/handler.go`](../trpcservice/web/handler.go)：先看请求从哪里进入；
2. [`trpcservice/agent/runtime.go`](../trpcservice/agent/runtime.go)：看 Runner 怎样被调用；
3. [`trpcservice/agent/mock_model.go`](../trpcservice/agent/mock_model.go)：看 Model 接口如何实现；
4. [`trpcservice/agent/model_factory.go`](../trpcservice/agent/model_factory.go)：看真实模型怎样替换 Mock；
5. [`trpcservice/config/model.go`](../trpcservice/config/model.go)：看环境变量如何变成配置；
6. [`cmd/trpc-service/main.go`](../cmd/trpc-service/main.go)：最后看所有组件如何装配和关闭。

不要一开始深入 tRPC-Agent-Go 的所有内部实现。先沿着上面六个文件跟完一条请求，再根据兴趣进入框架源码。

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

也可以运行 race test：

```bash
go test -race ./...
```

## 8. 当前示例故意没有实现的部分

这不是生产服务。当前限制包括：

- Session 只存在当前进程内，重启后丢失；
- 默认 Mock Model 只识别几种固定句式，真实模型需要自行配置凭据；
- 所有 Chat 请求使用一个进程内锁串行执行；
- 没有 Redis、数据库或消息队列；
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

### 实验三：重启服务

告诉 Agent 名字后重启进程，再问名字。Agent 会忘记，因为当前使用 InMemory Session。这就是下一步需要 Redis Session 的原因。

## 10. 下一步

理解当前示例后，按下面顺序扩展：

```text
InMemory Session
→ Redis Session
→ 移除进程内全局锁，加入 session 级协调
→ HTTP 测试 Channel
→ 企业微信 Channel Adapter
→ 两个租户和 Storage Scope
→ Tool 和权限控制
→ 持久化队列及幂等
```

下一阶段最合适的代码任务是把 InMemory Session 替换为可配置的 InMemory/Redis 后端，并用两个服务进程验证会话可以跨节点共享。
