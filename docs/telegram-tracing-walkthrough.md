# 看清一条 Telegram 消息的执行过程

前面的审计回答“审批有没有改变、工具有没有执行”。Trace 则帮助定位“这条消息经过了哪些组件、时间花在哪里、故障在哪一段”。两者互补：看到 `execute_tool` span 并不代表工具成功，因为权限检查也在这个 span 内；实际执行仍以 Tool Journal 为准。

## 1. 当前准备状态

本机 `.env` 已启用追踪，仍然手动启动服务，没有配置开机自启：

```dotenv
TRPC_AGENT_OTEL_ENABLED=true
TRPC_AGENT_OTEL_SERVICE_NAME=trpc-agent-service
TRPC_AGENT_OTEL_ENDPOINT=127.0.0.1:4317
TRPC_AGENT_OTEL_INSECURE=true
TRPC_AGENT_OTEL_SAMPLE_RATIO=1
```

这里的 insecure 指本机 OTLP 连接不启用 TLS，不是关闭 Telegram 验签。采样率 1 便于开发验收；生产环境需按容量、安全和保留策略调整。

Collector、Tempo、Prometheus 和 Grafana 已在本地启动，端口仅绑定 `127.0.0.1`，不通过 Telegram 的公网域名开放。已有模型配置、Agent Revision、Bot Binding 和历史会话没有被替换。

电脑重启后，在启动 Agent 前，手动启动依赖：

```bash
docker compose --profile observability up -d \
  postgres redis tempo otel-collector prometheus grafana
```

模型仍按原流程运行 `./start-workbuddy2api.sh`，然后使用 `./start-real.sh` 启动 Agent，并在另一个终端运行自己的 Cloudflare Tunnel。修改 `.env` 后需要重启 Agent，文件里的配置不会因重启电脑丢失。

## 2. 现在最简单的真实测试

不用再新建 Bot。进入之前已经验证过 `current_time` 的**时间查询 Topic**，发送：

```text
@trpc_agent_test_bot 请调用 current_time，告诉我现在的 UTC 时间。
```

该 Topic 锁定的是时间查询版本。不要在当前 stable 指向的审批测试新会话中测试时间工具，审批版本只开放 `dangerous_demo`。

收到 Bot 回复后，在仓库目录查询最近的真实 Telegram 请求：

```bash
docker compose exec -T postgres psql -U trpc_agent -d trpc_agent -X -c "
  SELECT i.received_at, r.request_id, r.revision_id, r.agent_name,
         r.trace_id, r.status AS run_status, o.status AS reply_status
  FROM inbound_message i
  JOIN agent_run r ON r.request_id = i.request_id
  LEFT JOIN outbound_message o ON o.request_id = i.request_id
  WHERE i.tenant_id = 'tutorial-tenant'
    AND i.channel_binding_id = 'telegram-tutorial-binding'
  ORDER BY i.received_at DESC LIMIT 5;
"
```

本次新消息应有非空的 32 位 `trace_id`、`run_status=completed` 和 `reply_status=sent`。开启追踪之前的历史记录仍可能为空，不会自动补出过去的 trace。

## 3. 从 Tempo 查到实际 span

把查询到的 trace ID 填入下面的命令。它不是密钥：

```bash
TRACE_ID=这里替换成32位trace_id
curl -fsS -H 'Accept: application/json' \
  "http://127.0.0.1:3200/api/traces/$TRACE_ID" \
  -o ./data/telegram-trace.json

go run ./cmd/trpc-tracecheck -file ./data/telegram-trace.json
```

`-file` 模式仅展示已返回的 span 名称，不宣称这条 trace 已覆盖所有业务分支。导出与采集存在批处理延迟，收到回复后等几秒再查；首次 404 或 span 暂不齐全时，可在 5–10 秒后重新获取。

时间查询链路应包含以下类型的 span：

```text
POST /callbacks/telegram/{callback_key}
  → channel.callback
  → gateway.accept
  → queue.publish
  → worker.agent.run
  → invoke_agent ... / chat glm-5.3-flash / execute_tool current_time
  → storage.session.get / storage.session.event.append
  → reply.send
```

实际父子顺序由运行过程决定，Session 读取可能发生在 Agent span 之前。只有业务确实读写 Memory 时，才应该看到 `storage.memory.*`；时间工具本身不写 Memory，不能因为没有 Memory span 就判断链路失败。

也可以打开本机 Grafana：`http://127.0.0.1:3000`。当前 Compose 开发账号为 `admin / admin`，不要把这套默认账号暴露到公网。在 Explore 中选择 Tempo，使用上述 trace ID 查看时间分布。Prometheus 数据源与平台 Dashboard 沿用现有配置。

## 4. 审批为什么有两条 trace

发起审批和稍后点击/输入批准是两个独立的外部消息，因此各自有 trace ID。系统不会为了等待用户确认，让一个 span 持续打开十五分钟。

- 申请时：把源工具请求的 `traceparent` 保存到 `tool_approval.origin_traceparent`。
- 决策时：新请求的 `approval.decide` span 通过 **span link** 指向原申请；它的 continuation 和平台回执沿用决策请求的 trace。
- 拒绝时：有平台回执和 `reply.send`，没有模型执行 span，这是正确行为。
- 重复操作时：可能只有幂等处理 span，没有新 Worker 或发送 span，不代表系统停止。

要验证 link，需要在开启追踪后生成一张新审批单。旧审批没有 origin_traceparent 时不能凭空关联。关联字段不参与批准权限判定，身份、Session 和参数哈希校验保持原样。

## 5. Trace 为什么不显示聊天正文

本平台默认只导出操作元数据：租户/应用、请求 ID、Agent/模型/工具名称、耗时、token 数和错误类别等。不导出 prompt、模型回复、系统指令、工具参数与结果、异常事件正文或错误描述。

这是开启框架追踪时额外加的边界。tRPC-Agent-Go 有独立的全局 tracer，必须在进程启动时绑定到平台 provider；直接启用它还可能采集较多内容，因此平台同时安装生产侧属性丢弃策略和导出侧白名单。Webhook 的 callback_key 也被路由模板替代。

这样可以排查调用耗时与关联关系，但不能依靠 trace 回放完整对话。新增 span 属性需要经过白名单检查；这不等于已完成所有日志或合规审计。

## 6. 可重复的本地链路预检

```bash
./scripts/e2e-telegram-tracing.sh
```

这个脚本使用模拟 Telegram API/模型和内存后端，但运行真实的 Callback、Runner、工具、Session/Memory Router、审批和 Sender，将 span 发送到真实 Collector/Tempo，再从 Tempo 读回检查名称、父子连接、跨请求 link 和敏感测试字符串是否遗漏过滤。

它会生成 `data/trace-preflight.XXXXXX/initial.json` 与 `decision.json`，不会读取真实 Bot Token，不会给真实 Telegram 发消息，也不会在测试结束时停止你正在使用的服务。旧的 `e2e-observability.sh` 现在也只清理自己的临时 Agent 进程，保留共享采集栈。

本阶段已通过这个预检、真实模型与 Redis/PostgreSQL 的 HTTP 异步请求验证，以及真实 Telegram 的 current_time 完整链路验证。第 2 节保留为以后查看新请求的方法，不是要求重新测试。证据见[2026-09-06 追踪验证记录](validation/tracing-2026-09-06.md)。

测试结束时，先按原流程停止 Agent 和 Tunnel。如需释放本地观测栈资源，再手动执行：

```bash
docker compose --profile observability stop grafana prometheus otel-collector tempo
```

这不会删除卷。Collector 停止后，即使 Agent 仍能聊天，也无法正常导出追踪；不要把“服务 ready”等同于“trace 已入库”。
