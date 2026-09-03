# 真实模型开发环境验收（2026-09-03）

本记录由开发环境操作者完成并确认，用于证明项目可以通过 tRPC-Agent-Go 的 OpenAI-compatible Model 调用真实模型。记录不包含 API Key、请求原文或供应商敏感信息。

## 配置范围

```text
provider: openai
model: glm-5.3-flash
protocol: OpenAI-compatible Chat Completions
stream: false
session backend: Redis
```

模型通过本机 OpenAI-compatible 网关接入，因此本次验收覆盖应用到网关再到真实模型的调用链路，不代表已经验证所有模型供应商或直连地址。

## 已确认项目

- `check-model.sh` 可以获得真实模型回复；
- `start-real.sh` 可以使用 `.env` 启动 Agent 服务；
- `/readyz` 返回就绪；
- 相同 `user_id + session_id` 的第二轮请求可以读取第一轮信息；
- Agent 进程重启且 Redis 保持运行后，会话历史仍可恢复。

## 结论

真实模型、tRPC-Agent-Go Runner、LLMAgent 和 Redis Session 的开发环境链路已经完成一次端到端验证。后续仍需在多租户模型配置、供应商限流、超时、流式响应和生产密钥注入条件下继续验收。
