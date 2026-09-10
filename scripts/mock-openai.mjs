#!/usr/bin/env node
// 本地 OpenAI 兼容 mock 模型服务：用于在没有真实 API key 时端到端验证
// 对话（SSE 流式）与完整管道（Kafka → Worker → Agent → Outbox）。
//
// 用法：
//   node scripts/mock-openai.mjs            # 监听 127.0.0.1:9999
//
// 然后设置环境变量（data/platform.env）：
// Tenant config: model.base_url=http://127.0.0.1:9999/v1, model.name=mock-model
// Runtime secret: MODEL_API_KEY=mock-key (referenced as env:MODEL_API_KEY)
// 并执行 ./stop.sh && ./start.sh。
import http from 'node:http'

const PORT = Number(process.env.PORT ?? 9999)
const HOST = process.env.HOST ?? '127.0.0.1'
const REPLY = process.env.MOCK_REPLY ?? '你好！我是本地 mock 模型，这条回复说明对话链路（Runner / 会话 / 审计 / Outbox）已经全部打通。'

const server = http.createServer((req, res) => {
  if (req.method === 'GET' && req.url === '/healthz') {
    res.writeHead(200, { 'Content-Type': 'application/json' })
    res.end(JSON.stringify({ status: 'ok' }))
    return
  }
  if (req.method !== 'POST' || !req.url?.endsWith('/chat/completions')) {
    res.writeHead(404, { 'Content-Type': 'application/json' })
    res.end(JSON.stringify({ error: 'not found' }))
    return
  }
  let body = ''
  req.on('data', (chunk) => (body += chunk))
  req.on('end', () => {
    let stream = false
    try {
      stream = Boolean(JSON.parse(body || '{}').stream)
    } catch {
      /* keep non-streaming */
    }
    if (stream) {
      res.writeHead(200, { 'Content-Type': 'text/event-stream' })
      const chunk = {
        id: 'mock-1',
        object: 'chat.completion.chunk',
        model: 'mock-model',
        choices: [{ index: 0, delta: { role: 'assistant', content: REPLY }, finish_reason: null }],
      }
      res.write(`data: ${JSON.stringify(chunk)}\n\n`)
      res.write('data: [DONE]\n\n')
      res.end()
      return
    }
    res.writeHead(200, { 'Content-Type': 'application/json' })
    res.end(
      JSON.stringify({
        id: 'mock-1',
        object: 'chat.completion',
        model: 'mock-model',
        choices: [
          {
            index: 0,
            message: { role: 'assistant', content: REPLY },
            finish_reason: 'stop',
          },
        ],
      }),
    )
  })
})

server.listen(PORT, HOST, () => {
  console.log(`mock OpenAI server listening on http://${HOST}:${PORT}/v1`)
  console.log(`model.base_url=http://${HOST}:${PORT}/v1  model.name=mock-model  MODEL_API_KEY=mock-key`)
})
