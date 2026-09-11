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
const TTFT_MS = nonNegativeNumber('MOCK_TTFT_MS', 0)
const OUTPUT_TOKENS = nonNegativeInteger('MOCK_OUTPUT_TOKENS', 0)
const TOKENS_PER_SECOND = nonNegativeNumber('MOCK_TOKENS_PER_SECOND', 0)
const PROMPT_TOKENS = nonNegativeInteger('MOCK_PROMPT_TOKENS', 2000)
const CACHED_PROMPT_TOKENS = Math.min(nonNegativeInteger('MOCK_CACHED_PROMPT_TOKENS', 0), PROMPT_TOKENS)
const TOKEN_TEXT = process.env.MOCK_TOKEN_TEXT ?? '测'

function nonNegativeNumber(name, fallback) {
  const raw = process.env[name]
  if (raw == null || raw.trim() === '') return fallback
  const value = Number(raw)
  if (!Number.isFinite(value) || value < 0) throw new Error(`${name} must be a non-negative number`)
  return value
}

function nonNegativeInteger(name, fallback) {
  const value = nonNegativeNumber(name, fallback)
  if (!Number.isInteger(value)) throw new Error(`${name} must be a non-negative integer`)
  return value
}

function sleep(ms) {
  if (ms <= 0) return Promise.resolve()
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function configuredReply() {
  return OUTPUT_TOKENS > 0 ? TOKEN_TEXT.repeat(OUTPUT_TOKENS) : REPLY
}

function usage() {
  const completionTokens = OUTPUT_TOKENS > 0 ? OUTPUT_TOKENS : 1
  return {
    prompt_tokens: PROMPT_TOKENS,
    completion_tokens: completionTokens,
    total_tokens: PROMPT_TOKENS + completionTokens,
    prompt_tokens_details: { cached_tokens: CACHED_PROMPT_TOKENS },
  }
}

const server = http.createServer((req, res) => {
  if (req.method === 'GET' && req.url === '/healthz') {
    res.writeHead(200, { 'Content-Type': 'application/json' })
    res.end(JSON.stringify({
      status: 'ok',
      profile: {
        ttft_ms: TTFT_MS,
        output_tokens: OUTPUT_TOKENS,
        tokens_per_second: TOKENS_PER_SECOND,
        prompt_tokens: PROMPT_TOKENS,
        cached_prompt_tokens: CACHED_PROMPT_TOKENS,
      },
    }))
    return
  }

  if (req.method === 'GET' && req.url?.endsWith('/models')) {
    res.writeHead(200, { 'Content-Type': 'application/json' })
    res.end(JSON.stringify({
      object: 'list',
      data: [
        { id: 'mock-model', object: 'model', owned_by: 'local-e2e' },
        { id: 'mock-discovered', object: 'model', owned_by: 'local-e2e' },
      ],
    }))
    return
  }

  if (req.method === 'POST' && req.url?.endsWith('/embeddings')) {
    let body = ''
    req.on('data', (chunk) => (body += chunk))
    req.on('end', () => {
      let inputs = ['']
      try {
        const parsed = JSON.parse(body || '{}')
        inputs = Array.isArray(parsed.input) ? parsed.input : [parsed.input ?? '']
      } catch {
        /* keep one deterministic embedding */
      }
      const embedding = Array.from({ length: 1536 }, (_, index) => (index === 0 ? 1 : 0))
      res.writeHead(200, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify({
        object: 'list',
        model: 'text-embedding-3-small',
        data: inputs.map((_, index) => ({ object: 'embedding', index, embedding })),
        usage: { prompt_tokens: inputs.length, total_tokens: inputs.length },
      }))
    })
    return
  }

  if (req.method !== 'POST' || !req.url?.endsWith('/chat/completions')) {
    res.writeHead(404, { 'Content-Type': 'application/json' })
    res.end(JSON.stringify({ error: 'not found' }))
    return
  }
  let body = ''
  req.on('data', (chunk) => (body += chunk))
  req.on('end', async () => {
    let stream = false
    try {
      stream = Boolean(JSON.parse(body || '{}').stream)
    } catch {
      /* keep non-streaming */
    }
    if (stream) {
      res.writeHead(200, { 'Content-Type': 'text/event-stream' })
      await sleep(TTFT_MS)
      if (res.destroyed) return
      const pieces = OUTPUT_TOKENS > 0 ? Array.from({ length: OUTPUT_TOKENS }, () => TOKEN_TEXT) : [REPLY]
      const interval = TOKENS_PER_SECOND > 0 ? 1000 / TOKENS_PER_SECOND : 0
      for (let index = 0; index < pieces.length; index += 1) {
        const chunk = {
          id: 'mock-1',
          object: 'chat.completion.chunk',
          model: 'mock-model',
          choices: [{ index: 0, delta: { role: index === 0 ? 'assistant' : undefined, content: pieces[index] }, finish_reason: null }],
        }
        res.write(`data: ${JSON.stringify(chunk)}\n\n`)
        if (interval > 0 && index + 1 < pieces.length) await sleep(interval)
        if (res.destroyed) return
      }
      res.write(`data: ${JSON.stringify({
        id: 'mock-1',
        object: 'chat.completion.chunk',
        model: 'mock-model',
        choices: [{ index: 0, delta: {}, finish_reason: 'stop' }],
        usage: usage(),
      })}\n\n`)
      res.write('data: [DONE]\n\n')
      res.end()
      return
    }
    await sleep(TTFT_MS)
    res.writeHead(200, { 'Content-Type': 'application/json' })
    res.end(
      JSON.stringify({
        id: 'mock-1',
        object: 'chat.completion',
        model: 'mock-model',
        choices: [
          {
            index: 0,
            message: { role: 'assistant', content: configuredReply() },
            finish_reason: 'stop',
          },
        ],
        usage: usage(),
      }),
    )
  })
})

server.listen(PORT, HOST, () => {
  console.log(`mock OpenAI server listening on http://${HOST}:${PORT}/v1`)
  console.log(`model.base_url=http://${HOST}:${PORT}/v1  model.name=mock-model  MODEL_API_KEY=mock-key`)
  console.log(`profile ttft=${TTFT_MS}ms output_tokens=${OUTPUT_TOKENS || 'reply'} tokens_per_second=${TOKENS_PER_SECOND || 'unlimited'} prompt_tokens=${PROMPT_TOKENS}`)
})
