import assert from 'node:assert/strict'
import { execFile } from 'node:child_process'
import fs from 'node:fs/promises'
import http from 'node:http'
import os from 'node:os'
import path from 'node:path'
import test from 'node:test'
import { promisify } from 'node:util'

import { buildReport, latencySummary, parsePrometheus, percentile } from './capacity-load.mjs'

const execFileAsync = promisify(execFile)

test('percentile uses nearest-rank semantics', () => {
  assert.equal(percentile([1, 2, 3, 4, 5], 50), 3)
  assert.equal(percentile([1, 2, 3, 4, 5], 95), 5)
  assert.equal(percentile([], 99), null)
})

test('latency summary reports p50 p95 p99 and max', () => {
  assert.deepEqual(latencySummary([10, 20, 30, 40]), { p50: 20, p95: 40, p99: 40, max: 40 })
})

test('capacity report separates failures and latency stages', () => {
  const report = buildReport({
    base: 'http://127.0.0.1:18080', tenant: 'support', app: 'assistant', concurrency: 2,
    requests: 3, warmup: 1, timeoutMs: 60000,
  }, [
    { ok: true, enqueueMs: 5, firstDeltaMs: 30, e2eMs: 100 },
    { ok: true, enqueueMs: 7, firstDeltaMs: 40, e2eMs: 120 },
    { ok: false, stage: 'stream', status: 500, error: 'failed', enqueueMs: 6, e2eMs: 20 },
  ], 1000)

  assert.equal(report.result.succeeded, 2)
  assert.equal(report.result.failed, 1)
  assert.equal(report.result.error_rate, 0.333333)
  assert.equal(report.result.successful_rps, 2)
  assert.deepEqual(report.result.failures_by_stage, { stream: 1 })
  assert.equal(report.result.e2e_ms.p95, 120)
})

test('prometheus parser preserves multiple labelled series under one metric', () => {
  const metrics = parsePrometheus(`# HELP example test\nagent_execution_active{tenant_id="a"} 2\nagent_execution_active{tenant_id="b"} 3\nredis_pool_timeouts_total 4\n`)
  assert.deepEqual(metrics.get('agent_execution_active'), [2, 3])
  assert.deepEqual(metrics.get('redis_pool_timeouts_total'), [4])
})

test('prometheus parser scopes tenant and application series without dropping node-wide metrics', () => {
  const metrics = parsePrometheus(`channel_inbound_messages_total{tenant_id="a",app_code="support"} 5\nchannel_inbound_messages_total{tenant_id="b",app_code="support"} 7\nmodel_tokens_total{tenant_id="a",app_code="other"} 11\nworker_execution_slots 4\n`, { tenant: 'a', app: 'support' })
  assert.deepEqual(metrics.get('channel_inbound_messages_total'), [5])
  assert.equal(metrics.has('model_tokens_total'), false)
  assert.deepEqual(metrics.get('worker_execution_slots'), [4])
})

test('capacity CLI exercises login, CSRF chat enqueue, SSE completion, metrics and report output', async (t) => {
  let chats = 0
  const conversations = new Set()
  const server = http.createServer(async (request, response) => {
    if (request.method === 'POST' && request.url === '/api/v1/auth/local/login') {
      let body = ''
      for await (const chunk of request) body += chunk
      assert.deepEqual(JSON.parse(body), { username: 'capacity-user', password: 'capacity-password' })
      response.setHeader('Set-Cookie', [
        'dsh_session=session-1; Path=/; HttpOnly',
        'csrf_token=csrf-1; Path=/',
      ])
      response.writeHead(200, { 'Content-Type': 'application/json' })
      response.end(JSON.stringify({ must_change_password: false }))
      return
    }
    if (request.method === 'POST' && request.url === '/api/v1/chat') {
      assert.match(request.headers.cookie ?? '', /dsh_session=session-1/)
      assert.equal(request.headers['x-csrf-token'], 'csrf-1')
      assert.equal(request.headers['x-active-tenant'], 'support')
      let body = ''
      for await (const chunk of request) body += chunk
      const input = JSON.parse(body)
      assert.equal(input.tenant_id, 'support')
      assert.equal(input.app_code, 'assistant')
      assert.match(input.conversation_id, /^capacity-session-[01]$/)
      assert.match(input.request_id, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i)
      conversations.add(input.conversation_id)
      chats += 1
      response.writeHead(202, { 'Content-Type': 'application/json' })
      response.end(JSON.stringify({
        event_id: input.request_id,
        stream_url: `/api/v1/chat/stream?tenant=support&event_id=${encodeURIComponent(input.request_id)}`,
      }))
      return
    }
    if (request.method === 'GET' && request.url?.startsWith('/api/v1/chat/stream?')) {
      assert.match(request.headers.cookie ?? '', /dsh_session=session-1/)
      assert.equal(request.headers['x-active-tenant'], 'support')
      response.writeHead(200, { 'Content-Type': 'text/event-stream' })
      response.write('data: {"type":"delta","content":"ok"}\n\n')
      response.end('data: {"type":"done","reply":"ok"}\n\n')
      return
    }
    if (request.method === 'GET' && request.url === '/metrics') {
      response.writeHead(200, { 'Content-Type': 'text/plain' })
      response.end([
        `channel_inbound_messages_total ${chats}`,
        `platform_store_operations_total ${chats * 3}`,
        `model_tokens_total ${chats * 100}`,
        'agent_execution_active 1',
        'worker_execution_slots 2',
        'database_pool_connections_in_use 3',
        'database_pool_connections_max 10',
        'redis_pool_timeouts_total 0',
        'messaging_kafka_consumer_lag 0',
        'messaging_kafka_topic_partitions 8',
        'outbox_pending_events 0',
        'outbox_oldest_age_seconds 0',
      ].join('\n'))
      return
    }
    response.writeHead(404)
    response.end()
  })
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
  t.after(() => new Promise((resolve) => server.close(resolve)))
  const address = server.address()
  assert.ok(address && typeof address !== 'string')
  const base = `http://127.0.0.1:${address.port}`
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'capacity-load-'))
  t.after(() => fs.rm(directory, { recursive: true, force: true }))
  const output = path.join(directory, 'nested', 'report.json')

  const { stdout, stderr } = await execFileAsync(process.execPath, [
    path.resolve('tests/capacity/capacity-load.mjs'),
    '--base', base,
    '--tenant', 'support',
    '--app', 'assistant',
    '--username', 'capacity-user',
    '--password', 'capacity-password',
    '--session-prefix', 'capacity-session',
    '--concurrency', '2',
    '--requests', '2',
    '--warmup', '0',
    '--timeout-ms', '5000',
    '--metrics-url', `${base}/metrics`,
    '--output', output,
  ])
  assert.equal(stderr, '')
  assert.match(stdout, /success=2 failed=0/)
  assert.equal(chats, 2)
  assert.deepEqual([...conversations].sort(), ['capacity-session-0', 'capacity-session-1'])
  const report = JSON.parse(await fs.readFile(output, 'utf8'))
  assert.equal(report.result.succeeded, 2)
  assert.equal(report.metrics.deltas.inbound, 2)
  assert.equal(report.metrics.deltas.store_operations, 6)
  assert.equal(report.metrics.deltas.model_tokens, 200)
  assert.equal(report.metrics.peaks.worker_slots, 2)
  assert.equal(report.metrics.peaks.kafka_partitions, 8)
})

test('capacity CLI times out a stalled SSE body instead of hanging after response headers', async (t) => {
  const server = http.createServer(async (request, response) => {
    if (request.method === 'POST' && request.url === '/api/v1/auth/local/login') {
      response.setHeader('Set-Cookie', ['dsh_session=session-1; Path=/', 'csrf_token=csrf-1; Path=/'])
      response.writeHead(200, { 'Content-Type': 'application/json' })
      response.end('{"must_change_password":false}')
      return
    }
    if (request.method === 'POST' && request.url === '/api/v1/chat') {
      let body = ''
      for await (const chunk of request) body += chunk
      const input = JSON.parse(body)
      response.writeHead(202, { 'Content-Type': 'application/json' })
      response.end(JSON.stringify({ stream_url: `/api/v1/chat/stream?tenant=support&event_id=${input.request_id}` }))
      return
    }
    if (request.method === 'GET' && request.url?.startsWith('/api/v1/chat/stream?')) {
      response.writeHead(200, { 'Content-Type': 'text/event-stream' })
      response.write(': stream opened but intentionally stalled\n\n')
      return
    }
    response.writeHead(404)
    response.end()
  })
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
  t.after(() => new Promise((resolve) => server.close(resolve)))
  const address = server.address()
  assert.ok(address && typeof address !== 'string')
  const base = `http://127.0.0.1:${address.port}`
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'capacity-timeout-'))
  t.after(() => fs.rm(directory, { recursive: true, force: true }))
  const output = path.join(directory, 'report.json')

  const started = performance.now()
  await execFileAsync(process.execPath, [
    path.resolve('tests/capacity/capacity-load.mjs'),
    '--base', base,
    '--tenant', 'support',
    '--app', 'assistant',
    '--username', 'capacity-user',
    '--password', 'capacity-password',
    '--session-prefix', 'capacity-timeout',
    '--concurrency', '1',
    '--requests', '1',
    '--warmup', '0',
    '--timeout-ms', '150',
    '--max-error-rate', '1',
    '--output', output,
  ])
  assert.ok(performance.now() - started < 2000, 'stalled SSE should be aborted by the configured request timeout')
  const report = JSON.parse(await fs.readFile(output, 'utf8'))
  assert.equal(report.result.failed, 1)
  assert.deepEqual(report.result.failures_by_stage, { stream: 1 })
  assert.match(report.result.failure_samples[0].error, /timed out|abort/i)
})

test('capacity CLI fails before measurement when warmup is unhealthy', async (t) => {
  let chatCalls = 0
  const server = http.createServer(async (request, response) => {
    if (request.method === 'POST' && request.url === '/api/v1/auth/local/login') {
      response.setHeader('Set-Cookie', ['dsh_session=session-1; Path=/', 'csrf_token=csrf-1; Path=/'])
      response.writeHead(200, { 'Content-Type': 'application/json' })
      response.end('{"must_change_password":false}')
      return
    }
    if (request.method === 'POST' && request.url === '/api/v1/chat') {
      chatCalls += 1
      response.writeHead(503, { 'Content-Type': 'application/json' })
      response.end('{"error":"worker unavailable"}')
      return
    }
    response.writeHead(404)
    response.end()
  })
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
  t.after(() => new Promise((resolve) => server.close(resolve)))
  const address = server.address()
  assert.ok(address && typeof address !== 'string')
  const base = `http://127.0.0.1:${address.port}`

  await assert.rejects(execFileAsync(process.execPath, [
    path.resolve('tests/capacity/capacity-load.mjs'),
    '--base', base,
    '--tenant', 'support',
    '--app', 'assistant',
    '--username', 'capacity-user',
    '--password', 'capacity-password',
    '--session-prefix', 'capacity-warmup',
    '--concurrency', '1',
    '--requests', '1',
    '--warmup', '1',
    '--max-error-rate', '1',
  ]), (error) => {
    assert.match(error.stderr, /warmup failed/)
    return true
  })
  assert.equal(chatCalls, 1, 'formal measurement must not start after a failed warmup')
})
