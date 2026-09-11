#!/usr/bin/env node
import fs from 'node:fs/promises'
import path from 'node:path'
import { pathToFileURL } from 'node:url'
import { randomUUID } from 'node:crypto'

export function percentile(values, percentileValue) {
  if (values.length === 0) return null
  const sorted = [...values].sort((a, b) => a - b)
  const rank = Math.max(0, Math.ceil((percentileValue / 100) * sorted.length) - 1)
  return Number(sorted[rank].toFixed(2))
}

export function latencySummary(values) {
  if (values.length === 0) return { p50: null, p95: null, p99: null, max: null }
  return {
    p50: percentile(values, 50),
    p95: percentile(values, 95),
    p99: percentile(values, 99),
    max: Number(Math.max(...values).toFixed(2)),
  }
}

function parseArgs(argv) {
  const options = {
    base: 'http://127.0.0.1:8080',
    concurrency: 4,
    requests: 40,
    warmup: 4,
    timeoutMs: 60000,
    maxErrorRate: 0,
    message: '容量测试：请简短回复。',
    output: '',
    metricsUrls: [],
    sessionPrefix: `capacity-${Date.now().toString(36)}-${process.pid}`,
  }
  for (let index = 0; index < argv.length; index += 1) {
    const name = argv[index]
    if (!name.startsWith('--')) throw new Error(`unexpected argument: ${name}`)
    const key = name.slice(2)
    const value = argv[++index]
    if (value == null) throw new Error(`missing value for --${key}`)
    switch (key) {
      case 'base': options.base = value; break
      case 'tenant': options.tenant = value; break
      case 'app': options.app = value; break
      case 'username': options.username = value; break
      case 'password': options.password = value; break
      case 'concurrency': options.concurrency = positiveInteger(key, value); break
      case 'requests': options.requests = positiveInteger(key, value); break
      case 'warmup': options.warmup = nonNegativeInteger(key, value); break
      case 'timeout-ms': options.timeoutMs = positiveInteger(key, value); break
      case 'max-error-rate': options.maxErrorRate = boundedNumber(key, value, 0, 1); break
      case 'message': options.message = value; break
      case 'session-prefix': options.sessionPrefix = value.trim(); break
      case 'output': options.output = value; break
      case 'metrics-url': options.metricsUrls.push(value); break
      default: throw new Error(`unknown option --${key}`)
    }
  }
  for (const required of ['tenant', 'app', 'username', 'password']) {
    if (!options[required]) throw new Error(`--${required} is required`)
  }
  if (!options.sessionPrefix) throw new Error('--session-prefix must not be empty')
  return options
}

export function parsePrometheus(text, scope = {}) {
  const values = new Map()
  for (const raw of text.split('\n')) {
    const line = raw.trim()
    if (!line || line.startsWith('#')) continue
    const match = line.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+([-+0-9.eE]+)$/)
    if (!match) continue
    const labels = match[2] ?? ''
    if (!prometheusSeriesMatchesScope(labels, scope)) continue
    const value = Number(match[3])
    if (!Number.isFinite(value)) continue
    const bucket = values.get(match[1]) ?? []
    bucket.push(value)
    values.set(match[1], bucket)
  }
  return values
}

function prometheusSeriesMatchesScope(labels, scope) {
  for (const [label, expected] of [['tenant_id', scope.tenant], ['app_code', scope.app]]) {
    if (!expected) continue
    const match = labels.match(new RegExp(`(?:^|,)${label}="([^"]*)"(?:,|$)`))
    if (match && match[1] !== expected) return false
  }
  return true
}

function metricSum(metrics, name) {
  return (metrics.get(name) ?? []).reduce((sum, value) => sum + value, 0)
}

function metricMax(metrics, name) {
  const values = metrics.get(name) ?? []
  return values.length ? Math.max(...values) : 0
}

async function scrapeMetrics(options) {
  if (options.metricsUrls.length === 0) return null
  const endpoints = []
  for (const url of options.metricsUrls) {
    const text = await withTimeout(Math.min(options.timeoutMs, 5000), async (signal) => {
      const response = await fetch(url, { signal })
      if (!response.ok) throw new Error(`metrics scrape failed: ${url} HTTP ${response.status}`)
      return response.text()
    })
    endpoints.push(parsePrometheus(text, { tenant: options.tenant, app: options.app }))
  }
  const sumAcrossEndpoints = (name) => endpoints.reduce((sum, metrics) => sum + metricSum(metrics, name), 0)
  const maxAcrossEndpoints = (name) => endpoints.reduce((maxValue, metrics) => Math.max(maxValue, metricMax(metrics, name)), 0)
  return {
    inbound_total: sumAcrossEndpoints('channel_inbound_messages_total'),
    store_operations_total: sumAcrossEndpoints('platform_store_operations_total'),
    model_tokens_total: sumAcrossEndpoints('model_tokens_total'),
    database_waits_total: sumAcrossEndpoints('database_pool_waits_total'),
    database_wait_ms_total: sumAcrossEndpoints('database_pool_wait_duration_ms_total'),
    redis_waits_total: sumAcrossEndpoints('redis_pool_waits_total'),
    redis_wait_ms_total: sumAcrossEndpoints('redis_pool_wait_duration_ms_total'),
    redis_timeouts_total: sumAcrossEndpoints('redis_pool_timeouts_total'),
    active_executions: sumAcrossEndpoints('agent_execution_active'),
    worker_slots: sumAcrossEndpoints('worker_execution_slots'),
    database_in_use: sumAcrossEndpoints('database_pool_connections_in_use'),
    database_max: sumAcrossEndpoints('database_pool_connections_max'),
    redis_connections: sumAcrossEndpoints('redis_pool_connections_total'),
    kafka_lag: maxAcrossEndpoints('messaging_kafka_consumer_lag'),
    kafka_partitions: maxAcrossEndpoints('messaging_kafka_topic_partitions'),
    outbox_pending_max: maxAcrossEndpoints('outbox_pending_events'),
    outbox_oldest_age_seconds: maxAcrossEndpoints('outbox_oldest_age_seconds'),
  }
}

function counterDelta(after, before, name) {
  return Math.max(0, (after?.[name] ?? 0) - (before?.[name] ?? 0))
}

function updatePeaks(peaks, snapshot) {
  if (!snapshot) return
  for (const name of [
    'active_executions', 'worker_slots', 'database_in_use', 'database_max', 'redis_connections',
    'kafka_lag', 'kafka_partitions', 'outbox_pending_max', 'outbox_oldest_age_seconds',
  ]) peaks[name] = Math.max(peaks[name] ?? 0, snapshot[name] ?? 0)
}

async function startMetricSampler(options, peaks) {
  if (options.metricsUrls.length === 0) return async () => null
  let stopped = false
  let failure = null
  const loop = (async () => {
    while (!stopped) {
      try {
        updatePeaks(peaks, await scrapeMetrics(options))
      } catch (error) {
        failure = error
        return
      }
      await new Promise((resolve) => setTimeout(resolve, 1000))
    }
  })()
  return async () => {
    stopped = true
    await loop
    if (failure) throw failure
  }
}

function positiveInteger(name, raw) {
  const value = Number(raw)
  if (!Number.isInteger(value) || value <= 0) throw new Error(`--${name} must be a positive integer`)
  return value
}

function nonNegativeInteger(name, raw) {
  const value = Number(raw)
  if (!Number.isInteger(value) || value < 0) throw new Error(`--${name} must be a non-negative integer`)
  return value
}

function boundedNumber(name, raw, min, max) {
  const value = Number(raw)
  if (!Number.isFinite(value) || value < min || value > max) throw new Error(`--${name} must be between ${min} and ${max}`)
  return value
}

function getSetCookies(headers) {
  if (typeof headers.getSetCookie === 'function') return headers.getSetCookie()
  const combined = headers.get('set-cookie')
  return combined ? combined.split(/,(?=\s*[^;,]+=)/) : []
}

function updateCookieJar(jar, headers) {
  for (const raw of getSetCookies(headers)) {
    const pair = raw.split(';', 1)[0]
    const separator = pair.indexOf('=')
    if (separator > 0) jar.set(pair.slice(0, separator).trim(), pair.slice(separator + 1).trim())
  }
}

function cookieHeader(jar) {
  return [...jar.entries()].map(([name, value]) => `${name}=${value}`).join('; ')
}

async function withTimeout(timeoutMs, action) {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(new Error(`request timed out after ${timeoutMs}ms`)), timeoutMs)
  try {
    return await action(controller.signal)
  } finally {
    clearTimeout(timer)
  }
}

async function login(options) {
  const jar = new Map()
  const { response, text } = await withTimeout(options.timeoutMs, async (signal) => {
    const response = await fetch(new URL('/api/v1/auth/local/login', options.base), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: options.username, password: options.password }),
      signal,
    })
    return { response, text: await response.text() }
  })
  updateCookieJar(jar, response.headers)
  if (!response.ok) throw new Error(`local login failed: HTTP ${response.status} ${text}`)
  const body = JSON.parse(text || '{}')
  if (body.must_change_password) throw new Error('load-test account must change its temporary password before running capacity tests')
  if (!jar.get('dsh_session') || !jar.get('csrf_token')) throw new Error('local login did not return session and CSRF cookies')
  return jar
}

async function readSSE(response, startedAt) {
  if (!response.ok || !response.body) throw new Error(`stream failed: HTTP ${response.status}`)
  const decoder = new TextDecoder()
  let buffer = ''
  let firstDeltaMs = null
  for await (const chunk of response.body) {
    buffer += decoder.decode(chunk, { stream: true }).replace(/\r\n/g, '\n')
    let boundary
    while ((boundary = buffer.indexOf('\n\n')) >= 0) {
      const block = buffer.slice(0, boundary)
      buffer = buffer.slice(boundary + 2)
      const data = block.split('\n').filter((line) => line.startsWith('data:')).map((line) => line.slice(5).trim()).join('\n')
      if (!data) continue
      const event = JSON.parse(data)
      if (event.type === 'delta' && firstDeltaMs == null) firstDeltaMs = performance.now() - startedAt
      if (event.type === 'error') throw new Error(`stream error: ${event.message ?? 'unknown error'}`)
      if (event.type === 'done') return { firstDeltaMs }
    }
  }
  throw new Error('stream closed before done event')
}

async function oneRequest(options, jar, slot, sequence) {
  const startedAt = performance.now()
  const requestID = randomUUID()
  const conversationID = `${options.sessionPrefix}-${slot}`
  const headers = {
    'Content-Type': 'application/json',
    'Cookie': cookieHeader(jar),
    'X-CSRF-Token': jar.get('csrf_token'),
    'X-Active-Tenant': options.tenant,
  }
  let enqueueResponse
  let raw
  try {
    const queuedResponse = await withTimeout(options.timeoutMs, async (signal) => {
      const response = await fetch(new URL('/api/v1/chat', options.base), {
        method: 'POST', headers,
        body: JSON.stringify({
          tenant_id: options.tenant,
          app_code: options.app,
          conversation_id: conversationID,
          text: `${options.message} [slot=${slot} seq=${sequence}]`,
          request_id: requestID,
        }),
        signal,
      })
      return { response, text: await response.text() }
    })
    enqueueResponse = queuedResponse.response
    raw = queuedResponse.text
  } catch (error) {
    return { ok: false, stage: 'enqueue', error: String(error), e2eMs: performance.now() - startedAt }
  }
  const enqueueMs = performance.now() - startedAt
  if (!enqueueResponse.ok) return { ok: false, stage: 'enqueue', status: enqueueResponse.status, error: raw, enqueueMs, e2eMs: enqueueMs }
  let queued
  try {
    queued = JSON.parse(raw)
  } catch (error) {
    return { ok: false, stage: 'enqueue_decode', error: String(error), enqueueMs, e2eMs: enqueueMs }
  }
  if (!queued.stream_url) return { ok: false, stage: 'enqueue_decode', error: 'response missing stream_url', enqueueMs, e2eMs: enqueueMs }
  try {
    const stream = await withTimeout(options.timeoutMs, async (signal) => {
      const streamResponse = await fetch(new URL(queued.stream_url, options.base), {
        headers: { 'Cookie': cookieHeader(jar), 'X-Active-Tenant': options.tenant },
        signal,
      })
      return readSSE(streamResponse, startedAt)
    })
    return { ok: true, enqueueMs, firstDeltaMs: stream.firstDeltaMs, e2eMs: performance.now() - startedAt }
  } catch (error) {
    return { ok: false, stage: 'stream', error: String(error), enqueueMs, e2eMs: performance.now() - startedAt }
  }
}

async function runClosedLoop(options, jar, count, collect) {
  let next = 0
  const workers = Array.from({ length: Math.min(options.concurrency, count) }, (_, slot) => (async () => {
    while (true) {
      const sequence = next++
      if (sequence >= count) return
      const result = await oneRequest(options, jar, slot, sequence)
      if (collect) collect.push(result)
    }
  })())
  await Promise.all(workers)
}

function assertWarmupSucceeded(results) {
  const failed = results.filter((result) => !result.ok)
  if (failed.length === 0) return
  const sample = failed[0]
  throw new Error(`warmup failed: ${failed.length}/${results.length} requests failed at ${sample.stage}: ${sample.error ?? `HTTP ${sample.status ?? 'unknown'}`}`)
}

export function buildReport(options, results, elapsedMs, metrics = null) {
  const successes = results.filter((result) => result.ok)
  const failures = results.filter((result) => !result.ok)
  const byStage = {}
  for (const failure of failures) byStage[failure.stage] = (byStage[failure.stage] ?? 0) + 1
  const seconds = elapsedMs / 1000
  return {
    generated_at: new Date().toISOString(),
    workload: {
      base: options.base,
      tenant: options.tenant,
      app: options.app,
      concurrency: options.concurrency,
      requests: options.requests,
      warmup: options.warmup,
      timeout_ms: options.timeoutMs,
      session_prefix: options.sessionPrefix,
      metrics_urls: options.metricsUrls ?? [],
    },
    result: {
      attempted: results.length,
      succeeded: successes.length,
      failed: failures.length,
      error_rate: results.length ? Number((failures.length / results.length).toFixed(6)) : 0,
      elapsed_seconds: Number(seconds.toFixed(3)),
      attempted_rps: Number((results.length / seconds).toFixed(3)),
      successful_rps: Number((successes.length / seconds).toFixed(3)),
      enqueue_ms: latencySummary(successes.map((result) => result.enqueueMs)),
      first_delta_ms: latencySummary(successes.flatMap((result) => result.firstDeltaMs == null ? [] : [result.firstDeltaMs])),
      e2e_ms: latencySummary(successes.map((result) => result.e2eMs)),
      failures_by_stage: byStage,
      failure_samples: failures.slice(0, 10).map(({ stage, status, error }) => ({ stage, status, error })),
    },
    metrics,
  }
}

function printReport(report) {
  const r = report.result
  console.log(`requests=${r.attempted} success=${r.succeeded} failed=${r.failed} error_rate=${(r.error_rate * 100).toFixed(2)}%`)
  console.log(`throughput=${r.successful_rps} req/s elapsed=${r.elapsed_seconds}s concurrency=${report.workload.concurrency}`)
  console.log(`enqueue p50/p95/p99=${r.enqueue_ms.p50}/${r.enqueue_ms.p95}/${r.enqueue_ms.p99} ms`)
  console.log(`first_delta p50/p95/p99=${r.first_delta_ms.p50}/${r.first_delta_ms.p95}/${r.first_delta_ms.p99} ms`)
  console.log(`e2e p50/p95/p99=${r.e2e_ms.p50}/${r.e2e_ms.p95}/${r.e2e_ms.p99} ms`)
  if (r.failed) console.log(`failures_by_stage=${JSON.stringify(r.failures_by_stage)}`)
  if (report.metrics) {
    console.log(`worker active/slots peak=${report.metrics.peaks.active_executions}/${report.metrics.peaks.worker_slots} kafka_lag_peak=${report.metrics.peaks.kafka_lag}`)
    console.log(`db_pool_peak=${report.metrics.peaks.database_in_use}/${report.metrics.peaks.database_max} redis_timeouts_delta=${report.metrics.deltas.redis_timeouts}`)
    console.log(`outbox pending/oldest peak=${report.metrics.peaks.outbox_pending_max}/${report.metrics.peaks.outbox_oldest_age_seconds}s`)
  }
}

async function main() {
  const options = parseArgs(process.argv.slice(2))
  const jar = await login(options)
  if (options.warmup > 0) {
    const warmupResults = []
    await runClosedLoop(options, jar, options.warmup, warmupResults)
    assertWarmupSucceeded(warmupResults)
  }
  const metricsBefore = await scrapeMetrics(options)
  const peaks = {}
  updatePeaks(peaks, metricsBefore)
  const stopSampler = await startMetricSampler(options, peaks)
  const results = []
  const startedAt = performance.now()
  await runClosedLoop(options, jar, options.requests, results)
  const elapsedMs = performance.now() - startedAt
  await stopSampler()
  const metricsAfter = await scrapeMetrics(options)
  updatePeaks(peaks, metricsAfter)
  const seconds = elapsedMs / 1000
  const metrics = metricsBefore && metricsAfter ? {
    deltas: {
      inbound: counterDelta(metricsAfter, metricsBefore, 'inbound_total'),
      store_operations: counterDelta(metricsAfter, metricsBefore, 'store_operations_total'),
      model_tokens: counterDelta(metricsAfter, metricsBefore, 'model_tokens_total'),
      database_waits: counterDelta(metricsAfter, metricsBefore, 'database_waits_total'),
      database_wait_ms: counterDelta(metricsAfter, metricsBefore, 'database_wait_ms_total'),
      redis_waits: counterDelta(metricsAfter, metricsBefore, 'redis_waits_total'),
      redis_wait_ms: counterDelta(metricsAfter, metricsBefore, 'redis_wait_ms_total'),
      redis_timeouts: counterDelta(metricsAfter, metricsBefore, 'redis_timeouts_total'),
    },
    rates: {
      inbound_per_second: Number((counterDelta(metricsAfter, metricsBefore, 'inbound_total') / seconds).toFixed(3)),
      observed_store_operations_per_second: Number((counterDelta(metricsAfter, metricsBefore, 'store_operations_total') / seconds).toFixed(3)),
      model_tokens_per_second: Number((counterDelta(metricsAfter, metricsBefore, 'model_tokens_total') / seconds).toFixed(3)),
    },
    peaks,
  } : null
  const report = buildReport(options, results, elapsedMs, metrics)
  printReport(report)
  if (options.output) {
    await fs.mkdir(path.dirname(path.resolve(options.output)), { recursive: true })
    await fs.writeFile(options.output, `${JSON.stringify(report, null, 2)}\n`, 'utf8')
    console.log(`report=${options.output}`)
  }
  if (report.result.error_rate > options.maxErrorRate) process.exitCode = 1
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error))
    process.exitCode = 1
  })
}
