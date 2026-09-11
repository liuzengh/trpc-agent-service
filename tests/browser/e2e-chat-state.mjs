// Chat state contract checks against isolated API fixtures; never writes to a live tenant.
// Start Vite, then E2E_BASE=http://127.0.0.1:5173 node tests/browser/e2e-chat-state.mjs
import assert from 'node:assert/strict'
import { chromium } from 'playwright'

const base = process.env.E2E_BASE ?? 'http://127.0.0.1:5173'
const browser = await chromium.launch({ executablePath: process.env.CHROMIUM_PATH || undefined })
const page = await browser.newPage({ viewport: { width: 1600, height: 1000 } })
page.setDefaultTimeout(4000)
const errors = []
page.on('pageerror', (error) => errors.push(error.message))

function application(tenant, code) {
  return {
    Config: {
      tenant_id: tenant,
      app_code: code,
      status: 'active',
      config_version: 3,
      instruction: 'test assistant',
      model: { provider_id: 'test', name: 'test-model' },
      tools: { allowed: [] },
      storage: { artifact: { driver: 'postgres' } },
      governance: { max_tool_calls: 8, budget_units: 100 },
      audit: { retention_days: 90 },
      channels: [],
    },
    PublishedAt: '2026-09-08T08:00:00Z',
    Checksum: 'fixture',
  }
}

const apps = [application('acme', 'support'), application('beta', 'support')]
const restoredSession = {
  TenantID: 'acme',
  AppCode: 'support',
  SessionKey: 'acme/support/web/conversation-1',
  SubjectID: 'admin',
  Status: 'active',
  UpdatedAt: '2026-09-09T09:00:00Z',
  Preview: '订单到哪了',
}
const restoredMessages = [
  { id: 'u1', role: 'user', content: '订单到哪了', time: '2026-09-09T09:00:00Z' },
  { id: 'a1', role: 'assistant', content: '正在查询订单', time: '2026-09-09T09:00:01Z' },
]
const olderMessages = [
  { id: 'u0', role: 'user', content: '这是更早的一条消息', time: '2026-09-09T08:59:00Z' },
  { id: 'a0', role: 'assistant', content: '这是更早的一条回复', time: '2026-09-09T08:59:01Z' },
]
let streamReads = 0
let transcriptReads = 0
const postedChats = []
const archivedSessions = []

await page.route('**/api/**', async (route) => {
  const request = route.request()
  const url = new URL(request.url())
  const path = url.pathname

  if (path === '/api/v1/auth/me') {
    return route.fulfill({ status: 200, json: {
      platform_user_id: 'admin', role: 'admin', is_system_admin: true,
      tenants: [{ tenant_id: 'acme', display_name: 'Acme', role: 'admin' }],
    } })
  }
  if (path === '/api/v1/tenants') {
    return route.fulfill({ status: 200, json: { tenants: [{ tenant_id: 'acme', display_name: 'Acme', role: 'admin' }] } })
  }
  if (path === '/api/v1/apps') {
    return route.fulfill({ status: 200, json: { applications: apps } })
  }
  if (path === '/api/v1/sessions/mine') {
    const app = url.searchParams.get('app')
    const channel = url.searchParams.get('channel')
    const sessions = app === 'support' && channel === 'web' ? [restoredSession] : []
    return route.fulfill({ status: 200, json: { sessions } })
  }
  if (path === '/api/v1/sessions/messages') {
    const sessionKey = url.searchParams.get('session_key')
    transcriptReads += 1
    if (sessionKey !== restoredSession.SessionKey) return route.fulfill({ status: 200, json: { messages: [], next_cursor: '' } })
    if (url.searchParams.get('before') === 'older-cursor') {
      return route.fulfill({ status: 200, json: { messages: olderMessages, next_cursor: '' } })
    }
    return route.fulfill({ status: 200, json: { messages: restoredMessages, next_cursor: 'older-cursor' } })
  }
  if (path === '/api/v1/sessions/archive' && request.method() === 'POST') {
    archivedSessions.push(request.postDataJSON())
    return route.fulfill({ status: 204, body: '' })
  }
  if (path === '/api/v1/chat' && request.method() === 'POST') {
    const contentType = request.headers()['content-type'] ?? ''
    postedChats.push(contentType.startsWith('multipart/form-data')
      ? { multipart: true, raw: request.postData() ?? '' }
      : request.postDataJSON())
    return route.fulfill({
      status: 202,
      json: { event_id: `event-${postedChats.length}`, session_key: `session-${postedChats.length}`, stream_url: `/api/v1/chat/stream?tenant=acme&event_id=event-${postedChats.length}` },
    })
  }
  if (path === '/api/v1/chat/stream') {
    streamReads += 1
    if (streamReads === 1) {
      return route.fulfill({
        status: 200,
        contentType: 'text/event-stream',
        body: 'id: 1\ndata: {"type":"delta","content":"半截"}\n\n',
      })
    }
    return route.fulfill({
      status: 200,
      contentType: 'text/event-stream',
      body: 'id: 2\ndata: {"type":"done","reply":"完整回复"}\n\n',
    })
  }
  if (path === '/api/v1/system') return route.fulfill({ status: 200, json: { info: { model_providers: [] }, status: {} } })
  if (path === '/api/v1/execution') {
    return route.fulfill({
      status: 200,
      json: {
        event_id: url.searchParams.get('event_id'),
        trace_id: url.searchParams.get('event_id'),
        status: 'completed',
        attempts: 0,
        agent_trace: {
          status: 'completed',
          started_at: '2026-09-09T09:29:58Z',
          ended_at: '2026-09-09T09:30:01Z',
          usage: { prompt_tokens: 8, completion_tokens: 12, total_tokens: 20 },
          steps: [
            {
              step_id: 'llm-1',
              node_type: 'llm',
              started_at: '2026-09-09T09:29:58Z',
              ended_at: '2026-09-09T09:30:01Z',
            },
          ],
        },
      },
    })
  }

  return route.fulfill({ status: 404, json: { error: `unknown fixture ${path}` } })
})

try {
  await page.goto(`${base}/console/?tab=chat`)
  await page.getByRole('combobox', { name: '当前机器人' }).waitFor()
  await page.locator('.msg.user .msg-text').getByText('订单到哪了', { exact: true }).waitFor()
  await page.locator('.msg.assistant .msg-text').getByText('正在查询订单', { exact: true }).waitFor()
  const readsBeforeEarlier = transcriptReads
  await page.getByRole('button', { name: '加载更早消息', exact: true }).click()
  await page.getByText('这是更早的一条消息', { exact: true }).waitFor()
  assert.equal(transcriptReads, readsBeforeEarlier + 1, 'load earlier must request exactly one older transcript page')
  assert.equal(await page.getByRole('button', { name: '加载更早消息', exact: true }).count(), 0, 'load earlier must disappear at the oldest page')
  await page.locator('.session-switch-btn').click()
  assert.equal(await page.locator('[data-chat-thread]').count(), 1, 'refresh must restore the durable web conversation')
  assert.equal(await page.locator('.chat-thread-name').getByText('订单到哪了', { exact: true }).count(), 1)
  await page.keyboard.press('Escape')

  const composer = page.locator('.composer-box textarea')
  const composerGeometry = await page.evaluate(() => {
    const input = document.querySelector('.composer-box textarea').getBoundingClientRect()
    const send = document.querySelector('.send-btn').getBoundingClientRect()
    const thread = document.querySelector('.thread').getBoundingClientRect()
    const workspace = document.querySelector('.chat-workspace').getBoundingClientRect()
    return {
      centerDelta: Math.abs((input.top + input.height / 2) - (send.top + send.height / 2)),
      containerBorder: getComputedStyle(document.querySelector('.composer-box')).borderTopWidth,
      inputBorder: getComputedStyle(document.querySelector('.composer-box textarea')).borderTopWidth,
      threadWidthDelta: Math.abs(workspace.width - thread.width),
    }
  })
  assert.ok(composerGeometry.centerDelta <= 3, 'message input and send button must align vertically')
  assert.notEqual(composerGeometry.containerBorder, '0px', 'composer should read as one input surface')
  assert.equal(composerGeometry.inputBorder, '0px', 'textarea should not draw a second nested border')
  assert.ok(composerGeometry.threadWidthDelta <= 1, 'conversation should use the full chat workspace after removing the robot rail')
  assert.equal(await page.locator('.bot-rail').count(), 0, 'chat must not render a second robot selector')
  assert.equal(await page.locator('.session-switch-label').getByText('订单到哪了', { exact: true }).count(), 1)
  assert.equal(await page.locator('.thread-head [aria-label="新建会话"]').count(), 1, 'new session lives once in the conversation header')
  assert.equal(await page.locator('.chat-context').count(), 0, 'session list should not occupy a third column')
  await composer.fill('第一条消息')
  await page.locator('.send-btn').click()
  await page.getByText('完整回复', { exact: true }).waitFor()
  assert.equal(await page.getByText('半截', { exact: true }).count(), 0, 'final reply must replace a partial reconnect delta')
  assert.equal(await page.locator('.msg.user .msg-text').getByText('第一条消息', { exact: true }).count(), 1, 'sent message must stay in the conversation')
  assert.equal(await page.locator('.msg.user .msg-text').getByText('订单到哪了', { exact: true }).count(), 1, 'continuing a restored thread must keep prior messages')
  assert.equal(await page.getByText('这次回复已完成', { exact: false }).count(), 0)
  assert.equal(postedChats[0].conversation_id, 'conversation-1', 'a restored web chat must reuse the durable conversation id')

  await page.getByRole('button', { name: '新建会话' }).click()
  await composer.fill('第二条消息')
  await page.locator('.send-btn').click()
  await page.locator('.msg-text').getByText('第二条消息', { exact: true }).waitFor()
  await page.locator('.session-switch-btn').click()
  assert.equal(await page.locator('[data-chat-thread]').count(), 2, 'one bot must keep multiple switchable chat threads')
  await page.keyboard.press('Escape')
  assert.equal(postedChats.length, 2)
  assert.notEqual(postedChats[0].conversation_id, postedChats[1].conversation_id)

  await page.locator('input[type="file"]').setInputFiles({
    name: 'notes.txt',
    mimeType: 'text/plain',
    buffer: Buffer.from('browser file input'),
  })
  await page.getByText('notes.txt', { exact: true }).waitFor()
  assert.equal(await page.locator('.send-btn').isEnabled(), true, 'a file alone should be sendable')
  await page.locator('.send-btn').click()
  await page.locator('.message-attachment').getByText('notes.txt', { exact: true }).waitFor()
  assert.equal(postedChats.length, 3)
  assert.equal(postedChats[2].multipart, true, 'file chat should use multipart')
  assert.match(postedChats[2].raw, /notes\.txt/)

  await page.locator('.session-switch-btn').click()
  await page.getByRole('button', { name: '删除会话 订单到哪了' }).click()
  const deleteDialog = page.getByRole('dialog', { name: '删除会话？' })
  await deleteDialog.waitFor()
  assert.match(await deleteDialog.innerText(), /删除后会从当前会话列表移除/)
  await deleteDialog.getByRole('button', { name: '删除会话', exact: true }).click()
  await deleteDialog.waitFor({ state: 'detached' })
  assert.deepEqual(archivedSessions, [{ tenant_id: 'acme', session_key: 'session-1' }])
  await page.locator('.session-switch-btn').click()
  assert.equal(await page.locator('[data-chat-thread]').count(), 1, 'deleted session should leave the active chat list')
  await page.keyboard.press('Escape')

  assert.deepEqual(errors, [])
  console.log('PASS: restored web transcript, continued conversation, multiple threads, zero browser errors')
} finally {
  await browser.close()
}
