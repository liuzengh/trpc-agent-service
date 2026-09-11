// Browser contract: a terminal worker failure must end the thinking state and
// surface concise user-facing feedback instead of leaving the chat pending.
import assert from 'node:assert/strict'
import { chromium } from 'playwright'

const base = process.env.E2E_BASE ?? 'http://127.0.0.1:5173'
const browser = await chromium.launch({ executablePath: process.env.CHROMIUM_PATH || undefined })
const page = await browser.newPage({ viewport: { width: 1500, height: 900 } })
page.setDefaultTimeout(5000)
const errors = []
page.on('pageerror', (error) => errors.push(error.message))

const app = {
  Config: {
    tenant_id: 'acme', app_code: 'support', status: 'active', config_version: 1,
    instruction: 'test', model: { provider_id: 'test', name: 'test-model' },
    tools: { allowed: [] }, storage: { artifact: { driver: 'postgres' } },
    governance: { max_tool_calls: 8, budget_units: 100 }, audit: { retention_days: 90 }, channels: [],
  },
  PublishedAt: '2026-09-11T00:00:00Z', Checksum: 'fixture',
}

await page.route('**/api/**', async (route) => {
  const request = route.request()
  const path = new URL(request.url()).pathname
  if (path === '/api/v1/auth/me') {
    return route.fulfill({ status: 200, json: {
      platform_user_id: 'admin', role: 'admin', is_system_admin: true,
      tenants: [{ tenant_id: 'acme', display_name: 'Acme', role: 'admin' }],
    } })
  }
  if (path === '/api/v1/tenants') {
    return route.fulfill({ status: 200, json: { tenants: [{ tenant_id: 'acme', display_name: 'Acme', role: 'admin' }] } })
  }
  if (path === '/api/v1/apps') return route.fulfill({ status: 200, json: { applications: [app] } })
  if (path === '/api/v1/sessions/mine') return route.fulfill({ status: 200, json: { sessions: [] } })
  if (path === '/api/v1/system') return route.fulfill({ status: 200, json: { info: { model_providers: [] }, status: {} } })
  if (path === '/api/v1/chat' && request.method() === 'POST') {
    return route.fulfill({ status: 202, json: {
      event_id: 'failed-request', session_key: 'acme/support/session/failed',
      stream_url: '/api/v1/chat/stream?tenant=acme&event_id=failed-request',
    } })
  }
  if (path === '/api/v1/chat/stream') {
    return route.fulfill({
      status: 200,
      contentType: 'text/event-stream',
      body: 'id: 9-0\ndata: {"type":"error","code":"model_unavailable","message":"模型服务暂时不可用，请稍后重试。"}\n\n',
    })
  }
  return route.fulfill({ status: 404, json: { error: `missing fixture ${request.method()} ${path}` } })
})

try {
  await page.goto(`${base}/console/?tab=chat`)
  const composer = page.locator('.composer-box textarea')
  await composer.waitFor()
  await composer.fill('你好')
  await page.locator('.send-btn').click()
  await page.locator('.msg.error .msg-text').getByText('模型服务暂时不可用，请稍后重试。', { exact: true }).waitFor()
  assert.equal(await page.getByText('正在思考…', { exact: true }).count(), 0, 'terminal failure must clear thinking state')
  assert.equal(await composer.isDisabled(), false, 'terminal failure must unlock the composer')
  assert.deepEqual(errors, [])
  console.log('PASS: terminal model failure clears thinking state and shows user-facing feedback')
} finally {
  await browser.close()
}
