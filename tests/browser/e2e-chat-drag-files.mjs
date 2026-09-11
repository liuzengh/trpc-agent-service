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

let multipartBody = ''
await page.route('**/api/**', async (route) => {
  const request = route.request()
  const path = new URL(request.url()).pathname
  if (path === '/api/v1/auth/me') {
    return route.fulfill({ status: 200, json: {
      platform_user_id: 'admin', role: 'admin', is_system_admin: true,
      tenants: [{ tenant_id: 'acme', display_name: 'Acme', role: 'admin' }],
    } })
  }
  if (path === '/api/v1/tenants') return route.fulfill({ status: 200, json: { tenants: [{ tenant_id: 'acme', display_name: 'Acme', role: 'admin' }] } })
  if (path === '/api/v1/apps') return route.fulfill({ status: 200, json: { applications: [app] } })
  if (path === '/api/v1/sessions/mine') return route.fulfill({ status: 200, json: { sessions: [] } })
  if (path === '/api/v1/system') return route.fulfill({ status: 200, json: { info: { model_providers: [] }, status: {} } })
  if (path === '/api/v1/chat' && request.method() === 'POST') {
    multipartBody = request.postDataBuffer()?.toString('utf8') ?? ''
    return route.fulfill({ status: 202, json: {
      event_id: 'drag-request', session_key: 'acme/support/session/drag',
      stream_url: '/api/v1/chat/stream?tenant=acme&event_id=drag-request',
    } })
  }
  if (path === '/api/v1/chat/stream') {
    return route.fulfill({
      status: 200,
      contentType: 'text/event-stream',
      body: 'id: 1-0\ndata: {"type":"done","reply":"已读取附件。"}\n\n',
    })
  }
  return route.fulfill({ status: 404, json: { error: `missing fixture ${request.method()} ${path}` } })
})

try {
  await page.goto(`${base}/console/?tab=chat`)
  await page.locator('.composer-box textarea').waitFor()

  await page.locator('.thread').evaluate((target) => {
    const transfer = new DataTransfer()
    transfer.items.add(new File(['drag body'], 'drag-note.txt', { type: 'text/plain' }))
    target.dispatchEvent(new DragEvent('dragenter', { bubbles: true, cancelable: true, dataTransfer: transfer }))
    target.dispatchEvent(new DragEvent('dragover', { bubbles: true, cancelable: true, dataTransfer: transfer }))
    target.dispatchEvent(new DragEvent('drop', { bubbles: true, cancelable: true, dataTransfer: transfer }))
  })

  await page.getByText('drag-note.txt', { exact: true }).waitFor()
  assert.equal(await page.getByText('松开以上传文件', { exact: true }).count(), 0, 'drop overlay must close after drop')

  await page.locator('.composer-box textarea').fill('读取附件')
  await page.locator('.send-btn').click()
  await page.getByText('已读取附件。', { exact: true }).waitFor()
  assert.match(multipartBody, /filename="drag-note\.txt"/)
  assert.match(multipartBody, /drag body/)
  assert.deepEqual(errors, [])
  console.log('PASS: dragged files join the normal web chat upload flow')
} finally {
  await browser.close()
}
