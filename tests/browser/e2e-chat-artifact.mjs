// Browser contract: a generated Artifact is rendered as a downloadable
// assistant attachment instead of disappearing after the final reply.
import assert from 'node:assert/strict'
import { chromium } from 'playwright'

const base = process.env.E2E_BASE ?? 'http://127.0.0.1:5173'
const browser = await chromium.launch({ executablePath: process.env.CHROMIUM_PATH || undefined })
const page = await browser.newPage({ viewport: { width: 1500, height: 900 }, acceptDownloads: true })
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
  if (path === '/api/v1/apps') return route.fulfill({ status: 200, json: { applications: [app] } })
  if (path === '/api/v1/sessions/mine') return route.fulfill({ status: 200, json: { sessions: [] } })
  if (path === '/api/v1/system') return route.fulfill({ status: 200, json: { info: { model_providers: [] }, status: {} } })
  if (path === '/api/v1/chat' && request.method() === 'POST') {
    return route.fulfill({ status: 202, json: {
      event_id: 'artifact-request', session_key: 'acme/support/session/artifact',
      stream_url: '/api/v1/chat/stream?tenant=acme&event_id=artifact-request',
    } })
  }
  if (path === '/api/v1/chat/stream') {
    return route.fulfill({
      status: 200,
      contentType: 'text/event-stream',
      body: 'id: 9-0\ndata: {"type":"done","reply":"文档已生成。","artifacts":[{"filename":"维修受理.md","version":2,"name":"维修受理说明","mime_type":"text/markdown"}]}\n\n',
    })
  }
  if (path === '/api/v1/artifacts' && request.method() === 'GET') {
    assert.equal(url.searchParams.get('session'), 'acme/support/session/artifact')
    assert.equal(url.searchParams.get('filename'), '维修受理.md')
    assert.equal(url.searchParams.get('version'), '2')
    assert.equal(url.searchParams.get('download'), '1')
    return route.fulfill({
      status: 200,
      contentType: 'text/markdown',
      headers: { 'Content-Disposition': "attachment; filename*=UTF-8''%E7%BB%B4%E4%BF%AE%E5%8F%97%E7%90%86.md" },
      body: '# 维修受理\n测试内容',
    })
  }
  return route.fulfill({ status: 404, json: { error: `missing fixture ${request.method()} ${path}` } })
})

try {
  await page.goto(`${base}/console/?tab=chat`)
  const composer = page.locator('.composer-box textarea')
  await composer.waitFor()
  await composer.fill('生成维修受理文档')
  await page.locator('.send-btn').click()

  const attachment = page.getByRole('button', { name: /维修受理\.md/ })
  await attachment.waitFor()
  assert.equal(await page.getByText('文档已生成。', { exact: true }).count(), 1)
  const downloadPromise = page.waitForEvent('download')
  await attachment.click()
  const download = await downloadPromise
  assert.equal(download.suggestedFilename(), '维修受理.md')
  assert.deepEqual(errors, [])
  console.log('PASS: generated web artifact is visible and downloadable')
} finally {
  await browser.close()
}
