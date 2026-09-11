import assert from 'node:assert/strict'
import { chromium } from 'playwright'

const base = process.env.E2E_BASE ?? 'http://127.0.0.1:5178'
const browser = await chromium.launch({ executablePath: process.env.CHROMIUM_PATH || undefined })
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } })
const errors = []
page.on('pageerror', (error) => errors.push(error.message))

const app = {
  Config: {
    tenant_id: 'acme', app_code: 'support', status: 'active', config_version: 1,
    instruction: '', model: { provider_id: 'primary', name: 'chat' }, tools: { allowed: [] },
    storage: { artifact: { driver: 'postgres' } }, governance: { max_tool_calls: 8, budget_units: 100 },
    audit: { retention_days: 90 }, channels: [],
  },
  Checksum: 'fixture', PublishedAt: '2026-09-09T00:00:00Z',
}

await page.route('**/api/**', async (route) => {
  const request = route.request()
  const url = new URL(request.url())
  const path = url.pathname
  if (path === '/api/v1/auth/me') {
    return route.fulfill({
      status: 200,
      json: {
        platform_user_id: 'member-1', role: 'member', is_system_admin: false,
        tenants: [{ tenant_id: 'acme', display_name: 'Acme', role: 'member' }],
      },
    })
  }
  if (path === '/api/v1/tenants') return route.fulfill({ status: 200, json: { tenants: [{ tenant_id: 'acme', display_name: 'Acme', role: 'member' }] } })
  if (path === '/api/v1/apps') return route.fulfill({ status: 200, json: { applications: [app] } })
  if (path === '/api/v1/sessions/mine') return route.fulfill({ status: 200, json: { sessions: [] } })
  if (path === '/api/v1/sessions/messages') return route.fulfill({ status: 200, json: { messages: [] } })
  if (path === '/api/v1/system' || path === '/api/v1/catalog' || path === '/api/v1/claims') {
    return route.fulfill({ status: 403, json: { error: 'forbidden' } })
  }
  return route.fulfill({ status: 404, json: { error: `missing fixture: ${path}` } })
})

try {
  await page.goto(`${base}/console/?tab=system`)
  const nav = page.getByRole('navigation', { name: '主导航' })
  await nav.waitFor()
  await nav.getByText('对话', { exact: true }).waitFor()

  assert.equal(await nav.getByText('工作区', { exact: true }).count(), 1)
  assert.equal(await nav.getByText('对话', { exact: true }).count(), 1)
  assert.equal(await nav.getByText('我的偏好', { exact: true }).count(), 1)
  assert.equal(await nav.getByText('我的会话', { exact: true }).count(), 0)
  assert.equal(await nav.getByText('机器人', { exact: true }).count(), 0)
  assert.equal(await nav.getByText('知识库', { exact: true }).count(), 0)
  assert.equal(await nav.getByText('执行记录', { exact: true }).count(), 0)
  assert.equal(await nav.getByText('模型资产', { exact: true }).count(), 0)
  assert.equal(await nav.getByText('系统状态', { exact: true }).count(), 0)
  assert.equal(await page.getByText('系统管理员', { exact: true }).count(), 0)
  assert.equal(await page.getByText('租户成员', { exact: true }).count(), 1)
  assert.equal(await page.getByRole('button', { name: '新建机器人' }).count(), 0)
  assert.equal(await page.getByRole('button', { name: '新建', exact: true }).count(), 0)

  const requestedPlatformEndpoints = await page.evaluate(() => performance.getEntriesByType('resource').map((entry) => entry.name))
  assert.equal(requestedPlatformEndpoints.some((url) => url.includes('/api/v1/system')), false, 'member deep-link must not render platform system page before redirect')
  assert.deepEqual(errors, [])
  console.log('PASS: member navigation exposes only personal workspace and does not render restricted system pages')
} finally {
  await browser.close()
}
