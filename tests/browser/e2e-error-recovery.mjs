import assert from 'node:assert/strict'
import { chromium } from 'playwright'

const base = process.env.E2E_BASE ?? 'http://127.0.0.1:5178'
const browser = await chromium.launch({ executablePath: process.env.CHROMIUM_PATH || undefined })
const page = await browser.newPage({ viewport: { width: 1200, height: 800 } })

const app = {
  Config: {
    tenant_id: 'support', app_code: 'assistant', status: 'active', config_version: 1,
    instruction: '', model: { provider_id: 'mock', name: 'mock-model' }, tools: { allowed: [] },
    storage: { artifact: { driver: 'postgres' } }, governance: { max_tool_calls: 8, budget_units: 100 },
    audit: { retention_days: 90 }, channels: [],
  },
  Checksum: 'fixture', PublishedAt: '2026-09-11T00:00:00Z',
}

await page.route('**/api/**', async (route) => {
  const path = new URL(route.request().url()).pathname
  if (path === '/api/v1/auth/me') {
    return route.fulfill({
      status: 200,
      json: {
        platform_user_id: 'member-1', role: 'member', is_system_admin: false,
        tenants: [{ tenant_id: 'support', display_name: '客服测试', role: 'member' }],
      },
    })
  }
  if (path === '/api/v1/tenants') return route.fulfill({ status: 200, json: { tenants: [{ tenant_id: 'support', display_name: '客服测试', role: 'member' }] } })
  if (path === '/api/v1/apps') return route.fulfill({ status: 200, json: { applications: [app] } })
  if (path === '/api/v1/sessions/mine') return route.fulfill({ status: 200, json: { sessions: [] } })
  if (path === '/api/v1/sessions/messages') return route.fulfill({ status: 200, json: { messages: [], next_cursor: '' } })
  if (path === '/api/v1/memory') return route.fulfill({ status: 200, json: { memories: [] } })
  return route.fulfill({ status: 404, json: { error: `missing fixture: ${path}` } })
})

const chunkPattern = '**/assets/PreferencesPage.js'
await page.route(chunkPattern, (route) => route.abort('failed'))

try {
  await page.goto(`${base}/console/?tab=chat`)
  await page.getByRole('tab', { name: '我的偏好', exact: true }).click()
  assert.match(page.url(), /[?&]tab=preferences(?:&|$)/, 'tab navigation must update the URL before a reload is needed')
  const failure = page.getByRole('alert').filter({ hasText: '页面资源已更新' })
  await failure.waitFor({ timeout: 5000 })
  await page.unroute(chunkPattern)
  const reload = page.waitForNavigation({ waitUntil: 'domcontentloaded' })
  await failure.getByRole('button', { name: '刷新页面', exact: true }).click()
  await reload
  await page.getByRole('searchbox', { name: '搜索我的偏好' }).waitFor({ timeout: 5000 })
  assert.match(page.url(), /[?&]tab=preferences(?:&|$)/, 'reload must preserve the page that failed')
  console.log('PASS: tab URL sync preserves the current page through error-boundary reload')
} finally {
  await browser.close()
}
