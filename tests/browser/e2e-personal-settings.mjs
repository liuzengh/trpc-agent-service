import assert from 'node:assert/strict'
import { chromium } from 'playwright'

const base = process.env.E2E_BASE ?? 'http://127.0.0.1:5178'
const browser = await chromium.launch({ executablePath: process.env.CHROMIUM_PATH || undefined })
const context = await browser.newContext({
  viewport: { width: 1440, height: 900 },
  permissions: ['clipboard-read', 'clipboard-write'],
})
const page = await context.newPage()
const errors = []
page.on('pageerror', (error) => errors.push(error.message))

const app = {
  Config: {
    tenant_id: 'support', app_code: 'assistant', status: 'active', config_version: 1,
    instruction: '', model: { provider_id: 'mock', name: 'mock-model' }, tools: { allowed: [] },
    storage: { artifact: { driver: 'postgres' } }, governance: { max_tool_calls: 8, budget_units: 100 },
    audit: { retention_days: 90 }, channels: [],
  },
  Checksum: 'fixture', PublishedAt: '2026-09-11T00:00:00Z',
}

let memories = [
  {
    id: 'memory-brief', app_name: 'support/assistant', user_id: 'member-1',
    memory: { kind: 'fact', memory: '回答尽量简洁。', topics: ['回答风格'] },
    created_at: '2026-09-10T00:00:00Z', updated_at: '2026-09-10T00:00:00Z', score: 0.95,
  },
  {
    id: 'memory-language', app_name: 'support/assistant', user_id: 'member-1',
    memory: { kind: 'fact', memory: '默认使用中文回答。', topics: ['语言'] },
    created_at: '2026-09-10T00:00:00Z', updated_at: '2026-09-10T00:00:00Z', score: 0.9,
  },
]
let memoryReads = 0
let deletedMemory = ''
let verifyProvider = ''

await page.route('**/api/**', async (route) => {
  const request = route.request()
  const url = new URL(request.url())
  const path = url.pathname

  if (path === '/api/v1/auth/me') {
    return route.fulfill({
      status: 200,
      json: {
        platform_user_id: 'member-1', role: 'admin', is_system_admin: true,
        tenants: [{ tenant_id: 'support', display_name: '客服测试', role: 'admin' }],
      },
    })
  }
  if (path === '/api/v1/tenants') {
    return route.fulfill({ status: 200, json: { tenants: [{ tenant_id: 'support', display_name: '客服测试', role: 'admin', status: 'active' }] } })
  }
  if (path === '/api/v1/apps') return route.fulfill({ status: 200, json: { applications: [app] } })
  if (path === '/api/v1/memory') {
    if (request.method() === 'DELETE') {
      deletedMemory = url.searchParams.get('memory_id') ?? ''
      memories = memories.filter((entry) => entry.id !== deletedMemory)
      return route.fulfill({ status: 204, body: '' })
    }
    memoryReads += 1
    const query = (url.searchParams.get('query') ?? '').trim()
    const visible = query ? memories.filter((entry) => entry.memory.memory.includes(query)) : memories
    return route.fulfill({ status: 200, json: { memories: visible } })
  }
  if (path === '/api/v1/account/profile') {
    if (request.method() === 'PUT') {
      const body = JSON.parse(request.postData() || '{}')
      return route.fulfill({
        status: 200,
        json: {
          platform_user_id: 'member-1', display_name: body.display_name, email: 'member@example.com',
          login_methods: [{ provider_id: 'local', type: 'local', display_name: '本地账号', subject_id: 'member', linked_at: '2026-09-10T00:00:00Z' }],
        },
      })
    }
    return route.fulfill({
      status: 200,
      json: {
        platform_user_id: 'member-1', display_name: '测试成员', email: 'member@example.com',
        login_methods: [{ provider_id: 'local', type: 'local', display_name: '本地账号', subject_id: 'member', linked_at: '2026-09-10T00:00:00Z' }],
      },
    })
  }
  if (path === '/api/v1/auth/configuration') {
    return route.fulfill({
      status: 200,
      json: {
        callback_url: `${base}/api/v1/auth/callback`,
        providers: [
          { type: 'local', provider_id: 'local', display_name: '本地账号', configured: true, enabled: true, registration_enabled: false },
          {
            type: 'wecom', provider_id: 'wecom-test', display_name: '企业微信', configured: true, enabled: true,
            metadata: { corp_id: 'ww-test', agent_id: '1000002' },
          },
        ],
      },
    })
  }
  if (path === '/api/v1/auth/verify-provider' && request.method() === 'POST') {
    const body = JSON.parse(request.postData() || '{}')
    verifyProvider = body.provider_id ?? ''
    return route.fulfill({
      status: 200,
      json: {
        auth_url: `${base}/console/?tab=login&provider_verified=${encodeURIComponent(verifyProvider)}`,
        provider: { provider_id: verifyProvider, type: 'wecom', display_name: '企业微信' },
      },
    })
  }
  if (path === '/api/v1/system') {
    return route.fulfill({
      status: 200,
      json: {
        info: {
          version: '1.0.0', listen_address: ':8080', redis_address: 'redis:6379',
          kafka_brokers: 'kafka:9092', kafka_topic: 'agent-events', model_providers: [],
        },
        status: { postgres: 'ok', redis: 'ok', kafka: 'ok' },
        nodes: [],
      },
    })
  }
  if (path === '/api/v1/sessions/mine') return route.fulfill({ status: 200, json: { sessions: [] } })
  if (path === '/api/v1/sessions/messages') return route.fulfill({ status: 200, json: { messages: [] } })
  return route.fulfill({ status: 404, json: { error: `missing fixture: ${request.method()} ${path}` } })
})

try {
  await page.goto(`${base}/console/?tab=preferences`)
  await page.getByText('回答尽量简洁。', { exact: true }).waitFor()
  await page.getByText('默认使用中文回答。', { exact: true }).waitFor()

  const readsBeforeRefresh = memoryReads
  await page.getByRole('button', { name: '刷新我的偏好', exact: true }).click()
  for (let index = 0; index < 100 && memoryReads <= readsBeforeRefresh; index += 1) {
    await page.waitForTimeout(20)
  }
  assert.ok(memoryReads > readsBeforeRefresh, 'refresh must request current memories again')

  const search = page.getByRole('searchbox', { name: '搜索我的偏好' })
  await search.fill('简洁')
  await search.press('Enter')
  await page.getByText('回答尽量简洁。', { exact: true }).waitFor()
  assert.equal(await page.getByText('默认使用中文回答。', { exact: true }).count(), 0, 'search must filter through the memory API')
  await page.getByRole('button', { name: '清空搜索我的偏好', exact: true }).click()
  await page.getByText('默认使用中文回答。', { exact: true }).waitFor()

  const brief = page.locator('.memory-entry').filter({ hasText: '回答尽量简洁。' })
  await brief.getByRole('button', { name: '删除', exact: true }).click()
  let confirm = page.getByRole('dialog', { name: '删除这条偏好？' })
  await confirm.getByRole('button', { name: '关闭确认弹窗', exact: true }).click()
  await confirm.waitFor({ state: 'detached' })
  assert.equal(deletedMemory, '', 'confirm-dialog close icon must not call delete memory')

  await brief.getByRole('button', { name: '删除', exact: true }).click()
  confirm = page.getByRole('dialog', { name: '删除这条偏好？' })
  await confirm.getByRole('button', { name: '取消', exact: true }).click()
  await confirm.waitFor({ state: 'detached' })
  assert.equal(deletedMemory, '', 'cancel must not call delete memory')

  await brief.getByRole('button', { name: '删除', exact: true }).click()
  confirm = page.getByRole('dialog', { name: '删除这条偏好？' })
  await confirm.getByRole('button', { name: '删除偏好', exact: true }).click()
  await page.getByText('回答尽量简洁。', { exact: true }).waitFor({ state: 'detached' })
  assert.equal(deletedMemory, 'memory-brief', 'confirmed delete must use the selected memory id')

  await page.getByRole('tab', { name: '登录设置', exact: true }).click()
  await page.getByText('统一登录回调地址', { exact: true }).waitFor()
  await page.getByRole('button', { name: '复制地址', exact: true }).click()
  await page.getByRole('button', { name: '已复制', exact: true }).waitFor()
  const wecom = page.locator('.login-provider-setting-card').filter({ hasText: '企业微信' })
  const guide = wecom.locator('details')
  await guide.locator('summary').click()
  assert.equal(await guide.getByText('平台配置', { exact: true }).isVisible(), true, 'provider guide must expand')
  await wecom.getByRole('button', { name: '验证登录', exact: true }).click()
  await page.getByText('登录验证成功。该企业身份已经加入当前平台账号。', { exact: true }).waitFor()
  assert.equal(verifyProvider, 'wecom-test', 'verify button must submit the selected provider id')

  assert.deepEqual(errors, [])
  console.log('PASS: preferences refresh/search/delete and login settings copy/guide/verify controls')
} finally {
  await context.close()
  await browser.close()
}
