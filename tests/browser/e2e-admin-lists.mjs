import assert from 'node:assert/strict'
import { chromium } from 'playwright'

const base = process.env.E2E_BASE ?? 'http://127.0.0.1:5178'
const browser = await chromium.launch({ executablePath: process.env.CHROMIUM_PATH || undefined })
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } })
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

function user(id, name, { role = 'member', systemAdmin = false } = {}) {
  return {
    platform_user_id: id,
    display_name: name,
    email: `${id}@example.com`,
    role,
    status: 'active',
    is_system_admin: systemAdmin,
    last_login_at: '2026-09-11T00:00:00Z',
    providers: ['本地账号'],
    conversation_content_audit: false,
  }
}

const firstMember = user('member-1', '客服成员一')
const secondMember = user('member-2', '客服成员二')
const firstCandidate = user('candidate-1', '候选成员一')
const secondCandidate = user('candidate-2', '候选成员二')
const firstPlatformUser = user('platform-1', '平台用户一')
const secondPlatformUser = user('platform-2', '平台用户二')
const cursors = []

await page.route('**/api/**', async (route) => {
  const request = route.request()
  const url = new URL(request.url())
  const path = url.pathname

  if (path === '/api/v1/auth/me') {
    return route.fulfill({
      status: 200,
      json: {
        platform_user_id: 'admin', role: 'admin', is_system_admin: true,
        tenants: [{ tenant_id: 'support', display_name: '客服测试', role: 'admin' }],
      },
    })
  }
  if (path === '/api/v1/tenants') {
    return route.fulfill({ status: 200, json: { tenants: [{ tenant_id: 'support', display_name: '客服测试', role: 'admin', status: 'active' }] } })
  }
  if (path === '/api/v1/apps') return route.fulfill({ status: 200, json: { applications: [app] } })
  if (path === '/api/v1/tenant-members') {
    const cursor = url.searchParams.get('cursor') ?? ''
    const candidates = url.searchParams.get('view') === 'candidates'
    cursors.push(`tenant:${candidates ? 'candidates' : 'members'}:${cursor || 'first'}`)
    if (candidates) {
      return route.fulfill({
        status: 200,
        json: cursor === 'candidate-next'
          ? { candidates: [secondCandidate], next_cursor: '' }
          : { candidates: [firstCandidate], next_cursor: 'candidate-next' },
      })
    }
    return route.fulfill({
      status: 200,
      json: cursor === 'member-next'
        ? { members: [secondMember], next_cursor: '' }
        : { members: [firstMember], next_cursor: 'member-next' },
    })
  }
  if (path === '/api/v1/users') {
    const cursor = url.searchParams.get('cursor') ?? ''
    cursors.push(`users:${cursor || 'first'}`)
    return route.fulfill({
      status: 200,
      json: cursor === 'user-next'
        ? { users: [secondPlatformUser], next_cursor: '' }
        : { users: [firstPlatformUser], next_cursor: 'user-next' },
    })
  }
  if (path === '/api/v1/sessions/mine') return route.fulfill({ status: 200, json: { sessions: [] } })
  if (path === '/api/v1/sessions/messages') return route.fulfill({ status: 200, json: { messages: [], next_cursor: '' } })
  return route.fulfill({ status: 404, json: { error: `missing fixture: ${request.method()} ${path}` } })
})

try {
  await page.goto(`${base}/console/?tab=members`)
  await page.getByText(firstMember.display_name, { exact: true }).waitFor()
  await page.getByRole('button', { name: '加载更多成员', exact: true }).click()
  await page.getByText(secondMember.display_name, { exact: true }).waitFor()
  assert.equal(await page.getByRole('button', { name: '加载更多成员', exact: true }).count(), 0, 'member pagination must end after the last page')
  assert.ok(cursors.includes('tenant:members:member-next'), 'member pagination must pass the returned cursor')

  await page.getByRole('button', { name: '添加成员', exact: true }).click()
  const memberDialog = page.getByRole('dialog', { name: '添加租户成员' })
  const candidateSelect = memberDialog.getByRole('combobox', { name: '选择用户' })
  await candidateSelect.waitFor()
  await memberDialog.getByRole('button', { name: '加载更多用户', exact: true }).click()
  await candidateSelect.click()
  await page.getByRole('option', { name: secondCandidate.display_name, exact: true }).click()
  assert.equal(await memberDialog.getByRole('button', { name: '加载更多用户', exact: true }).count(), 0, 'candidate pagination must end after the last page')
  assert.ok(cursors.includes('tenant:candidates:candidate-next'), 'candidate pagination must pass the returned cursor')
  await memberDialog.getByRole('button', { name: '取消', exact: true }).click()
  await memberDialog.waitFor({ state: 'detached' })

  await page.getByRole('tab', { name: '用户', exact: true }).click()
  await page.getByText(firstPlatformUser.display_name, { exact: true }).waitFor()
  await page.getByRole('button', { name: '加载更多用户', exact: true }).click()
  await page.getByText(secondPlatformUser.display_name, { exact: true }).waitFor()
  assert.equal(await page.getByRole('button', { name: '加载更多用户', exact: true }).count(), 0, 'platform user pagination must end after the last page')
  assert.ok(cursors.includes('users:user-next'), 'platform user pagination must pass the returned cursor')

  await page.getByRole('button', { name: '新建本地用户', exact: true }).click()
  const createDialog = page.getByRole('dialog', { name: '新建本地用户' })
  await createDialog.getByPlaceholder('例如 ming').fill('cancelled-user')
  await createDialog.getByRole('button', { name: '取消', exact: true }).click()
  await createDialog.waitFor({ state: 'detached' })

  assert.deepEqual(errors, [])
  console.log('PASS: member/candidate/user pagination cursors and admin dialog cancellation')
} finally {
  await browser.close()
}
