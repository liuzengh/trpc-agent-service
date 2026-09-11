import { test, expect, type Page } from '@playwright/test'

/*
 * member 资产自助链路（阶段 41）
 *
 * 覆盖需求：member 作为员工可创建并共享租户资产（Agent / 知识库 等），
 * 作者制下默认私有、共享后同租户成员只读可见，且租户/成员/密钥/审计仍不可见。
 *
 * 结构：serial 套件，每个用例只登一种身份。原因是路由守卫读取的是「内存中的
 * auth store」——同一个页面上下文里换身份必须整页重载，而整页重载又依赖
 * localStorage 恢复会话；把「清会话」和「保留会话」放在同一个 page 上会互相
 * 打架。每个用例拿到的是全新 context（localStorage 天然为空），因此无需清会话。
 *
 * 状态由 playwright webServer 起的内存后端持有，id 必须每次运行唯一。
 */

const ts = Date.now()
const aliceId = `e2e-alice-${ts}`
const bobId = `e2e-bob-${ts}`
const embEpId = `ep-emb-${ts}`
const embEpName = `E2E 嵌入 ${ts}`
const aliceKbId = `kb-alice-${ts}`
const aliceKbName = `Alice 私有库 ${ts}`
const aliceAgentId = `ag-alice-${ts}`
const aliceAgentName = `Alice 助手 ${ts}`
const ownerUser = 'admin'
const ownerPass = 'admin123'
const memberPass = 'member123'
const loginUrl = /\/login(\?|$)/
// The API base must match whatever stack the run points the SPA at. The default
// is the checked-in config's backend; the side-stack config overrides it, so a
// stray request can never land on a different deployment's database.
const apiBase = process.env.E2E_API_BASE ?? 'http://localhost:8080'

test.describe.configure({ mode: 'serial' })

/** Signs in from the login page (the context starts anonymous). */
async function login(page: Page, user: string, password: string) {
  await page.goto('/login')
  await page.getByPlaceholder('用户名').fill(user)
  await page.getByPlaceholder('密码').fill(password)
  const res = page.waitForResponse((r) => r.url().includes('/auth/login'))
  await page.getByRole('button', { name: '登录' }).click()
  const login = await res
  expect(login.status(), `login ${user} failed: ${await login.text()}`).toBe(200)
  await expect(page).not.toHaveURL(loginUrl)
}

async function ownerToken(page: Page): Promise<string> {
  await login(page, ownerUser, ownerPass)
  const token = await page.evaluate(() => localStorage.getItem('auth_token'))
  expect(token).toBeTruthy()
  return token as string
}

async function createMemberViaAPI(page: Page, token: string, userId: string) {
  const res = await page.request.post(`${apiBase}/members`, {
    headers: { Authorization: `Bearer ${token}` },
    data: { user_id: userId, password: memberPass, role: 'member' },
  })
  expect(res.ok(), `create member ${userId}: ${res.status()}`).toBeTruthy()
}

/** Logs in over the API and returns the bearer token (no browser involved). */
async function apiToken(page: Page, userId: string, password: string): Promise<string> {
  const res = await page.request.post(`${apiBase}/auth/login`, {
    data: { user_id: userId, password },
  })
  expect(res.ok(), `api login ${userId}: ${res.status()}`).toBeTruthy()
  const body = (await res.json()) as { token?: string }
  expect(body.token, `api login ${userId}: no token`).toBeTruthy()
  return body.token as string
}

test.describe('member 资产创建与共享', () => {
  let ownerTok = ''

  test.beforeAll(async ({ browser }) => {
    // Bootstrap runs in its own context; the owner token is reused through the
    // API for member management, and the member's own token for seeding the
    // embedding endpoint (it must live in the MEMBER's tenant — the owner is
    // platform-wide and a tenant-scoped create with another tenant would not be
    // visible to the member's KB form).
    const page = await browser.newPage()
    ownerTok = await ownerToken(page)

    await createMemberViaAPI(page, ownerTok, aliceId)
    await createMemberViaAPI(page, ownerTok, bobId)

    const aliceTok = await apiToken(page, aliceId, memberPass)
    const ep = await page.request.post(`${apiBase}/endpoints`, {
      headers: { Authorization: `Bearer ${aliceTok}` },
      data: {
        id: embEpId,
        scope: 'tenant',
        name: embEpName,
        provider: 'openai',
        type: 'embedding',
        base_url: 'https://api.openai.com/v1',
        model_name: 'text-embedding-3-small',
        api_key: 'sk-e2e-emb',
        visibility: 'shared',
      },
    })
    expect(ep.ok(), `seed embedding endpoint: ${ep.status()} ${await ep.text()}`).toBeTruthy()
    await page.close()
  })

  test.afterAll(async ({ browser }) => {
    const page = await browser.newPage()
    for (const id of [aliceId, bobId]) {
      const res = await page.request.delete(`${apiBase}/members/${encodeURIComponent(id)}`, {
        headers: { Authorization: `Bearer ${ownerTok}` },
      })
      expect(res.ok(), `delete member ${id}`).toBeTruthy()
    }
    await page.close()
  })

  test('member 建知识库：默认私有、作者是自己', async ({ page }) => {
    const forbidden: string[] = []
    page.on('response', (res) => {
      if (res.status() === 403) forbidden.push(`${res.request().method()} ${res.url()}`)
    })

    await login(page, aliceId, memberPass)
    await page.goto('/kbs')
    await page.getByRole('heading', { name: '知识库管理' }).waitFor()
    await expect(page.getByRole('button', { name: '新建知识库' })).toBeVisible()

    await page.getByRole('button', { name: '新建知识库' }).click()
    await page.getByPlaceholder('留空自动生成').fill(aliceKbId)
    await page.getByPlaceholder('KB 名称').fill(aliceKbName)
    // A KB must reference an embedding endpoint (the owner seeded a shared one).
    const kbDialog = page.locator('.el-dialog')
    await kbDialog.locator('.el-select').first().click()
    await page.locator('.el-select-dropdown__item', { hasText: embEpName }).first().click()
    await page.keyboard.press('Escape')
    await page.getByRole('button', { name: '创建' }).click()

    const row = page.locator('tr', { hasText: aliceKbName })
    await expect(row).toBeVisible()
    await expect(row).toContainText('私有')
    await expect(row).toContainText(aliceId)
    expect(forbidden, `member 不应触发 403：${forbidden.join(' | ')}`).toEqual([])
  })

  test('同租户同事看不到未共享的私有资产', async ({ page }) => {
    await login(page, bobId, memberPass)
    await page.goto('/kbs')
    await page.getByRole('heading', { name: '知识库管理' }).waitFor()
    await expect(page.locator('tr', { hasText: aliceKbName })).toHaveCount(0)
  })

  test('作者共享后全租户可读', async ({ page }) => {
    await login(page, aliceId, memberPass)
    await page.goto('/kbs')
    await page.getByRole('heading', { name: '知识库管理' }).waitFor()
    const row = page.locator('tr', { hasText: aliceKbName })
    await row.getByRole('button', { name: '共享' }).click()
    await expect(row).toContainText('租户共享')
  })

  test('共享资产对同事只读：可检索、无删除/收回', async ({ page }) => {
    await login(page, bobId, memberPass)
    await page.goto('/kbs')
    await page.getByRole('heading', { name: '知识库管理' }).waitFor()
    const row = page.locator('tr', { hasText: aliceKbName })
    await expect(row).toBeVisible()
    await expect(row).toContainText('租户共享')
    await expect(row.getByRole('button', { name: '删除' })).toHaveCount(0)
    await expect(row.getByRole('button', { name: '收回' })).toHaveCount(0)
    await expect(row.getByRole('button', { name: '检索' })).toBeVisible()
  })

  test('member 建 Agent 并可共享；导航与用量符合角色', async ({ page }) => {
    await login(page, aliceId, memberPass)
    await page.goto('/agents')
    await page.getByRole('heading', { name: 'Agent 配置' }).waitFor()
    await page.getByRole('button', { name: '新建 Agent' }).click()
    const dialog = page.locator('.el-dialog')
    await dialog.getByPlaceholder('agent id').fill(aliceAgentId)
    await dialog.getByPlaceholder('Agent 名称').fill(aliceAgentName)
    await dialog.getByRole('button', { name: '保存' }).click()

    const agRow = page.locator('tr', { hasText: aliceAgentName })
    await expect(agRow).toBeVisible()
    await expect(agRow).toContainText('私有')
    await agRow.getByRole('button', { name: '共享' }).click()
    await expect(agRow).toContainText('租户共享')

    // 员工自助面可见，管理面仍不可见
    const navLabels = await page.locator('.nav-label').allInnerTexts()
    for (const label of ['知识库', 'Skill 资产', 'IM 通道', '模型端点', '用量计量', '工具目录']) {
      expect(navLabels, `member 应看到「${label}」`).toContain(label)
    }
    for (const label of ['租户管理', '用户管理', '密钥管理', '审计日志']) {
      expect(navLabels, `member 不应看到「${label}」`).not.toContain(label)
    }

    // 用量页是「个人用量」视角
    await page.goto('/usage')
    await page.getByRole('heading', { name: '用量计量' }).waitFor()
    await expect(page.getByText('个人用量', { exact: false })).toBeVisible()
  })
})
