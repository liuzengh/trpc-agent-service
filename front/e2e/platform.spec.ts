import { test, expect } from '@playwright/test'

const ts = Date.now()
const tenantId = `e2e-t${ts}`
const epId = `ep-${ts}`
const epName = `E2E GPT ${ts}`
const kbId = `kb-${ts}`
const kbName = `E2E FAQ ${ts}`
const agentId = `ag-${ts}`
const agentName = `E2E 助手 ${ts}`

// State is held by the in-memory backend that the playwright webServer boots,
// so ids must be unique per run (they persist across tests within one run).

async function openTenant(page: import('@playwright/test').Page) {
  await page.goto('/')
  await page.getByRole('heading', { name: '租户管理' }).waitFor()
}

async function selectFirst(page: import('@playwright/test').Page, dialog: import('@playwright/test').Locator, text: string) {
  // Element Plus select: click the select input inside the dialog, then pick
  // the first option whose text matches. Multi-select stays open; Escape closes.
  await dialog.locator('.el-select').last().click()
  const opt = page.locator('.el-select-dropdown__item', { hasText: text }).first()
  await opt.click()
  await page.keyboard.press('Escape')
}

test.describe('租户管理页', () => {
  test('新建租户出现在列表', async ({ page }) => {
    await openTenant(page)
    await page.getByRole('button', { name: '新建租户' }).click()
    await page.getByPlaceholder('tenant id').fill(tenantId)
    await page.getByPlaceholder('租户名称').fill('E2E 租户')
    await page.getByRole('button', { name: '保存' }).click()
    await expect(page.locator('tr', { hasText: tenantId })).toBeVisible()
  })

  test('删除租户', async ({ page }) => {
    await openTenant(page)
    const row = page.locator('tr', { hasText: tenantId })
    await row.getByRole('button', { name: '删除' }).click()
    await expect(page.locator('tr', { hasText: tenantId })).toHaveCount(0)
  })
})

test.describe('端点 → 知识库 → Agent 发布链路', () => {
  test('新建模型端点', async ({ page }) => {
    await page.goto('/endpoints')
    await page.getByRole('heading', { name: '模型端点' }).waitFor()
    await page.getByRole('button', { name: '新建端点' }).click()

    await page.getByPlaceholder('endpoint id').fill(epId)
    await page.getByPlaceholder('管理用名称').fill(epName)
    await page.getByPlaceholder('https://api.openai.com/v1').fill('https://api.openai.com/v1')
    await page.getByPlaceholder('gpt-4o / claude-3-5 / glm-4.7').fill('gpt-4o')
    await page.getByPlaceholder('sk-...').fill('sk-e2e')
    const dialog = page.locator('.el-dialog')
    await dialog.getByPlaceholder('tenant id').fill(tenantId)
    await page.getByRole('button', { name: '保存' }).click()
    // 端点表格不展示 ID 列，用名称匹配
    await expect(page.locator('tr', { hasText: epName })).toBeVisible()
  })

  test('新建知识库（挂载端点）', async ({ page }) => {
    await page.goto('/kbs')
    await page.getByRole('heading', { name: '知识库管理' }).waitFor()
    await page.getByRole('button', { name: '新建知识库' }).click()

    await page.getByPlaceholder('留空自动生成').fill(kbId)
    await page.getByPlaceholder('tenant id').fill(tenantId)
    await page.getByPlaceholder('KB 名称').fill(kbName)
    await selectFirst(page, page.locator('.el-dialog'), epName)
    await page.getByRole('button', { name: '创建' }).click()
    // KB 表格无 ID 列，用名称匹配
    await expect(page.locator('tr', { hasText: kbName })).toBeVisible()
  })

  test('发布 Agent 挂载端点 + 知识库', async ({ page }) => {
    // Seed the bare agent over the API (the UI only edits/publishes).
    const res = await page.request.post('http://localhost:8080/agents', {
      data: { id: agentId, tenant_id: tenantId, name: agentName, status: 'draft', current_version: 0 },
    })
    expect(res.ok()).toBeTruthy()

    await page.goto('/agents')
    await page.getByRole('heading', { name: 'Agent 配置' }).waitFor()
    // Agent 表格无 ID 列，用名称定位
    const row = page.locator('tr', { hasText: agentName })
    await row.getByRole('button', { name: '发布' }).click()

    const dialog = page.locator('.el-dialog')
    await dialog.getByPlaceholder('系统提示词，如：你是客服助手，引用知识库回答…').fill('你是 E2E 助手')
    await dialog.locator('.el-select').first().click()
    await page.locator('.el-select-dropdown__item', { hasText: epName }).first().click()
    await page.keyboard.press('Escape')
    await dialog.locator('.el-select').nth(2).click()
    await page.locator('.el-select-dropdown__item', { hasText: kbName }).first().click()
    await page.keyboard.press('Escape')

    await dialog.getByRole('button', { name: '发布', exact: true }).click()
    await expect(page.getByText('已发布 v1')).toBeVisible()
  })

  test('Agent 列表显示已发布版本', async ({ page }) => {
    await page.goto('/agents')
    await page.getByRole('heading', { name: 'Agent 配置' }).waitFor()
    const row = page.locator('tr', { hasText: agentName })
    await expect(row).toBeVisible()
    await expect(row.locator('td').nth(3)).toContainText('1')
    await expect(row).toContainText('published')
  })
})
