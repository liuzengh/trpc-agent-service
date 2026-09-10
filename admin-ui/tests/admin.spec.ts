import { readFileSync } from 'node:fs'
import { expect, test } from '@playwright/test'

type Fixture = {
  tenant_id: string
  app_id: string
  source_config_version: string
  target_config_version: string
  published_config_version: string
  binding_id: string
  execution_request_id: string
  approval_id: string
  migration_id: string
}

const secretMarker = 'admin-ui-e2e-raw-secret'

function fixture(): Fixture {
  const path = process.env.TRPC_ADMIN_UI_E2E_FIXTURE
  if (!path) throw new Error('TRPC_ADMIN_UI_E2E_FIXTURE is required')
  return JSON.parse(readFileSync(path, 'utf8')) as Fixture
}

function scopeURL(path: string, value: Fixture): string {
  const params = new URLSearchParams({ tenant_id: value.tenant_id, app_id: value.app_id })
  return `${path}?${params}`
}

test('admin golden path uses the real control-plane API', async ({ page }, testInfo) => {
  const value = fixture()
  const adminToken = process.env.TRPC_AGENT_SERVICE_ADMIN_TOKEN ?? 'development-only-admin-token'
  const failedRequests: string[] = []
  const failedResponses: string[] = []
  const consoleErrors: string[] = []
  const responseBodies: string[] = []
  const sensitiveNetwork: string[] = []

  page.on('requestfailed', request => failedRequests.push(`${request.method()} ${request.url()}`))
  page.on('console', message => {
    if (message.type() === 'error') consoleErrors.push(message.text())
  })
  page.on('request', request => {
    const material = `${request.url()}\n${request.postData() ?? ''}`
    if (material.includes(adminToken) || material.includes(secretMarker)) sensitiveNetwork.push(request.url())
  })
  page.on('response', async response => {
    if (!response.url().includes('/admin/v1/')) return
    if (response.status() >= 400) failedResponses.push(`${response.status()} ${response.url()}`)
    try {
      responseBodies.push(await response.text())
    } catch {
      // The browser may dispose a response before its body is inspected.
    }
  })

  await page.goto('/')
  await page.getByTestId('admin-token').fill(adminToken)
  await page.getByRole('button', { name: '登录', exact: true }).click()
  await expect(page.getByRole('heading', { name: '总览', exact: true })).toBeVisible()
  await page.screenshot({ path: testInfo.outputPath('overview.png'), fullPage: true })

  await page.getByRole('button', { name: '租户', exact: true }).click()
  await expect(page.getByText(value.tenant_id, { exact: true }).first()).toBeVisible()
  await page.getByText(value.tenant_id, { exact: true }).first().click()
  await expect(page.getByRole('heading', { name: `租户 · ${value.tenant_id}`, exact: true })).toBeVisible()
  await page.getByRole('link', { name: value.app_id, exact: true }).click()
  await expect(page.getByRole('heading', { name: `Agent 应用 · ${value.app_id}`, exact: true })).toBeVisible()

  await page.getByRole('button', { name: '配置版本', exact: true }).click()
  await page.getByLabel('租户', { exact: true }).fill(value.tenant_id)
  await page.getByLabel('应用', { exact: true }).fill(value.app_id)
  await page.getByRole('button', { name: '应用范围', exact: true }).click()
  await expect(page.getByRole('heading', { name: '配置版本', exact: true })).toBeVisible()
  await expect(page.getByText(value.source_config_version, { exact: true }).first()).toBeVisible()

  const draft = page.getByLabel('不可变配置草稿')
  await expect(draft).not.toHaveValue('')
  await page.getByLabel('版本', { exact: true }).fill(value.published_config_version)
  await page.getByRole('button', { name: '发布新版本', exact: true }).click()
  await expect(page.getByText(`已发布不可变版本 ${value.published_config_version}`, { exact: true })).toBeVisible()
  await page.screenshot({ path: testInfo.outputPath('config-publish.png'), fullPage: true })

  await page.reload()
  await expect(page.getByRole('heading', { name: '配置版本', exact: true })).toBeVisible()
  const publishedRow = page.locator('tr').filter({ hasText: value.published_config_version }).first()
  await publishedRow.getByRole('button', { name: '激活', exact: true }).click()
  await expect(page.getByText(/当前版本：v1/)).toBeVisible()
  await page.getByRole('button', { name: '确认', exact: true }).click()
  await expect(page.getByText(`已激活 ${value.published_config_version}`, { exact: true })).toBeVisible()

  await page.reload()
  await expect(page.getByRole('heading', { name: '配置版本', exact: true })).toBeVisible()
  const sourceRow = page.locator('tr').filter({ hasText: value.source_config_version }).first()
  await sourceRow.getByRole('button', { name: '回滚', exact: true }).click()
  await expect(page.getByText(/目标版本：v1/)).toBeVisible()
  await page.getByRole('button', { name: '确认', exact: true }).click()
  await expect(page.getByText(`已回滚 ${value.source_config_version}`, { exact: true })).toBeVisible()
  await page.screenshot({ path: testInfo.outputPath('config-rollback.png'), fullPage: true })

  await page.getByRole('button', { name: '后端', exact: true }).click()
  await expect(page.getByText('redis', { exact: true }).first()).toBeVisible()

  await page.getByRole('button', { name: '通道', exact: true }).click()
  const bindingRow = page.locator('tr').filter({ hasText: value.binding_id }).first()
  await expect(bindingRow).toBeVisible()
  await bindingRow.getByRole('button', { name: '暂停', exact: true }).click()
  await expect(page.getByText(/停止入站消息/)).toBeVisible()
  await page.getByRole('button', { name: '确认', exact: true }).click()
  await expect(page.getByText(`${value.binding_id}：已提交暂停`, { exact: true })).toBeVisible()

  await page.getByRole('button', { name: '执行记录', exact: true }).click()
  const executionRow = page.locator('tr').filter({ hasText: value.execution_request_id }).first()
  await expect(executionRow).toBeVisible()
  await executionRow.getByRole('button', { name: '打开', exact: true }).click()
  await expect(page.getByText('Worker 所有权', { exact: true })).toBeVisible()

  await page.goto(scopeURL('/approvals', value))
  await expect(page.getByRole('heading', { name: '审批', exact: true })).toBeVisible()
  const approvalRow = page.locator('tr').filter({ hasText: value.approval_id }).first()
  await expect(approvalRow).toBeVisible()
  await approvalRow.getByRole('button', { name: '详情', exact: true }).click()
  await expect(page.getByText('请求摘要', { exact: true })).toBeVisible()
  await page.getByRole('button', { name: '关闭', exact: true }).click()
  await approvalRow.getByRole('button', { name: '批准', exact: true }).click()
  await expect(page.getByText(/参数摘要/)).toBeVisible()
  await page.getByRole('button', { name: '确认', exact: true }).click()
  await expect(page.getByText(`${value.approval_id}：已批准`, { exact: true })).toBeVisible()

  await page.goto(scopeURL('/executions', value))
  await expect(page.getByRole('heading', { name: '执行记录', exact: true })).toBeVisible()
  let executionSucceeded = false
  for (let attempt = 0; attempt < 30; attempt += 1) {
    if (await page.getByText('成功', { exact: true }).count()) {
      executionSucceeded = true
      break
    }
    await page.reload()
    await expect(page.getByRole('heading', { name: '执行记录', exact: true })).toBeVisible()
  }
  expect(executionSucceeded).toBe(true)
  await page.screenshot({ path: testInfo.outputPath('execution-approved.png'), fullPage: true })

  await page.goto(scopeURL('/audit', value))
  await expect(page.getByRole('heading', { name: '审计日志', exact: true })).toBeVisible()
  await page.goto(scopeURL('/migrations', value))
  await expect(page.getByRole('heading', { name: '数据迁移', exact: true })).toBeVisible()
  const migrationRow = page.locator('tr').filter({ hasText: value.migration_id }).first()
  await expect(migrationRow).toBeVisible()
  await migrationRow.getByRole('button', { name: '开始', exact: true }).click()
  await expect(page.getByText(/Session 数据将从 Redis 迁移到 PostgreSQL/)).toBeVisible()
  await page.getByRole('button', { name: '确认', exact: true }).click()
  await expect(page.getByText(`${value.migration_id}：已提交开始迁移`, { exact: true })).toBeVisible()
  await page.reload()
  await expect(page.getByRole('heading', { name: '数据迁移', exact: true })).toBeVisible()
  await expect(page.locator('tr').filter({ hasText: value.migration_id }).first()).toContainText('排空中')

  await page.getByRole('button', { name: '运行状态', exact: true }).click()
  await expect(page.getByRole('heading', { name: '运行状态', exact: true })).toBeVisible()
  await page.screenshot({ path: testInfo.outputPath('operations.png'), fullPage: true })

  expect(consoleErrors).toEqual([])
  expect(failedRequests).toEqual([])
  expect(failedResponses).toEqual([])
  expect(sensitiveNetwork).toEqual([])
  expect(responseBodies.join('\n')).not.toContain(secretMarker)
  const bodyText = await page.locator('body').innerText()
  expect(bodyText).not.toContain(secretMarker)
  expect(bodyText).not.toContain('undefined')
  expect(bodyText).not.toContain('NaN%')
})

test('invalid admin credentials are rejected by the real API', async ({ request }) => {
  const response = await request.get('/admin/v1/session', {
    headers: { Authorization: 'Bearer invalid-admin-ui-token' },
  })
  expect(response.status()).toBe(401)
})
