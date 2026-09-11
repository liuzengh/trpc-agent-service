// UI contract checks against isolated API fixtures; never writes to a live tenant.
// Start Vite, then E2E_BASE=http://127.0.0.1:5178 node tests/browser/e2e-applications.mjs
import assert from 'node:assert/strict'
import { mkdir } from 'node:fs/promises'
import { chromium } from 'playwright'

const base = process.env.E2E_BASE ?? 'http://127.0.0.1:5173'
const artifacts = new URL('./artifacts/', import.meta.url)
await mkdir(artifacts, { recursive: true })
const browser = await chromium.launch({ executablePath: process.env.CHROMIUM_PATH || undefined })
const page = await browser.newPage({ viewport: { width: 1600, height: 1000 } })
const errors = []
page.on('pageerror', (error) => errors.push(error.message))

function application(tenant, code, status, channels = [], tools = { allowed: [] }) {
  return {
    Config: {
      tenant_id: tenant, app_code: code, status, config_version: 3,
      instruction: '你是 Acme 的智能客服助手，为客户提供专业、友好、准确的帮助。\n\n请优先参考知识库；无法解答时，引导客户联系人工客服。',
      model: { provider_id: 'openai-primary', name: 'gpt-4o-mini' },
      tools,
      storage: { artifact: { driver: 'postgres' } },
      governance: { max_tool_calls: 8, budget_units: 100 }, audit: { retention_days: 90 }, channels,
    },
    PublishedAt: '2026-09-08T08:00:00Z', Checksum: 'fixture',
  }
}

const apps = [
  application(
    'acme',
    'support',
    'active',
    [
      { type: 'telegram', binding_id: 'support', credential_ref: 'env:TELEGRAM_SUPPORT' },
      { type: 'wecom', binding_id: 'support-cn', credential_ref: 'env:WECOM_SUPPORT' },
    ],
    {
      allowed: ['query_order', 'refund_order'],
      allowed_roles: { query_order: ['operator', 'admin'] },
      require_confirmation: ['refund_order'],
    },
  ),
  application('acme', 'sales-assistant', 'active', [{ type: 'wecom', binding_id: 'sales', credential_ref: 'env:WECOM_SALES' }]),
  application('acme', 'internal-help', 'disabled'),
  application('other', 'other-support', 'active'),
]

let lastWriteBody = null

await page.route('**/api/**', async (route) => {
  const path = new URL(route.request().url()).pathname
  const responses = {
    '/api/v1/auth/me': {
      platform_user_id: 'admin', role: 'admin', is_system_admin: true,
      tenants: [
        { tenant_id: 'acme', display_name: 'Acme', role: 'admin' },
        { tenant_id: 'other', display_name: 'Other', role: 'admin' },
      ],
    },
    '/api/v1/tenants': { tenants: [{ tenant_id: 'acme', display_name: 'Acme', role: 'admin' }, { tenant_id: 'other', display_name: 'Other', role: 'admin' }] },
    '/api/v1/apps': { applications: apps },
    '/api/v1/sessions/tenant': { sessions: [] },
    '/api/v1/sessions/messages': { messages: [] },
    '/api/v1/catalog': {
      model_providers: [
        {
          id: 'openai-primary', type: 'openai',
          models: [
            { name: 'gpt-4o-mini' },
            {
              name: 'reasoning-model',
              capabilities: {
                reasoning_efforts: ['low', 'high'],
                thinking_toggle: true,
                thinking_budget: true,
              },
            },
          ],
        },
        { id: 'backup-provider', type: 'openai', models: [{ name: 'backup-chat' }] },
      ],
      backend_profiles: [
        { profile_id: 'platform-postgres', display_name: '平台 PostgreSQL', driver: 'postgres', domains: ['session', 'memory', 'artifact'], available: true, capabilities: { multi_node: true, memory_console_browsing: true } },
        { profile_id: 'platform-pgvector', display_name: '平台 pgvector', driver: 'pgvector', domains: ['knowledge'], available: true, capabilities: { multi_node: true, memory_console_browsing: false } },
        { profile_id: 'artifact-s3', display_name: 'S3 兼容对象存储', driver: 's3', domains: ['artifact'], available: true, capabilities: { multi_node: true, memory_console_browsing: false } },
        { profile_id: 'artifact-cos', display_name: '腾讯云 COS', driver: 'cos', domains: ['artifact'], available: true, capabilities: { multi_node: true, memory_console_browsing: false } },
      ],
      channel_credential_refs: ['env:TELEGRAM_SUPPORT', 'env:WECOM_SUPPORT', 'env:WECOM_SALES'],
      tools: [
        { name: 'query_order', description: '查询订单与物流信息' },
        { name: 'refund_order', description: '发起订单退款' },
        { name: 'send_coupon', description: '向用户发放优惠券' },
      ],
    },
    '/api/v1/system': {
      info: {
        version: '1.0.0',
        listen_address: ':8080',
        kafka_topic: 'agent-events',
        kafka_brokers: 'kafka:9092',
        redis_address: 'redis:6379',
        channel_credential_refs: ['env:TELEGRAM_SUPPORT', 'env:WECOM_SUPPORT', 'env:WECOM_SALES'],
        model_providers: [
          {
            id: 'openai-primary', type: 'openai', configured: true,
            models: [
              { name: 'gpt-4o-mini' },
              {
                name: 'reasoning-model',
                capabilities: {
                  reasoning_efforts: ['low', 'high'],
                  thinking_toggle: true,
                  thinking_budget: true,
                },
              },
            ],
          },
          { id: 'backup-provider', type: 'openai', configured: true, models: [{ name: 'backup-chat' }] },
        ],
      },
      status: { postgres: 'ok', redis: 'ok', kafka: 'ok' },
    },
    '/api/v1/channels/status': {
      channels: [
        { channel: 'telegram', binding_id: 'support', state: 'connected' },
        { channel: 'wecom', binding_id: 'support-cn', state: 'connected' },
        { channel: 'wecom', binding_id: 'sales', state: 'connecting' },
      ],
    },
  }
  if (route.request().method() !== 'GET') {
    const rawBody = route.request().postData()
    if (rawBody) lastWriteBody = JSON.parse(rawBody)
    return route.fulfill({ status: 403, json: { error: '测试：当前角色没有写权限' } })
  }
  await route.fulfill({ status: path in responses ? 200 : 404, json: responses[path] ?? { error: 'Unknown fixture endpoint' } })
})

try {
  await page.goto(`${base}/console/?tab=bots`)
  await page.getByRole('button', { name: 'support', exact: true }).waitFor()
  assert.equal(await page.locator('.application-table tbody tr').count(), 3)
  const tableGeometry = await page.evaluate(() => {
    const content = document.querySelector('.content').getBoundingClientRect()
    const table = document.querySelector('.application-table').getBoundingClientRect()
    const actionCell = document.querySelector('.application-table tbody tr td:last-child').getBoundingClientRect()
    const actions = document.querySelector('.application-table tbody tr .row-actions')
    const buttons = [...actions.querySelectorAll('button')]
    const buttonRects = buttons.map((button) => button.getBoundingClientRect())
    return {
      contentWidth: content.width,
      tableRightGap: Math.abs(table.right - actionCell.right),
      actionCellWidth: actionCell.width,
      actionCount: buttons.length,
      actionsFit: actions.scrollWidth <= actions.clientWidth + 1,
      sameLine: buttonRects.every((rect) => Math.abs(rect.top - buttonRects[0].top) <= 1),
    }
  })
  assert.ok(tableGeometry.contentWidth >= 1200, 'robot workspace should use available desktop width')
  assert.ok(tableGeometry.tableRightGap <= 2, 'robot actions should align with the right edge of the table')
  assert.equal(tableGeometry.actionCount, 4, 'robot rows should keep chat, configuration, release, and status as direct actions')
  assert.equal(tableGeometry.actionsFit, true, 'four robot actions must fit without horizontal clipping')
  assert.equal(tableGeometry.sameLine, true, 'four robot actions must stay on one line')
  assert.ok(tableGeometry.actionCellWidth <= tableGeometry.contentWidth * 0.35, 'robot actions should not consume more than 35% of the desktop workspace')
  await page.screenshot({ path: new URL('applications-desktop.png', artifacts).pathname, fullPage: true })
  await page.getByRole('searchbox', { name: '搜索机器人' }).fill('sales')
  assert.equal(await page.locator('.application-table tbody tr').count(), 1)
  await page.getByRole('searchbox').fill('no-match')
  assert.match(await page.locator('.application-table').innerText(), /没有符合条件/)
  await page.getByRole('searchbox').fill('')
  await page.locator('.application-filters').getByRole('button', { name: /^已停用\s*1$/ }).click()
  assert.equal(await page.locator('.application-table tbody tr').count(), 1)
  assert.equal(await page.locator('.application-table').getByRole('button', { name: '对话', exact: true }).isDisabled(), true)
  await page.locator('.application-filters').getByRole('button', { name: /^全部\s*3$/ }).click()
  await page.getByRole('button', { name: 'support', exact: true }).click()
  const supportDialog = page.getByRole('dialog', { name: /编辑机器人/ })
  await supportDialog.waitFor()
  const dialogGeometry = await supportDialog.evaluate((dialog) => {
    const body = dialog.querySelector('.bot-dialog-body')
    const footer = dialog.querySelector('.bot-dialog-footer')
    return {
      width: dialog.getBoundingClientRect().width,
      bodyScrollable: body ? body.scrollHeight >= body.clientHeight : false,
      footerHeight: footer?.getBoundingClientRect().height ?? 0,
    }
  })
  assert.ok(dialogGeometry.width >= 800, 'desktop robot editor should use a wide configuration surface')
  assert.ok(dialogGeometry.bodyScrollable, 'robot editor body should own vertical scrolling')
  assert.ok(dialogGeometry.footerHeight >= 56, 'robot editor should keep a distinct footer action area')
  assert.match(await supportDialog.innerText(), /业务指令/)
  assert.match(await supportDialog.innerText(), /基本信息/)
  assert.match(await supportDialog.innerText(), /高级模型设置/)
  assert.match(await supportDialog.innerText(), /备用模型/)
  assert.match(await supportDialog.innerText(), /数据后端/)
  assert.match(await supportDialog.innerText(), /平台 PostgreSQL/)
  assert.match(await supportDialog.innerText(), /S3 兼容对象存储/)
  assert.match(await supportDialog.innerText(), /腾讯云 COS/)
  assert.match(await supportDialog.innerText(), /运行配额/)
  assert.match(await supportDialog.innerText(), /工具权限与审批/)
  assert.match(await supportDialog.innerText(), /外部渠道/)
  assert.equal(await supportDialog.getByRole('checkbox', { name: '启用 query_order' }).isChecked(), true)
  assert.equal(await supportDialog.getByRole('checkbox', { name: '启用 refund_order' }).isChecked(), true)
  assert.equal(await supportDialog.getByRole('checkbox', { name: '启用 send_coupon' }).isChecked(), false)
  assert.equal(await supportDialog.getByRole('checkbox', { name: 'refund_order 执行前审批' }).isChecked(), true)
  assert.equal(await supportDialog.getByRole('checkbox', { name: 'query_order 执行前审批' }).isChecked(), false)
  assert.equal(await supportDialog.locator('.bot-tool-list input[type="text"]').count(), 0, 'tool policy must not use free-form text inputs')
  assert.match(await supportDialog.innerText(), /查询订单与物流信息/)
  assert.match(await supportDialog.innerText(), /发起订单退款/)
  assert.doesNotMatch(await supportDialog.innerText(), /platform\.save_artifact/)
  assert.doesNotMatch(await supportDialog.innerText(), /获取当前时间/)
  assert.doesNotMatch(await supportDialog.innerText(), /platform\.current_time/)
  assert.equal(
    await supportDialog.locator('select').evaluateAll((selects) => selects.filter((select) => {
      const style = getComputedStyle(select)
      const rect = select.getBoundingClientRect()
      return rect.width > 1 && rect.height > 1 && style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0'
    }).length),
    0,
    'robot editor should not expose browser-native select elements',
  )
  assert.ok(await supportDialog.locator('.select-control').count() >= 4, 'robot editor dropdowns must share one custom select control')
  assert.equal(await supportDialog.locator('.bot-model-advanced[open]').count(), 1, 'advanced model settings should be visible by default')
  assert.equal(await supportDialog.locator('.channel-select.channel-telegram .select-control-leading svg').count(), 1, 'configured Telegram channel should use its brand mark')
  assert.equal(await supportDialog.locator('.channel-select.channel-wecom .select-control-leading :is(svg, img)').count(), 1, 'configured WeCom channel should use its brand mark')
  assert.equal(await supportDialog.getByText('已连接', { exact: true }).count(), 2, 'configured channel runtime states should be visible')
  assert.equal(await supportDialog.getByRole('combobox', { name: 'Telegram平台凭据' }).count(), 1, 'channel credentials must use the shared platform credential dropdown')
  assert.equal(await supportDialog.getByText('TELEGRAM_SUPPORT', { exact: true }).count() > 0, true, 'credential refs should be shown as platform credential labels, not env syntax inputs')
  const selectGeometry = await supportDialog.locator('.select-control').evaluateAll((controls) => controls.map((control) => {
    const trigger = control.getBoundingClientRect()
    const icon = control.querySelector('.select-control-icon')?.getBoundingClientRect()
    return {
      rightGap: icon ? trigger.right - icon.right : -1,
      centerDelta: icon ? Math.abs((trigger.top + trigger.height / 2) - (icon.top + icon.height / 2)) : 999,
    }
  }))
  assert.equal(selectGeometry.every((entry) => entry.rightGap >= 8 && entry.rightGap <= 16), true, 'all robot editor dropdown arrows should share one right inset')
  assert.equal(selectGeometry.every((entry) => entry.centerDelta <= 1), true, 'all robot editor dropdown arrows should be vertically centered')

  assert.equal(await supportDialog.getByText('手动填写其他模型…', { exact: true }).count(), 0, 'robot editor must only select managed platform models')
  assert.equal(await supportDialog.getByText('模型服务标识', { exact: true }).count(), 0, 'robot editor must not expose manual provider input')
  const modelSelect = supportDialog.locator('label').filter({ hasText: '平台模型' }).locator('.select-control')
  assert.match(await modelSelect.innerText(), /gpt-4o-mini/)
  assert.match(await modelSelect.innerText(), /openai-primary/)
  await modelSelect.click()
  await page.locator('.select-content').waitFor()
  assert.ok(await page.locator('.select-group-label').count() > 0, 'model dropdown should render provider groups in the shared menu')
  assert.ok(await page.getByRole('option').count() > 0, 'model dropdown should render styled menu options')
  await page.getByRole('option', { name: 'gpt-4o-mini', exact: true }).click()
  assert.match(await supportDialog.innerText(), /当前模型没有声明可调推理参数/)

  await modelSelect.click()
  await page.getByRole('option', { name: /reasoning-model/ }).click()
  assert.match(await supportDialog.innerText(), /仅显示当前模型实际支持的推理参数/)
  const effortSelect = supportDialog.locator('label').filter({ hasText: '推理强度' }).locator('.select-control')
  await effortSelect.click()
  assert.equal(await page.getByRole('option', { name: '低', exact: true }).count(), 1)
  assert.equal(await page.getByRole('option', { name: '高', exact: true }).count(), 1)
  assert.equal(await page.getByRole('option', { name: '中', exact: true }).count(), 0, 'reasoning effort must not invent unsupported levels')
  await page.getByRole('option', { name: '高', exact: true }).click()
  assert.equal(await supportDialog.getByText('思考深度', { exact: true }).count(), 0)
  assert.equal(await supportDialog.getByText('思考预算', { exact: true }).count(), 0)
  await supportDialog.locator('label').filter({ hasText: '启用推理模式' }).locator('input[type="checkbox"]').check()
  assert.equal(await supportDialog.locator('label').filter({ hasText: '推理 Token 上限' }).count(), 1)

  const fallbackSelect = supportDialog.getByRole('combobox', { name: '添加备用模型' })
  assert.equal(await fallbackSelect.count(), 1, 'adding a fallback model must use the shared dropdown')
  await fallbackSelect.click()
  await page.getByRole('option', { name: 'backup-chat', exact: true }).click()
  assert.equal(await supportDialog.getByRole('combobox', { name: '备用模型 1' }).count(), 1, 'each configured fallback remains a dropdown')
  assert.match(await supportDialog.getByRole('combobox', { name: '备用模型 1' }).innerText(), /backup-chat/)

  const storageSelect = supportDialog.locator('.bot-backend-field').filter({ hasText: '文件' }).locator('.select-control')
  await storageSelect.click()
  await page.locator('.select-content').waitFor()
  assert.equal(await page.getByRole('option', { name: /^平台 PostgreSQL/ }).count(), 1)
  assert.equal(await page.getByRole('option', { name: /^S3 兼容对象存储/ }).count(), 1)
  assert.equal(await page.getByRole('option', { name: /^腾讯云 COS/ }).count(), 1)
  await page.getByRole('option', { name: /^平台 PostgreSQL/ }).click()

  const channelSelect = supportDialog.locator('.channel-select.channel-telegram').first()
  await channelSelect.click()
  await page.locator('.select-content').waitFor()
  assert.ok(await page.locator('.select-item-leading :is(svg, img)').count() >= 3, 'channel dropdown should show native brand marks in its menu')
  await page.getByRole('option', { name: 'Telegram', exact: true }).click()
  assert.ok(await supportDialog.locator('.channels-body input').evaluateAll((inputs) => inputs.some((i) => i.value.includes('support-cn'))))
  await supportDialog.getByRole('button', { name: '取消', exact: true }).click()
  await supportDialog.waitFor({ state: 'detached' })
  await page.getByRole('navigation', { name: '主导航' }).getByText('对话', { exact: true }).click()
  await page.getByRole('combobox', { name: '当前机器人' }).click()
  await page.getByRole('option', { name: 'other-support', exact: true }).click()
  await page.getByRole('button', { name: '机器人设置' }).click()
  assert.equal(await page.locator('.application-table tbody tr').count(), 1)
  await page.getByRole('button', { name: 'other-support', exact: true }).click()
  const otherDialog = page.getByRole('dialog', { name: /编辑机器人/ })
  await otherDialog.waitFor()
  assert.match(await otherDialog.innerText(), /other-support/)
  assert.doesNotMatch(await otherDialog.innerText(), /support-cn/)
  await otherDialog.getByRole('button', { name: '取消', exact: true }).click()
  await otherDialog.waitFor({ state: 'detached' })
  await page.getByRole('navigation', { name: '主导航' }).getByText('对话', { exact: true }).click()
  await page.getByRole('combobox', { name: '当前机器人' }).click()
  await page.getByRole('option', { name: 'support', exact: true }).click()
  await page.getByRole('button', { name: '机器人设置' }).click()
  await page.locator('.application-table').getByRole('button', { name: '停用', exact: true }).first().click()
  await page.getByRole('alert').waitFor()
  assert.match(await page.getByRole('alert').innerText(), /没有写权限/)
  assert.deepEqual(lastWriteBody?.tools, {
    allowed: ['query_order', 'refund_order'],
    allowed_roles: { query_order: ['operator', 'admin'] },
    require_confirmation: ['refund_order'],
  }, 'status changes must preserve the complete tool governance policy')
  await page.setViewportSize({ width: 390, height: 844 })
  await page.screenshot({ path: new URL('applications-mobile.png', artifacts).pathname, fullPage: true })
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth), true, 'page must not overflow horizontally')
  assert.equal(await page.locator('main h1').count(), 0, 'workspace pages must not repeat a page title')
  await page.getByRole('button', { name: '创建机器人', exact: true }).click()
  const dialog = page.getByRole('dialog', { name: '创建机器人' })
  await dialog.waitFor()
  assert.equal(await dialog.locator('input').count() > 0, true)
  assert.equal(await dialog.locator('.bot-form-section').count(), 6, 'create and edit should share the same six-section configuration layout')
  assert.match(await dialog.innerText(), /基本信息/)
  assert.match(await dialog.innerText(), /高级模型设置/)
  assert.match(await dialog.innerText(), /运行配额/)
  assert.equal(await dialog.getByRole('checkbox', { name: '启用 query_order' }).isChecked(), false, 'create dialog should use the same tool catalog with no default grants')
  assert.match(await dialog.innerText(), /外部渠道/)
  assert.equal(await dialog.getByText('所属租户', { exact: true }).count(), 0, 'create dialog must inherit the current tenant instead of exposing a tenant selector')
  assert.equal(await dialog.getByText('使用新租户', { exact: true }).count(), 0, 'tenant provisioning must stay outside robot creation')
  assert.equal(await dialog.getByText('状态', { exact: true }).count(), 0, 'create dialog must not ask for runtime status')
  assert.equal(await dialog.getByText(/app_code/i).count(), 0, 'create dialog must not expose app_code terminology')
  assert.equal(await dialog.getByText(/Artifact|工具白名单/i).count(), 0, 'create dialog must not expose backend jargon')
  assert.equal(await dialog.getByText('可用工具', { exact: true }).count(), 0)
  assert.equal(await dialog.getByText('数据后端', { exact: true }).count(), 1)
  assert.equal(await dialog.evaluate((element) => element.contains(document.activeElement)), true, 'dialog must own keyboard focus')
  await page.keyboard.press('Escape')
  assert.equal(await dialog.count(), 0, 'Escape must close the dialog')
  assert.deepEqual(errors, [])
  console.log('PASS: search, empty state, status, disabled chat, config details, tenant isolation, action error, mobile layout, accessible dialog; zero browser errors')
} finally {
  await browser.close()
}
