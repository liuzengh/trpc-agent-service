import assert from 'node:assert/strict'
import { chromium } from 'playwright'

const base = process.env.E2E_BASE ?? 'http://127.0.0.1:5178'
const browser = await chromium.launch({ executablePath: process.env.CHROMIUM_PATH || undefined })
const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } })
const errors = []
page.on('pageerror', (error) => errors.push(error.message))

let createPayload = null

const catalog = {
  model_providers: [
    {
      id: 'primary', type: 'openai',
      models: [
        { name: 'chat-basic' },
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
    { id: 'backup', type: 'openai', models: [{ name: 'backup-chat' }] },
  ],
  backend_profiles: [
    { profile_id: 'pg', display_name: 'PostgreSQL', driver: 'postgres', domains: ['session', 'memory', 'artifact'], available: true, capabilities: { multi_node: true, memory_console_browsing: true } },
    { profile_id: 'vec', display_name: 'pgvector', driver: 'pgvector', domains: ['knowledge'], available: true, capabilities: { multi_node: true, memory_console_browsing: false } },
  ],
  tools: [
    { name: 'duckduckgo_search', description: '搜索公开网页' },
    { name: 'platform_present_card', description: 'Present a compact result card' },
    { name: 'query_order', description: '查询订单' },
  ],
  channel_credential_refs: ['env:FEISHU_SUPPORT', 'env:TELEGRAM_SUPPORT'],
  tool_credential_refs: ['env:TOOL_TOKEN'],
}

function snapshot(payload) {
  return {
    Config: { ...payload, config_version: 1 },
    Checksum: 'fixture', PublishedAt: '2026-09-11T00:00:00Z',
  }
}

await page.route('**/api/**', async (route) => {
  const request = route.request()
  const url = new URL(request.url())
  const path = url.pathname
  if (path === '/api/v1/auth/me') {
    return route.fulfill({
      status: 200,
      json: {
        platform_user_id: 'tenant-admin', role: 'admin', is_system_admin: false,
        tenants: [{ tenant_id: 'support', display_name: '客服测试', role: 'admin' }],
      },
    })
  }
  if (path === '/api/v1/tenants') {
    return route.fulfill({ status: 200, json: { tenants: [{ tenant_id: 'support', display_name: '客服测试', role: 'admin', status: 'active' }] } })
  }
  if (path === '/api/v1/apps' && request.method() === 'POST') {
    createPayload = request.postDataJSON()
    return route.fulfill({ status: 200, json: { application: snapshot(createPayload) } })
  }
  if (path === '/api/v1/apps') return route.fulfill({ status: 200, json: { applications: createPayload ? [snapshot(createPayload)] : [] } })
  if (path === '/api/v1/catalog') return route.fulfill({ status: 200, json: catalog })
  if (path === '/api/v1/sessions/mine') return route.fulfill({ status: 200, json: { sessions: [] } })
  if (path === '/api/v1/sessions/messages') return route.fulfill({ status: 200, json: { messages: [], next_cursor: '' } })
  return route.fulfill({ status: 404, json: { error: `missing fixture: ${request.method()} ${path}` } })
})

async function choose(trigger, name) {
  await trigger.click()
  await page.getByRole('option', { name, exact: false }).click()
}

try {
  await page.goto(`${base}/console/?tab=bots`)
  await page.getByRole('button', { name: '创建机器人', exact: true }).click()
  const dialog = page.getByRole('dialog', { name: '创建机器人' })
  await dialog.waitFor()

  assert.equal(await dialog.getByText('所属租户', { exact: true }).count(), 0)
  assert.equal(await dialog.getByText('使用新租户', { exact: true }).count(), 0)
  assert.equal(await dialog.getByText('结果卡片', { exact: true }).count(), 0, 'result-card rendering is platform-managed and must not be configurable')
  await dialog.getByPlaceholder('例如 support').fill('support-assistant')
  await dialog.getByPlaceholder('描述机器人的角色、业务范围和回答规则。').fill('处理客服问题。')

  await choose(dialog.getByRole('combobox', { name: '平台模型' }), 'reasoning-model')
  await dialog.getByRole('checkbox', { name: '自定义采样温度' }).check()
  const temperature = dialog.locator('.slider-control input[type="range"]')
  await temperature.fill('1.1')
  await dialog.locator('label').filter({ hasText: '最大输出长度' }).locator('input').fill('1024')
  await dialog.locator('label').filter({ hasText: '采样阈值' }).locator('input').fill('0.9')
  const reasoningEffort = dialog.getByRole('combobox', { name: '推理强度' })
  assert.equal(await reasoningEffort.isDisabled(), true, 'reasoning effort must be disabled until reasoning mode is enabled')
  await dialog.getByRole('checkbox', { name: '启用推理模式' }).check()
  assert.equal(await reasoningEffort.isDisabled(), false)
  await choose(reasoningEffort, '高')
  await dialog.locator('label').filter({ hasText: '推理 Token 上限' }).locator('input').fill('2048')

  const failover = dialog.getByRole('combobox', { name: '添加备用模型' })
  await choose(failover, 'backup-chat')
  await dialog.getByRole('button', { name: '删除备用模型 1', exact: true }).waitFor()
  await dialog.getByRole('button', { name: '删除备用模型 1', exact: true }).click()
  assert.equal(await dialog.getByRole('button', { name: '删除备用模型 1', exact: true }).count(), 0)
  await choose(failover, 'backup-chat')

  const webSearch = dialog.getByRole('checkbox', { name: '启用 网络搜索' })
  const webSearchApproval = dialog.getByRole('checkbox', { name: '网络搜索 执行前审批' })
  assert.equal(await webSearchApproval.isDisabled(), true, 'approval must be disabled until the tool is enabled')
  await webSearch.check()
  await webSearchApproval.check()
  await webSearch.uncheck()
  assert.equal(await webSearchApproval.isChecked(), false, 'disabling a tool must clear its approval requirement')
  await webSearch.check()

  const httpSection = dialog.locator('.bot-tool-policy').filter({ hasText: '自定义 HTTP 工具' })
  await httpSection.getByRole('button', { name: '添加', exact: true }).click()
  await httpSection.getByPlaceholder('例如 query_order').fill('lookup_ticket')
  await httpSection.getByPlaceholder('https://api.example.com/query').fill('https://api.example.com/ticket')
  await httpSection.getByPlaceholder('告诉模型什么时候调用').fill('查询客服工单')
  await httpSection.getByPlaceholder('可选').fill('env:TOOL_TOKEN')
  await httpSection.locator('textarea').fill('{"type":"object","properties":{"id":{"type":"string"}}}')
  const httpApproval = httpSection.locator('label').filter({ hasText: '执行前审批' }).getByRole('checkbox')
  await httpApproval.check()

  const mcpSection = dialog.locator('.bot-tool-policy').filter({ hasText: 'MCP 服务' })
  await mcpSection.getByRole('button', { name: '添加', exact: true }).click()
  await mcpSection.getByPlaceholder('例如 crm').fill('crm')
  await choose(mcpSection.getByRole('combobox', { name: '传输协议' }), 'SSE（兼容）')
  await mcpSection.getByPlaceholder('https://mcp.example.com/mcp').fill('https://mcp.example.com/mcp')
  await mcpSection.getByPlaceholder('例如客户资料查询与工单工具').fill('客户资料工具')
  await mcpSection.getByPlaceholder('find_customer, create_ticket').fill('find_customer, create_ticket')
  await mcpSection.getByPlaceholder('create_ticket', { exact: true }).fill('create_ticket')

  await dialog.getByRole('button', { name: '添加渠道', exact: true }).click()
  await choose(dialog.getByRole('combobox', { name: '渠道 1' }), '飞书')
  await dialog.getByRole('textbox', { name: '飞书机器人账号标识' }).fill('support-feishu')
  await choose(dialog.getByRole('combobox', { name: '飞书平台凭据' }), 'FEISHU_SUPPORT')
  await choose(dialog.getByRole('combobox', { name: '飞书访问范围' }), '成员或白名单')
  const allowlist = dialog.getByRole('textbox', { name: '飞书白名单' })
  await allowlist.fill('user-a, user-b')
  await choose(dialog.getByRole('combobox', { name: '飞书访问范围' }), '公开访问')
  assert.equal(await dialog.getByRole('textbox', { name: '飞书白名单' }).count(), 0, 'public access must hide the allowlist editor')
  await choose(dialog.getByRole('combobox', { name: '飞书访问范围' }), '成员或白名单')
  await dialog.getByRole('textbox', { name: '飞书白名单' }).fill('user-a, user-b')

  await dialog.getByRole('button', { name: '创建机器人', exact: true }).click()
  for (let index = 0; index < 100 && !createPayload; index += 1) await page.waitForTimeout(20)
  assert.ok(createPayload, 'create button must send the configured robot')
  assert.equal(createPayload.tenant_id, 'support', 'robot tenant must come from the active tenant context')
  assert.deepEqual(createPayload.model.failover_candidates, [{ provider_id: 'backup', name: 'backup-chat' }])
  assert.deepEqual(createPayload.model.generation, {
    temperature: 1.1,
    max_tokens: 1024,
    top_p: 0.9,
    thinking_enabled: true,
    reasoning_effort: 'high',
    thinking_tokens: 2048,
  })
  assert.ok(createPayload.tools.allowed.includes('duckduckgo_search'))
  assert.ok(createPayload.tools.allowed.includes('lookup_ticket'))
  assert.ok(createPayload.tools.allowed.includes('crm_find_customer'))
  assert.ok(createPayload.tools.require_confirmation.includes('lookup_ticket'))
  assert.ok(createPayload.tools.require_confirmation.includes('crm_create_ticket'))
  assert.equal(createPayload.tools.http[0].credential_ref, 'env:TOOL_TOKEN')
  assert.equal(createPayload.tools.mcp[0].transport, 'sse')
  assert.deepEqual(createPayload.channels, [{
    type: 'feishu',
    binding_id: 'support-feishu',
    credential_ref: 'env:FEISHU_SUPPORT',
    access_policy: 'allowlist',
    allowlist: ['user-a', 'user-b'],
  }])

  assert.deepEqual(errors, [])
  console.log('PASS: robot model/failover/tool/channel form controls serialize through the public create API')
} finally {
  await browser.close()
}
