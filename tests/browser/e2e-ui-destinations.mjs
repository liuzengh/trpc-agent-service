import assert from 'node:assert/strict'
import { mkdir } from 'node:fs/promises'
import { chromium } from 'playwright'

const base = process.env.E2E_BASE ?? 'http://127.0.0.1:5173'
const artifacts = new URL('./artifacts/apple-destinations/', import.meta.url)
await mkdir(artifacts, { recursive: true })

const browser = await chromium.launch({ executablePath: process.env.CHROMIUM_PATH || undefined })
const page = await browser.newPage({ viewport: { width: 1600, height: 1000 } })
const browserErrors = []
page.on('pageerror', (error) => browserErrors.push(error.message))

const app = {
  Config: {
    tenant_id: 'acme',
    app_code: 'support',
    status: 'active',
    config_version: 3,
    instruction: '帮助客户快速解决售后问题。',
    model: { provider_id: 'openai-primary', name: 'gpt-5.6-mini' },
    tools: { allowed: [] },
    storage: { artifact: { driver: 'postgres' } },
    governance: { max_tool_calls: 8, budget_units: 100 },
    audit: { retention_days: 90 },
    channels: [{ type: 'wecom', binding_id: 'support-cn' }],
  },
  PublishedAt: '2026-09-09T08:00:00Z',
  Checksum: 'fixture',
}

const salesApp = {
  ...app,
  Config: {
    ...app.Config,
    app_code: 'sales',
    instruction: '帮助客户了解产品与方案。',
    channels: [{ type: 'telegram', binding_id: 'sales-tg' }],
  },
  Checksum: 'fixture-sales',
}

const system = {
  info: {
    version: '1.0.0',
    listen_address: ':8080',
    kafka_topic: 'agent-events',
    kafka_brokers: 'kafka:9092',
    redis_address: 'redis:6379',
    channel_credential_refs: ['env:WECOM_SUPPORT', 'env:TELEGRAM_SUPPORT', 'env:FEISHU_SUPPORT'],
    model_providers: [
      {
        id: 'openai-primary',
        type: 'openai-responses',
        base_url: 'https://api.example.test/v1',
        configured: true,
        models: [
          { name: 'gpt-5.6-mini', source: 'discovered' },
          { name: 'gpt-5.6', source: 'configured', capabilities: { reasoning_efforts: ['low', 'medium', 'high'] } },
        ],
      },
    ],
  },
  status: { postgres: 'ok', redis: 'ok', kafka: 'ok' },
}

const sessions = [
  {
    TenantID: 'acme',
    AppCode: 'support',
    SessionKey: 'acme/support/session/session-canonical',
    SubjectID: 'admin',
    OwnerPlatformUserID: 'admin',
    Status: 'active',
    UpdatedAt: '2026-09-09T09:35:00Z',
    Preview: '帮我查询一下订单状态',
    Conversations: [
      {
        TenantID: 'acme', AppCode: 'support', SessionKey: 'acme/support/session/session-canonical',
        Channel: 'web', BindingID: 'web-console', ConversationID: 'web-admin', ExternalUserID: 'admin',
        Scope: 'direct', StartedAt: '2026-09-09T09:10:00Z', UpdatedAt: '2026-09-09T09:15:00Z',
      },
      {
        TenantID: 'acme', AppCode: 'support', SessionKey: 'acme/support/session/session-canonical',
        Channel: 'wecom', BindingID: 'support-cn', ConversationID: 'wx-user-42', ExternalUserID: 'wx-user-42',
        Scope: 'direct', StartedAt: '2026-09-09T09:20:00Z', UpdatedAt: '2026-09-09T09:35:00Z',
      },
      {
        TenantID: 'acme', AppCode: 'support', SessionKey: 'acme/support/session/session-canonical',
        Channel: 'telegram', BindingID: 'support-tg', ConversationID: '42001', ExternalUserID: '42001',
        Scope: 'direct', StartedAt: '2026-09-09T09:22:00Z', UpdatedAt: '2026-09-09T09:30:00Z',
      },
    ],
  },
  {
    TenantID: 'acme',
    AppCode: 'support',
    SessionKey: 'acme/support/session/session-anonymous-telegram',
    SubjectID: 'external:telegram:support-tg:42002',
    Status: 'active',
    UpdatedAt: '2026-09-09T09:30:00Z',
    Preview: '请帮我看一下发货时间',
    Conversations: [{
      TenantID: 'acme', AppCode: 'support', SessionKey: 'acme/support/session/session-anonymous-telegram',
      Channel: 'telegram', BindingID: 'support-tg', ConversationID: '42002', ExternalUserID: '42002',
      Scope: 'direct', StartedAt: '2026-09-09T09:26:00Z', UpdatedAt: '2026-09-09T09:30:00Z',
    }],
  },
  {
    TenantID: 'acme',
    AppCode: 'support',
    SessionKey: 'acme/support/session/session-feishu',
    SubjectID: 'group:feishu:support-feishu:oc_group_1',
    Status: 'active',
    UpdatedAt: '2026-09-09T09:25:00Z',
    Preview: '你们支持无理由退货吗',
    Conversations: [{
      TenantID: 'acme', AppCode: 'support', SessionKey: 'acme/support/session/session-feishu',
      Channel: 'feishu', BindingID: 'support-feishu', ConversationID: 'oc_group_1', ExternalUserID: 'ou_35a71644c5',
      Scope: 'group', StartedAt: '2026-09-09T09:15:00Z', UpdatedAt: '2026-09-09T09:25:00Z',
    }],
  },
]

for (let index = sessions.length; index < 10; index += 1) {
  const suffix = String(index + 1).padStart(2, '0')
  sessions.push({
    TenantID: 'acme',
    AppCode: 'support',
    SessionKey: `acme/support/session/session-web-${suffix}`,
    SubjectID: `web:user-${suffix}`,
    Status: 'active',
    UpdatedAt: `2026-09-09T09:${String(24 - index).padStart(2, '0')}:00Z`,
    Preview: `会话 ${suffix} 的最近消息`,
    Conversations: [{
      TenantID: 'acme', AppCode: 'support', SessionKey: `acme/support/session/session-web-${suffix}`,
      Channel: 'web', BindingID: 'console', ConversationID: `web-user-${suffix}`, ExternalUserID: `user-${suffix}`,
      Scope: 'direct', StartedAt: `2026-09-09T09:${String(20 - index).padStart(2, '0')}:00Z`, UpdatedAt: `2026-09-09T09:${String(24 - index).padStart(2, '0')}:00Z`,
    }],
  })
}

const salesSession = {
  TenantID: 'acme',
  AppCode: 'sales',
  SessionKey: 'acme/sales/session/session-sales',
  SubjectID: 'sales-user',
  OwnerPlatformUserID: 'admin',
  Status: 'active',
  UpdatedAt: '2026-09-09T09:40:00Z',
  Preview: '销售机器人会话',
  Conversations: [{
    TenantID: 'acme', AppCode: 'sales', SessionKey: 'acme/sales/session/session-sales',
    Channel: 'web', BindingID: 'console', ConversationID: 'sales-conversation', ExternalUserID: 'sales-user',
    Scope: 'direct', StartedAt: '2026-09-09T09:39:00Z', UpdatedAt: '2026-09-09T09:40:00Z',
  }],
}

const usage = { prompt_tokens: 120, completion_tokens: 42, total_tokens: 162, cached_tokens: 80 }
const claim = {
  app_code: 'support',
  channel: 'web',
  binding_id: 'web',
  message_id: 'msg-20260909-000001',
  status: 'completed',
  trace_id: 'trace-20260909-000001',
  updated_at: '2026-09-09T09:30:00Z',
}

const salesClaim = {
  ...claim,
  app_code: 'sales',
  message_id: 'msg-20260909-sales-000001',
  trace_id: 'trace-20260909-sales-000001',
}

const outbox = {
  ID: 'outbox-1',
  TenantID: 'acme',
  AggregateKey: 'conversation-1',
  Type: 'channel.reply',
  Payload: Buffer.from(JSON.stringify({ channel: 'web', conversation_id: 'conversation-1', text: '问题已经处理完成。' })).toString('base64'),
  CreatedAt: '2026-09-09T09:30:01Z',
  DeliveredAt: '2026-09-09T09:30:02Z',
}

let accountDisplayName = 'admin'

await page.route('**/api/**', async (route) => {
  const request = route.request()
  const url = new URL(request.url())
  const path = url.pathname

  if (path === '/api/v1/auth/me') {
    return route.fulfill({ status: 200, json: {
      platform_user_id: 'admin', display_name: accountDisplayName, role: 'admin', is_system_admin: true,
      tenants: [{ tenant_id: 'acme', display_name: 'Acme', role: 'admin' }],
    } })
  }
  if (path === '/api/v1/tenants') return route.fulfill({ status: 200, json: { tenants: [{ tenant_id: 'acme', display_name: 'Acme', role: 'admin' }] } })
  if (path === '/api/v1/apps') return route.fulfill({ status: 200, json: { applications: [app, salesApp] } })
  if (path === '/api/v1/system') return route.fulfill({ status: 200, json: system })
  if (path === '/api/v1/models/sync' && request.method() === 'POST') {
    return route.fulfill({ status: 200, json: { model_providers: system.info.model_providers } })
  }
  if (path === '/api/v1/models' && request.method() === 'DELETE') {
    const providerID = url.searchParams.get('provider_id')
    const modelName = url.searchParams.get('model')
    system.info.model_providers = system.info.model_providers.map((provider) => (
      provider.id === providerID
        ? { ...provider, models: provider.models.filter((model) => model.name !== modelName) }
        : provider
    ))
    return route.fulfill({ status: 204, body: '' })
  }
  if (path === '/api/v1/claims') {
    return route.fulfill({ status: 200, json: { claims: [url.searchParams.get('app') === 'sales' ? salesClaim : claim] } })
  }
  if (path === '/api/v1/execution') {
    const selectedClaim = url.searchParams.get('event_id') === salesClaim.message_id ? salesClaim : claim
    return route.fulfill({
      status: 200,
      json: {
        event_id: selectedClaim.message_id,
        trace_id: selectedClaim.trace_id,
        status: 'completed',
        attempts: 1,
        claim: selectedClaim,
        outbox: [outbox],
        agent_trace: {
          status: 'completed',
          root_agent_name: 'support',
          started_at: '2026-09-09T09:29:58Z',
          ended_at: '2026-09-09T09:30:01Z',
          usage,
          steps: [
            {
              step_id: 'agent-1',
              node_id: 'assistant',
              node_type: 'agent',
              started_at: '2026-09-09T09:29:58Z',
              ended_at: '2026-09-09T09:30:01Z',
              usage,
            },
          ],
        },
        tool_executions: [{
          request_id: selectedClaim.message_id,
          tool_call_id: 'call-1',
          tool_name: 'query_order',
          status: 'completed',
          trace_id: selectedClaim.trace_id,
          started_at: '2026-09-09T09:29:59Z',
          completed_at: '2026-09-09T09:30:00Z',
        }],
      },
    })
  }
  if (path === '/api/v1/knowledge/documents') {
    const selectedAppCode = url.searchParams.get('app') || 'support'
    return route.fulfill({
      status: 200,
      json: {
        documents: [{
          tenant_id: 'acme', app_code: selectedAppCode,
          document_id: selectedAppCode === 'sales' ? 'sales-handbook' : 'refund-policy',
          name: selectedAppCode === 'sales' ? '销售手册' : '退款政策', content: '',
          embedding: null, metadata: { source: 'fixture' }, status: 'ready', total_chunks: 4, updated_at: '2026-09-09T09:00:00Z',
        }],
      },
    })
  }
  if (path === '/api/v1/knowledge/chunks') return route.fulfill({ status: 200, json: { chunks: [] } })
  if (path === '/api/v1/knowledge' && request.method() === 'GET') return route.fulfill({ status: 200, json: { chunks: [] } })
  if (path === '/api/v1/memory') return route.fulfill({ status: 200, json: { memories: [] } })
  if (path === '/api/v1/artifacts') return route.fulfill({ status: 200, json: { artifacts: [] } })
  if (path === '/api/v1/sessions/tenant') {
    const selectedAppCode = url.searchParams.get('app') || 'support'
    return route.fulfill({ status: 200, json: { sessions: selectedAppCode === 'sales' ? [salesSession] : sessions } })
  }
  if (path === '/api/v1/sessions/messages') return route.fulfill({ status: 200, json: { messages: [
    { id: 'u1', role: 'user', content: '帮我查询一下订单状态', time: '2026-09-09T09:34:00Z' },
    { id: 'a1', role: 'assistant', content: '**订单正在配送中。**\n\n- 预计明天到达', time: '2026-09-09T09:35:00Z' },
  ] } })
  if (path === '/api/v1/channels/status') return route.fulfill({ status: 200, json: { channels: [] } })
  if (path === '/api/v1/account/profile') {
    if (request.method() === 'PUT') {
      accountDisplayName = JSON.parse(request.postData() || '{}').display_name || accountDisplayName
    }
    return route.fulfill({ status: 200, json: {
      platform_user_id: 'admin', display_name: accountDisplayName, email: 'admin@example.com',
      login_methods: [
        { provider_id: 'local', type: 'local', display_name: '本地账号', subject_id: 'admin', linked_at: '2026-09-09T08:00:00Z' },
        { provider_id: 'corp-sso', type: 'oidc', display_name: '企业 SSO', subject_id: 'employee-admin', linked_at: '2026-09-09T08:10:00Z' },
      ],
    } })
  }
  return route.fulfill({ status: 404, json: { error: `Unknown fixture endpoint: ${path}` } })
})

const destinations = [
  ['chat', '对话'],
  ['bots', '机器人'],
  ['account', '账号设置'],
  ['executions', '执行记录'],
  ['models', '模型资产'],
  ['data', '知识与偏好'],
  ['system', '系统状态'],
]

const appScopedTabs = new Set(['chat', 'executions', 'data'])

async function assertWorkspace(viewportLabel) {
  await page.locator('main').waitFor()
  assert.equal(await page.locator('main h1').count(), 0, `${viewportLabel}: workspace must not contain a page h1`)
  const geometry = await page.evaluate(() => ({
    viewport: window.innerWidth,
    scrollWidth: document.documentElement.scrollWidth,
    overflowers: [...document.querySelectorAll('body *')]
      .filter((element) => element.getClientRects().length > 0 && element.getBoundingClientRect().right > window.innerWidth + 1)
      .slice(0, 8)
      .map((element) => ({
        tag: element.tagName,
        className: typeof element.className === 'string' ? element.className : '',
        right: Math.round(element.getBoundingClientRect().right),
        width: Math.round(element.getBoundingClientRect().width),
      })),
    unwrappedSelects: [...document.querySelectorAll('select')]
      .filter((element) => {
        const style = getComputedStyle(element)
        const rect = element.getBoundingClientRect()
        return rect.width > 1 && rect.height > 1 && style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0' && !element.closest('.select-control')
      }).length,
    targets: [...document.querySelectorAll('button:not([disabled]):not(.switch-root), [role="tab"]')]
      .filter((element) => element.getClientRects().length > 0)
      .map((element) => ({
        height: element.getBoundingClientRect().height,
        text: element.textContent?.trim() ?? '',
        className: element.className,
        ariaLabel: element.getAttribute('aria-label') ?? '',
      }))
      .sort((left, right) => left.height - right.height)
      .slice(0, 5),
  }))
  assert.ok(geometry.scrollWidth <= geometry.viewport, `${viewportLabel}: horizontal overflow ${geometry.scrollWidth}px > ${geometry.viewport}px ${JSON.stringify(geometry.overflowers)}`)
  assert.equal(geometry.unwrappedSelects, 0, `${viewportLabel}: every dropdown must use the shared select control`)
  assert.ok(geometry.targets[0]?.height >= 36, `${viewportLabel}: unexpectedly tiny interactive target ${JSON.stringify(geometry.targets[0])}`)
}

try {
  for (const [tab, label] of destinations) {
    await page.setViewportSize({ width: 1600, height: 1000 })
    await page.goto(`${base}/console/?tab=${tab}`)
    await page.locator('.sidebar').waitFor()
    assert.equal(await page.getByRole('navigation', { name: '主导航' }).count(), 1, `${label}: desktop navigation must be available`)
    assert.match(page.url(), new RegExp(`[?&]tab=${tab}(?:&|$)`))
    assert.equal(
      await page.getByRole('combobox', { name: '当前机器人' }).count(),
      appScopedTabs.has(tab) ? 1 : 0,
      `${label}: robot switcher visibility must match page scope`,
    )
    await assertWorkspace(`${label} desktop`)
    if (tab === 'chat') {
      assert.equal(await page.locator('.bot-rail').count(), 0, 'chat must not duplicate the global robot switcher')
    }
    if (tab === 'account') {
      await page.getByRole('combobox', { name: '当前机器人' }).click()
      await page.getByRole('option', { name: 'sales', exact: true }).click()
      await page.locator('.account-connection-target strong').getByText('sales', { exact: true }).waitFor()
      assert.equal(await page.locator('.account-connection-target strong').getByText('support', { exact: true }).count(), 0)
      await page.getByRole('combobox', { name: '当前机器人' }).click()
      await page.getByRole('option', { name: 'support', exact: true }).click()
      await page.locator('.account-connection-target strong').getByText('support', { exact: true }).first().waitFor()
    }
    if (tab === 'executions') {
      await page.locator('.execution-summary-card').first().waitFor()
      assert.equal(await page.getByText('Agent Trace', { exact: true }).count(), 0)
      assert.equal(await page.getByText('可靠性状态', { exact: true }).count(), 0)
      assert.equal(await page.getByText('这次回复已完成', { exact: false }).count(), 0)
      assert.equal(await page.locator('.execution-summary-card').count(), 4, 'execution page should use four summary cards')
      assert.equal(await page.getByRole('searchbox', { name: '搜索执行记录' }).count(), 1, 'execution list should expose one real search control')
      assert.equal(await page.locator('.execution-detail-card').count(), 3, 'execution detail should use the shared three-card layout')
      assert.equal(await page.getByText(claim.message_id, { exact: true }).count() > 0, true)
      assert.equal(await page.getByText('工具调用', { exact: true }).count() > 0, true)
      assert.equal(await page.getByText('query_order', { exact: false }).count() > 0, true)
      assert.equal(await page.getByText('机器人处理', { exact: true }).count(), 0)
      assert.equal(await page.getByText('已送达', { exact: true }).count() > 0, true)
      assert.equal(await page.getByText('输入', { exact: true }).count() > 0, true)
      assert.equal(await page.getByText('输出', { exact: true }).count() > 0, true)
      assert.equal(await page.getByText('缓存率', { exact: true }).count() > 0, true)
      assert.equal(await page.getByText('66.7%', { exact: true }).count() > 0, true)
      const selectedExecutionRow = page.locator('.execution-list tbody tr.is-selected')
      await selectedExecutionRow.getByText('3.00 s', { exact: true }).waitFor()
      assert.equal(await selectedExecutionRow.getByText('入 120 / 出 42', { exact: true }).count(), 0, 'execution list should not duplicate token detail')
      assert.equal(await selectedExecutionRow.getByText('66.7%', { exact: true }).count(), 0, 'execution list should not duplicate cache detail')
      assert.equal(await page.getByText('推理', { exact: true }).count(), 0)
      assert.equal(await page.getByText('assistant#model', { exact: true }).count(), 0)
      const executionColumnOrder = await page.locator('.execution-list thead th').evaluateAll((headers) => headers.map((header) => header.textContent?.trim()))
      assert.deepEqual(executionColumnOrder, ['渠道', '状态', '更新时间', '耗时'])
      const executionFilterStyle = await page.locator('.execution-filter').evaluate((element) => {
        const active = element.querySelector('[role="tab"][data-state="active"]')
        const style = getComputedStyle(element)
        const activeStyle = active ? getComputedStyle(active) : null
        return {
          background: style.backgroundColor,
          activeBackground: activeStyle?.backgroundColor ?? '',
          activeBoxShadow: activeStyle?.boxShadow ?? '',
        }
      })
      assert.match(executionFilterStyle.background, /rgba\(0, 0, 0, 0\)|transparent/)
      assert.match(executionFilterStyle.activeBackground, /rgba\(0, 0, 0, 0\)|transparent/)
      assert.ok(executionFilterStyle.activeBoxShadow === 'none' || executionFilterStyle.activeBoxShadow === '')
      const executionDetailGeometry = await page.evaluate(() => {
        const primary = [...document.querySelectorAll('.execution-primary-facts > div')]
        const channel = primary.find((element) => element.querySelector('dt')?.textContent === '渠道')?.getBoundingClientRect()
        const status = primary.find((element) => element.querySelector('dt')?.textContent === '状态')?.getBoundingClientRect()
        const facts = [...document.querySelectorAll('.execution-facts > div')]
        const message = facts.find((element) => element.querySelector('dt')?.textContent === '消息')?.getBoundingClientRect()
        const steps = facts.find((element) => element.querySelector('dt')?.textContent === '步骤')?.getBoundingClientRect()
        return {
          statusToRight: Boolean(channel && status && status.x > channel.x),
          messageBeforeSteps: Boolean(message && steps && message.top < steps.top),
        }
      })
      assert.equal(executionDetailGeometry.statusToRight, true, 'execution status should sit on the right of the primary facts')
      assert.equal(executionDetailGeometry.messageBeforeSteps, true, 'message should appear before steps in execution details')
      await page.getByRole('combobox', { name: '当前机器人' }).click()
      await page.getByRole('option', { name: 'sales', exact: true }).click()
      await page.getByText(salesClaim.message_id, { exact: true }).waitFor()
      assert.equal(await page.getByText(claim.message_id, { exact: true }).count(), 0, 'execution list must follow the selected robot')
    }
    if (tab === 'data') {
      assert.equal(await page.getByRole('combobox', { name: '当前机器人' }).count(), 1, 'knowledge page must use only the global robot switcher')
      await page.getByRole('combobox', { name: '当前机器人' }).click()
      await page.getByRole('option', { name: 'sales', exact: true }).click()
      await page.getByText('销售手册', { exact: true }).waitFor()
      assert.equal(await page.getByRole('combobox', { name: '当前机器人' }).count(), 1, 'knowledge page must not render a second robot selector')
    }
    if (tab === 'account') {
      assert.equal(await page.getByText('账号', { exact: true }).count(), 1)
      assert.equal(await page.getByText('登录身份', { exact: true }).count(), 1)
      assert.equal(await page.getByText('普通账号', { exact: true }).count(), 1)
      assert.equal(await page.getByText('企业 SSO', { exact: true }).count() >= 1, true)
      assert.equal(await page.getByRole('combobox', { name: '当前机器人' }).count(), 0, 'account settings must not be app-scoped')
      const nameInput = page.getByLabel('账号名称')
      await nameInput.fill('平台管理员')
      await page.getByRole('button', { name: '保存', exact: true }).click()
      await page.getByText('账号名称已更新', { exact: true }).waitFor()
      assert.equal(accountDisplayName, '平台管理员')
    }
    if (tab === 'data') {
      assert.equal(await page.locator('.data-view-tabs').count(), 1, 'platform data should keep only the data-view control in its local toolbar')
      assert.equal(await page.locator('.data-panel-card').count(), 1, 'knowledge should use the shared data panel surface')
      assert.equal(await page.locator('.data-panel-card').getByText('知识文档', { exact: true }).count(), 1)
      await page.getByRole('tab', { name: '用户偏好', exact: true }).click()
      assert.equal(await page.getByText('用户偏好', { exact: true }).count() >= 1, true)
      assert.equal(await page.locator('.data-panel-card .memory-filter-surface').count(), 1)
      assert.equal(await page.locator('.data-empty-state').count(), 1)
      await page.getByRole('tab', { name: '知识文档', exact: true }).click()
      assert.equal(await page.locator('.data-panel-card').getByText('知识文档', { exact: true }).count(), 1)
    }
    if (tab === 'system') {
      const stateCell = page.locator('.dependency-state-cell').first()
      const row = stateCell.locator('..')
      const geometry = await Promise.all([stateCell.boundingBox(), row.boundingBox()])
      assert.ok(geometry[0] && geometry[1] && geometry[0].x > geometry[1].x + geometry[1].width / 2, 'system status should sit at the far right')
      assert.equal(await page.locator('.system-summary-card').count(), 2, 'system should show version and service-health summary cards')
      assert.equal(await page.locator('.dependency-brand-kafka').count(), 1, 'Kafka should use its brand mark')
      assert.equal(await page.locator('.dependency-brand-postgres').count(), 1, 'PostgreSQL should use its brand mark')
      assert.equal(await page.locator('.dependency-brand-redis').count(), 1, 'Redis should use its brand mark')
      assert.equal(await page.getByText('依赖连通性', { exact: true }).count(), 0)
      assert.equal(await page.getByText('模型供应商', { exact: true }).count(), 0)
    }
    if (tab === 'models') {
      await page.locator('.model-summary-card').first().waitFor()
      assert.equal(await page.getByText('就绪', { exact: true }).count(), 0)
      assert.equal(await page.getByText('模型服务', { exact: true }).count() > 0, true)
      assert.equal(await page.locator('.model-summary-card').count(), 3, 'model assets should use three summary cards')
      assert.equal(await page.locator('.provider-card').count(), 1, 'configured providers should render as service cards')
      assert.equal(await page.locator('.provider-endpoint-value').count(), 1, 'provider endpoint should use the shared inset surface')
      assert.equal(await page.locator('.models-search').count(), 1, 'model catalog should expose one integrated search control')
      await page.getByRole('button', { name: '同步模型', exact: true }).click()
      const syncToast = page.locator('.toast')
      await syncToast.waitFor()
      assert.match(await syncToast.innerText(), /模型目录已同步/)
      assert.equal(await page.locator('.success-banner').count(), 0, 'model sync should use toast instead of an inline success banner')
      await syncToast.getByRole('button', { name: '关闭通知' }).click()

      const deleteModelButton = page.getByRole('button', { name: '删除模型 gpt-5.6-mini' })
      assert.equal(await deleteModelButton.count(), 1, 'discovered models should expose a delete action')
      await deleteModelButton.click()
      const confirm = page.getByRole('dialog', { name: '删除模型？' })
      await confirm.waitFor()
      assert.match(await confirm.innerText(), /正在被机器人使用的模型不能删除/)
      await confirm.getByRole('button', { name: '删除模型', exact: true }).click()
      await confirm.waitFor({ state: 'detached' })
      assert.equal(await page.getByText('gpt-5.6-mini', { exact: true }).count(), 0, 'deleted discovered model should leave the catalog')
    }
    await page.screenshot({ path: new URL(`${tab}-desktop.png`, artifacts).pathname })

    await page.setViewportSize({ width: 390, height: 844 })
    await page.getByRole('navigation', { name: '移动导航' }).waitFor()
    await assertWorkspace(`${label} mobile`)
    await page.screenshot({ path: new URL(`${tab}-mobile.png`, artifacts).pathname })
  }

  await page.setViewportSize({ width: 900, height: 900 })
  await page.goto(`${base}/console/?tab=chat`)
  assert.equal(await page.getByRole('navigation', { name: '主导航' }).count(), 0, '900px uses compact navigation')
  await page.getByRole('navigation', { name: '移动导航' }).waitFor()
  await assertWorkspace('chat tablet')

  assert.deepEqual(browserErrors, [])
  console.log('PASS: all primary destinations render at desktop/mobile, 900px compact shell, no repeated page h1, no horizontal overflow or browser errors')
} finally {
  await browser.close()
}
