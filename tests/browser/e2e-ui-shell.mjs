import assert from 'node:assert/strict'
import { mkdir } from 'node:fs/promises'
import { chromium } from 'playwright'

const base = process.env.E2E_BASE ?? 'http://127.0.0.1:5173'
const artifacts = new URL('./artifacts/', import.meta.url)
await mkdir(artifacts, { recursive: true })

const browser = await chromium.launch({ executablePath: process.env.CHROMIUM_PATH || undefined })
const page = await browser.newPage({ viewport: { width: 1600, height: 1000 } })
const browserErrors = []
let authenticated = true
page.on('pageerror', (error) => browserErrors.push(error.message))

const application = {
  Config: {
    tenant_id: 'acme',
    app_code: 'support',
    status: 'active',
    config_version: 3,
    instruction: '帮助客户快速解决售后问题。',
    model: { provider_id: 'openai-primary', name: 'gpt-4o-mini' },
    tools: { allowed: [] },
    storage: {
      memory: { driver: 'postgres' },
      knowledge: { driver: 'postgres' },
      artifact: { driver: 'postgres' },
    },
    governance: { max_tool_calls: 8, budget_units: 100 },
    audit: { retention_days: 90 },
    channels: [],
  },
  PublishedAt: '2026-09-09T08:00:00Z',
  Checksum: 'fixture',
}

await page.route('**/api/**', async (route) => {
  const path = new URL(route.request().url()).pathname
  if (path === '/api/v1/auth/me') {
    await route.fulfill({
      status: authenticated ? 200 : 401,
      json: authenticated
        ? {
            platform_user_id: 'admin', display_name: 'Admin', role: 'admin', is_system_admin: true,
            tenants: [{ tenant_id: 'acme', display_name: 'Acme', role: 'admin', status: 'active' }],
          }
        : { error: 'unauthorized' },
    })
    return
  }
  if (path === '/api/v1/auth/providers') {
    await route.fulfill({ status: 200, json: { providers: [
      { provider_id: 'wecom-acme', type: 'wecom', display_name: '企业微信', configured: true, enabled: true },
      { type: 'feishu', display_name: '飞书', configured: false, enabled: false },
      { type: 'oidc', display_name: '企业 SSO', configured: false, enabled: false },
      { provider_id: 'mock', type: 'mock', display_name: 'Mock 测试登录', configured: true, enabled: true },
    ] } })
    return
  }
  if (path === '/api/v1/auth/configuration') {
    await route.fulfill({ status: 200, json: {
      callback_url: 'https://console.example.com/api/v1/auth/callback',
      providers: [
        { type: 'wecom', display_name: '企业微信', provider_id: 'wecom-acme', configured: true, enabled: true, metadata: { corp_id: 'ww-acme', agent_id: '1000002' } },
        { type: 'feishu', display_name: '飞书', provider_id: 'feishu-acme', configured: true, enabled: true, metadata: { app_id: 'cli_acme', tenant_key: 'tenant-acme' } },
        { type: 'oidc', display_name: '企业 SSO', provider_id: 'oidc-acme', configured: true, enabled: true, metadata: { issuer: 'https://sso.example.com', client_id: 'console' } },
        { type: 'mock', display_name: 'Mock 测试登录', provider_id: 'mock', configured: true, enabled: true, metadata: { subject_id: 'mock-admin' } },
      ],
    } })
    return
  }
  if (path === '/api/v1/auth/login') {
    await route.fulfill({
      status: 200,
      json: {
        provider: { provider_id: 'wecom-acme', type: 'wecom', display_name: '企业微信' },
        auth_url: `${base}/api/v1/auth/callback?state=fixture`,
      },
    })
    return
  }
  if (path === '/api/v1/apps') {
    await route.fulfill({ status: 200, json: { applications: [application] } })
    return
  }
  if (path === '/api/v1/tenants') {
    await route.fulfill({ status: 200, json: { tenants: [{ tenant_id: 'acme', display_name: 'Acme', role: 'admin' }] } })
    return
  }
  if (path === '/api/v1/tenant-members') {
    await route.fulfill({ status: 200, json: { members: [], candidates: [] } })
    return
  }
  if (path === '/api/v1/sessions/tenant' || path === '/api/v1/sessions/messages') {
    await route.fulfill({ status: 200, json: path === '/api/v1/sessions/tenant' ? { sessions: [] } : { messages: [] } })
    return
  }
  await route.fulfill({ status: 404, json: { error: 'Unknown fixture endpoint' } })
})

try {
  await page.goto(`${base}/console/?tab=chat`)
  const primaryNavigation = page.getByRole('navigation', { name: '主导航' })
  await primaryNavigation.waitFor({ timeout: 3000 })
  await page.getByRole('combobox', { name: '当前机器人' }).waitFor({ timeout: 3000 })
  await page.getByRole('button', { name: '机器人设置' }).waitFor({ timeout: 3000 })
  await page.getByLabel('打开账号菜单').waitFor({ timeout: 3000 })
  assert.equal(await primaryNavigation.getByText('机器人', { exact: true }).count(), 0, 'robot management should not occupy a primary navigation item')
  assert.equal(await page.getByText('选择应用，开始对话或渠道测试。', { exact: true }).count(), 0)

  // Navigation colors animate for 180ms. Wait for the settled state instead
  // of sampling a transition frame and turning a visual assertion flaky.
  await page.waitForFunction(() => {
    const active = document.querySelector('.nav-item.active')
    return active && getComputedStyle(active).color === 'rgb(0, 113, 227)'
  })

  const visualContract = await page.evaluate(() => {
    const body = getComputedStyle(document.body)
    const sidebar = getComputedStyle(document.querySelector('.sidebar'))
    const activeNavigation = getComputedStyle(document.querySelector('.nav-item.active'))
    const navigationTarget = document.querySelector('.nav-item').getBoundingClientRect()
    const appShell = document.querySelector('.app-switcher-shell').getBoundingClientRect()
    const appValue = document.querySelector('.app-switcher-shell .select-control-value').getBoundingClientRect()
    const appChevron = document.querySelector('.app-switcher-shell .select-control-icon').getBoundingClientRect()
    return {
      bodyBackground: body.backgroundColor,
      bodyFont: body.fontFamily,
      sidebarBackdrop: sidebar.backdropFilter || sidebar.webkitBackdropFilter,
      activeColor: activeNavigation.color,
      navigationTargetHeight: navigationTarget.height,
      appShellHeight: appShell.height,
      appValueCenterDelta: Math.abs((appShell.top + appShell.height / 2) - (appValue.top + appValue.height / 2)),
      appSelectWidth: appShell.width,
      appChevronRightGap: appShell.right - appChevron.right,
      appChevronCenterDelta: Math.abs((appShell.top + appShell.height / 2) - (appChevron.top + appChevron.height / 2)),
    }
  })
  assert.equal(visualContract.bodyBackground, 'rgb(245, 245, 247)')
  assert.match(visualContract.bodyFont, /-apple-system|SF Pro/)
  assert.match(visualContract.sidebarBackdrop, /blur\(20px\)/)
  assert.equal(visualContract.activeColor, 'rgb(0, 113, 227)')
  assert.ok(visualContract.navigationTargetHeight >= 44, 'desktop navigation target must be at least 44px')
  assert.ok(visualContract.appShellHeight >= 40 && visualContract.appShellHeight <= 46, 'robot selector should render as one compact control')
  assert.ok(visualContract.appValueCenterDelta <= 2, 'robot selector value should share the control centerline')
  assert.ok(visualContract.appSelectWidth <= 260, 'robot selector should not consume excessive horizontal space')
  assert.ok(visualContract.appChevronRightGap >= 6 && visualContract.appChevronRightGap <= 14, 'robot selector chevron should sit at the shared right inset')
  assert.ok(visualContract.appChevronCenterDelta <= 1, 'robot selector chevron should be vertically centered')
  await page.getByRole('combobox', { name: '当前机器人' }).click()
  const appMenu = page.locator('.select-content')
  await appMenu.waitFor()
  const dropdownContract = await appMenu.evaluate((content) => {
    const contentRect = content.getBoundingClientRect()
    const item = content.querySelector('[role="option"]')
    const itemRect = item?.getBoundingClientRect()
    const style = getComputedStyle(content)
    return {
      width: contentRect.width,
      itemHeight: itemRect?.height ?? 0,
      radius: parseFloat(style.borderRadius),
      background: style.backgroundColor,
    }
  })
  assert.ok(dropdownContract.width >= visualContract.appSelectWidth - 2, 'dropdown should be at least as wide as its trigger')
  assert.ok(dropdownContract.itemHeight >= 40, 'dropdown options should use a comfortable shared target height')
  assert.ok(dropdownContract.radius >= 12, 'dropdown should use the shared rounded menu surface')
  await page.keyboard.press('Escape')
  await appMenu.waitFor({ state: 'detached' })
  await page.getByRole('button', { name: '机器人设置' }).click()
  await page.getByRole('button', { name: '创建机器人', exact: true }).waitFor()
  assert.equal(await primaryNavigation.getByText('机器人', { exact: true }).count(), 0, 'robot settings should stay outside primary navigation')
  await primaryNavigation.getByText('对话', { exact: true }).click()
  await page.getByRole('combobox', { name: '当前机器人' }).waitFor()
  await page.getByLabel('打开账号菜单').click()
  assert.equal(await page.getByText('组织', { exact: true }).count(), 0, 'account menu should not expose tenant context')
  await page.getByRole('button', { name: '账号设置', exact: true }).waitFor()
  await page.getByRole('button', { name: '退出登录', exact: true }).waitFor()
  await page.getByLabel('打开账号菜单').click()
  assert.equal(await page.locator('main h1').count(), 0, 'workspace pages must not repeat the breadcrumb title')
  assert.equal(await page.getByText('配置应用、连接渠道，让 Agent 开始工作。', { exact: true }).count(), 0)
  await primaryNavigation.getByText('登录设置', { exact: true }).click()
  await page.getByText('统一回调地址', { exact: true }).waitFor()
  assert.equal(await page.locator('.login-provider-setting-card').count(), 4, 'login settings should show every supported provider state')
  assert.equal(await page.getByText('https://console.example.com/api/v1/auth/callback', { exact: true }).count(), 1)
  assert.equal(await page.getByText('App ID', { exact: true }).count(), 1)
  assert.equal(await page.getByText('cli_acme', { exact: true }).count(), 1)
  await page.screenshot({ path: new URL('shell-desktop.png', artifacts).pathname, fullPage: true })

  await page.setViewportSize({ width: 390, height: 844 })
  await page.getByRole('navigation', { name: '移动导航' }).waitFor({ timeout: 3000 })
  await page.getByRole('button', { name: '账号设置', exact: true }).waitFor({ timeout: 3000 })
  const mobileContract = await page.evaluate(() => {
    const mobileNavigation = document.querySelector('.mobile-nav')
    const navigationTarget = document.querySelector('.mobile-nav-item').getBoundingClientRect()
    const style = getComputedStyle(mobileNavigation)
    const overflowers = [...document.querySelectorAll('body *')]
      .filter((element) => element.getBoundingClientRect().right > window.innerWidth + 1)
      .slice(0, 8)
      .map((element) => `${element.tagName.toLowerCase()}.${element.className || ''}:${Math.round(element.getBoundingClientRect().right)}`)
    return {
      fitsViewport: document.documentElement.scrollWidth <= window.innerWidth,
      navigationTargetHeight: navigationTarget.height,
      backdrop: style.backdropFilter || style.webkitBackdropFilter,
      overflowers,
    }
  })
  assert.equal(mobileContract.fitsViewport, true, `mobile overflow: ${mobileContract.overflowers.join(', ')}`)
  assert.ok(mobileContract.navigationTargetHeight >= 44, 'mobile navigation target must be at least 44px')
  assert.match(mobileContract.backdrop, /blur\(20px\)/)
  const mobileTabs = page.getByRole('navigation', { name: '移动导航' }).getByRole('tab')
  await mobileTabs.first().click()
  await page.screenshot({ path: new URL('shell-mobile.png', artifacts).pathname, fullPage: true })

  authenticated = false
  await page.setViewportSize({ width: 1600, height: 1000 })
  await page.goto(`${base}/console/`)
  await page.getByRole('heading', { name: 'Agent 平台' }).waitFor()
  await page.getByRole('button', { name: /企业微信/ }).waitFor()
  await page.getByRole('button', { name: /飞书/ }).waitFor()
  await page.getByRole('button', { name: /企业 SSO/ }).waitFor()
  await page.getByRole('button', { name: /Mock 测试登录/ }).waitFor()
  assert.equal(await page.getByText('扫码登录', { exact: true }).count(), 1, 'only the configured QR provider should be actionable')
  assert.equal(await page.getByText('开发测试', { exact: true }).count(), 1, 'mock login should be visually separated from the enterprise login')
  assert.equal(await page.locator('.login-provider-button').count(), 4, 'login should show all supported login capabilities')
  assert.equal(await page.locator('.login-provider-button.is-unavailable').count(), 2, 'unconfigured providers should stay visible but disabled')
  assert.equal(await page.getByText(/trpc-agent-service/).count(), 0, 'login copy should omit implementation detail')
  assert.match(await page.locator('.login-brand').innerText(), /多租户 Agent 工作台/)
  await page.screenshot({ path: new URL('login-desktop.png', artifacts).pathname, fullPage: true })

  assert.deepEqual(browserErrors, [])
  console.log('PASS: Apple visual contract, accessible shell, mobile navigation, login copy; zero browser errors')
} finally {
  await browser.close()
}
