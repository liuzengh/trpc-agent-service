import { test, expect, type Page } from '@playwright/test'

/*
 * Admin chat e2e: the one flow that was silently broken end to end.
 *
 * The page publishes a turn over POST /chat, the worker answers through the
 * outbox → stream:outbound, and the browser is supposed to render that reply.
 * Two independent defects made it invisible: the reply stream was opened with
 * `EventSource`, which cannot attach the Bearer token the endpoint requires
 * (401, so nothing ever arrived), and the follower re-passed `cursor=$` on every
 * poll, which re-resolves to that moment's stream tail and skips whatever was
 * published between two polls.
 *
 * The spec drives the real UI against whatever backend and model the stack is
 * configured with, then reloads the page and reads the turn back from the
 * ledger, which also proves the history half of the page.
 */

const apiBase = process.env.E2E_API_BASE ?? 'http://127.0.0.1:8080'
const adminUser = process.env.E2E_ADMIN_USER ?? 'admin'
const adminPassword = process.env.E2E_ADMIN_PASSWORD ?? 'admin123'
// A real model answers this; the platform's own end-to-end budget is 300s, so a
// generous but bounded wait keeps the spec from flaking on a slow upstream.
const replyTimeout = Number(process.env.E2E_REPLY_TIMEOUT_MS ?? 120_000)

async function login(page: Page) {
  await page.goto('/login')
  await page.getByPlaceholder('用户名').fill(adminUser)
  await page.getByPlaceholder('密码').fill(adminPassword)
  await page.getByRole('button', { name: '登录' }).click()
  await page.waitForURL(/\/(agents|endpoints|chat|)$/)
}

/**
 * pickFirstOption opens an Element Plus select and picks its first option.
 *
 * Two details make this deterministic. The dropdown is teleported to the body
 * and Element Plus keeps the closed poppers in the DOM, so a page-wide item
 * lookup matches a hidden one from a previous select (the header's tenant
 * switcher, or the select this helper just used); the helper therefore waits for
 * every popper to be closed before opening the next one and scopes the lookup to
 * the visible popper. And because clicking a teleported list item races its
 * open/close animation, the option is confirmed with the keyboard instead.
 */
async function pickFirstOption(page: Page, select: ReturnType<Page['locator']>) {
  const openPoppers = page.locator('.el-select-dropdown:visible')
  await expect(openPoppers).toHaveCount(0)
  await select.click()
  await openPoppers.last().waitFor({ state: 'visible' })
  await page.keyboard.press('ArrowDown')
  await page.keyboard.press('Enter')
  await expect(openPoppers).toHaveCount(0)
}

test.describe('Agent 对话页', () => {
  test('回复在页面上实时出现，并在刷新后从账本读回', async ({ page }) => {
    await login(page)
    await page.goto('/chat')
    await expect(page.getByRole('heading', { name: 'Agent 对话' })).toBeVisible()

    // 租户是选择式的（owner 可选任意租户），Agent 与会话同样从下拉里选。
    const config = page.locator('.chat-config .el-select')
    await pickFirstOption(page, config.nth(0))
    await pickFirstOption(page, config.nth(1))

    // Ask for a fixed, short answer so the assertion cannot depend on the model
    // being creative.
    const marker = `e2e-${Date.now()}`
    await page.getByPlaceholder('输入消息，回车发送').fill(`只回复这两个字：收到（${marker}）`)
    await page.getByRole('button', { name: '发送' }).click()

    // The user's own bubble appears immediately...
    await expect(page.locator('.bubble-user').last()).toContainText(marker)
    // ...and the reply arrives over the live stream.
    await expect(page.locator('.bubble-assistant').last()).toContainText('收到', { timeout: replyTimeout })
    await expect(page.locator('.live')).toHaveText(/已连接/)

    // Reload: the page starts a fresh session, so pick the one we just used from
    // the ledger-backed session dropdown and the turn must come back.
    await page.reload()
    await expect(page.getByRole('heading', { name: 'Agent 对话' })).toBeVisible()
    await pickFirstOption(page, page.locator('.chat-config .el-select').nth(2))

    await expect(page.locator('.bubble-user').last()).toContainText(marker)
    await expect(page.locator('.bubble-assistant').last()).toContainText('收到')
  })

  test('member 的租户被固定，无法选择别的租户', async ({ page }) => {
    // The tenant control is a select for the owner and a fixed tag for everyone
    // else: a member must not be offered a tenant picker at all.
    const member = `e2e-chat-member-${Date.now()}`
    const token = await ownerToken()
    const created = await fetch(`${apiBase}/members`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: JSON.stringify({ user_id: member, password: 'member123', role: 'member' }),
    })
    expect([201, 409]).toContain(created.status)

    try {
      await page.goto('/login')
      await page.getByPlaceholder('用户名').fill(member)
      await page.getByPlaceholder('密码').fill('member123')
      await page.getByRole('button', { name: '登录' }).click()
      await page.waitForURL(/\/(agents|chat|)$/)

      await page.goto('/chat')
      await expect(page.getByRole('heading', { name: 'Agent 对话' })).toBeVisible()
      // Exactly two selects (Agent + session) and a fixed-tenant tag.
      await expect(page.locator('.chat-config .el-select')).toHaveCount(2)
      await expect(page.locator('.chat-config .el-tag')).toContainText('已固定')
    } finally {
      await fetch(`${apiBase}/members/${member}`, {
        method: 'DELETE',
        headers: { Authorization: `Bearer ${token}` },
      })
    }
  })
})

/** ownerToken logs in over the API for setup/cleanup calls. */
async function ownerToken(): Promise<string> {
  const res = await fetch(`${apiBase}/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ user_id: adminUser, password: adminPassword }),
  })
  if (!res.ok) throw new Error(`login failed: ${res.status}`)
  const body = (await res.json()) as { token: string }
  return body.token
}
