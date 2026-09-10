// 企业登录全链路浏览器验收：Mock 开发登录、假企微扫码登录与会话过期。
import { chromium } from 'playwright'
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const here = path.dirname(fileURLToPath(import.meta.url))
const BASE = process.env.E2E_BASE ?? 'http://127.0.0.1:8080'
const scenario = process.argv[2] ?? 'wecom'
const artifacts = path.join(here, 'artifacts')
fs.mkdirSync(artifacts, { recursive: true })

let assertions = 0
function expect(condition, message) {
  assertions += 1
  if (!condition) throw new Error(`断言失败: ${message}`)
}

async function clickWeComLogin(page) {
  await page.goto(`${BASE}/console/`, { waitUntil: 'domcontentloaded' })
  const button = page.getByRole('button', { name: '使用 企业微信 登录' })
  await button.waitFor({ timeout: 10000 })
  await button.click()
  await page.waitForURL(/\/console\/?(?:\?.*)?$/, { timeout: 20000 })
  await page.waitForSelector('.sidebar-user-name', { timeout: 15000 })
  expect((await page.getByRole('tab', { name: '租户', exact: true }).count()) === 1, '企微引导账号应获得系统管理导航权限')
}

async function scenarioMock(page) {
  await page.goto(`${BASE}/console/`, { waitUntil: 'domcontentloaded' })
  const button = page.getByRole('button', { name: '使用 Mock 登录' })
  await button.waitFor({ timeout: 10000 })
  expect((await page.locator('.login-development-access').innerText()).includes('开发测试'), 'Mock 登录应位于开发测试区域')
  await button.click()
  await page.waitForURL(/\/console\/?(?:\?.*)?$/, { timeout: 10000 })
  await page.waitForSelector('.sidebar-user-name', { timeout: 10000 })
  expect((await page.locator('.sidebar-user-name').innerText()).trim() === 'Mock Admin', 'Mock 登录用户应为默认测试用户')
  expect((await page.getByRole('tab', { name: '租户', exact: true }).count()) === 1, '默认 Mock 测试用户应获得系统管理导航权限')
}

async function scenarioWeCom(page) {
  await clickWeComLogin(page)
  await page.getByRole('combobox', { name: '当前机器人' }).waitFor({ timeout: 15000 })

  await page.getByLabel('打开账号菜单').click()
  await page.getByRole('button', { name: '退出登录', exact: true }).click()
  await page.getByRole('button', { name: '使用 企业微信 登录' }).waitFor({ timeout: 10000 })
  expect((await page.locator('.sidebar').count()) === 0, '登出后侧栏不应可见')
}

async function scenarioExpiry(page) {
  await clickWeComLogin(page)
  await page.waitForTimeout(7000)
  await page.locator('.nav-item', { hasText: '执行记录' }).first().click()
  await page.waitForURL(/expired=1/, { timeout: 10000 })
  const banner = page.locator('.error-banner')
  await banner.waitFor({ timeout: 10000 })
  expect((await banner.innerText()).includes('会话已过期'), '应提示会话已过期')
}

async function run() {
  const browser = await chromium.launch()
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } })
  try {
    if (scenario === 'mock') await scenarioMock(page)
    else if (scenario === 'wecom') await scenarioWeCom(page)
    else if (scenario === 'expiry') await scenarioExpiry(page)
    else throw new Error(`未知场景: ${scenario}`)
    console.log(`[PASS] ${scenario}（${assertions} 项断言）`)
  } catch (error) {
    try { await page.screenshot({ path: path.join(artifacts, `${scenario}-fail.png`) }) } catch { /* ignore */ }
    throw error
  } finally {
    await browser.close()
  }
}

await run().catch((error) => {
  console.error(`[FAIL] ${scenario}: ${error.message}`)
  process.exit(1)
})
