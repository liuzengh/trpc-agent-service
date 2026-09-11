import assert from 'node:assert/strict'
import { chromium } from 'playwright'

const base = process.env.E2E_BASE ?? 'http://127.0.0.1:5178'
const browser = await chromium.launch({ executablePath: process.env.CHROMIUM_PATH || undefined })
const page = await browser.newPage({ viewport: { width: 1200, height: 900 } })
const errors = []
page.on('pageerror', (error) => errors.push(error.message))

let qrBegins = 0
let fallbackBegins = 0

await page.route('**/fake-feishu-qr-sdk.js', async (route) => {
  await route.fulfill({
    status: 200,
    contentType: 'application/javascript',
    body: `window.QRLogin = function (options) {
      const target = document.getElementById(options.id);
      if (target) target.textContent = 'QR READY';
      return { matchOrigin: () => true, matchData: () => true };
    };`,
  })
})

await page.route('**/api/**', async (route) => {
  const request = route.request()
  const url = new URL(request.url())
  const path = url.pathname
  if (path === '/api/v1/auth/me') return route.fulfill({ status: 401, json: { error: 'unauthorized' } })
  if (path === '/api/v1/auth/providers') {
    return route.fulfill({
      status: 200,
      json: {
        providers: [{
          provider_id: 'feishu-test', type: 'feishu', display_name: '飞书',
          configured: true, enabled: true, qr_supported: true, qr_enabled: true,
        }],
      },
    })
  }
  if (path === '/api/v1/auth/qr/begin') {
    qrBegins += 1
    return route.fulfill({
      status: 200,
      json: {
        goto: `${base}/console/?qr_callback=1`,
        state: `state-${qrBegins}`,
        expires_in: qrBegins === 1 ? 1 : 30,
        sdk_url: `${base}/fake-feishu-qr-sdk.js`,
        provider: { provider_id: 'feishu-test', type: 'feishu', display_name: '飞书', configured: true, enabled: true },
      },
    })
  }
  if (path === '/api/v1/auth/login') {
    fallbackBegins += 1
    return route.fulfill({
      status: 200,
      json: {
        auth_url: `${base}/console/?fallback=feishu`,
        provider: { provider_id: 'feishu-test', type: 'feishu', display_name: '飞书', configured: true, enabled: true },
      },
    })
  }
  return route.fulfill({ status: 404, json: { error: `missing fixture: ${request.method()} ${path}` } })
})

try {
  await page.goto(`${base}/console/`)
  await page.getByText('请使用飞书 App「扫一扫」，在手机上确认授权', { exact: true }).waitFor()
  assert.equal(qrBegins, 1, 'QR login panel must request a QR session automatically')
  assert.equal(await page.getByText('QR READY', { exact: true }).count(), 1, 'QR SDK must render into the supplied container')

  await page.getByText('二维码已失效，请刷新后重新扫码', { exact: true }).waitFor({ timeout: 4000 })
  await page.getByRole('button', { name: '刷新二维码', exact: true }).click()
  await page.getByText('请使用飞书 App「扫一扫」，在手机上确认授权', { exact: true }).waitFor()
  assert.equal(qrBegins, 2, 'refresh QR must request a fresh QR session')

  await page.getByRole('button', { name: '无法扫码？使用跳转登录', exact: true }).click()
  await page.waitForURL(/fallback=feishu/, { timeout: 5000 })
  assert.equal(fallbackBegins, 1, 'fallback button must start the provider redirect login flow')

  assert.deepEqual(errors, [])
  console.log('PASS: Feishu QR automatic load, expiry refresh, and redirect fallback controls')
} finally {
  await browser.close()
}
