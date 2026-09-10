import { existsSync } from 'node:fs'
import path from 'node:path'
import { defineConfig, devices } from '@playwright/test'

const cachedChromium = path.join(process.env.LOCALAPPDATA ?? '', 'ms-playwright', 'chromium-1228', 'chrome-win64', 'chrome.exe')
const executablePath = process.env.PLAYWRIGHT_EXECUTABLE_PATH ?? (existsSync(cachedChromium) ? cachedChromium : undefined)
const webServer = {
  command: 'npm run dev -- --host 127.0.0.1 --port 4173',
  url: 'http://127.0.0.1:4173',
  reuseExistingServer: true,
  timeout: 30_000,
}

export default defineConfig({
  testDir: './tests',
  timeout: 30_000,
  expect: { timeout: 5_000 },
  outputDir: './test-results',
  use: {
    baseURL: 'http://127.0.0.1:4173',
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    launchOptions: executablePath ? { executablePath } : undefined,
    ...devices['Desktop Chrome'],
  },
  webServer,
})
