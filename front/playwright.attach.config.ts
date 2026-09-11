import { defineConfig, devices } from '@playwright/test'

/*
 * Attach-mode e2e config: like playwright.side.config.ts, but it does not start
 * anything. It is used to run the suite against a side stack started by hand,
 * which makes the backend's own logs available while debugging.
 */
const base = 'http://127.0.0.1'
const webPort = 5174

export default defineConfig({
  testDir: './e2e',
  timeout: 60_000,
  fullyParallel: false,
  workers: 1,
  retries: 0,
  reporter: [['list']],
  use: {
    baseURL: `${base}:${webPort}`,
    trace: 'on-first-retry',
  },
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
})
