import { defineConfig, devices } from '@playwright/test'

/*
 * Attach-mode config for the running `docker compose` deployment (frontend on
 * :5173, backend on :8080). Nothing is started: the spec exercises the images
 * that are actually deployed, which is the only way to check a fix for a
 * deployment-only defect (a rebuilt frontend bundle, a reply that only arrives
 * over the deployed Redis/MySQL path).
 *
 *   cd front && npx playwright test --config playwright.deployed.config.ts
 */
const base = 'http://127.0.0.1'
const apiPort = 8080
const webPort = 5173

export default defineConfig({
  testDir: './e2e',
  timeout: 180_000,
  fullyParallel: false,
  workers: 1,
  retries: 0,
  reporter: [['list']],
  use: {
    baseURL: `${base}:${webPort}`,
    trace: 'on-first-retry',
  },
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
  // The SPA proxies /api to the backend through nginx; direct API calls in the
  // spec must hit the same deployment.
  metadata: { E2E_API_BASE: `${base}:${apiPort}` },
})

process.env.E2E_API_BASE = process.env.E2E_API_BASE ?? `${base}:${apiPort}`
