import { defineConfig, devices } from '@playwright/test'

/*
 * Side-stack e2e config.
 *
 * The checked-in playwright.config.ts boots the stack on :8080 /:5173, which a
 * running `docker compose` deployment already occupies — and `reuseExistingServer`
 * would then test the deployed images instead of the working tree. This config
 * moves both servers to free ports and wires the SPA to the fresh backend through
 * VITE_API_BASE, so the suite exercises the code under test without touching the
 * running deployment.
 */
const base = 'http://127.0.0.1'
const apiPort = 8081
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
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
  webServer: [
    {
      command: `cd .. && go run ./cmd/trpc-service -role all -config front/.e2e/side-config.yaml`,
      url: `${base}:${apiPort}/healthz`,
      reuseExistingServer: false,
      timeout: 300_000,
      env: {
        GOMODCACHE: 'D:/Develop/Git/trpc-agent-service/.mc8',
        GOCACHE: 'D:/Develop/Git/trpc-agent-service/.gc14',
        GOSUMDB: 'off',
        GOPROXY: 'https://goproxy.cn,direct',
      },
    },
    {
      command: `npm run dev -- --port ${webPort} --host 127.0.0.1`,
      url: `${base}:${webPort}`,
      reuseExistingServer: false,
      timeout: 90_000,
      env: {
        VITE_API_BASE: `${base}:${apiPort}`,
        // Direct API calls in the spec (bootstrap + cleanup) must hit the same
        // backend the SPA was pointed at.
        E2E_API_BASE: `${base}:${apiPort}`,
      },
    },
  ],
})
