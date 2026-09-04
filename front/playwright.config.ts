import { defineConfig, devices } from '@playwright/test'

// E2E against the real management UI. The webServer block boots the full
// stack in dev mode: the Go backend (in-memory, role=all, :8080) and the Vite
// dev server (:5173). The SPA calls the API at http://localhost:8080 directly
// (front api base default), which CORS on the backend permits.
export default defineConfig({
  testDir: './e2e',
  timeout: 60_000,
  fullyParallel: false,
  workers: 1,
  retries: 0,
  reporter: [['list']],
  use: {
    baseURL: 'http://localhost:5173',
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
      command: 'cd .. && go run ./cmd/trpc-service -role all -config configs/config.yaml',
      url: 'http://localhost:8080/healthz',
      reuseExistingServer: true,
      timeout: 120_000,
      env: {
        GOMODCACHE: 'D:/Develop/Git/trpc-agent-service/.mc12',
        GOCACHE: 'D:/Develop/Git/trpc-agent-service/.gc14',
        GOSUMDB: 'off',
      },
    },
    {
      command: 'npm run dev -- --port 5173 --host 127.0.0.1',
      url: 'http://localhost:5173',
      reuseExistingServer: true,
      timeout: 60_000,
      env: {},
    },
  ],
})
