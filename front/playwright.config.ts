import { defineConfig, devices } from '@playwright/test'

// E2E against the real management UI. The webServer block boots the full
// stack in dev mode: the Go backend (in-memory, role=all, :8080) and the Vite
// dev server (:5173). The SPA calls the API at http://localhost:8080 directly
// (front api base default), which CORS on the backend permits.
//
// 127.0.0.1 (not localhost) is used deliberately: Vite binds 127.0.0.1, while
// on Windows `localhost` resolves to ::1 first, so health-checking
// http://localhost:5173 against a 127.0.0.1-only listener never succeeds and
// the dev server is reported as "not able to start".
const base = 'http://127.0.0.1'

export default defineConfig({
  testDir: './e2e',
  timeout: 60_000,
  fullyParallel: false,
  workers: 1,
  retries: 0,
  reporter: [['list']],
  use: {
    baseURL: `${base}:5173`,
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
      url: `${base}:8080/healthz`,
      reuseExistingServer: true,
      // The first `go run` compiles the whole service, so allow more than the
      // default budget on a cold build cache.
      timeout: 300_000,
      // Local module/build caches keep the sandbox off the network cache dirs.
      // GOPROXY matters: without it `go run` falls back to proxy.golang.org,
      // which is unreachable here, and the server never boots.
      env: {
        GOMODCACHE: 'D:/Develop/Git/trpc-agent-service/.mc8',
        GOCACHE: 'D:/Develop/Git/trpc-agent-service/.gc14',
        GOSUMDB: 'off',
        GOPROXY: 'https://goproxy.cn,direct',
      },
    },
    {
      command: 'npm run dev -- --port 5173 --host 127.0.0.1',
      url: `${base}:5173`,
      reuseExistingServer: true,
      timeout: 60_000,
      env: {},
    },
  ],
})
