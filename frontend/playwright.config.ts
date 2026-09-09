import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  fullyParallel: false,
  workers: 1,
  reporter: "line",
  use: { baseURL: "http://127.0.0.1:18080", trace: "retain-on-failure", channel: "chromium" },
  webServer: {
    command:
      "cd .. && ./build.sh && state=$(mktemp -d) && TRPC_CONTROL_PLANE_SQLITE_PATH=$state/control-plane.db ./bin/control-migrate && TRPC_CONTROL_PLANE_SQLITE_PATH=$state/control-plane.db TRPC_GOVERNANCE_PATH=$state/governance.json TRPC_BOT_ROUTES_PATH=$state/bot-routes.json TRPC_SQLITE_PATH=$state/service.db ./bin/trpc-service -addr 127.0.0.1:18080",
    url: "http://127.0.0.1:18080/healthz",
    reuseExistingServer: false,
    timeout: 120_000,
  },
  projects: [
    { name: "desktop", use: { ...devices["Desktop Chrome"], browserName: "chromium" } },
    { name: "mobile", use: { ...devices["iPhone 13"], browserName: "chromium" } },
  ],
});
