import { expect, test, type Page } from "@playwright/test";

async function expectNoHorizontalOverflow(page: Page) {
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth)).toBe(true);
}

async function createActiveApp(page: Page, suffix: string) {
  const appId = `stage3-app-${suffix}`;
  const deploymentId = `stage3-deploy-${suffix}`;
  const appName = `Stage 3 App ${suffix}`;
  const versionIdempotencyKey = `stage3-${suffix}`;
  const result = await page.evaluate(async ({ appId, deploymentId, appName, versionIdempotencyKey }) => {
    const request = async (path: string, method: string, body?: unknown, headers?: Record<string, string>) => {
      const response = await fetch(path, {
        method,
        headers: { "Content-Type": "application/json", ...headers },
        body: body === undefined ? undefined : JSON.stringify(body),
      });
      const json = await response.json() as Record<string, unknown>;
      if (!response.ok) throw new Error(`${path}: ${response.status} ${JSON.stringify(json)}`);
      return json;
    };
    await request("/api/v1/admin/agent-apps", "POST", { id: appId, name: appName });
    await request("/api/v1/admin/deployments", "POST", { id: deploymentId, agent_app_id: appId });
    const version = await request(`/api/v1/admin/deployments/${deploymentId}/versions`, "POST", { config: { runner: "echo" } }, { "Idempotency-Key": versionIdempotencyKey }) as { id: string };
    await request(`/api/v1/admin/deployments/${deploymentId}/transition`, "POST", { status: "published", version_id: version.id });
    await request(`/api/v1/admin/deployments/${deploymentId}/transition`, "POST", { status: "active" });
    await request("/api/v1/admin/governance/policy", "POST", {
      agent_app_id: appId, token_budget: 10000, estimated_tokens_per_run: 5, rate_limit: 1000, rate_window_seconds: 60,
    });
    return { appId, deploymentId };
  }, { appId, deploymentId, appName, versionIdempotencyKey });
  return result;
}

test("complete Stage 3 chat and Mock IM workflow", async ({ page }, testInfo) => {
  const suffix = `${testInfo.project.name}-${Date.now()}`;
  const sessionID = `stage3-chat-${suffix}`;
  await page.goto("/");
  const app = await createActiveApp(page, suffix);

  await page.getByRole("button", { name: "Chat" }).click();
  await page.getByLabel("Agent 应用").selectOption(app.appId);
  await page.getByLabel("Session ID").fill(sessionID);
  await page.getByRole("button", { name: "创建 / 打开" }).click();
  await expect(page.getByRole("status").filter({ hasText: sessionID })).toBeVisible();
  await page.getByLabel("消息内容").fill("hello-stage3");
  await page.getByRole("button", { name: "发送" }).click();
  await expect(page.getByText("hello-stage3", { exact: true })).toBeVisible();
  await expect(page.getByText("framework:hello-stage3")).toBeVisible();
  await expect(page.getByText(`${sessionID} · completed`)).toBeVisible();

  await page.reload();
  await page.getByRole("button", { name: "Chat" }).click();
  await expect(page.getByText("hello-stage3", { exact: true })).toBeVisible();
  await expect(page.getByText("framework:hello-stage3")).toBeVisible();

  await page.getByLabel("Mock 故障").selectOption("message_length");
  await page.getByRole("button", { name: "设置故障" }).click();
  await expect(page.getByText("当前：message_length")).toBeVisible();
  await page.getByLabel("消息内容").fill("hi");
  await page.getByRole("button", { name: "发送" }).click();
  await expect(page.getByRole("alert").filter({ hasText: "channel_message_too_long" })).toBeVisible();
  await expect(page.getByText(`${sessionID} · failed`)).toBeVisible();

  await page.getByLabel("Mock 故障").selectOption("timeout");
  await page.getByRole("button", { name: "设置故障" }).click();
  await expect(page.getByText("当前：timeout")).toBeVisible();
  await page.getByLabel("消息内容").fill("cancel-me");
  await page.getByRole("button", { name: "发送" }).click();
  await page.getByRole("button", { name: "取消" }).click();
  await expect(page.getByText(`${sessionID} · cancelled`)).toBeVisible({ timeout: 5000 });

  await page.getByRole("button", { name: "清除故障" }).click();
  await expect(page.getByText("当前：none")).toBeVisible();
  await page.getByRole("button", { name: "重试" }).click();
  await expect(page.getByText(`${sessionID} · completed`)).toBeVisible({ timeout: 5000 });

  await expectNoHorizontalOverflow(page);
  await page.screenshot({ path: testInfo.outputPath("stage3-chat.png"), fullPage: true });
});
