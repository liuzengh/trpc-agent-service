import { expect, test } from "@playwright/test";

test("manage and replay a Stage 4 provider route", async ({ page }, testInfo) => {
  const suffix = `${testInfo.project.name}-${Date.now()}`;
  const appID = `stage4-app-${suffix}`;
  const deploymentID = `stage4-deploy-${suffix}`;
  const subject = String(Date.now());
  await page.goto("/");
  await page.evaluate(async ({ appID, deploymentID, suffix }) => {
    const request = async (path: string, body: unknown, headers?: Record<string, string>) => {
      const response = await fetch(path, { method: "POST", headers: { "Content-Type": "application/json", ...headers }, body: JSON.stringify(body) });
      if (!response.ok) throw new Error(`${path}: ${response.status}`);
      return response.json() as Promise<Record<string, unknown>>;
    };
    await request("/api/v1/admin/agent-apps", { id: appID, name: `Stage 4 ${suffix}` });
    await request("/api/v1/admin/deployments", { id: deploymentID, agent_app_id: appID });
    const version = await request(`/api/v1/admin/deployments/${deploymentID}/versions`, { config: { runner: "echo" } }, { "Idempotency-Key": `stage4-${suffix}` });
    await request(`/api/v1/admin/deployments/${deploymentID}/transition`, { status: "published", version_id: version.id });
    await request(`/api/v1/admin/deployments/${deploymentID}/transition`, { status: "active" });
    await request("/api/v1/admin/governance/policy", {
      agent_app_id: appID, token_budget: 10000, estimated_tokens_per_run: 5, rate_limit: 1000, rate_window_seconds: 60,
    });
  }, { appID, deploymentID, suffix });

  await page.getByRole("button", { name: "IM 通道" }).click();
  await page.getByLabel("Provider").selectOption("telegram");
  await page.getByLabel("Agent 应用").selectOption(appID);
  await page.getByLabel("会话 ID").fill(subject);
  await page.getByRole("button", { name: "创建路由" }).click();
  await expect(page.getByText(subject, { exact: true })).toBeVisible();

  await page.getByRole("button", { name: `重放 ${subject} 测试消息` }).click();
  await page.getByLabel("消息内容").fill("stage4 replay");
  await page.getByRole("button", { name: "重放", exact: true }).click();
  await expect(page.getByRole("status")).toContainText("Replay 已接受");
  await expect(page.getByText("delivered", { exact: true }).first()).toBeVisible();

  await page.getByLabel(`${subject} 路由启用状态`).uncheck();
  await expect(page.getByText("停用", { exact: true }).first()).toBeVisible();
  await page.getByLabel(`${subject} 路由启用状态`).check();
  await expect(page.getByRole("button", { name: `重放 ${subject} 测试消息` })).toBeEnabled();
  await page.getByRole("button", { name: `删除 ${subject} 路由` }).click();
  await expect(page.getByRole("button", { name: `删除 ${subject} 路由` })).toHaveCount(0);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth)).toBe(true);
});
