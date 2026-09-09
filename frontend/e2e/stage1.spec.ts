import { expect, test, type Page } from "@playwright/test";

async function expectNoHorizontalOverflow(page: Page) {
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth)).toBe(true);
}

async function createVersion(page: Page, config: string) {
  await page.getByRole("button", { name: "创建版本" }).first().click();
  await page.getByLabel("JSON 配置").fill(config);
  await page.locator("form").getByRole("button", { name: "创建版本" }).click();
}

async function createVersionWithAmbiguousRetry(page: Page, tenantId: string) {
  const keys: string[] = [];
  const versionsPattern = "**/api/v1/admin/deployments/*/versions";
  await page.route(versionsPattern, async (route) => {
    if (route.request().method() !== "POST") {
      await route.continue();
      return;
    }
    keys.push(route.request().headers()["idempotency-key"] ?? "");
    if (keys.length === 1) {
      const response = await route.fetch();
      expect(response.status()).toBe(201);
      await route.abort("failed");
      return;
    }
    await route.continue();
  });

  await createVersion(page, `{"runner":"fake","scope":"${tenantId}"}`);
  await expect(page.getByRole("alert")).toContainText("无法连接服务，请重试");
  await page.locator("form").getByRole("button", { name: "创建版本" }).click();
  await expect(page.getByText("v1", { exact: true })).toBeVisible();
  await page.unroute(versionsPattern);
  expect(keys).toHaveLength(2);
  expect(keys[0]).toBeTruthy();
  expect(keys[1]).toBe(keys[0]);

  const nextRequestPromise = page.waitForRequest((request) => request.method() === "POST" && request.url().includes("/versions"));
  await createVersion(page, `{"runner":"fake","scope":"${tenantId}","revision":2}`);
  const nextRequest = await nextRequestPromise;
  expect(nextRequest.headers()["idempotency-key"]).toBeTruthy();
  expect(nextRequest.headers()["idempotency-key"]).not.toBe(keys[0]);
  await expect(page.getByText("v2", { exact: true })).toBeVisible();
}

async function createActiveTenantApp(page: Page, suffix: string, ordinal: string, verifyIdempotency = false) {
  const tenantId = `tenant-${suffix}-${ordinal}`;
  const appId = `app-${suffix}-${ordinal}`;
  const deploymentId = `deploy-${suffix}-${ordinal}`;

  await page.getByRole("button", { name: "租户" }).click();
  await page.getByRole("button", { name: "新建租户" }).click();
  await page.getByLabel("租户标识").fill(tenantId);
  await page.getByLabel("显示名称").fill(`验收租户 ${suffix} ${ordinal}`);
  await page.getByRole("button", { name: "创建租户" }).click();
  await expect(page.getByRole("heading", { name: `验收租户 ${suffix} ${ordinal}` })).toBeVisible();
  await page.getByRole("combobox", { name: "当前租户" }).selectOption(tenantId);

  await page.getByRole("button", { name: "Agent 应用" }).click();
  await page.getByRole("button", { name: "新建应用" }).click();
  await page.getByLabel("应用标识").fill(appId);
  await page.getByLabel("显示名称").fill(`验收应用 ${suffix} ${ordinal}`);
  await page.getByRole("button", { name: "创建应用" }).click();
  await expect(page.getByText(`验收应用 ${suffix} ${ordinal}`)).toBeVisible();

  await page.getByRole("button", { name: "部署" }).click();
  await page.getByRole("button", { name: "新建部署" }).click();
  await page.getByLabel("部署标识").fill(deploymentId);
  await page.getByRole("button", { name: "创建部署" }).click();
  if (verifyIdempotency) {
    await createVersionWithAmbiguousRetry(page, tenantId);
  } else {
    await createVersion(page, `{"runner":"fake","scope":"${tenantId}"}`);
    await expect(page.getByText("v1", { exact: true })).toBeVisible();
  }
  await page.getByRole("button", { name: "发布", exact: true }).click();
  await expect(page.getByText("published").first()).toBeVisible();
  await page.getByRole("button", { name: "激活" }).click();
  await expect(page.getByText("active").first()).toBeVisible();
  await page.evaluate(async (appId) => {
    const response = await fetch("/api/v1/admin/governance/policy", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ agent_app_id: appId, token_budget: 10000, estimated_tokens_per_run: 5, rate_limit: 1000, rate_window_seconds: 60 }),
    });
    if (!response.ok) throw new Error(`policy: ${response.status}`);
  }, appId);
  return { tenantId, appId, deploymentId };
}

async function verifyCompetingActivation(page: Page, appId: string, deploymentId: string) {
  await page.getByRole("button", { name: "关闭详情" }).click();
  await page.getByRole("button", { name: "新建部署" }).click();
  await page.getByLabel("部署标识").fill(deploymentId);
  await page.getByLabel("Agent 应用").selectOption(appId);
  await page.getByRole("button", { name: "创建部署" }).click();
  await createVersion(page, '{"runner":"fake","candidate":true}');
  await expect(page.getByText("v1", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "发布", exact: true }).click();
  await expect(page.getByText("published").first()).toBeVisible();
  const conflictPromise = page.waitForResponse((response) => response.url().endsWith(`/deployments/${deploymentId}/transition`) && response.status() === 409);
  await page.getByRole("button", { name: "激活" }).click();
  const conflict = await conflictPromise;
  expect((await conflict.json()).error.code).toBe("agent_app_already_has_active_deployment");
  await expect(page.getByRole("alert")).toContainText("Agent App already has an active Deployment");
  await expect(page.getByRole("heading", { name: deploymentId })).toBeVisible();
  await expect(page.getByText("published").first()).toBeVisible();
}

async function runApp(page: Page, appId: string, sessionId: string, input: string) {
  return page.evaluate(async (request) => {
    const response = await fetch("/api/v1/admin/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(request),
    });
    return { status: response.status, body: await response.json() as { output?: string; error?: { code: string } } };
  }, { app_id: appId, session_id: sessionId, input });
}

test("complete Stage 1 management workflow", async ({ page }, testInfo) => {
  const suffix = testInfo.project.name === "desktop" ? "desk" : "mobile";
  await page.goto("/");
  await expect(page.getByText("Local Developer")).toBeVisible();
  await expect(page.getByRole("navigation", { name: "主导航" })).toBeVisible();

  const first = await createActiveTenantApp(page, suffix, "one", true);
  const firstRun = await runApp(page, first.appId, `session-${suffix}-one`, "hello-one");
  expect(firstRun).toEqual({ status: 200, body: { session_id: `session-${suffix}-one`, output: "framework:hello-one" } });
  await verifyCompetingActivation(page, first.appId, `candidate-${suffix}-one`);

  const second = await createActiveTenantApp(page, suffix, "two");
  const secondRun = await runApp(page, second.appId, `session-${suffix}-two`, "hello-two");
  expect(secondRun).toEqual({ status: 200, body: { session_id: `session-${suffix}-two`, output: "framework:hello-two" } });
  const crossTenant = await runApp(page, first.appId, `session-${suffix}-cross`, "guess");
  expect(crossTenant.status).toBe(404);
  expect(crossTenant.body.error?.code).toBe("active_deployment_not_found");

  await page.getByRole("button", { name: "暂停" }).click();
  await expect(page.getByText("paused").first()).toBeVisible();

  await page.getByRole("button", { name: "运行节点" }).click();
  await expect(page.getByText("gateway-local")).toBeVisible();
  await expect(page.getByText("worker-local")).toBeVisible();
  await expectNoHorizontalOverflow(page);
  await page.screenshot({ path: testInfo.outputPath("runtime-status.png"), fullPage: true });

  await page.getByRole("button", { name: "数据管理" }).click();
  await expect(page.getByText("healthy")).toBeVisible();
  await page.getByLabel("Session ID").fill(`session-${suffix}-two`);
  await expect(page.getByText("message.input")).toBeVisible();
  await expect(page.getByText("message.output")).toBeVisible();
  await page.getByLabel("后端").selectOption("sqlite");
  await page.getByRole("button", { name: "应用后端" }).click();
  await expect(page.getByText("sqlite").first()).toBeVisible();
  await expectNoHorizontalOverflow(page);

  await page.getByRole("combobox", { name: "当前租户" }).selectOption("tenant-view");
  await expect(page.getByText("message.input")).toHaveCount(0);
  await expect(page.getByText("message.output")).toHaveCount(0);
  await page.getByRole("button", { name: "租户" }).click();
  await expect(page.getByRole("button", { name: "新建租户" })).toHaveCount(0);
  await page.getByRole("button", { name: "Agent 应用" }).click();
  await expect(page.getByRole("button", { name: "新建应用" })).toHaveCount(0);
  await page.getByRole("button", { name: "运行节点" }).click();
  await expect(page.getByText("没有访问权限")).toBeVisible();
  await page.getByRole("button", { name: "数据管理" }).click();
  await expect(page.getByRole("button", { name: "应用后端" })).toHaveCount(0);
  await expect(page.getByRole("button", { name: "启动迁移" })).toHaveCount(0);
});
