import { expect, test } from "@playwright/test";

test("govern governance decisions and inspect their trace without secret disclosure", { timeout: 90_000 }, async ({ page }, testInfo) => {
  const suffix = `${testInfo.project.name}-${Date.now()}`;
  const appID = `stage5-app-${suffix}`;
  const deploymentID = `stage5-deploy-${suffix}`;
  const sessionID = `stage5-session-${suffix}`;
  const requestID = `stage5-request-${suffix}`;
  const secretCanary = `stage5-secret-${suffix}`;
  await page.goto("/");
  const pending = await page.evaluate(async ({ appID, deploymentID, sessionID, requestID, secretCanary, suffix }) => {
    const request = async (path: string, body: unknown, headers?: Record<string, string>) => {
      const response = await fetch(path, { method: "POST", headers: { "Content-Type": "application/json", ...headers }, body: JSON.stringify(body) });
      const payload = await response.json();
      if (!response.ok && response.status !== 409) throw new Error(`${path}: ${response.status}`);
      return payload as Record<string, unknown>;
    };
    await request("/api/v1/admin/agent-apps", { id: appID, name: `Stage 5 ${suffix}` });
    await request("/api/v1/admin/deployments", { id: deploymentID, agent_app_id: appID });
    await request(`/api/v1/admin/deployments/${deploymentID}/versions`, { config: { runner: "framework", tools: ["deploy"], deterministic_tool_call: "deploy" } }, { "Idempotency-Key": `stage5-${suffix}` });
    const versionResponse = await fetch(`/api/v1/admin/deployments/${deploymentID}/versions`);
    const versionList = await versionResponse.json() as { items: { id: string }[] };
    const version = versionList.items[0];
    await request(`/api/v1/admin/deployments/${deploymentID}/transition`, { status: "published", version_id: version.id });
    await request(`/api/v1/admin/deployments/${deploymentID}/transition`, { status: "active" });
    await request("/api/v1/admin/governance/policy", { agent_app_id: appID, allowed_tools: ["deploy"], dangerous_tools: ["deploy"], redacted_patterns: [secretCanary], token_budget: 10000, estimated_tokens_per_run: 5, rate_limit: 1000, rate_window_seconds: 60, runtime_timeout_ms: 60000 });
    await request("/api/v1/chat/sessions", { app_id: appID, session_id: sessionID });
    await request(`/api/v1/chat/sessions/${sessionID}/messages`, { input: `release ${secretCanary}` }, { "X-Request-ID": requestID });
    for (let attempt = 0; attempt < 600; attempt += 1) {
      const response = await fetch("/api/v1/admin/governance/confirmations");
      const payload = await response.json() as { items: { request_id: string; confirmation_id?: string; id: string; trace_id: string; status: string }[] };
      const item = payload.items.find((candidate) => candidate.request_id === requestID && candidate.status === "pending");
      if (item) return item;
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    throw new Error("confirmation was not created by actual Tool invocation");
  }, { appID, deploymentID, sessionID, requestID, secretCanary, suffix });

  expect(String(pending.id)).not.toBe("");
  expect(String(pending.trace_id)).not.toBe("");
  await page.reload();
  await page.getByRole("button", { name: "治理观测" }).click();
  await page.getByRole("button", { name: "确认" }).click();
  const confirmationRow = page.getByText(requestID).locator("xpath=ancestor::tr");
  await expect(confirmationRow).toBeVisible();
  await confirmationRow.getByTitle("批准").click();
  await expect(confirmationRow.getByText("approved", { exact: true })).toBeVisible();

  await page.evaluate(async ({ sessionID, requestID, secretCanary }) => {
    const response = await fetch(`/api/v1/chat/sessions/${sessionID}/messages`, { method: "POST", headers: { "Content-Type": "application/json", "X-Request-ID": requestID }, body: JSON.stringify({ input: `release ${secretCanary}` }) });
    if (!response.ok) throw new Error(`retry: ${response.status}`);
    for (let attempt = 0; attempt < 600; attempt += 1) {
      const events = await fetch(`/api/v1/admin/sessions/${sessionID}/events`).then((item) => item.json()) as { items: { type: string; idempotency_key: string }[] };
      if (events.items.some((item) => item.type === "run.completed" && (item.idempotency_key === `${requestID}:terminal` || item.idempotency_key === `${requestID}:run-completed`))) return;
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    throw new Error("approved Tool request did not complete");
  }, { sessionID, requestID, secretCanary });
  await page.getByRole("button", { name: "指标与成本" }).click();
  await page.getByLabel("Request 或 Trace ID").fill(String(pending.trace_id));
  await page.getByRole("button", { name: "查询 Trace" }).click();
  await expect(page.getByText("gateway.receive").first()).toBeVisible();
  await expect(page.getByText("policy.evaluate").first()).toBeVisible();
  expect(await page.locator("body").innerText()).not.toContain(secretCanary);
  const exposedOperationalState = await page.evaluate(async ({ sessionID }) => {
    const [events, audits] = await Promise.all([
      fetch(`/api/v1/admin/sessions/${sessionID}/events`).then((response) => response.text()),
      fetch("/api/v1/admin/governance/audit").then((response) => response.text()),
    ]);
    return JSON.stringify({ events, audits, local: { ...localStorage }, session: { ...sessionStorage } });
  }, { sessionID });
  expect(exposedOperationalState).not.toContain(secretCanary);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth)).toBe(true);
});
