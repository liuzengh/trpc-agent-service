import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, expect, test, vi } from "vitest";
import { GovernancePage } from "./GovernancePage";
import type { Identity } from "./api";

const identity: Identity = { id: "admin", name: "Admin", active_tenant_id: "tenant-a", active_role: "tenant_admin", assignments: [] };

beforeEach(() => {
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input);
    if (path === "/api/v1/admin/agent-apps") return response({ items: [{ id: "app-a", tenant_id: "tenant-a", name: "App A", created_at: "now" }] });
    if (path.startsWith("/api/v1/admin/governance/policy") && init?.method === "POST") return response({ tenant_id: "tenant-a", agent_app_id: "app-a", revision: 1, allowed_tools: ["search"], allowed_mcp: [], dangerous_tools: [], denied_input_patterns: [], denied_output_patterns: [], redacted_patterns: [], allowed_im_users: [], allowed_im_subjects: [], allowed_provider_accounts: [], allowed_conversation_types: [], token_budget: 100, cost_budget: 1, cost_per_token: 0.01, tool_costs: { search: 0.02 }, estimated_tokens_per_run: 10, rate_limit: 10, rate_window_seconds: 60, updated_at: "now" });
    if (path.startsWith("/api/v1/admin/governance/policy")) return new Response(JSON.stringify({ error: { code: "policy_not_found", message: "not found" } }), { status: 404, headers: { "Content-Type": "application/json" } });
    if (path.startsWith("/api/v1/admin/governance/audit")) return response({ items: [{ id: "audit-1", tenant_id: "tenant-a", decision: "policy.allowed", trace_id: "trace-1", request_id: "request-1", latency: 0, cost: 0, occurred_at: "now" }] });
    if (path.startsWith("/api/v1/admin/governance/metrics")) return response({ tenant_id: "tenant-a", requests: 4, active_executions: 0, completed_executions: 3, failed_executions: 1, denied_requests: 1, rate_limited_requests: 2, tokens: 42, cost: 0.42, model_latency_ms: 20, execution_latency_ms: 30, tool_latency_ms: 7, storage_latency_ms: 3, im_delivered: 1, im_failed: 1 });
    if (path === "/api/v1/admin/governance/confirmations") return response({ items: [] });
    if (path.startsWith("/api/v1/admin/governance/traces")) return response({ trace_id: "trace-1", tenant_id: "tenant-a", request_id: "request-1", session_id: "session-1", agent_app_id: "app-a", spans: [{ name: "gateway.receive", status: "ok", occurred_at: "now" }] });
    throw new Error(`unexpected request ${path}`);
  }));
});

test("manages policy and inspects audit metrics and traces", async () => {
  const user = userEvent.setup();
  render(<GovernancePage identity={identity} />);
  await screen.findByRole("option", { name: "App A" });
  await user.type(screen.getByLabelText("允许的 Tools"), "search");
  await user.clear(screen.getByLabelText("Token 预算"));
  await user.type(screen.getByLabelText("Token 预算"), "100");
  await user.click(screen.getByRole("button", { name: "保存策略" }));
  await waitFor(() => expect(screen.getByRole("status")).toHaveTextContent("策略 revision 1 已生效"));

  await user.click(screen.getByRole("button", { name: "审计" }));
  expect(await screen.findByText("policy.allowed")).toBeVisible();
  await user.type(screen.getByLabelText("决策"), "policy.allowed");
  await user.type(screen.getByLabelText("Request ID"), "request-1");
  await user.type(screen.getByLabelText("Trace ID"), "trace-1");
  await user.click(screen.getByRole("button", { name: "查询审计" }));
  await waitFor(() => expect(fetch).toHaveBeenCalledWith(
    "/api/v1/admin/governance/audit?decision=policy.allowed&request_id=request-1&trace_id=trace-1",
    expect.anything(),
  ));
  await user.click(screen.getByRole("button", { name: "指标与成本" }));
  expect(await screen.findByText("0.42")).toBeVisible();
	expect(screen.getByText("Tool 延迟 ms")).toBeVisible();
	expect(screen.getByText("执行延迟 ms")).toBeVisible();
	expect(screen.getByText("IM 失败")).toBeVisible();
	await user.selectOptions(screen.getByLabelText("Provider"), "telegram");
	await user.click(screen.getByRole("button", { name: "查询指标" }));
	await waitFor(() => expect(fetch).toHaveBeenCalledWith(expect.stringContaining("provider=telegram"), expect.anything()));
  await user.type(screen.getByLabelText("Request 或 Trace ID"), "trace-1");
  await user.click(screen.getByRole("button", { name: "查询 Trace" }));
  expect(await screen.findByText("gateway.receive")).toBeVisible();
});

function response(body: unknown) {
  return new Response(JSON.stringify(body), { status: 200, headers: { "Content-Type": "application/json" } });
}
