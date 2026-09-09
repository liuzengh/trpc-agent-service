import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { DeploymentsPage } from "./DeploymentsPage";
import type { Identity } from "./api";

const identity: Identity = {
  id: "admin",
  name: "Admin",
  active_tenant_id: "tenant-one",
  active_role: "tenant_admin",
  assignments: [],
};

const deployment = {
  id: "deploy-one",
  tenant_id: "tenant-one",
  agent_app_id: "app-one",
  status: "draft",
  desired_replicas: 1,
  created_at: "2026-01-01T00:00:00Z",
};

function jsonResponse(body: unknown) {
  return { ok: true, status: 200, json: async () => body };
}

test("reuses a Version idempotency key after an ambiguous failure and rotates it after success", async () => {
  const fetchMock = vi.fn()
    .mockResolvedValueOnce(jsonResponse({ items: [deployment] }))
    .mockResolvedValueOnce(jsonResponse({ items: [{ id: "app-one", tenant_id: "tenant-one", name: "App One", created_at: "2026-01-01T00:00:00Z" }] }))
    .mockResolvedValueOnce(jsonResponse({ items: [] }))
    .mockRejectedValueOnce(new TypeError("connection lost"))
    .mockResolvedValueOnce(jsonResponse({ ...deployment, id: "deploy-one-v1", deployment_id: "deploy-one", agent_app_id: "app-one", number: 1, config: { runner: "fake" } }))
    .mockResolvedValueOnce(jsonResponse({ ...deployment, id: "deploy-one-v2", deployment_id: "deploy-one", agent_app_id: "app-one", number: 2, config: { runner: "other" } }));
  vi.stubGlobal("fetch", fetchMock);

  render(<DeploymentsPage identity={identity} />);
  await userEvent.click(await screen.findByText("deploy-one"));
  await userEvent.click(await screen.findByRole("button", { name: "创建版本" }));
  const config = screen.getByLabelText("JSON 配置");
  const versionForm = config.closest("form");
  if (!versionForm) throw new Error("Version form was not rendered");
  fireEvent.change(config, { target: { value: '{"runner":"fake"}' } });
  await userEvent.click(within(versionForm).getByRole("button", { name: "创建版本" }));
  await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(4));
  await userEvent.click(within(versionForm).getByRole("button", { name: "创建版本" }));
  await screen.findByText("v1");

  const firstKey = new Headers(fetchMock.mock.calls[3][1].headers).get("Idempotency-Key");
  const replayKey = new Headers(fetchMock.mock.calls[4][1].headers).get("Idempotency-Key");
  expect(firstKey).toBeTruthy();
  expect(replayKey).toBe(firstKey);

  await userEvent.click(screen.getByRole("button", { name: "创建版本" }));
  const nextConfig = screen.getByLabelText("JSON 配置");
  const nextVersionForm = nextConfig.closest("form");
  if (!nextVersionForm) throw new Error("Version form was not rendered");
  fireEvent.change(nextConfig, { target: { value: '{"runner":"other"}' } });
  await userEvent.click(within(nextVersionForm).getByRole("button", { name: "创建版本" }));
  await screen.findAllByText("v2");
  const nextKey = new Headers(fetchMock.mock.calls[5][1].headers).get("Idempotency-Key");
  expect(nextKey).toBeTruthy();
  expect(nextKey).not.toBe(firstKey);
});

test("shows an Active Deployment conflict and refreshes server-authoritative state", async () => {
  const first = { ...deployment, id: "deploy-one", status: "active", version_id: "deploy-one-v1" };
  const second = { ...deployment, id: "deploy-two", status: "published", version_id: "deploy-two-v1" };
  const fetchMock = vi.fn()
    .mockResolvedValueOnce(jsonResponse({ items: [first, second] }))
    .mockResolvedValueOnce(jsonResponse({ items: [{ id: "app-one", tenant_id: "tenant-one", name: "App One", created_at: "2026-01-01T00:00:00Z" }] }))
    .mockResolvedValueOnce(jsonResponse({ items: [{ id: "deploy-two-v1", deployment_id: "deploy-two", agent_app_id: "app-one", number: 1, config: { runner: "fake" } }] }))
    .mockResolvedValueOnce({ ok: false, status: 409, json: async () => ({ error: { code: "agent_app_already_has_active_deployment", message: "Agent App already has an active Deployment" } }) })
    .mockResolvedValueOnce(jsonResponse({ items: [first, second] }));
  vi.stubGlobal("fetch", fetchMock);

  render(<DeploymentsPage identity={identity} />);
  await userEvent.click(await screen.findByText("deploy-two"));
  await userEvent.click(await screen.findByRole("button", { name: "激活" }));
  expect(await screen.findByRole("alert")).toHaveTextContent("Agent App already has an active Deployment");
  await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(5));
  expect(fetchMock.mock.calls[4][0]).toBe("/api/v1/admin/deployments");
  expect(screen.getByRole("heading", { name: "deploy-two" })).toBeInTheDocument();
  expect(screen.getAllByText("active")).toHaveLength(1);
  expect(screen.getAllByText("published").length).toBeGreaterThan(0);
});

test("manages rollout rollback and capacity results with confirmation", async () => {
  const activeDeployment = { ...deployment, status: "active", version_id: "deploy-one-v1", rollout_status: "idle" };
  const versions = [
    { id: "deploy-one-v1", deployment_id: "deploy-one", agent_app_id: "app-one", number: 1, config: { runner: "one" }, created_at: "2026-01-01T00:00:00Z" },
    { id: "deploy-one-v2", deployment_id: "deploy-one", agent_app_id: "app-one", number: 2, config: { runner: "two" }, created_at: "2026-01-02T00:00:00Z" },
  ];
  const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    if (url === "/api/v1/admin/deployments") return jsonResponse({ items: [activeDeployment] });
    if (url === "/api/v1/admin/agent-apps") return jsonResponse({ items: [{ id: "app-one", tenant_id: "tenant-one", name: "App One", created_at: "2026-01-01T00:00:00Z" }] });
    if (url === "/api/v1/admin/deployments/deploy-one/versions") return jsonResponse({ items: versions });
    if (url === "/api/v1/admin/deployments/deploy-one/rollout" && init?.method === "POST") {
      expect(JSON.parse(String(init.body))).toEqual({ target_version_id: "deploy-one-v2", gray_percentage: 25, confirm: true });
      return jsonResponse({ ...activeDeployment, rollout_status: "rolling", target_version_id: "deploy-one-v2", current_version_id: "deploy-one-v1", previous_version_id: "deploy-one-v1", gray_percentage: 25 });
    }
    if (url === "/api/v1/admin/deployments/deploy-one/rollback-preview") return jsonResponse({ tenant_id: "tenant-one", agent_app_id: "app-one", deployment_id: "deploy-one", current_version_id: "deploy-one-v1", previous_version_id: "deploy-one-v1", active_executions: 0, expected_result: "active routing returns to the previous immutable Version" });
    if (url === "/api/v1/admin/deployments/deploy-one/rollback" && init?.method === "POST") return jsonResponse({ ...activeDeployment, rollout_status: "completed", target_version_id: "deploy-one-v1", current_version_id: "deploy-one-v1", previous_version_id: "deploy-one-v1", gray_percentage: 100 });
    if (url === "/api/v1/admin/capacity" && init?.method === "POST") {
      expect(JSON.parse(String(init.body))).toEqual({ agent_app_id: "app-one", concurrency: 2, runs: 4, timeout_ms: 1000, peak_im_callbacks_per_second: 120, average_tokens_per_session: 800, redis_operations_per_session: 6, sql_operations_per_session: 4, headroom_percent: 25 });
      return jsonResponse({ id: "capacity-one", request_id: "capacity-request", trace_id: "trace-one", tenant_id: "tenant-one", agent_app_id: "app-one", status: "running", concurrency: 2, runs: 4, completed: 0, failed: 0, active: 2, safe_concurrency: 2, throughput_per_second: 0, model_latency_ms: 0, tool_latency_ms: 0, storage_latency_ms: 0, estimated_tokens: 0, estimated_cost: 0, first_bottleneck: "none", sessions_per_node: 1, recommended_worker_nodes: 1, average_tokens_per_session: 800, token_throughput_per_second: 96000, im_callback_peak_qps: 120, redis_qps: 720, sql_qps: 480, headroom_percent: 25, started_at: "2026-01-01T00:00:00Z" });
    }
    if (url === "/api/v1/admin/capacity/capacity-one") return jsonResponse({ id: "capacity-one", request_id: "capacity-request", trace_id: "trace-one", tenant_id: "tenant-one", agent_app_id: "app-one", status: "completed", concurrency: 2, runs: 4, completed: 4, failed: 0, active: 0, safe_concurrency: 2, throughput_per_second: 10, model_latency_ms: 2, tool_latency_ms: 0, storage_latency_ms: 1, estimated_tokens: 3200, estimated_cost: 0.2, first_bottleneck: "none", sessions_per_node: 1, recommended_worker_nodes: 16, average_tokens_per_session: 800, token_throughput_per_second: 96000, im_callback_peak_qps: 120, redis_qps: 720, sql_qps: 480, headroom_percent: 25, started_at: "2026-01-01T00:00:00Z", completed_at: "2026-01-01T00:00:01Z" });
    throw new Error(`unexpected request ${url}`);
  });
  const confirmMock = vi.fn(() => true);
  vi.stubGlobal("fetch", fetchMock);
  vi.stubGlobal("confirm", confirmMock);
  render(<DeploymentsPage identity={identity} />);
  await userEvent.click(await screen.findByText("deploy-one"));
  await screen.findAllByText("v2");
  fireEvent.change(screen.getByLabelText("目标版本"), { target: { value: "deploy-one-v2" } });
  fireEvent.change(screen.getByLabelText("灰度 %"), { target: { value: "25" } });
  await userEvent.click(screen.getByRole("button", { name: "开始/推进灰度" }));
  expect(await screen.findByText("rolling · 25%")).toBeInTheDocument();
  await userEvent.click(screen.getByRole("button", { name: "回滚预览" }));
  expect(await screen.findByText("active routing returns to the previous immutable Version")).toBeInTheDocument();
  await userEvent.click(screen.getByRole("button", { name: "确认回滚" }));
  await screen.findByText("completed · 100%");
  await userEvent.click(screen.getByRole("button", { name: "容量评估" }));
  expect(await screen.findByText("trace-one")).toBeInTheDocument();
  await waitFor(() => expect(screen.getByText("10.00")).toBeInTheDocument());
  expect(screen.getByText("720.00 / 480.00")).toBeInTheDocument();
  expect(screen.getByText("120.00 / 96000.00")).toBeInTheDocument();
  await waitFor(() => expect(confirmMock).toHaveBeenCalledTimes(2));
});
