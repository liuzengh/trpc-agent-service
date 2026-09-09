import { render, screen, waitFor } from "@testing-library/react";
import { fireEvent } from "@testing-library/react";
import { RuntimePage } from "./RuntimePage";
import type { BackendHealth, DrainStatus, ProviderStatus, RuntimeStatus } from "./api";

test("renders backend-reported gateway and worker state without request data", async () => {
  const items: RuntimeStatus[] = [
    { id: "gateway-local", role: "gateway", available: true, lifecycle: "healthy", active_executions: 0, completed_executions: 2, failed_executions: 0 },
    { id: "worker-local", role: "worker", available: false, lifecycle: "error", active_executions: 0, completed_executions: 2, failed_executions: 1 },
    { id: "dependency-postgres", role: "dependency", available: false, lifecycle: "unavailable", active_executions: 0, completed_executions: 0, failed_executions: 0 },
  ];
  const drain: DrainStatus = { state: "idle", active_executions: 1 };
  const health: BackendHealth = { backend: "redis", status: "healthy", checked_at: "2026-09-04T00:00:00Z" };
  const providers: ProviderStatus[] = [{ provider: "telegram", status: "connected", credential_smoke_status: "not_run" }];
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input);
    const body = url.includes("/runtime/status") ? { items } : url.includes("/operations/drain") ? drain : url.includes("/storage/backend") ? { health } : url.includes("/agent-apps") ? { items: [] } : url.includes("/operations/faults") ? { enabled: true, scenarios: [] } : { items: providers };
    return { ok: true, json: async () => body };
  }));
  render(<RuntimePage />);
  await waitFor(() => expect(screen.getByText("gateway-local")).toBeInTheDocument());
  expect(screen.getByText("worker-local")).toBeInTheDocument();
  expect(screen.getByText("dependency-postgres")).toBeInTheDocument();
  expect(screen.getByText("error")).toHaveClass("status", "error");
  expect(document.body.textContent).not.toContain("request input");
});

test("starts a confirmed graceful drain and shows dependency health", async () => {
  let drainStatus: DrainStatus = { state: "idle", active_executions: 1 };
  const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    if (url === "/api/v1/admin/operations/drain" && init?.method === "POST") {
      expect(JSON.parse(String(init.body))).toEqual({ confirm: true });
      drainStatus = { state: "draining", active_executions: 1 };
      return { ok: true, json: async () => drainStatus };
    }
    const body = url.includes("/runtime/status") ? { items: [] } : url.includes("/operations/drain") ? drainStatus : url.includes("/storage/backend") ? { health: { backend: "redis", status: "healthy", checked_at: "2026-09-04T00:00:00Z" } } : url.includes("/agent-apps") ? { items: [] } : url.includes("/operations/faults") ? { enabled: true, scenarios: [] } : { items: [] };
    return { ok: true, json: async () => body };
  });
  const confirmMock = vi.fn(() => true);
  vi.stubGlobal("confirm", confirmMock);
  vi.stubGlobal("fetch", fetchMock);
  render(<RuntimePage identity={{ id: "operator", name: "Operator", active_tenant_id: "tenant", active_role: "operator", assignments: [] }} />);
  fireEvent.click(await screen.findByText("开始优雅排水"));
  await waitFor(() => expect(screen.getByText("draining")).toBeInTheDocument());
  drainStatus = { state: "closed", active_executions: 0 };
  await waitFor(() => expect(screen.getByText("closed")).toBeInTheDocument());
  expect(confirmMock).toHaveBeenCalledWith("确认开始优雅排水？新请求将被拒绝，活跃请求会等待完成。");
});
