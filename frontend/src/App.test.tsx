import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import App from "./App";

test("loads the server identity and renders stable console navigation", async () => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
    ok: true,
    json: async () => ({
      id: "developer", name: "Local Developer", active_tenant_id: "tenant-a", active_role: "platform_admin",
      assignments: [{ tenant_id: "tenant-a", tenant_name: "Tenant A", role: "platform_admin" }],
    }),
  }));
  render(<App />);
  expect(screen.getByText("正在加载")).toBeInTheDocument();
  await waitFor(() => expect(screen.getByText("Local Developer")).toBeInTheDocument());
  expect(screen.getByRole("navigation", { name: "主导航" })).toBeInTheDocument();
  expect(screen.getByRole("combobox", { name: "当前租户" })).toHaveValue("tenant-a");
});

test("exchanges a production token without storing it in the browser", async () => {
  let authenticated = false;
  const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input);
    if (path.endsWith("/auth/login")) {
      expect(String(init?.body)).toContain("stage5-token-canary");
      authenticated = true;
    }
    if (!authenticated) return new Response(JSON.stringify({ error: { code: "identity_required", message: "required" } }), { status: 401, headers: { "Content-Type": "application/json" } });
    return new Response(JSON.stringify({ id: "operator", name: "Operator", active_tenant_id: "tenant-a", active_role: "operator", assignments: [{ tenant_id: "tenant-a", tenant_name: "A", role: "operator" }] }), { status: 200, headers: { "Content-Type": "application/json" } });
  });
  vi.stubGlobal("fetch", fetchMock);
  const user = userEvent.setup();
  render(<App />);
  await user.type(await screen.findByLabelText("Identity Token"), "stage5-token-canary");
  await user.click(screen.getByRole("button", { name: "登录" }));
  expect(await screen.findByText("Operator")).toBeVisible();
  expect(fetchMock).toHaveBeenCalledWith("/api/v1/auth/login", expect.objectContaining({ method: "POST" }));
  expect(document.body.textContent).not.toContain("stage5-token-canary");
});

test("remounts the data page when the active tenant changes", async () => {
  let activeTenant = "tenant-a";
  const sessionRequests: string[] = [];
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input);
    if (path.endsWith("/auth/switch-tenant")) {
      activeTenant = "tenant-b";
      return new Response(JSON.stringify({ id: "developer", name: "Local Developer", active_tenant_id: "tenant-b", active_role: "platform_admin", assignments: [] }), { status: 200 });
    }
    if (path.endsWith("/storage/backend")) {
      return new Response(JSON.stringify({ backend: "inmemory", available_backends: ["inmemory"], health: { backend: "inmemory", status: "healthy", checked_at: "now" } }), { status: 200 });
    }
    if (path.includes("/sessions/") && !path.endsWith("/events")) {
      sessionRequests.push(path);
      const summary = activeTenant === "tenant-a" ? "Tenant A summary" : "Tenant B summary";
      return new Response(JSON.stringify({ id: "demo-session", tenant_id: activeTenant, sequence: 1, summary, event_count: 1, updated_at: "now" }), { status: 200 });
    }
    if (path.endsWith("/events")) return new Response(JSON.stringify({ items: [] }), { status: 200 });
    if (path.includes("/memory/")) return new Response(JSON.stringify({ items: [] }), { status: 200 });
    return new Response(JSON.stringify({ id: "developer", name: "Local Developer", active_tenant_id: activeTenant, active_role: "platform_admin", assignments: [] }), { status: 200 });
  }));

  render(<App />);
  fireEvent.click(await screen.findByRole("button", { name: "数据管理" }));
  expect(await screen.findByText("Tenant A summary")).toBeInTheDocument();
  fireEvent.change(screen.getByRole("combobox", { name: "当前租户" }), { target: { value: "tenant-b" } });
  expect(await screen.findByText("Tenant B summary")).toBeInTheDocument();
  await waitFor(() => expect(screen.queryByText("Tenant A summary")).not.toBeInTheDocument());
  expect(sessionRequests.length).toBeGreaterThanOrEqual(2);
});
