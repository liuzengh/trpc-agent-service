import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { ChannelsPage } from "./ChannelsPage";
import type { Identity } from "./api";

test("platform administrator can disable and delete a bot tenant route", async () => {
  const route = { provider: "telegram", external_subject: "chat-1", tenant_id: "tenant-one", app_id: "app-one", conversation_type: "single", enabled: true };
  let replayed = false;
  const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input);
    if (path === "/api/v1/chat/bindings") return { ok: true, status: 200, json: async () => ({ items: [] }) };
    if (path === "/api/v1/admin/agent-apps") return { ok: true, status: 200, json: async () => ({ items: [{ id: "app-one", name: "App One" }] }) };
    if (path === "/api/v1/admin/providers/status") return { ok: true, status: 200, json: async () => ({ items: [{ provider: "telegram", status: "connected", credential_smoke_status: "not_run" }] }) };
    if (path === "/api/v1/admin/providers/routes" && !init?.method) return { ok: true, status: 200, json: async () => ({ items: [route] }) };
    if (path === "/api/v1/admin/providers/deliveries") return { ok: true, status: 200, json: async () => ({ items: replayed ? [{ provider: "telegram", external_subject: "chat-1", request_id: "channel-replay", status: "delivered", attempts: 1 }] : [] }) };
    if (path.startsWith("/api/v1/admin/providers/routes?") && init?.method === "PATCH") return { ok: true, status: 200, json: async () => ({ ...route, enabled: false }) };
    if (path.startsWith("/api/v1/admin/providers/routes?") && init?.method === "DELETE") return { ok: true, status: 204, json: async () => ({}) };
    if (path === "/api/v1/admin/providers/replay" && init?.method === "POST") { replayed = true; return { ok: true, status: 202, json: async () => ({ session_id: "replay-session", request_id: "channel-replay", status: "running" }) }; }
    throw new Error(`unexpected request: ${init?.method ?? "GET"} ${path}`);
  });
  vi.stubGlobal("fetch", fetchMock);
  const identity: Identity = { id: "admin", name: "Admin", active_tenant_id: "tenant-one", active_role: "platform_admin", assignments: [] };
  render(<ChannelsPage identity={identity} />);

  expect(await screen.findByText("凭据 Smoke：not_run")).toBeInTheDocument();

  await userEvent.click(await screen.findByRole("button", { name: "重放 chat-1 测试消息" }));
  await userEvent.type(screen.getByLabelText("消息内容"), "hello replay");
  await userEvent.click(screen.getByRole("button", { name: "重放" }));
  await waitFor(() => expect(screen.getByRole("status")).toHaveTextContent("channel-replay"));

  await userEvent.click(screen.getByLabelText("chat-1 路由启用状态"));
  await waitFor(() => expect(fetchMock).toHaveBeenCalledWith(expect.stringContaining("provider=telegram"), expect.objectContaining({ method: "PATCH" })));
  const patchCall = fetchMock.mock.calls.find(([, init]) => init?.method === "PATCH");
  expect(JSON.parse(String(patchCall?.[1]?.body))).toEqual({ tenant_id: "tenant-one", app_id: "app-one", conversation_type: "single", enabled: false });

  await userEvent.click(screen.getByRole("button", { name: "删除 chat-1 路由" }));
  await waitFor(() => expect(fetchMock).toHaveBeenCalledWith(expect.stringContaining("external_subject=chat-1"), expect.objectContaining({ method: "DELETE" })));
});
