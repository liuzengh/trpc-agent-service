import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { vi } from "vitest";
import { DataPage } from "./DataPage";

test("shows backend health and starts a migration dry-run", async () => {
  const fetchMock = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
    const path = String(input);
    if (path.endsWith("/storage/backend")) return new Response(JSON.stringify({ backend: "inmemory", available_backends: ["inmemory"], health: { backend: "inmemory", status: "healthy", checked_at: "now" } }), { status: 200 });
    if (path.includes("/sessions/") && path.endsWith("/events")) return new Response(JSON.stringify({ items: [] }), { status: 200 });
    if (path.includes("/sessions/")) return new Response(JSON.stringify({ error: { code: "session_not_found", message: "missing" } }), { status: 404 });
    if (path.includes("/memory/")) return new Response(JSON.stringify({ items: [] }), { status: 200 });
    if (path.endsWith("/migrations") && init?.method === "POST") {
      expect(JSON.parse(String(init.body))).toEqual({ dry_run: true, batch_size: 100, cutover: false });
      return new Response(JSON.stringify({ id: "migration-1", status: "completed", dry_run: true, sessions: 0, processed_sessions: 0, source_count: 0, destination_count: 0, matched: true }), { status: 202 });
    }
    throw new Error(`unexpected request ${path}`);
  });
  render(<DataPage identity={{ id: "admin", name: "Admin", active_tenant_id: "tenant-a", active_role: "platform_admin", assignments: [] }} />);
  expect(await screen.findByText("healthy")).toBeInTheDocument();
  fireEvent.click(screen.getByRole("button", { name: /启动迁移/ }));
  await waitFor(() => expect(screen.getByText(/迁移 completed/)).toBeInTheDocument());
  expect(screen.getByText(/校验一致/)).toBeInTheDocument();
  fetchMock.mockRestore();
});

test("ignores a stale session response", async () => {
  let resolveOld: ((value: Response) => void) | undefined;
  const oldResponse = new Promise<Response>((resolve) => { resolveOld = resolve; });
  const fetchMock = vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const path = String(input);
    if (path.endsWith("/storage/backend")) return new Response(JSON.stringify({ backend: "inmemory", available_backends: ["inmemory"], health: { backend: "inmemory", status: "healthy", checked_at: "now" } }), { status: 200 });
    if (path.endsWith("/sessions/old")) return oldResponse;
    if (path.endsWith("/sessions/new")) return new Response(JSON.stringify({ id: "new", tenant_id: "tenant-a", sequence: 2, summary: "new summary", event_count: 2, updated_at: "now" }), { status: 200 });
    if (path.includes("/sessions/") && !path.endsWith("/events")) return new Response(JSON.stringify({ error: { code: "session_not_found", message: "missing" } }), { status: 404 });
    return new Response(JSON.stringify({ items: [] }), { status: 200 });
  });
  render(<DataPage identity={{ id: "admin", name: "Admin", active_tenant_id: "tenant-a", active_role: "platform_admin", assignments: [] }} />);
  await screen.findByText("healthy");
  const input = screen.getByLabelText("Session ID");
  fireEvent.change(input, { target: { value: "old" } });
  fireEvent.change(input, { target: { value: "new" } });
  expect(await screen.findByText("new summary")).toBeInTheDocument();
  resolveOld?.(new Response(JSON.stringify({ id: "old", tenant_id: "tenant-a", sequence: 1, summary: "old summary", event_count: 1, updated_at: "now" }), { status: 200 }));
  await waitFor(() => expect(screen.queryByText("old summary")).not.toBeInTheDocument());
  fetchMock.mockRestore();
});

test("keeps backend health visible when tenant data reads fail", async () => {
  const fetchMock = vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const path = String(input);
    if (path.endsWith("/storage/backend")) {
      return new Response(JSON.stringify({ backend: "redis", available_backends: ["redis"], health: { backend: "redis", status: "unavailable", checked_at: "now" } }), { status: 200 });
    }
    return new Response(JSON.stringify({ error: { code: "storage_error", message: "storage unavailable" } }), { status: 503 });
  });
  render(<DataPage identity={{ id: "admin", name: "Admin", active_tenant_id: "tenant-a", active_role: "platform_admin", assignments: [] }} />);
  expect(await screen.findByText("unavailable")).toBeInTheDocument();
  expect(screen.getAllByText("redis").length).toBeGreaterThan(0);
  fetchMock.mockRestore();
});
