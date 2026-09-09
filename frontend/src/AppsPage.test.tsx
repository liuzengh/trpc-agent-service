import { render, screen, waitFor } from "@testing-library/react";
import { AppsPage } from "./AppsPage";
import type { Identity } from "./api";

function deferredResponse() {
  let resolve!: (value: { ok: boolean; json: () => Promise<unknown> }) => void;
  const promise = new Promise<{ ok: boolean; json: () => Promise<unknown> }>((done) => { resolve = done; });
  return { promise, resolve };
}

test("discards a stale list response after Tenant switching", async () => {
  const oldTenant = deferredResponse();
  const newTenant = deferredResponse();
  vi.stubGlobal("fetch", vi.fn().mockReturnValueOnce(oldTenant.promise).mockReturnValueOnce(newTenant.promise));
  const identity = (tenant: string): Identity => ({ id: "admin", name: "Admin", active_tenant_id: tenant, active_role: "tenant_admin", assignments: [] });
  const view = render(<AppsPage identity={identity("tenant-old")} />);
  view.rerender(<AppsPage identity={identity("tenant-new")} />);
  newTenant.resolve({ ok: true, json: async () => ({ items: [{ id: "app-new", tenant_id: "tenant-new", name: "New App", created_at: "2026-01-01T00:00:00Z" }] }) });
  await waitFor(() => expect(screen.getByText("New App")).toBeInTheDocument());
  oldTenant.resolve({ ok: true, json: async () => ({ items: [{ id: "app-old", tenant_id: "tenant-old", name: "Old App", created_at: "2026-01-01T00:00:00Z" }] }) });
  await Promise.resolve();
  expect(screen.queryByText("Old App")).not.toBeInTheDocument();
});
