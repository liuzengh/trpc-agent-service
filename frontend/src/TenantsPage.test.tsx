import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { TenantsPage } from "./TenantsPage";
import type { Identity } from "./api";

const identity: Identity = { id: "admin", name: "Admin", active_tenant_id: "tenant-home", active_role: "platform_admin", assignments: [] };

test("creates a tenant from server-backed list", async () => {
  const fetchMock = vi.fn()
    .mockResolvedValueOnce({ ok: true, json: async () => ({ items: [] }) })
    .mockResolvedValueOnce({ ok: true, json: async () => ({ id: "tenant-east", name: "East Team", created_at: "2026-01-01T00:00:00Z" }) });
  vi.stubGlobal("fetch", fetchMock);
  render(<TenantsPage identity={identity} identityChanged={vi.fn()} />);
  await waitFor(() => expect(screen.getByText("暂无数据")).toBeInTheDocument());
  await userEvent.click(screen.getByRole("button", { name: "新建租户" }));
  await userEvent.type(screen.getByLabelText("租户标识"), "tenant-east");
  await userEvent.type(screen.getByLabelText("显示名称"), "East Team");
  await userEvent.click(screen.getByRole("button", { name: "创建租户" }));
  await waitFor(() => expect(screen.getAllByText("East Team").length).toBeGreaterThan(0));
  expect(fetchMock).toHaveBeenLastCalledWith("/api/v1/admin/tenants", expect.objectContaining({ method: "POST" }));
});
