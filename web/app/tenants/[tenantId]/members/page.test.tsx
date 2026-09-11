import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import TenantMembersPage from "./page";

const navigation = vi.hoisted(() => ({ replace: vi.fn() }));
const api = vi.hoisted(() => ({
  getTenant: vi.fn(),
  listMembers: vi.fn(),
  searchMemberCandidates: vi.fn(),
  addMember: vi.fn(),
  removeMember: vi.fn(),
}));

vi.mock("next/navigation", () => ({
  useParams: () => ({ tenantId: "tenant-1" }),
  useRouter: () => navigation,
}));

vi.mock("../../../../lib/control-api", async () => {
  const actual = await vi.importActual<typeof import("../../../../lib/control-api")>("../../../../lib/control-api");
  return { ...actual, controlApi: api };
});

const tenant = {
  id: "tenant-1",
  slug: "team-a",
  name: "Team A",
  status: "ACTIVE",
  role: "OWNER",
  created_at: "2026-09-03T00:00:00Z",
  updated_at: "2026-09-03T00:00:00Z",
};

beforeEach(() => {
  vi.clearAllMocks();
  api.getTenant.mockResolvedValue(tenant);
  api.listMembers.mockResolvedValue({
    members: [{ id: "membership-1", user_id: "owner-1", role: "OWNER", created_by: "bootstrap", created_at: "2026-09-03T00:00:00Z" }],
  });
  api.searchMemberCandidates.mockResolvedValue({
    candidates: [{ user_id: "user-2", username: "alice", display_name: "Alice" }],
    offset: 0,
    limit: 10,
    total: 1,
  });
  api.addMember.mockResolvedValue({
    id: "membership-2", user_id: "user-2", role: "MEMBER", created_by: "owner-1", created_at: "2026-09-03T00:00:00Z",
  });
});

afterEach(() => cleanup());

describe("OWNER-only Tenant members page", () => {
  it("redirects a MEMBER before requesting or rendering the member list", async () => {
    api.getTenant.mockResolvedValue({ ...tenant, role: "MEMBER" });

    render(<TenantMembersPage />);

    await waitFor(() => expect(navigation.replace).toHaveBeenCalledWith(
      "/tenants/tenant-1/agents?notice=owner-only",
    ));
    expect(api.listMembers).not.toHaveBeenCalled();
    expect(screen.queryByRole("button", { name: "添加成员" })).not.toBeInTheDocument();
    expect(screen.queryByText("owner-1")).not.toBeInTheDocument();
  });

  it("lets an OWNER search real candidates and add the selected user id", async () => {
    const user = userEvent.setup();
    render(<TenantMembersPage />);

    expect(await screen.findByText("owner-1")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "添加成员" }));
    await user.type(screen.getByRole("textbox", { name: "搜索账号" }), " ali ");
    await user.click(screen.getByRole("button", { name: "搜索" }));

    await waitFor(() => expect(api.searchMemberCandidates).toHaveBeenCalledWith(
      "tenant-1",
      { query: "ali", offset: 0, limit: 10 },
    ));
    expect(await screen.findByText("Alice")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "添加" }));
    await waitFor(() => expect(api.addMember).toHaveBeenCalledWith("tenant-1", "user-2"));
  });
});
