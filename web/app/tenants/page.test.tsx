import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import MyTenantsPage from "./page";

const api = vi.hoisted(() => ({ listMyTenants: vi.fn() }));

vi.mock("../../lib/control-api", () => ({ controlApi: api }));

afterEach(() => cleanup());

describe("Tenant chooser", () => {
  it("opens Agent workflows for every role and exposes member management only to OWNER", async () => {
    api.listMyTenants.mockResolvedValue({ tenants: [
      { id: "owner-tenant", slug: "owner-team", name: "Owner Team", status: "ACTIVE", role: "OWNER", created_at: "", updated_at: "" },
      { id: "member-tenant", slug: "member-team", name: "Member Team", status: "ACTIVE", role: "MEMBER", created_at: "", updated_at: "" },
    ] });

    render(<MyTenantsPage />);

    const agentLinks = await screen.findAllByRole("link", { name: "进入 Agent 工作台" });
    expect(agentLinks).toHaveLength(2);
    expect(agentLinks[0]).toHaveAttribute("href", "/tenants/owner-tenant/agents");
    expect(agentLinks[1]).toHaveAttribute("href", "/tenants/member-tenant/agents");
    const memberLink = screen.getByRole("link", { name: "成员管理" });
    expect(memberLink).toHaveAttribute("href", "/tenants/owner-tenant/members");
  });
});
