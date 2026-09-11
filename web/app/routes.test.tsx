import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import AdminOverview from "./admin/page";
import OperatorsPage from "./admin/operators/page";
import AdminTenantsPage from "./admin/tenants/page";
import UsersPage from "./admin/users/page";
import TenantRootPage from "./tenants/[tenantId]/page";
import TenantMembersPage from "./tenants/[tenantId]/members/page";
import AgentVersionPage from "./tenants/[tenantId]/agents/[agentId]/versions/[versionNumber]/page";
import NewAgentPage from "./tenants/[tenantId]/agents/new/page";
import TenantAgentsPage from "./tenants/[tenantId]/agents/page";
import MyTenantsPage from "./tenants/page";

const navigation = vi.hoisted(() => ({ redirect: vi.fn(), replace: vi.fn(), refresh: vi.fn() }));

const api = vi.hoisted(() => ({
  listUsers: vi.fn().mockResolvedValue({
    users: [{ id: "usr-1", username: "alice", display_name: "Alice", status: "ACTIVE", created_at: "2026-09-01T00:00:00Z" }],
    total: 1, offset: 0, limit: 20,
  }),
  listOperators: vi.fn().mockResolvedValue({
    operators: [{ user_id: "usr-1", username: "alice", display_name: "Alice", granted_by: "bootstrap", granted_at: "2026-09-01T00:00:00Z" }],
  }),
  listAdminTenants: vi.fn().mockResolvedValue({
    tenants: [{ id: "tenant-1", slug: "team-a", name: "Team A", status: "ACTIVE", created_at: "2026-09-01T00:00:00Z", updated_at: "2026-09-01T00:00:00Z" }],
    total: 1, offset: 0, limit: 20,
  }),
  listMyTenants: vi.fn().mockResolvedValue({
    tenants: [{ id: "tenant-1", slug: "team-a", name: "Team A", status: "ACTIVE", role: "OWNER", created_at: "2026-09-01T00:00:00Z", updated_at: "2026-09-01T00:00:00Z" }],
  }),
  getTenant: vi.fn().mockResolvedValue({
    id: "tenant-1", slug: "team-a", name: "Team A", status: "ACTIVE", role: "OWNER", created_at: "2026-09-01T00:00:00Z", updated_at: "2026-09-01T00:00:00Z",
  }),
  listMembers: vi.fn().mockResolvedValue({
    members: [{ id: "membership-1", user_id: "usr-1", role: "OWNER", created_by: "bootstrap", created_at: "2026-09-01T00:00:00Z" }],
  }),
  listAgents: vi.fn().mockResolvedValue({
    agents: [{ id: "agent-1", tenant_id: "tenant-1", name: "Research Agent", description: "Researches topics", latest_version_number: 1, created_by: "usr-1", created_at: "2026-09-01T00:00:00Z", updated_at: "2026-09-01T01:00:00Z" }],
    total: 1, offset: 0, limit: 20,
  }),
  getAgent: vi.fn().mockResolvedValue({
    id: "agent-1", tenant_id: "tenant-1", name: "Research Agent", description: "Researches topics", latest_version_number: 1, created_by: "usr-1", created_at: "2026-09-01T00:00:00Z", updated_at: "2026-09-01T01:00:00Z",
  }),
  getAgentVersion: vi.fn().mockResolvedValue({
    id: "version-1", tenant_id: "tenant-1", agent_id: "agent-1", version_number: 1, source_draft_revision: 2, schema_version: "v1", spec: { schema_version: "v1", root: "assistant", requirements: { models: { primary: { capabilities: ["chat"] } }, tools: {}, knowledge: {} }, nodes: { assistant: { kind: "llm", instruction: "Answer", model_slot: "primary", tool_slots: [], knowledge_slots: [] } } }, spec_digest: `sha256:${"a".repeat(64)}`, published_by: "usr-1", published_at: "2026-09-01T01:00:00Z",
  }),
  createUser: vi.fn(), grantOperator: vi.fn(), revokeOperator: vi.fn(),
  createTenant: vi.fn(), addMember: vi.fn(), removeMember: vi.fn(), createAgent: vi.fn(), searchMemberCandidates: vi.fn(),
}));

vi.mock("../lib/control-api", () => ({ controlApi: api, ControlApiError: class extends Error {} }));
vi.mock("next/navigation", () => ({
  useParams: () => ({ tenantId: "tenant-1", agentId: "agent-1", versionNumber: "1" }),
  usePathname: () => "/admin",
  useRouter: () => navigation,
  redirect: navigation.redirect,
}));

afterEach(() => cleanup());

describe("current Control API route pages", () => {
  it.each([
    ["overview", <AdminOverview />, "管理入口"],
    ["users", <UsersPage />, "alice"],
    ["operators", <OperatorsPage />, "OPERATOR"],
    ["admin tenants", <AdminTenantsPage />, "team-a"],
    ["my tenants", <MyTenantsPage />, "Team A"],
    ["tenant members", <TenantMembersPage />, "usr-1"],
    ["tenant agents", <TenantAgentsPage />, "Research Agent"],
    ["new agent", <NewAgentPage />, "后端会原子创建 Agent 和 revision 1 的空 Draft；随后在画布中初始化 AgentSpec。"],
    ["immutable agent version", <AgentVersionPage />, "Canonical AgentSpec"],
  ])("renders %s from its API response", async (_name, page, expected) => {
    render(page);
    expect(await screen.findByText(expected)).toBeInTheDocument();
  });

  it("redirects a tenant root to its Agent workflow", async () => {
    await TenantRootPage({ params: Promise.resolve({ tenantId: "tenant/a" }) });
    expect(navigation.redirect).toHaveBeenCalledWith("/tenants/tenant%2Fa/agents");
  });
});
