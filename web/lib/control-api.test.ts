import { afterEach, describe, expect, it, vi } from "vitest";

import { ControlApiError, controlApi } from "./control-api";

afterEach(() => vi.restoreAllMocks());

describe("Control API browser client", () => {
  it("logs in through the same-origin proxy and includes the session cookie", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({
        user: { id: "user-1", username: "admin", display_name: "Platform Admin" },
        password_change_required: false,
      }), { status: 200, headers: { "content-type": "application/json" } }),
    );

    await controlApi.login({ username: "admin", password: "secret" });

    expect(fetchMock).toHaveBeenCalledWith("/api/control/v1/auth/login", {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username: "admin", password: "secret" }),
    });
  });

  it("uses the implemented Admin users endpoint with pagination", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ users: [], offset: 20, limit: 20, total: 0 }), {
        status: 200,
        headers: { "content-type": "application/json" },
      }),
    );

    await controlApi.listUsers({ offset: 20, limit: 20 });

    expect(fetchMock).toHaveBeenCalledWith(
      "/api/control/v1/admin/users?offset=20&limit=20",
      { credentials: "include" },
    );
  });

  it("exposes the backend error code to the page", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({
        error: { code: "INVALID_CREDENTIALS", message: "invalid username or password" },
      }), { status: 401, headers: { "content-type": "application/json" } }),
    );

    await expect(controlApi.login({ username: "admin", password: "bad" })).rejects.toMatchObject({
      status: 401,
      code: "INVALID_CREDENTIALS",
    });
  });

  it("maps every currently implemented authenticated operation", async () => {
    const jsonResponse = (body: unknown) => new Response(JSON.stringify(body), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
    const fetchMock = vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(jsonResponse({ user: { id: "u", username: "a", display_name: "A" }, password_change_required: false }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }))
      .mockResolvedValueOnce(jsonResponse({ capabilities: [] }))
      .mockResolvedValueOnce(jsonResponse({ operators: [] }))
      .mockResolvedValueOnce(jsonResponse({ user_id: "u", granted_at: "2026-09-01T00:00:00Z" }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }))
      .mockResolvedValueOnce(jsonResponse({ id: "u", username: "a", display_name: "A" }))
      .mockResolvedValueOnce(jsonResponse({ tenants: [], offset: 0, limit: 20, total: 0 }))
      .mockResolvedValueOnce(jsonResponse({ id: "t", slug: "team", name: "Team", status: "ACTIVE", created_at: "", updated_at: "" }))
      .mockResolvedValueOnce(jsonResponse({ tenants: [] }))
      .mockResolvedValueOnce(jsonResponse({ id: "t", slug: "team", name: "Team", status: "ACTIVE", created_at: "", updated_at: "" }))
      .mockResolvedValueOnce(jsonResponse({ members: [] }))
      .mockResolvedValueOnce(jsonResponse({ id: "m", user_id: "u", role: "MEMBER", created_by: "owner", created_at: "" }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }));

    await controlApi.getMe();
    await controlApi.logout();
    await controlApi.changePassword({ current_password: "old", new_password: "new" });
    await controlApi.getCapabilities();
    await controlApi.listOperators();
    await controlApi.grantOperator("u");
    await controlApi.revokeOperator("u");
    await controlApi.createUser({ username: "a", display_name: "A", temporary_password: "secret" });
    await controlApi.listAdminTenants({ offset: 0, limit: 20 });
    await controlApi.createTenant({ slug: "team", name: "Team", owner_user_id: "u" });
    await controlApi.listMyTenants();
    await controlApi.getTenant("t");
    await controlApi.listMembers("t");
    await controlApi.addMember("t", "u");
    await controlApi.removeMember("t", "u");

    expect(fetchMock.mock.calls.map(([url, init]) => [url, init?.method ?? "GET"])).toEqual([
      ["/api/control/v1/me", "GET"],
      ["/api/control/v1/auth/logout", "POST"],
      ["/api/control/v1/me/change-password", "POST"],
      ["/api/control/v1/admin/capabilities", "GET"],
      ["/api/control/v1/admin/operators", "GET"],
      ["/api/control/v1/admin/operators", "POST"],
      ["/api/control/v1/admin/operators/u", "DELETE"],
      ["/api/control/v1/admin/users", "POST"],
      ["/api/control/v1/admin/tenants?offset=0&limit=20", "GET"],
      ["/api/control/v1/admin/tenants", "POST"],
      ["/api/control/v1/me/tenants", "GET"],
      ["/api/control/v1/tenants/t", "GET"],
      ["/api/control/v1/tenants/t/members", "GET"],
      ["/api/control/v1/tenants/t/members", "POST"],
      ["/api/control/v1/tenants/t/members/u", "DELETE"],
    ]);
  });

  it("maps all Agent V1 operations through the same-origin proxy", async () => {
    const jsonResponse = (body: unknown, status = 200) => new Response(JSON.stringify(body), {
      status,
      headers: { "content-type": "application/json" },
    });
    const agent = {
      id: "agent b",
      tenant_id: "tenant/a",
      name: "Research agent",
      description: "Searches primary sources",
      latest_version_number: null,
      created_by: "user-1",
      created_at: "2026-09-03T00:00:00Z",
      updated_at: "2026-09-03T00:00:00Z",
    };
    const draft = {
      agent_id: agent.id,
      tenant_id: agent.tenant_id,
      revision: 1,
      spec: {},
      updated_by: "user-1",
      updated_at: "2026-09-03T00:00:00Z",
    };
    const validation = {
      valid: true,
      schema_version: "v1",
      draft_revision: 1,
      diagnostics: [],
    };
    const version = {
      id: "version-1",
      tenant_id: agent.tenant_id,
      agent_id: agent.id,
      version_number: 7,
      source_draft_revision: 1,
      schema_version: "v1",
      spec: {
        schema_version: "v1",
        root: "answer",
        requirements: { models: {}, tools: {}, knowledge: {} },
        nodes: {},
      },
      spec_digest: `sha256:${"a".repeat(64)}`,
      published_by: "user-1",
      published_at: "2026-09-03T00:00:00Z",
    };
    const fetchMock = vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(jsonResponse({ agent, draft }, 201))
      .mockResolvedValueOnce(jsonResponse({ agents: [agent], offset: 20, limit: 10, total: 1 }))
      .mockResolvedValueOnce(jsonResponse(agent))
      .mockResolvedValueOnce(jsonResponse({ ...agent, name: "Updated agent" }))
      .mockResolvedValueOnce(jsonResponse(draft))
      .mockResolvedValueOnce(jsonResponse({ ...draft, revision: 2 }))
      .mockResolvedValueOnce(jsonResponse(validation))
      .mockResolvedValueOnce(jsonResponse({ version, validation }, 201))
      .mockResolvedValueOnce(jsonResponse({ versions: [version], offset: 0, limit: 25, total: 1 }))
      .mockResolvedValueOnce(jsonResponse(version));

    const tenantId = "tenant/a";
    const agentId = "agent b";
    await controlApi.createAgent(tenantId, {
      name: "Research agent",
      description: "Searches primary sources",
    });
    await controlApi.listAgents(tenantId, { offset: 20, limit: 10 });
    await controlApi.getAgent(tenantId, agentId);
    await controlApi.updateAgent(tenantId, agentId, { name: "Updated agent" });
    await controlApi.getAgentDraft(tenantId, agentId);
    await controlApi.saveAgentDraft(tenantId, agentId, { expected_revision: 1, spec: {} });
    await controlApi.validateAgentDraft(tenantId, agentId, { expected_revision: 2 });
    await controlApi.publishAgentVersion(tenantId, agentId, { expected_revision: 2 });
    await controlApi.listAgentVersions(tenantId, agentId, { offset: 0, limit: 25 });
    await controlApi.getAgentVersion(tenantId, agentId, 7);

    const agentBase = "/api/control/v1/tenants/tenant%2Fa/agents";
    expect(fetchMock.mock.calls.map(([url, init]) => [url, init?.method ?? "GET"])).toEqual([
      [agentBase, "POST"],
      [`${agentBase}?offset=20&limit=10`, "GET"],
      [`${agentBase}/agent%20b`, "GET"],
      [`${agentBase}/agent%20b`, "PATCH"],
      [`${agentBase}/agent%20b/draft`, "GET"],
      [`${agentBase}/agent%20b/draft`, "PUT"],
      [`${agentBase}/agent%20b/draft/validate`, "POST"],
      [`${agentBase}/agent%20b/versions`, "POST"],
      [`${agentBase}/agent%20b/versions?offset=0&limit=25`, "GET"],
      [`${agentBase}/agent%20b/versions/7`, "GET"],
    ]);

    expect(fetchMock.mock.calls[0]?.[1]).toEqual({
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        name: "Research agent",
        description: "Searches primary sources",
      }),
    });
    expect(fetchMock.mock.calls[3]?.[1]?.body).toBe(JSON.stringify({ name: "Updated agent" }));
    expect(fetchMock.mock.calls[5]?.[1]?.body).toBe(JSON.stringify({
      expected_revision: 1,
      spec: {},
    }));
    expect(fetchMock.mock.calls[6]?.[1]?.body).toBe(JSON.stringify({ expected_revision: 2 }));
    expect(fetchMock.mock.calls[7]?.[1]?.body).toBe(JSON.stringify({ expected_revision: 2 }));
    expect(fetchMock.mock.calls.every(([, init]) => init?.credentials === "include")).toBe(true);
  });

  it("preserves the backend Validation Report on a 422 AgentSpec error", async () => {
    const validation = {
      valid: false,
      schema_version: "v1",
      draft_revision: 3,
      diagnostics: [{
        code: "AGENT_SPEC_ROOT_NOT_FOUND",
        severity: "error" as const,
        pointer: "/root",
        node_id: null,
        message: "root must reference an existing node",
      }],
    };
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({
        error: { code: "AGENT_SPEC_INVALID", message: "AgentSpec validation failed" },
        validation,
      }), { status: 422, headers: { "content-type": "application/json" } }),
    );

    const promise = controlApi.saveAgentDraft("tenant-1", "agent-1", {
      expected_revision: 3,
      spec: { schema_version: "v1", root: "missing" },
    });

    await expect(promise).rejects.toBeInstanceOf(ControlApiError);
    await expect(promise).rejects.toMatchObject({
      status: 422,
      code: "AGENT_SPEC_INVALID",
      message: "AgentSpec validation failed",
      validation,
    });
  });

  it("searches OWNER member candidates through the tenant-scoped endpoint", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({
        candidates: [{ user_id: "user-2", username: "alice", display_name: "Alice" }],
        offset: 10,
        limit: 10,
        total: 21,
      }), { status: 200, headers: { "content-type": "application/json" } }),
    );

    const result = await controlApi.searchMemberCandidates("tenant/a", {
      query: "  Alice Zhang  ",
      offset: 10,
      limit: 10,
    });

    expect(result.total).toBe(21);
    expect(fetchMock).toHaveBeenCalledWith(
      "/api/control/v1/tenants/tenant%2Fa/member-candidates?query=Alice+Zhang&offset=10&limit=10",
      { credentials: "include" },
    );
  });
});
