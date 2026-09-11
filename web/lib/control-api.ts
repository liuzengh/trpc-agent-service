import type { AgentSpecV1 } from "./agent-spec-v1";

export type User = {
  id: string;
  username: string;
  display_name: string;
  status?: "ACTIVE" | "DISABLED";
  created_at?: string;
  updated_at?: string;
};

export type Session = {
  user: User;
  password_change_required: boolean;
};

export type Operator = {
  user_id: string;
  username?: string;
  display_name?: string;
  granted_by?: string;
  granted_at: string;
};

export type Tenant = {
  id: string;
  slug: string;
  name: string;
  status: "ACTIVE" | "SUSPENDED";
  role?: "OWNER" | "MEMBER";
  owner_user_id?: string;
  created_at: string;
  updated_at: string;
};

export type Membership = {
  id: string;
  user_id: string;
  role: "OWNER" | "MEMBER";
  created_by: string;
  created_at: string;
};

export type MemberCandidate = {
  user_id: string;
  username: string;
  display_name: string;
};

export type MemberCandidatePage = {
  candidates: MemberCandidate[];
  offset: number;
  limit: number;
  total: number;
};

export type Agent = {
  id: string;
  tenant_id: string;
  name: string;
  description: string;
  latest_version_number: number | null;
  created_by: string;
  created_at: string;
  updated_at: string;
};

/**
 * Drafts are persisted before they are necessarily publication-valid. The
 * initial Draft returned by the API is `{}`, so callers must narrow it before
 * rendering a complete AgentSpec V1 document.
 */
export type AgentSpecDocument = AgentSpecV1 | Record<string, unknown>;

export type AgentDraft = {
  agent_id: string;
  tenant_id: string;
  revision: number;
  spec: AgentSpecDocument;
  updated_by: string;
  updated_at: string;
};

export type AgentVersion = {
  id: string;
  tenant_id: string;
  agent_id: string;
  version_number: number;
  source_draft_revision: number;
  schema_version: "v1";
  spec: AgentSpecV1;
  spec_digest: string;
  published_by: string;
  published_at: string;
};

export type AgentPage = {
  agents: Agent[];
  offset: number;
  limit: number;
  total: number;
};

export type AgentVersionPage = {
  versions: AgentVersion[];
  offset: number;
  limit: number;
  total: number;
};

export type ValidationDiagnostic = {
  code: string;
  severity: "error" | "warning";
  pointer: string;
  node_id: string | null;
  message: string;
};

export type ValidationReport = {
  valid: boolean;
  schema_version: string;
  draft_revision: number;
  diagnostics: ValidationDiagnostic[];
};

export type CreateAgentResponse = {
  agent: Agent;
  draft: AgentDraft;
};

export type PublishAgentVersionResponse = {
  version: AgentVersion;
  validation: ValidationReport;
};

export type RunSummary = {
  run_id: string; session_id: string; status: string; stage: string;
  wait_reason?: string; failure_reason?: string; attempts: number;
  usage_status: "UNAVAILABLE" | "PARTIAL" | "COMPLETE";
  input_tokens: number; output_tokens: number; total_tokens: number;
  memory_status?: string; reply_status: string; accepted_at: string;
  execution_deadline?: string;
};
export type RunAttempt = {
  attempt_id: string; worker_id: string; generation: number; status: string;
  reason?: string; created_at: string; started_at?: string; ended_at?: string;
};
export type TimelineEvent = {
  source: string; category: string; status: string; reason?: string;
  occurred_at: string; attributes?: Record<string, unknown>;
};
export type RunDetail = RunSummary & {
  admission_id: string; manifest_ref?: string; manifest_digest?: string;
  session_head?: string; attempt_log: RunAttempt[]; timeline: TimelineEvent[];
  coverage: string[];
};
export type AuditEvent = {
  event_id: string; source: string; category: string; action: string;
  outcome: string; actor_id?: string; resource_type: string; resource_id: string;
  reason?: string; occurred_at: string; attributes?: Record<string, unknown>;
};
export type UsagePolicy = {
  schema_version: 1; tenant_id: string; revision: number; enabled: boolean;
  im: { allow_all: boolean; rules: Array<{ account_id: string; binding_id?: string; user_ids: string[]; group_ids: string[] }> };
  requests: { tenant_per_minute: number; user_per_minute: number };
  execution: { max_concurrent_runs: number };
  tokens: { period_seconds: number; limit: number; reservation_per_run: number; input_micros_per_million_tokens: number; output_micros_per_million_tokens: number };
};
export type UsageSummary = {
  tenant_id: string; policy_revision: number; period_start?: string;
  period_seconds: number; token_limit: number; used_tokens: number;
  reserved_tokens: number; unknown_usage_count: number;
  pending_usage_count: number; estimated_cost_micros: number;
};
export type ToolApproval = {
  operation_id: string; tenant_id: string; run_id: string; attempt_id: string;
  node_id: string; tool_name: string; tool_resource: string; capability: string;
  target: string; parameter_summary: string; arguments_digest: string; status: string;
  requested_at: string; expires_at: string; decided_by?: string; decided_at?: string;
  decision_reason?: string; execution_started_at?: string; execution_finished_at?: string;
  result_summary?: string; result_digest?: string;
};

export class ControlApiError extends Error {
  constructor(
    public readonly status: number,
    public readonly code: string,
    message: string,
    public readonly validation?: ValidationReport,
  ) {
    super(message);
    this.name = "ControlApiError";
  }
}

const base = "/api/control";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${base}${path}`, init ? {
    ...init,
    credentials: "include",
  } : { credentials: "include" });

  if (!response.ok) {
    const body = await response.json().catch(() => null) as {
      error?: { code?: string; message?: string };
      validation?: ValidationReport;
    } | null;
    throw new ControlApiError(
      response.status,
      body?.error?.code ?? "HTTP_ERROR",
      body?.error?.message ?? `Control API returned HTTP ${response.status}`,
      body?.validation,
    );
  }

  if (response.status === 204) return undefined as T;
  return response.json() as Promise<T>;
}

function json(method: string, body: unknown): RequestInit {
  return {
    method,
    credentials: "include",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  };
}

export const controlApi = {
  login(input: { username: string; password: string }) {
    return request<Session>("/v1/auth/login", json("POST", input));
  },
  getMe() {
    return request<Session>("/v1/me");
  },
  logout() {
    return request<void>("/v1/auth/logout", { method: "POST" });
  },
  changePassword(input: { current_password: string; new_password: string }) {
    return request<void>("/v1/me/change-password", json("POST", input));
  },
  getCapabilities() {
    return request<{ capabilities: string[] }>("/v1/admin/capabilities");
  },
  listOperators() {
    return request<{ operators: Operator[] }>("/v1/admin/operators");
  },
  grantOperator(userId: string) {
    return request<Operator>("/v1/admin/operators", json("POST", { user_id: userId }));
  },
  revokeOperator(userId: string) {
    return request<void>(`/v1/admin/operators/${encodeURIComponent(userId)}`, { method: "DELETE" });
  },
  listUsers(page: { offset: number; limit: number }) {
    const query = new URLSearchParams({ offset: String(page.offset), limit: String(page.limit) });
    return request<{ users: User[]; offset: number; limit: number; total: number }>(
      `/v1/admin/users?${query}`,
    );
  },
  createUser(input: { username: string; display_name: string; temporary_password: string }) {
    return request<User>("/v1/admin/users", json("POST", input));
  },
  listAdminTenants(page: { offset: number; limit: number }) {
    const query = new URLSearchParams({ offset: String(page.offset), limit: String(page.limit) });
    return request<{ tenants: Tenant[]; offset: number; limit: number; total: number }>(
      `/v1/admin/tenants?${query}`,
    );
  },
  createTenant(input: { slug: string; name: string; owner_user_id: string }) {
    return request<Tenant>("/v1/admin/tenants", json("POST", input));
  },
  listMyTenants() {
    return request<{ tenants: Tenant[] }>("/v1/me/tenants");
  },
  getTenant(tenantId: string) {
    return request<Tenant>(`/v1/tenants/${encodeURIComponent(tenantId)}`);
  },
  listMembers(tenantId: string) {
    return request<{ members: Membership[] }>(
      `/v1/tenants/${encodeURIComponent(tenantId)}/members`,
    );
  },
  searchMemberCandidates(
    tenantId: string,
    page: { query: string; offset: number; limit: number },
  ) {
    const query = new URLSearchParams({
      query: page.query.trim(),
      offset: String(page.offset),
      limit: String(page.limit),
    });
    return request<MemberCandidatePage>(
      `/v1/tenants/${encodeURIComponent(tenantId)}/member-candidates?${query}`,
    );
  },
  addMember(tenantId: string, userId: string) {
    return request<Membership>(
      `/v1/tenants/${encodeURIComponent(tenantId)}/members`,
      json("POST", { user_id: userId }),
    );
  },
  removeMember(tenantId: string, userId: string) {
    return request<void>(
      `/v1/tenants/${encodeURIComponent(tenantId)}/members/${encodeURIComponent(userId)}`,
      { method: "DELETE" },
    );
  },
  createAgent(tenantId: string, input: { name: string; description?: string }) {
    return request<CreateAgentResponse>(
      `/v1/tenants/${encodeURIComponent(tenantId)}/agents`,
      json("POST", input),
    );
  },
  listAgents(tenantId: string, page: { offset: number; limit: number }) {
    const query = new URLSearchParams({ offset: String(page.offset), limit: String(page.limit) });
    return request<AgentPage>(
      `/v1/tenants/${encodeURIComponent(tenantId)}/agents?${query}`,
    );
  },
  getAgent(tenantId: string, agentId: string) {
    return request<Agent>(
      `/v1/tenants/${encodeURIComponent(tenantId)}/agents/${encodeURIComponent(agentId)}`,
    );
  },
  updateAgent(
    tenantId: string,
    agentId: string,
    input: { name?: string; description?: string },
  ) {
    return request<Agent>(
      `/v1/tenants/${encodeURIComponent(tenantId)}/agents/${encodeURIComponent(agentId)}`,
      json("PATCH", input),
    );
  },
  getAgentDraft(tenantId: string, agentId: string) {
    return request<AgentDraft>(
      `/v1/tenants/${encodeURIComponent(tenantId)}/agents/${encodeURIComponent(agentId)}/draft`,
    );
  },
  saveAgentDraft(
    tenantId: string,
    agentId: string,
    input: { expected_revision: number; spec: AgentSpecDocument },
  ) {
    return request<AgentDraft>(
      `/v1/tenants/${encodeURIComponent(tenantId)}/agents/${encodeURIComponent(agentId)}/draft`,
      json("PUT", input),
    );
  },
  validateAgentDraft(
    tenantId: string,
    agentId: string,
    input: { expected_revision: number },
  ) {
    return request<ValidationReport>(
      `/v1/tenants/${encodeURIComponent(tenantId)}/agents/${encodeURIComponent(agentId)}/draft/validate`,
      json("POST", input),
    );
  },
  publishAgentVersion(
    tenantId: string,
    agentId: string,
    input: { expected_revision: number },
  ) {
    return request<PublishAgentVersionResponse>(
      `/v1/tenants/${encodeURIComponent(tenantId)}/agents/${encodeURIComponent(agentId)}/versions`,
      json("POST", input),
    );
  },
  listAgentVersions(
    tenantId: string,
    agentId: string,
    page: { offset: number; limit: number },
  ) {
    const query = new URLSearchParams({ offset: String(page.offset), limit: String(page.limit) });
    return request<AgentVersionPage>(
      `/v1/tenants/${encodeURIComponent(tenantId)}/agents/${encodeURIComponent(agentId)}/versions?${query}`,
    );
  },
  getAgentVersion(tenantId: string, agentId: string, versionNumber: number) {
    return request<AgentVersion>(
      `/v1/tenants/${encodeURIComponent(tenantId)}/agents/${encodeURIComponent(agentId)}/versions/${encodeURIComponent(String(versionNumber))}`,
    );
  },
  listRuns(tenantId: string, page: { offset: number; limit: number }) {
    const query = new URLSearchParams({ offset: String(page.offset), limit: String(page.limit) });
    return request<{ runs: RunSummary[]; offset: number; limit: number; total: number }>(
      `/v1/tenants/${encodeURIComponent(tenantId)}/runs?${query}`,
    );
  },
  getRun(tenantId: string, runId: string) {
    return request<RunDetail>(
      `/v1/tenants/${encodeURIComponent(tenantId)}/runs/${encodeURIComponent(runId)}`,
    );
  },
  listAuditEvents(tenantId: string, page: { offset: number; limit: number }) {
    const query = new URLSearchParams({ offset: String(page.offset), limit: String(page.limit) });
    return request<{ events: AuditEvent[]; offset: number; limit: number; total: number }>(
      `/v1/tenants/${encodeURIComponent(tenantId)}/audit-events?${query}`,
    );
  },
  getUsagePolicy(tenantId: string) {
    return request<UsagePolicy>(`/v1/tenants/${encodeURIComponent(tenantId)}/usage-policy`);
  },
  replaceUsagePolicy(tenantId: string, expectedRevision: number, policy: UsagePolicy, idempotencyKey: string) {
    return request<UsagePolicy>(`/v1/tenants/${encodeURIComponent(tenantId)}/usage-policy`, {
      ...json("PUT", { expected_revision: expectedRevision, policy }),
      headers: { "Content-Type": "application/json", "Idempotency-Key": idempotencyKey },
    });
  },
  getUsageSummary(tenantId: string) {
    return request<UsageSummary>(`/v1/tenants/${encodeURIComponent(tenantId)}/usage-summary`);
  },
  listToolApprovals(tenantId: string, page: { offset: number; limit: number }) {
    const query = new URLSearchParams({ offset: String(page.offset), limit: String(page.limit) });
    return request<{ operations: ToolApproval[]; offset: number; limit: number; total: number }>(
      `/v1/tenants/${encodeURIComponent(tenantId)}/tool-approvals?${query}`,
    );
  },
  decideToolApproval(tenantId: string, operationId: string, input: { action: "approve" | "reject"; reason?: string; expected_arguments_digest: string }) {
    return request<{ operation: ToolApproval; outcome: string }>(
      `/v1/tenants/${encodeURIComponent(tenantId)}/tool-approvals/${encodeURIComponent(operationId)}/decision`, json("POST", input),
    );
  },
};
