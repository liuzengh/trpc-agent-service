export type Role = "platform_admin" | "tenant_admin" | "operator" | "viewer";

export interface TenantAssignment {
  tenant_id: string;
  tenant_name: string;
  role: Role;
}

export interface Identity {
  id: string;
  name: string;
  active_tenant_id: string;
  active_role: Role;
  assignments: TenantAssignment[];
  auth_mode?: "development" | "production";
}

export interface Tenant {
  id: string;
  name: string;
  created_at: string;
}

export interface ListResponse<T> { items: T[] }
export interface AgentApp { id: string; tenant_id: string; name: string; created_at: string }
export type DeploymentStatus = "draft" | "published" | "active" | "paused";
export type DeploymentRolloutStatus = "idle" | "rolling" | "completed";
export interface Deployment { id: string; tenant_id: string; agent_app_id: string; version_id?: string; status: DeploymentStatus; desired_replicas: number; rollout_status?: DeploymentRolloutStatus; current_version_id?: string; target_version_id?: string; previous_version_id?: string; gray_percentage?: number; created_at: string }
export interface DeploymentRollbackPreview { tenant_id: string; agent_app_id: string; deployment_id: string; current_version_id: string; previous_version_id: string; active_executions: number; expected_result: string }
export interface DeploymentVersion { id: string; deployment_id: string; agent_app_id: string; number: number; config: Record<string, unknown>; created_at: string }
export type RuntimeFaultScenario = "none" | "runner_delay" | "runner_error" | "tool_error";
export interface RuntimeFaultConfiguration { scenario: RuntimeFaultScenario; delay_ms?: number }
export interface RuntimeStatus { id: string; role: "gateway" | "worker" | "dependency"; available: boolean; lifecycle: "healthy" | "degraded" | "unavailable" | "closing" | "error"; active_executions: number; completed_executions: number; failed_executions: number }
export interface DrainStatus { state: "idle" | "draining" | "closed" | "failed"; started_at?: string; completed_at?: string; error?: string; active_executions: number }
export interface BackendHealth { backend: string; status: string; message?: string; checked_at: string }
export interface SessionState { id: string; tenant_id: string; sequence: number; summary: string; summary_source_sequence?: number; event_count: number; updated_at: string }
export interface SessionEvent { id: string; tenant_id: string; session_id: string; sequence: number; idempotency_key: string; type: string; payload: string; occurred_at: string }
export interface MemoryRecord { id: string; tenant_id: string; session_id: string; key: string; value: string; updated_at: string }
export interface MigrationResult { id: string; status: string; dry_run: boolean; sessions: number; processed_sessions: number; source_count: number; destination_count: number; checksum?: string; matched?: boolean; message?: string }
export interface ChatSession { id: string; tenant_id: string; app_id: string; user_id?: string; sequence?: number }
export interface ChatEvent { id: string; tenant_id: string; session_id: string; sequence: number; idempotency_key: string; type: string; payload: string; occurred_at: string }
export interface ChatRunResponse { session_id: string; request_id: string; status: "running" | "pending" | "completed" | "failed" | "cancelled" }
export interface ChatStreamEvent {
  event_id: string;
  request_id: string;
  session_id: string;
  sequence: number;
  type: "run.started" | "message.delta" | "message.completed" | "run.failed" | "run.cancelled" | "run.completed" | string;
  data: Record<string, unknown>;
}
export interface MockFaultConfiguration {
  scenario: "none" | "timeout" | "retry" | "rate_limit" | "message_length" | "attachment";
  message_length_limit: number;
  attachment_size_limit: number;
  rate_limit: number;
  timeout_ms: number;
  retry_limit: number;
}
export type ChannelProvider = "mock" | "enterprise_wechat" | "telegram";
export interface ChannelBinding {
  id: string;
  tenant_id: string;
  app_id: string;
  channel: ChannelProvider;
  conversation_type: "single" | "group";
  external_conversation_id: string;
  external_user_id: string;
  session_id: string;
  enabled: boolean;
  created_at: string;
  secret?: string;
}
export interface ProviderStatus { provider: string; status: string; credential_smoke_status: "not_run" | "unavailable" | "passed"; last_error?: string }
export interface BotRoute { provider: "enterprise_wechat" | "telegram"; provider_account?: string; external_subject: string; tenant_id: string; app_id: string; conversation_type: "single" | "group"; enabled: boolean }
export interface ProviderDelivery { provider: BotRoute["provider"]; external_subject: string; tenant_id: string; app_id: string; request_id: string; status: "accepted" | "retried" | "rejected" | "delivered" | "terminal_failed"; code?: string; attempts: number; updated_at: string }
export interface TenantPolicy { tenant_id: string; agent_app_id: string; revision: number; allowed_tools: string[]; allowed_mcp: string[]; dangerous_tools: string[]; denied_input_patterns: string[]; denied_output_patterns: string[]; redacted_patterns: string[]; allowed_im_users: string[]; allowed_im_subjects: string[]; allowed_provider_accounts: string[]; allowed_conversation_types: string[]; token_budget: number; cost_budget: number; cost_per_token: number; tool_costs: Record<string, number>; estimated_tokens_per_run: number; rate_limit: number; rate_window_seconds: number; runtime_timeout_ms?: number; updated_at?: string }
export interface AuditEvent { id: string; tenant_id: string; channel?: string; user_id?: string; session_id?: string; agent_name?: string; tool_name?: string; decision: string; latency: number; error_type?: string; cost: number; trace_id: string; request_id?: string; occurred_at: string; policy_revision?: number; checkpoint?: string; rule?: string; reason?: string }
export interface AuditFilters {
  from?: string;
  to?: string;
  channel?: string;
  user_id?: string;
  session_id?: string;
  agent_name?: string;
  decision?: string;
  error_type?: string;
  request_id?: string;
  trace_id?: string;
}
export interface MetricsFilters { app_id?: string; provider?: ChannelProvider | ""; from?: string; to?: string }
export interface TenantMetrics { tenant_id: string; requests: number; active_executions: number; completed_executions: number; failed_executions: number; denied_requests: number; rate_limited_requests: number; tokens: number; cost: number; model_latency_ms: number; execution_latency_ms: number; tool_latency_ms: number; storage_latency_ms: number; im_delivered: number; im_failed: number; token_budget: number; tokens_remaining: number; cost_budget: number; cost_remaining: number; budget_period_from?: string }
export interface ToolConfirmation { id: string; tenant_id: string; agent_app_id: string; session_id: string; request_id: string; user_id: string; tool_name: string; argument_summary: string; policy_revision: number; trace_id: string; status: "pending" | "approved" | "rejected" | "expired" | "running" | "completed" | "failed" | "cancelled"; created_at: string; expires_at: string; decided_at?: string; decided_by?: string; invoked_at?: string; completed_at?: string }
export interface PlatformTrace { trace_id: string; tenant_id: string; request_id: string; session_id: string; agent_app_id: string; spans: { name: string; status: string; occurred_at: string }[] }
export interface CapacityTestResult {
  id: string;
  request_id: string;
  trace_id: string;
  tenant_id: string;
  agent_app_id: string;
  status: "running" | "completed" | "failed" | "cancelled";
  concurrency: number;
  runs: number;
  completed: number;
  failed: number;
  active: number;
  safe_concurrency: number;
  throughput_per_second: number;
  model_latency_ms: number;
  tool_latency_ms: number;
  storage_latency_ms: number;
  estimated_tokens: number;
  estimated_cost: number;
  first_bottleneck: string;
  sessions_per_node: number;
  recommended_worker_nodes: number;
  average_tokens_per_session: number;
  token_throughput_per_second: number;
  im_callback_peak_qps: number;
  redis_qps: number;
  sql_qps: number;
  headroom_percent: number;
  started_at: string;
  completed_at?: string;
  error?: string;
}

export class APIError extends Error {
  constructor(
    public readonly status: number,
    public readonly code: string,
    message: string,
  ) {
    super(message);
  }
}

export async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    credentials: "same-origin",
    ...init,
    headers: { "Content-Type": "application/json", ...init?.headers },
  });
  if (response.status === 204) return undefined as T;
  const body = (await response.json()) as T | { error: { code: string; message: string } };
  if (!response.ok) {
    const error = (body as { error?: { code?: string; message?: string } }).error;
    if (response.status === 401 && typeof window !== "undefined") window.dispatchEvent(new Event("trpc-auth-required"));
    throw new APIError(response.status, error?.code ?? "unknown_error", error?.message ?? "服务请求失败");
  }
  return body as T;
}

export const api = {
  identity: () => request<Identity>("/api/v1/auth/me"),
  login: (token: string) => request<Identity>("/api/v1/auth/login", { method: "POST", body: JSON.stringify({ token }) }),
  logout: () => request<void>("/api/v1/auth/logout", { method: "POST" }),
  switchTenant: (tenant_id: string) =>
    request<Identity>("/api/v1/auth/switch-tenant", {
      method: "POST",
      body: JSON.stringify({ tenant_id }),
    }),
  tenants: () => request<ListResponse<Tenant>>("/api/v1/admin/tenants"),
  tenant: (id: string) => request<Tenant>(`/api/v1/admin/tenants/${encodeURIComponent(id)}`),
  createTenant: (input: { id: string; name: string }) =>
    request<Tenant>("/api/v1/admin/tenants", { method: "POST", body: JSON.stringify(input) }),
  apps: () => request<ListResponse<AgentApp>>("/api/v1/admin/agent-apps"),
  createApp: (input: { id: string; name: string }) => request<AgentApp>("/api/v1/admin/agent-apps", { method: "POST", body: JSON.stringify(input) }),
  deployments: () => request<ListResponse<Deployment>>("/api/v1/admin/deployments"),
  createDeployment: (input: { id: string; agent_app_id: string }) => request<Deployment>("/api/v1/admin/deployments", { method: "POST", body: JSON.stringify(input) }),
  versions: (id: string) => request<ListResponse<DeploymentVersion>>(`/api/v1/admin/deployments/${encodeURIComponent(id)}/versions`),
  createVersion: (id: string, config: Record<string, unknown>, idempotencyKey: string) => request<DeploymentVersion>(`/api/v1/admin/deployments/${encodeURIComponent(id)}/versions`, { method: "POST", headers: { "Idempotency-Key": idempotencyKey }, body: JSON.stringify({ config }) }),
  transition: (id: string, status: DeploymentStatus, version_id?: string) => request<Deployment>(`/api/v1/admin/deployments/${encodeURIComponent(id)}/transition`, { method: "POST", body: JSON.stringify({ status, version_id }) }),
  rollout: (id: string, input: { target_version_id: string; gray_percentage: number; confirm: boolean }) => request<Deployment>(`/api/v1/admin/deployments/${encodeURIComponent(id)}/rollout`, { method: "POST", body: JSON.stringify(input) }),
  rollbackPreview: (id: string) => request<DeploymentRollbackPreview>(`/api/v1/admin/deployments/${encodeURIComponent(id)}/rollback-preview`),
  rollback: (id: string) => request<Deployment>(`/api/v1/admin/deployments/${encodeURIComponent(id)}/rollback`, { method: "POST", body: JSON.stringify({ confirm: true }) }),
  runtimeStatus: () => request<ListResponse<RuntimeStatus>>("/api/v1/admin/runtime/status"),
  operationsDrain: () => request<DrainStatus>("/api/v1/admin/operations/drain"),
  startOperationsDrain: () => request<DrainStatus>("/api/v1/admin/operations/drain", { method: "POST", body: JSON.stringify({ confirm: true }) }),
  runtimeFaults: () => request<{ enabled: boolean; scenarios: RuntimeFaultScenario[] }>("/api/v1/admin/operations/faults"),
  setRuntimeFaults: (agentAppID: string, scenario: RuntimeFaultScenario, delayMS = 0) => request<RuntimeFaultConfiguration>("/api/v1/admin/operations/faults", { method: "POST", body: JSON.stringify({ agent_app_id: agentAppID, scenario, delay_ms: delayMS }) }),
  startCapacity: (input: { agent_app_id: string; concurrency: number; runs: number; timeout_ms: number; peak_im_callbacks_per_second: number; average_tokens_per_session: number; redis_operations_per_session: number; sql_operations_per_session: number; headroom_percent: number }) => request<CapacityTestResult>("/api/v1/admin/capacity", { method: "POST", body: JSON.stringify(input) }),
  capacity: (id: string) => request<CapacityTestResult>(`/api/v1/admin/capacity/${encodeURIComponent(id)}`),
  cancelCapacity: (id: string) => request<CapacityTestResult>(`/api/v1/admin/capacity/${encodeURIComponent(id)}/cancel`, { method: "POST" }),
  backend: () => request<{ backend: string; health: BackendHealth; available_backends: string[] }>("/api/v1/admin/storage/backend"),
  selectBackend: (backend: string) => request<{ backend: string; health: BackendHealth }>("/api/v1/admin/storage/backend", { method: "POST", body: JSON.stringify({ backend }) }),
  session: (id: string) => request<SessionState>(`/api/v1/admin/sessions/${encodeURIComponent(id)}`),
  sessionEvents: (id: string) => request<ListResponse<SessionEvent>>(`/api/v1/admin/sessions/${encodeURIComponent(id)}/events`),
  memory: (id: string) => request<ListResponse<MemoryRecord>>(`/api/v1/admin/memory/${encodeURIComponent(id)}`),
  migrate: (input: { dry_run: boolean; batch_size: number; cutover: boolean }) => request<MigrationResult>("/api/v1/admin/migrations", { method: "POST", body: JSON.stringify(input) }),
  migration: (id: string) => request<MigrationResult>(`/api/v1/admin/migrations/${encodeURIComponent(id)}`),
  createChatSession: (app_id: string, session_id: string) =>
    request<ChatSession>("/api/v1/chat/sessions", { method: "POST", body: JSON.stringify({ app_id, session_id }) }),
  chatEvents: (id: string) => request<ListResponse<ChatEvent>>(`/api/v1/chat/sessions/${encodeURIComponent(id)}/events`),
  sendChatMessage: (id: string, input: string, requestID: string) =>
    request<ChatRunResponse>(`/api/v1/chat/sessions/${encodeURIComponent(id)}/messages`, {
      method: "POST", headers: { "X-Request-ID": requestID }, body: JSON.stringify({ input }),
    }),
  cancelChatRun: (id: string, requestID: string) =>
    request<ChatRunResponse>(`/api/v1/chat/sessions/${encodeURIComponent(id)}/cancel`, {
      method: "POST", body: JSON.stringify({ request_id: requestID }),
    }),
  mockFaults: (id: string) => request<MockFaultConfiguration>(`/api/v1/chat/mock/faults?session_id=${encodeURIComponent(id)}`),
  setMockFaults: (id: string, scenario: MockFaultConfiguration["scenario"]) =>
    request<MockFaultConfiguration>("/api/v1/chat/mock/faults", {
      method: "POST", body: JSON.stringify({ scenario, session_id: id }),
    }),
  bindings: () => request<ListResponse<ChannelBinding>>("/api/v1/chat/bindings"),
  createBinding: (input: { channel: ChannelProvider; app_id: string; conversation_type: "single" | "group"; external_conversation_id: string; external_user_id: string }) =>
    request<ChannelBinding>("/api/v1/chat/bindings", { method: "POST", body: JSON.stringify(input) }),
  setBindingEnabled: (id: string, enabled: boolean) => request<ChannelBinding>(`/api/v1/chat/bindings/${encodeURIComponent(id)}`, { method: "PATCH", body: JSON.stringify({ enabled }) }),
  deleteBinding: (id: string) => request<void>(`/api/v1/chat/bindings/${encodeURIComponent(id)}`, { method: "DELETE" }),
  providerStatuses: () => request<ListResponse<ProviderStatus>>("/api/v1/admin/providers/status"),
  providerRoutes: () => request<ListResponse<BotRoute>>("/api/v1/admin/providers/routes"),
  providerDeliveries: () => request<ListResponse<ProviderDelivery>>("/api/v1/admin/providers/deliveries"),
  createProviderRoute: (route: BotRoute) => request<BotRoute>("/api/v1/admin/providers/routes", { method: "POST", body: JSON.stringify(route) }),
  updateProviderRoute: (route: BotRoute) => request<BotRoute>(`/api/v1/admin/providers/routes?provider=${encodeURIComponent(route.provider)}&provider_account=${encodeURIComponent(route.provider_account ?? "")}&external_subject=${encodeURIComponent(route.external_subject)}`, { method: "PATCH", body: JSON.stringify({ tenant_id: route.tenant_id, app_id: route.app_id, provider_account: route.provider_account, conversation_type: route.conversation_type, enabled: route.enabled }) }),
  deleteProviderRoute: (route: BotRoute) => request<void>(`/api/v1/admin/providers/routes?provider=${encodeURIComponent(route.provider)}&provider_account=${encodeURIComponent(route.provider_account ?? "")}&external_subject=${encodeURIComponent(route.external_subject)}`, { method: "DELETE" }),
  replayProviderRoute: (route: BotRoute, text: string) => request<ChatRunResponse>("/api/v1/admin/providers/replay", { method: "POST", body: JSON.stringify({ provider: route.provider, provider_account: route.provider_account, external_subject: route.external_subject, text }) }),
  governancePolicy: (appID: string) => request<TenantPolicy>(`/api/v1/admin/governance/policy?app_id=${encodeURIComponent(appID)}`),
  saveGovernancePolicy: (policy: TenantPolicy) => request<TenantPolicy>("/api/v1/admin/governance/policy", { method: "POST", body: JSON.stringify(policy) }),
  governanceAudit: (filters: AuditFilters = {}) => {
    const query = new URLSearchParams();
    Object.entries(filters).forEach(([key, value]) => { if (value?.trim()) query.set(key, value.trim()); });
    const suffix = query.size ? `?${query.toString()}` : "";
    return request<ListResponse<AuditEvent>>(`/api/v1/admin/governance/audit${suffix}`);
  },
  governanceMetrics: (filters: MetricsFilters = {}) => {
    const query = new URLSearchParams();
    Object.entries(filters).forEach(([key, value]) => { if (value?.trim()) query.set(key, value.trim()); });
    const suffix = query.size ? `?${query.toString()}` : "";
    return request<TenantMetrics>(`/api/v1/admin/governance/metrics${suffix}`);
  },
  confirmations: () => request<ListResponse<ToolConfirmation>>("/api/v1/admin/governance/confirmations"),
  decideConfirmation: (id: string, approve: boolean) => request<ToolConfirmation>(`/api/v1/admin/governance/confirmations/${encodeURIComponent(id)}/decision`, { method: "POST", body: JSON.stringify({ approve }) }),
  governanceTrace: (id: string) => request<PlatformTrace>(`/api/v1/admin/governance/traces?trace_id=${encodeURIComponent(id)}&request_id=${encodeURIComponent(id)}`),
};
