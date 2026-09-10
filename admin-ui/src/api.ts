export type Role = 'system_admin' | 'operator' | 'auditor'

export type Tenant = {
  tenant_id: string
  name: string
  status: string
  audit?: AuditPolicy
  agent_app_count: number
  anomaly_status: string
  updated_at: string
}

export type AgentApp = {
  tenant_id: string
  app_id: string
  name: string
  active_config_version: string
  canary_config_version?: string
  canary_percentage: number
  canary_status: string
  status: string
  backend_summary?: { kind: string; provider: string; name: string; status: string; secret_ref?: SecretRef }[]
  channel_summary?: { provider: string; binding_id: string; status: string; connection_status: string }[]
  updated_at: string
}

export type SecretRef = { name: string; version?: string }
export type BackendRef = {
  kind?: string
  provider: string
  name?: string
  secret_ref?: SecretRef
  options?: Record<string, string>
}
export type AppConfig = {
  tenant_id: string
  app_id: string
  version: string
  model: Record<string, unknown>
  tools: Record<string, unknown>
  im_access?: Record<string, unknown>
  budget?: Record<string, unknown>
  backend_config: {
    name: string
    session: BackendRef
    memory?: BackendRef
    knowledge?: BackendRef
    artifact?: BackendRef
  }
  audit?: AuditPolicy
  secret_refs?: SecretRef[]
  channel_binding?: string[]
  knowledge_base_ids?: string[]
}

export type AuditPolicy = {
  enabled?: boolean
  record_tool_decisions?: boolean
  record_executions?: boolean
  retention_days?: number
  redact_pii?: boolean
}

export type AppConfigView = {
  config: AppConfig
  status: string
  active: boolean
  created_at: string
}

export type Binding = {
  tenant_id: string
  app_id: string
  binding_id: string
  channel: string
  external_account: string
  secret_ref: SecretRef
  binding_revision: number
  status: string
  connection_status?: string
  last_connected_at?: string
  last_error?: string
}

export type Execution = {
  tenant_id: string
  app_id: string
  request_id: string
  session_principal_id: string
  session_id: string
  user_id: string
  turn_seq: number
  config_version: string
  status: string
  attempt: number
  next_attempt_at: string
  lease_owner?: string
  lease_until?: string
  last_error?: string
  trace_id: string
  duration_ms: number
  error_type?: string
  tool_calls?: { tool_name: string; event_type: string; decision: string; count: number }[]
  started_at?: string
  finished_at?: string
  created_at: string
  updated_at: string
}

export type Approval = {
  approval_id: string
  tenant_id: string
  app_id: string
  config_version: string
  request_id: string
  session_id: string
  tool_name: string
  tool_call_id?: string
  argument_digest: string
  status: string
  expires_at: string
  created_at: string
  decided_at?: string
}

export type Migration = {
  migration_id: string
  tenant_id: string
  app_id: string
  domain?: 'SESSION' | 'KNOWLEDGE' | string
  source_config_version: string
  target_config_version: string
  status: string
  lease_owner?: string
  lease_until?: string
  drain_deadline?: string
  failure_reason?: string
  total_sessions: number
  copy_progress: number
  verify_progress: number
  success_count: number
  last_checkpoint_at?: string
  last_failure_stage?: string
  created_at?: string
  updated_at?: string
}

export type AuditEvent = {
  tenant_id: string
  app_id: string
  actor_id?: string
  actor_role?: string
  channel?: string
  user_id?: string
  session_id?: string
  agent_name?: string
  tool_name?: string
  decision: string
  latency: number
  error_type?: string
  cost?: number
  trace_id: string
  request_id: string
  config_version: string
  event_type: string
  created_at: string
}

export type Operations = {
  gateway_readiness: string
  worker_readiness: string
  worker_count: number
  worker_capacity: number
  worker_utilization: number
  active_executions: number
  queue_backlog: number
  retry_backlog: number
  reply_backlog: number
  pending_approvals: number
  active_migrations: number
  stuck_migrations: number
  migration_progress: number
  audit_backlog: number
  channel_readiness: string
  recent_errors?: RecentError[]
  backends: { name: string; provider: string; status: string }[]
  jaeger_url?: string
  prometheus_url?: string
  grafana_url?: string
  generated_at: string
}

export type RecentError = {
  tenant_id: string
  app_id: string
  event_type: string
  error_type: string
  trace_id: string
  occurred_at: string
}

export type SessionInfo = {
  role: Role
  actor_id: string
  tenant_ids?: string[]
}

const tokenKey = 'trpc-agent-admin-token'
const apiBase = (import.meta.env.VITE_ADMIN_API_BASE as string | undefined) ?? ''

export function getToken(): string {
  return window.sessionStorage.getItem(tokenKey) ?? ''
}

export function saveToken(token: string): void {
  window.sessionStorage.setItem(tokenKey, token)
}

export function clearToken(): void {
  window.sessionStorage.removeItem(tokenKey)
}

export class AdminApiError extends Error {
  readonly status: number

  constructor(message: string, status: number) {
    super(message)
    this.name = 'AdminApiError'
    this.status = status
  }
}

export async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers)
  headers.set('Accept', 'application/json')
  const token = getToken()
  if (token) headers.set('Authorization', `Bearer ${token}`)
  if (init.body && !headers.has('Content-Type')) headers.set('Content-Type', 'application/json')
  const response = await fetch(`${apiBase}${path}`, { ...init, headers })
  const raw = await response.text()
  let value: unknown = undefined
  if (raw) {
    try {
      value = JSON.parse(raw)
    } catch {
      value = undefined
    }
  }
  if (!response.ok) {
    const message = typeof value === 'object' && value !== null && 'error' in value
      ? String((value as { error: unknown }).error)
      : `Admin API request failed (${response.status})`
    throw new AdminApiError(message, response.status)
  }
  return value as T
}

export function get<T>(path: string): Promise<T> {
  return request<T>(path)
}

export function post<T>(path: string, body: unknown): Promise<T> {
  return request<T>(path, { method: 'POST', body: JSON.stringify(body) })
}
