import {
	 outboxReplyPayload,
  type AuditEvent,
  type Claim,
  type ExecutionTrace,
  type ChatRequest,
  type Snapshot,
  type SSEEvent,
  type ChatArtifact,
  type InteractiveCard,
  type SystemStatus,
  type ModelConfig,
  type ToolPolicy,
  type StoragePolicy,
  type GovernancePolicy,
  type AuditPolicy,
  type MemoryEntry,
  type KnowledgeDocument,
  type ArtifactRecord,
  type ModelProviderInfo,
  type RolloutPolicy,
  type PlatformSession,
  type SessionChatMessage,
  type ChannelBindingStatus,
  type TenantCatalog,
} from './types'

// SessionUser 是 /api/v1/auth/me 返回的登录主体。
export interface SessionUser {
  platform_user_id: string
  display_name?: string
  email?: string
  role: string
  is_system_admin: boolean
  must_change_password?: boolean
  active_tenant_id?: string
  active_role?: string
	tenants?: Array<{
		tenant_id: string
		display_name: string
		role: string
		status?: string
		conversation_content_audit?: boolean
	}>
}

export interface TenantSummary {
  tenant_id: string
  display_name: string
  role: string
  status: 'active' | 'suspended'
}

export interface TenantModelGrant {
  provider_id: string
  name: string
}

export interface TenantModelPolicy {
  tenant_id: string
  models: TenantModelGrant[]
}

export interface TenantToolGrant {
  name: string
}

export interface TenantToolPolicy {
  tenant_id: string
  tools: TenantToolGrant[]
  catalog: import('./types').ToolInfo[]
}

export interface TenantBackendPolicy {
  tenant_id: string
  profiles: import('./types').TenantBackendProfile[]
}

export interface LoginProvider {
  provider_id?: string
  type: 'local' | 'wecom' | 'feishu' | 'oidc' | 'mock'
  display_name: string
  configured: boolean
  enabled: boolean
	registration_enabled?: boolean
  /** 该登录源是否支持内嵌二维码。 */
  qr_supported?: boolean
  /** 部署侧是否已开启二维码登录。 */
  qr_enabled?: boolean
}

export interface LoginBegin {
  auth_url: string
  provider: LoginProvider
}

export interface LoginQRBegin {
  goto: string
  state: string
  expires_in: number
  sdk_url: string
  provider: LoginProvider
}

export interface AuthProviderConfiguration {
  type: 'local' | 'wecom' | 'feishu' | 'oidc' | 'mock'
  display_name: string
  provider_id?: string
  configured: boolean
  enabled: boolean
	registration_enabled?: boolean
  metadata?: Record<string, string>
  last_successful_login_at?: string
}

export interface AuthConfiguration {
  callback_url: string
  providers: AuthProviderConfiguration[]
}

export interface MemberSummary {
  platform_user_id: string
  display_name: string
  email: string
  role: string
  status: 'active' | 'suspended'
  is_system_admin: boolean
  last_login_at: string
	providers: string[]
	conversation_content_audit: boolean
}

export interface AccountLoginMethod {
  provider_id: string
  type: 'local' | 'wecom' | 'feishu' | 'oidc' | 'mock'
  display_name: string
  subject_id: string
  linked_at: string
}

export interface AccountProfile {
  platform_user_id: string
  display_name: string
  email?: string
  login_methods: AccountLoginMethod[]
}

export class ApiError extends Error {
  status: number
  constructor(status: number, message: string) {
    super(message)
    this.status = status
  }
}

// 401 全局回调：App 注册它完成「会话过期 → 跳登录页」；未注册时静默。
let onUnauthorized: (() => void) | null = null

export function setUnauthorizedHandler(handler: (() => void) | null): void {
  onUnauthorized = handler
}

let activeTenantHeader = ''

export function setActiveTenantHeader(tenant: string): void {
  activeTenantHeader = tenant.trim()
}

export function getActiveTenantHeader(): string {
  return activeTenantHeader
}

// 从 Cookie 读取 Double-Submit CSRF token（登录成功时由服务端下发）。
function csrfToken(): string {
  if (typeof document === 'undefined') return ''
  const match = document.cookie.match(/(?:^|;\s*)csrf_token=([^;]+)/)
  return match ? decodeURIComponent(match[1]) : ''
}

async function requestResponse(path: string, init?: RequestInit): Promise<Response> {
  const method = (init?.method ?? 'GET').toUpperCase()
  const headers = new Headers(init?.headers)
  if (activeTenantHeader && !headers.has('X-Active-Tenant')) {
    headers.set('X-Active-Tenant', activeTenantHeader)
  }
  // 写操作自动携带 CSRF 头（与 Cookie 双提交校验）。
  if (method !== 'GET' && method !== 'HEAD') {
    const token = csrfToken()
    if (token) headers.set('X-CSRF-Token', token)
  }
  const response = await fetch(path, { ...init, headers, credentials: 'same-origin' })
  if (!response.ok) {
    let message = `HTTP ${response.status}`
    try {
      const body = (await response.json()) as { error?: string }
      if (body.error) message = body.error
    } catch {
      /* keep the status-only message */
    }
    if (message === 'tenant application configuration does not exist') {
      message = '机器人配置已不存在，请重新选择当前机器人。'
    }
    if (response.status === 401) onUnauthorized?.()
    throw new ApiError(response.status, message)
  }
  return response
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await requestResponse(path, init)
  if (response.status === 204 || response.status === 205) return undefined as T
  const body = await response.text()
  if (!body.trim()) return undefined as T
  return JSON.parse(body) as T
}

// ---- 登录域 -------------------------------------------------------------

export function getLoginProviders(signal?: AbortSignal): Promise<LoginProvider[]> {
  return request<{ providers: LoginProvider[] }>('/api/v1/auth/providers', { signal }).then((result) => result.providers ?? [])
}

export function beginLogin(providerID: string): Promise<LoginBegin> {
  return request<LoginBegin>(`/api/v1/auth/login?provider=${encodeURIComponent(providerID)}`)
}

export function beginFeishuQR(providerID: string, signal?: AbortSignal): Promise<LoginQRBegin> {
  return request<LoginQRBegin>(`/api/v1/auth/qr/begin?provider=${encodeURIComponent(providerID)}`, { signal })
}

export function verifyLoginProvider(providerID: string): Promise<LoginBegin> {
  return request<LoginBegin>('/api/v1/auth/verify-provider', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ provider_id: providerID }),
  })
}

export function loginLocal(username: string, password: string): Promise<{ must_change_password: boolean }> {
  return request<{ must_change_password: boolean }>('/api/v1/auth/local/login', {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ username, password }),
  })
}

export function registerLocal(username: string, display_name: string, email: string, password: string): Promise<{ platform_user_id: string }> {
	return request<{ platform_user_id: string }>('/api/v1/auth/local/register', {
		method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ username, display_name, email, password }),
	})
}

export function changeLocalPassword(current_password: string, new_password: string): Promise<void> {
  return request<void>('/api/v1/auth/local/password', {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ current_password, new_password }),
  })
}

export function getAuthConfiguration(signal?: AbortSignal): Promise<AuthConfiguration> {
  return request<AuthConfiguration>('/api/v1/auth/configuration', { signal })
}

export function getMe(): Promise<SessionUser> {
  return request<SessionUser>('/api/v1/auth/me')
}

export function getAccountProfile(signal?: AbortSignal): Promise<AccountProfile> {
  return request<AccountProfile>('/api/v1/account/profile', { signal })
}

export function updateAccountProfile(display_name: string): Promise<AccountProfile> {
  return request<AccountProfile>('/api/v1/account/profile', {
    method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ display_name }),
  })
}

export function getTenants(signal?: AbortSignal): Promise<TenantSummary[]> {
  return request<{ tenants: TenantSummary[] }>('/api/v1/tenants', { signal }).then((result) => result.tenants ?? [])
}

export function createTenant(tenant_id: string, display_name: string, initial_admin_platform_user_id: string): Promise<void> {
  return request<void>('/api/v1/tenants', {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ tenant_id, display_name, initial_admin_platform_user_id }),
  })
}

export function updateTenantStatus(tenant_id: string, status: 'active' | 'suspended'): Promise<void> {
  return request<void>(`/api/v1/tenants/${encodeURIComponent(tenant_id)}`, {
    method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ status }),
  })
}

export function getTenantModelPolicy(tenant: string, signal?: AbortSignal): Promise<TenantModelPolicy> {
  return request<TenantModelPolicy>(`/api/v1/tenant-model-policy?tenant=${encodeURIComponent(tenant)}`, { signal }).then((result) => ({
    tenant_id: result.tenant_id,
    models: result.models ?? [],
  }))
}

export function replaceTenantModelPolicy(tenant: string, models: TenantModelGrant[]): Promise<TenantModelPolicy> {
  return request<TenantModelPolicy>(`/api/v1/tenant-model-policy?tenant=${encodeURIComponent(tenant)}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ tenant_id: tenant, models }),
  })
}

export function getTenantToolPolicy(tenant: string, signal?: AbortSignal): Promise<TenantToolPolicy> {
  return request<TenantToolPolicy>(`/api/v1/tenant-tool-policy?tenant=${encodeURIComponent(tenant)}`, { signal }).then((result) => ({
    tenant_id: result.tenant_id,
    tools: result.tools ?? [],
    catalog: result.catalog ?? [],
  }))
}

export function replaceTenantToolPolicy(tenant: string, tools: TenantToolGrant[]): Promise<TenantToolPolicy> {
  return request<TenantToolPolicy>(`/api/v1/tenant-tool-policy?tenant=${encodeURIComponent(tenant)}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ tenant_id: tenant, tools }),
  }).then((result) => ({ tenant_id: result.tenant_id, tools: result.tools ?? [], catalog: result.catalog ?? [] }))
}

export function getBackendDrivers(signal?: AbortSignal): Promise<import('./types').BackendDriverSpec[]> {
  return request<{ drivers: import('./types').BackendDriverSpec[] }>('/api/v1/backend-drivers', { signal })
    .then((result) => result.drivers ?? [])
}

export function getBackendProfiles(signal?: AbortSignal): Promise<import('./types').BackendProfile[]> {
  return request<{ profiles: import('./types').BackendProfile[] }>('/api/v1/backend-profiles', { signal })
    .then((result) => result.profiles ?? [])
}

export function createBackendProfile(profile: Pick<import('./types').BackendProfile, 'profile_id' | 'display_name' | 'driver' | 'domains'> & { connection_ref?: string }): Promise<import('./types').BackendProfile> {
  return request<import('./types').BackendProfile>('/api/v1/backend-profiles', {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(profile),
  })
}

export function updateBackendProfile(profileID: string, profile: Pick<import('./types').BackendProfile, 'display_name' | 'status'>): Promise<import('./types').BackendProfile> {
  return request<import('./types').BackendProfile>(`/api/v1/backend-profiles/${encodeURIComponent(profileID)}`, {
    method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(profile),
  })
}

export function deleteBackendProfile(profileID: string): Promise<void> {
  return request<void>(`/api/v1/backend-profiles/${encodeURIComponent(profileID)}`, { method: 'DELETE' })
}

export function getTenantBackendPolicy(tenant: string, signal?: AbortSignal): Promise<TenantBackendPolicy> {
  return request<TenantBackendPolicy>(`/api/v1/tenant-backend-policy?tenant=${encodeURIComponent(tenant)}`, { signal })
    .then((result) => ({ tenant_id: result.tenant_id, profiles: result.profiles ?? [] }))
}

export function replaceTenantBackendPolicy(tenant: string, profileIDs: string[]): Promise<TenantBackendPolicy> {
  return request<TenantBackendPolicy>(`/api/v1/tenant-backend-policy?tenant=${encodeURIComponent(tenant)}`, {
    method: 'PUT', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ tenant_id: tenant, profile_ids: profileIDs }),
  })
}

export interface LocalUserCredentialResult {
  platform_user_id?: string
  username?: string
  temporary_password: string
  must_change_password: boolean
}

export function createLocalUser(username: string, display_name: string, email: string): Promise<LocalUserCredentialResult> {
  return request<LocalUserCredentialResult>('/api/v1/users/local', {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ username, display_name, email }),
  })
}

export function resetLocalUserPassword(platform_user_id: string): Promise<LocalUserCredentialResult> {
  return request<LocalUserCredentialResult>('/api/v1/users/local/reset', {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ platform_user_id }),
  })
}

export interface MemberListOptions {
  cursor?: string
  query?: string
  limit?: number
}

export interface UserPage {
  users: MemberSummary[]
  next_cursor: string
}

export interface TenantMemberPage {
  members: MemberSummary[]
  next_cursor: string
}

export interface TenantMemberCandidatePage {
  candidates: MemberSummary[]
  next_cursor: string
}

function memberListQuery(options?: MemberListOptions) {
  const query = new URLSearchParams()
  if (options?.cursor) query.set('cursor', options.cursor)
  if (options?.query?.trim()) query.set('q', options.query.trim())
  if (options?.limit) query.set('limit', String(options.limit))
  return query
}

function normalizeMemberSummary(member: MemberSummary): MemberSummary {
  return {
    ...member,
    providers: Array.isArray(member.providers) ? member.providers : [],
    conversation_content_audit: Boolean(member.conversation_content_audit),
  }
}

export function getUsers(options?: MemberListOptions, signal?: AbortSignal): Promise<UserPage> {
  const query = memberListQuery(options)
  return request<UserPage>(`/api/v1/users${query.size ? `?${query}` : ''}`, { signal }).then((result) => ({
    users: (result.users ?? []).map(normalizeMemberSummary),
    next_cursor: result.next_cursor ?? '',
  }))
}

export function updateUser(platform_user_id: string, status: string, is_system_admin: boolean): Promise<void> {
  return request<void>('/api/v1/users', {
    method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ platform_user_id, status, is_system_admin }),
  })
}

export function getTenantMembers(tenantID: string, options?: MemberListOptions, signal?: AbortSignal): Promise<TenantMemberPage> {
  const query = memberListQuery(options)
  query.set('tenant', tenantID)
  return request<TenantMemberPage>(`/api/v1/tenant-members?${query}`, { signal }).then((result) => ({
    members: (result.members ?? []).map(normalizeMemberSummary),
    next_cursor: result.next_cursor ?? '',
  }))
}

export function getTenantMemberCandidates(tenantID: string, options?: MemberListOptions, signal?: AbortSignal): Promise<TenantMemberCandidatePage> {
  const query = memberListQuery(options)
  query.set('tenant', tenantID)
  query.set('view', 'candidates')
  return request<TenantMemberCandidatePage>(`/api/v1/tenant-members?${query}`, { signal }).then((result) => ({
    candidates: (result.candidates ?? []).map(normalizeMemberSummary),
    next_cursor: result.next_cursor ?? '',
  }))
}

export function updateTenantMember(tenant_id: string, platform_user_id: string, role: string, status: string, conversation_content_audit?: boolean): Promise<void> {
	return request<void>('/api/v1/tenant-members', {
		method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ tenant_id, platform_user_id, role, status, conversation_content_audit }),
	})
}

// postLogout 走会话 + CSRF；响应体为空，单独处理 401。
export async function postLogout(): Promise<void> {
  const response = await fetch('/api/v1/auth/logout', {
    method: 'POST',
    headers: { 'X-CSRF-Token': csrfToken() },
    credentials: 'same-origin',
  })
  if (response.status === 401) {
    onUnauthorized?.()
    throw new ApiError(401, '会话已过期')
  }
  if (!response.ok) throw new ApiError(response.status, `HTTP ${response.status}`)
}

// ---- 控制台 API ---------------------------------------------------------

export function getApps(signal?: AbortSignal): Promise<Snapshot[]> {
  return request<{ applications: Snapshot[] }>('/api/v1/apps', { signal, headers: { 'X-Active-Tenant': '' } }).then((r) => r.applications)
}

export function listSessions(
  tenant: string,
  filters: { app?: string; channel?: string; status?: string; scope?: string; ownerPlatformUserId?: string; identity?: 'unlinked' } = {},
  view: 'mine' | 'tenant' = 'mine',
  signal?: AbortSignal,
): Promise<PlatformSession[]> {
  const query = new URLSearchParams({ tenant })
  if (filters.app) query.set('app', filters.app)
  if (filters.channel) query.set('channel', filters.channel)
  if (filters.status) query.set('status', filters.status)
  if (filters.scope) query.set('scope', filters.scope)
  if (filters.ownerPlatformUserId) query.set('owner_platform_user_id', filters.ownerPlatformUserId)
  if (filters.identity) query.set('identity', filters.identity)
  return request<{ sessions: PlatformSession[] }>(`/api/v1/sessions/${view}?${query}`, { signal }).then((r) => r.sessions ?? [])
}

export interface SessionMessagePage {
  messages: SessionChatMessage[]
  next_cursor: string
}

export function getSessionMessages(
  tenant: string,
  sessionKey: string,
  options?: { before?: string; limit?: number },
  signal?: AbortSignal,
): Promise<SessionMessagePage> {
  const query = new URLSearchParams({ tenant, session_key: sessionKey })
  if (options?.before) query.set('before', options.before)
  if (options?.limit) query.set('limit', String(options.limit))
  return request<SessionMessagePage>(`/api/v1/sessions/messages?${query}`, { signal }).then((result) => ({
    messages: result.messages ?? [],
    next_cursor: result.next_cursor ?? '',
  }))
}

export function getClaims(tenant: string, app = '', signal?: AbortSignal): Promise<Claim[]> {
  const query = new URLSearchParams({ tenant })
  if (app.trim()) query.set('app', app.trim())
  return request<unknown>(`/api/v1/claims?${query}`, { signal }).then(parseClaimsResponse)
}

export function getAudit(tenant: string, trace: string, signal?: AbortSignal): Promise<AuditEvent[]> {
  const query = trace ? `&trace=${encodeURIComponent(trace)}` : ''
  return request<{ events: AuditEvent[] }>(
    `/api/v1/audit?tenant=${encodeURIComponent(tenant)}${query}`,
    { signal },
  ).then((r) => r.events)
}

export function getExecution(tenant: string, channel: string, bindingId: string, eventId: string, sessionKey = '', signal?: AbortSignal): Promise<ExecutionTrace> {
  const params: Record<string, string> = { tenant, channel, binding_id: bindingId, event_id: eventId }
  if (sessionKey) params.session_key = sessionKey
  const query = new URLSearchParams(params)
  return request<ExecutionTrace>(`/api/v1/execution?${query}`, { signal })
}

export function getSystem(signal?: AbortSignal): Promise<SystemStatus> {
  return request<unknown>('/api/v1/system', { signal }).then(parseSystemStatus)
}

export function getTenantCatalog(tenant: string, signal?: AbortSignal): Promise<TenantCatalog> {
  const query = new URLSearchParams({ tenant })
  return request<TenantCatalog>(`/api/v1/catalog?${query}`, { signal })
}

export function getChannelStatuses(tenant: string, app = '', signal?: AbortSignal): Promise<ChannelBindingStatus[]> {
  const query = new URLSearchParams({ tenant })
  if (app.trim()) query.set('app', app.trim())
  return request<{ channels: ChannelBindingStatus[] }>(`/api/v1/channels/status?${query}`, { signal })
    .then((result) => result.channels ?? [])
}

function parseClaimsResponse(value: unknown): Claim[] {
  const body = requireRecord(value, '/api/v1/claims')
  if (!Array.isArray(body.claims)) throw contractError('/api/v1/claims', 'claims must be an array')
  return body.claims.map((claim, index) => parseClaim(claim, `/api/v1/claims.claims[${index}]`))
}

function parseClaim(value: unknown, location: string): Claim {
  const claim = requireRecord(value, location)
  if (['Channel', 'MessageID', 'Status', 'TraceID', 'UpdatedAt'].some((field) => field in claim)) {
    throw contractError(location, '检测到旧版 PascalCase 字段，请更新后端服务')
  }
  for (const field of ['channel', 'binding_id', 'message_id', 'status', 'trace_id', 'updated_at'] as const) {
    if (typeof claim[field] !== 'string' || claim[field] === '') {
      throw contractError(location, `${field} must be a non-empty string`)
    }
  }
  for (const field of ['started_at', 'ended_at'] as const) {
    if (claim[field] !== undefined && typeof claim[field] !== 'string') {
      throw contractError(location, `${field} must be a string when present`)
    }
  }
  if (claim.app_code !== undefined && typeof claim.app_code !== 'string') {
    throw contractError(location, 'app_code must be a string when present')
  }
  if (claim.failed !== undefined && typeof claim.failed !== 'boolean') {
    throw contractError(location, 'failed must be a boolean when present')
  }
  return claim as unknown as Claim
}

function parseSystemStatus(value: unknown): SystemStatus {
  const body = requireRecord(value, '/api/v1/system')
  const info = requireRecord(body.info, '/api/v1/system.info')
  if (['Version', 'ListenAddress', 'KafkaTopic', 'KafkaBrokers', 'RedisAddress', 'ModelProviders'].some((field) => field in info)) {
    throw contractError('/api/v1/system.info', '检测到旧版 PascalCase 字段，请更新后端服务')
  }
  for (const field of ['version', 'listen_address', 'kafka_topic', 'kafka_brokers', 'redis_address'] as const) {
    if (typeof info[field] !== 'string') {
      throw contractError('/api/v1/system.info', `${field} must be a string`)
    }
  }
  if (!Array.isArray(info.model_providers)) {
    throw contractError('/api/v1/system.info', 'model_providers must be an array')
  }
  if (info.channel_credential_refs !== undefined && (!Array.isArray(info.channel_credential_refs) || info.channel_credential_refs.some((value) => typeof value !== 'string'))) {
    throw contractError('/api/v1/system.info', 'channel_credential_refs must be a string array when present')
  }
  const statuses = requireRecord(body.status, '/api/v1/system.status')
  if (Object.values(statuses).some((status) => typeof status !== 'string')) {
    throw contractError('/api/v1/system.status', 'dependency statuses must be strings')
  }
  if (body.nodes !== undefined && !Array.isArray(body.nodes)) {
    throw contractError('/api/v1/system', 'nodes must be an array when present')
  }
  return body as unknown as SystemStatus
}

function requireRecord(value: unknown, location: string): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw contractError(location, 'expected an object')
  }
  return value as Record<string, unknown>
}

function contractError(location: string, detail: string): Error {
  return new Error(`API 契约不一致：${location} ${detail}`)
}

export function syncModels(): Promise<{ model_providers: ModelProviderInfo[] }> {
  return request<{ model_providers: ModelProviderInfo[] }>('/api/v1/models/sync', {
    method: 'POST',
  })
}

export function deleteModel(providerID: string, modelName: string): Promise<void> {
  const query = new URLSearchParams({ provider_id: providerID, model: modelName })
  return request<void>(`/api/v1/models?${query}`, { method: 'DELETE' })
}

export function deleteSession(tenantID: string, sessionKey: string): Promise<void> {
  return request<void>('/api/v1/sessions/archive', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ tenant_id: tenantID, session_key: sessionKey }),
  })
}

export function getMemories(params: {
  tenant: string
  app: string
  query?: string
  kind?: 'fact' | 'episode'
  timeAfter?: string
  timeBefore?: string
  order?: 'event_time'
  signal?: AbortSignal
}): Promise<{ memories: MemoryEntry[]; managed_externally: boolean }> {
  const query = new URLSearchParams({ tenant: params.tenant, app: params.app })
  if (params.query?.trim()) query.set('query', params.query.trim())
  if (params.kind) query.set('kind', params.kind)
  if (params.timeAfter) query.set('time_after', params.timeAfter)
  if (params.timeBefore) query.set('time_before', params.timeBefore)
  if (params.order) query.set('order', params.order)
  return request<{ memories: MemoryEntry[]; managed_externally?: boolean }>(`/api/v1/memory?${query}`, { signal: params.signal })
    .then((result) => ({ memories: result.memories ?? [], managed_externally: Boolean(result.managed_externally) }))
}

export function deleteMemory(tenant: string, app: string, memoryId: string): Promise<void> {
  const query = new URLSearchParams({ tenant, app, memory_id: memoryId })
  return request<void>(`/api/v1/memory?${query}`, { method: 'DELETE' })
}

export interface IngestKnowledgeParams {
  tenant_id: string
  app_code: string
  document_id: string
  name?: string
  content?: string
  source_type?: 'url' | 'repo'
  source_url?: string
  branch?: string
  chunk_size?: number
  overlap?: number
  metadata?: Record<string, string>
}

export function ingestKnowledgeDocument(params: IngestKnowledgeParams): Promise<void> {
  return request<void>('/api/v1/knowledge', {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(params),
  })
}

export function uploadKnowledgeDocument(params: {
  tenant_id: string
  app_code: string
  document_id: string
  name?: string
  file: File
  chunk_size?: number
  overlap?: number
  metadata?: Record<string, string>
}): Promise<{ job_id: string; status: string }> {
  const form = new FormData()
  form.set('tenant_id', params.tenant_id)
  form.set('app_code', params.app_code)
  form.set('document_id', params.document_id)
  form.set('name', params.name ?? params.file.name)
  form.set('file', params.file)
  if (params.chunk_size !== undefined) form.set('chunk_size', String(params.chunk_size))
  if (params.overlap !== undefined) form.set('overlap', String(params.overlap))
  if (params.metadata) form.set('metadata', JSON.stringify(params.metadata))
  return request<{ job_id: string; status: string }>('/api/v1/knowledge', { method: 'POST', body: form })
}

export function listKnowledgeDocuments(tenant: string, app: string, signal?: AbortSignal): Promise<KnowledgeDocument[]> {
  const query = new URLSearchParams({ tenant, app })
  return request<{ documents: KnowledgeDocument[] }>(`/api/v1/knowledge/documents?${query}`, { signal }).then((res) => res.documents)
}

export function deleteKnowledgeDocument(tenant: string, app: string, documentId: string): Promise<void> {
  const query = new URLSearchParams({ tenant, app, document_id: documentId })
  return request<void>(`/api/v1/knowledge?${query}`, { method: 'DELETE' })
}

export function listArtifacts(tenant: string, app: string, session: string, signal?: AbortSignal): Promise<string[]> {
  const query = new URLSearchParams({ tenant, app, session })
  return request<{ artifacts: string[] }>(`/api/v1/artifacts?${query}`, { signal }).then((result) => result.artifacts)
}

export function getArtifact(tenant: string, app: string, session: string, filename: string, version?: number, signal?: AbortSignal): Promise<ArtifactRecord> {
  const query = new URLSearchParams({ tenant, app, session, filename })
  if (version !== undefined) query.set('version', String(version))
  return requestResponse(`/api/v1/artifacts?${query}`, { signal }).then(async (response) => {
    const mimeType = response.headers.get('Content-Type') ?? 'application/octet-stream'
    const data = await response.blob()
    const text = mimeType.startsWith('text/') || mimeType.includes('json') || mimeType.includes('xml')
      ? await data.text()
      : undefined
    return {
      filename,
      version: Number(response.headers.get('X-Artifact-Version') ?? 0),
      mime_type: mimeType,
      data,
      text,
    }
  })
}

export function artifactDownloadURL(tenant: string, app: string, session: string, filename: string, version?: number): string {
  const query = new URLSearchParams({ tenant, app, session, filename, download: '1' })
  if (version !== undefined) query.set('version', String(version))
  return `/api/v1/artifacts?${query}`
}

export interface ApplicationPayload {
  tenant_id: string
  app_code: string
  status: string
  instruction: string
  model: ModelConfig
  tools: ToolPolicy
  storage: StoragePolicy
  governance: GovernancePolicy
  audit: AuditPolicy
	channels: { type: string; binding_id: string; credential_ref?: string; access_policy?: 'member_only' | 'allowlist' | 'public'; allowlist?: string[] }[]
}

export function createApplication(payload: ApplicationPayload): Promise<Snapshot> {
  return request<{ application: Snapshot }>('/api/v1/apps', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  }).then((response) => response.application)
}

export function updateApplication(tenant: string, app: string, payload: ApplicationPayload): Promise<Snapshot> {
  return request<{ application: Snapshot }>(
    `/api/v1/apps/${encodeURIComponent(tenant)}/${encodeURIComponent(app)}`,
    {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    },
  ).then((response) => response.application)
}

export function stageApplicationVersion(tenant: string, app: string, payload: ApplicationPayload): Promise<Snapshot> {
  return request<{ application: Snapshot }>(
    `/api/v1/apps/${encodeURIComponent(tenant)}/${encodeURIComponent(app)}/versions`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    },
  ).then((response) => response.application)
}

const CHAT_STREAM_MAX_ATTEMPTS = 5
const CHAT_STREAM_RETRY_INITIAL_MS = 500
const CHAT_STREAM_RETRY_MAX_MS = 4000

export function generateUUID(): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID()
  }
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0
    const v = c === 'x' ? r : (r & 0x3) | 0x8
    return v.toString(16)
  })
}

class RetryableChatStreamError extends Error {}

interface ChatStreamResult {
  lastEventID: string
  reply: string
  card?: InteractiveCard
  artifacts?: ChatArtifact[]
  done: boolean
  sawDelta: boolean
}

// postChat accepts a durable asynchronous request, then reconnects its SSE
// stream with Last-Event-ID whenever a proxy timeout or transient stream close
// occurs. Event IDs prevent duplicate delta rendering after a reconnect.
export interface ChatResult {
  reply: string
  card?: InteractiveCard
  artifacts?: ChatArtifact[]
  eventId: string
  sessionKey: string
}

export async function postChat(
  body: ChatRequest,
  onDelta: (content: string) => void,
  signal?: AbortSignal,
  files: File[] = [],
  onCard?: (card: InteractiveCard) => void,
): Promise<ChatResult> {
  const headers = new Headers()
  const token = csrfToken()
  if (token) headers.set('X-CSRF-Token', token)
  const requestId = body.request_id ?? generateUUID()
  const request = { ...body, request_id: requestId }
  let requestBody: BodyInit
  if (files.length > 0) {
    const form = new FormData()
    for (const [key, value] of Object.entries(request)) form.set(key, String(value ?? ''))
    for (const file of files) form.append('files', file, file.name)
    requestBody = form
  } else {
    headers.set('Content-Type', 'application/json')
    requestBody = JSON.stringify(request)
  }
  const accepted = await fetch('/api/v1/chat', {
    method: 'POST', headers, credentials: 'same-origin', body: requestBody, signal,
  })
  if (!accepted.ok) throw new ApiError(accepted.status, `HTTP ${accepted.status}`)
  const queued = (await accepted.json()) as { stream_url?: string; event_id?: string; session_key?: string }
  if (!queued.stream_url) throw new ApiError(500, 'chat stream URL missing')
  const eventId = queued.event_id ?? requestId
  const sessionKey = queued.session_key ?? ''
  const resultOf = (text: string, card?: InteractiveCard, artifacts?: ChatArtifact[]): ChatResult => ({ reply: text, card, artifacts, eventId, sessionKey })

  let lastEventID = ''
  let sawDelta = false
  let reply = ''
  let card: InteractiveCard | undefined
  const seenEventIDs = new Set<string>()
  for (let attempt = 0; attempt < CHAT_STREAM_MAX_ATTEMPTS; attempt += 1) {
    try {
      const result = await readChatStream(queued.stream_url, lastEventID, seenEventIDs, onDelta, onCard, signal)
      lastEventID = result.lastEventID
      sawDelta = sawDelta || result.sawDelta
      if (result.reply) reply = result.reply
      if (result.card) card = result.card
      if (result.done) {
        if (!sawDelta && result.reply) onDelta(result.reply)
        return resultOf(result.reply, result.card, result.artifacts)
      }
    } catch (error) {
      if (signal?.aborted || (error as Error).name === 'AbortError') throw error
      if (!(error instanceof RetryableChatStreamError) || attempt === CHAT_STREAM_MAX_ATTEMPTS - 1) throw error
    }
    if (attempt === CHAT_STREAM_MAX_ATTEMPTS - 1) break
    await waitForChatRetry(attempt, signal)
  }

  // Fallback: If reconnect attempts exhausted or stream disconnected, check if execution already completed in DB
  let executionFailed = false
  if (body.tenant_id && eventId) {
    try {
      const trace = await getExecution(body.tenant_id, 'web', 'web-console', eventId, sessionKey)
      if (trace.outbox && trace.outbox.length > 0) {
        for (const out of trace.outbox) {
          const payload = outboxReplyPayload(out.Payload)
          if (payload && (payload.text || payload.card || payload.artifacts?.length)) {
            const text = payload.text
            if (!sawDelta) onDelta(text)
            return resultOf(text, payload.card, payload.artifacts)
          }
        }
      }
      executionFailed = trace.status === 'failed' || trace.agent_trace?.status === 'failed' || trace.claim?.status === 'failed'
    } catch {
      /* ignore */
    }
  }

  if (executionFailed) throw new ApiError(500, '请求处理失败，请稍后重试。')
  if (reply || card) return resultOf(reply, card)
  throw new ApiError(504, 'chat stream reconnect limit reached')
}

async function readChatStream(
  streamURL: string,
  lastEventID: string,
  seenEventIDs: Set<string>,
  onDelta: (content: string) => void,
  onCard?: (card: InteractiveCard) => void,
  signal?: AbortSignal,
): Promise<ChatStreamResult> {
  const headers = new Headers()
  if (lastEventID) headers.set('Last-Event-ID', lastEventID)
  const response = await fetch(streamURL, { headers, credentials: 'same-origin', signal })
  if (!response.ok || !response.body) throw new ApiError(response.status || 500, 'chat stream unavailable')

  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''
  let nextEventID = lastEventID
  let reply = ''
  let card: InteractiveCard | undefined
  let sawDelta = false
  for (;;) {
    const { done, value } = await reader.read()
    if (done) {
      if (buffer.trim()) {
        for (const line of buffer.split('\n')) {
          if (line.startsWith('data: ')) {
            try {
              const event = JSON.parse(line.slice(6)) as SSEEvent
              if (event.type === 'done') {
                return { lastEventID: nextEventID, reply: event.reply || reply, card: event.card, artifacts: event.artifacts, done: true, sawDelta }
              }
            } catch { /* ignore */ }
          }
        }
      }
      return { lastEventID: nextEventID, reply, done: false, sawDelta }
    }
    buffer += decoder.decode(value, { stream: true }).replace(/\r\n/g, '\n')
    let boundary = buffer.indexOf('\n\n')
    while (boundary >= 0) {
      const chunk = buffer.slice(0, boundary)
      buffer = buffer.slice(boundary + 2)
      let eventID = ''
      let event: SSEEvent | null = null
      for (const line of chunk.split('\n')) {
        if (line.startsWith('id: ')) eventID = line.slice(4)
        if (line.startsWith('data: ')) {
          try {
            event = JSON.parse(line.slice(6)) as SSEEvent
          } catch {
            event = null
          }
        }
      }
      boundary = buffer.indexOf('\n\n')
      if (!event) continue
      if (eventID) nextEventID = eventID
      if (eventID && seenEventIDs.has(eventID)) continue
      if (eventID) seenEventIDs.add(eventID)
      if (event.type === 'delta' && event.content) {
        sawDelta = true
        reply += event.content
        onDelta(event.content)
      } else if (event.type === 'card' && event.card) {
        card = event.card
        onCard?.(event.card)
      } else if (event.type === 'done') {
        const finalReply = event.reply || reply
        card = event.card
        return { lastEventID: nextEventID, reply: finalReply, card, artifacts: event.artifacts, done: true, sawDelta }
      } else if (event.type === 'error') {
        const message = event.message ?? 'agent execution failed'
        if (message.includes('timed out') || message.includes('stream closed')) {
          throw new RetryableChatStreamError(message)
        }
        throw new ApiError(500, message)
      }
    }
  }
}

export function resolveChatApproval(tenantID: string, actionID: string): Promise<InteractiveCard> {
  return request<{ card: InteractiveCard }>('/api/v1/chat/approval', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ tenant_id: tenantID, action_id: actionID }),
  }).then((response) => response.card)
}

function waitForChatRetry(attempt: number, signal?: AbortSignal): Promise<void> {
  const delay = Math.min(CHAT_STREAM_RETRY_INITIAL_MS * 2 ** attempt, CHAT_STREAM_RETRY_MAX_MS)
  return new Promise((resolve, reject) => {
    const timer = window.setTimeout(resolve, delay)
    signal?.addEventListener('abort', () => {
      window.clearTimeout(timer)
      reject(new DOMException('The operation was aborted', 'AbortError'))
    }, { once: true })
  })
}

export function getApplicationVersion(tenant: string, app: string, version: number): Promise<Snapshot> {
  return request<{ application: Snapshot }>(
    `/api/v1/apps/${encodeURIComponent(tenant)}/${encodeURIComponent(app)}/versions/${version}`,
  ).then((response) => response.application)
}

export function listApplicationVersions(tenant: string, app: string): Promise<Snapshot[]> {
  return request<{ versions: Snapshot[] }>(
    `/api/v1/apps/${encodeURIComponent(tenant)}/${encodeURIComponent(app)}/versions`,
  ).then((response) => response.versions)
}

export function getApplicationCandidate(tenant: string, app: string): Promise<Snapshot | null> {
  return request<{ candidate: Snapshot | null }>(
    `/api/v1/apps/${encodeURIComponent(tenant)}/${encodeURIComponent(app)}/candidate`,
  ).then((response) => response.candidate)
}

export function discardApplicationCandidate(tenant: string, app: string, expectedVersion: number): Promise<void> {
  return request<void>(
    `/api/v1/apps/${encodeURIComponent(tenant)}/${encodeURIComponent(app)}/candidate`,
    {
      method: 'DELETE',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ expected_version: expectedVersion }),
    },
  )
}

export function getApplicationRollout(tenant: string, app: string): Promise<RolloutPolicy | null> {
  return request<{ rollout: RolloutPolicy | null }>(
    `/api/v1/apps/${encodeURIComponent(tenant)}/${encodeURIComponent(app)}/rollout`,
  ).then((response) => response.rollout)
}

export function updateApplicationRollout(
  tenant: string,
  app: string,
  rollout: Pick<RolloutPolicy, 'basis_points' | 'test_user_ids' | 'ingresses'> & { expected_generation: number },
): Promise<RolloutPolicy> {
  return request<{ rollout: RolloutPolicy }>(
    `/api/v1/apps/${encodeURIComponent(tenant)}/${encodeURIComponent(app)}/rollout`,
    {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(rollout),
    },
  ).then((response) => response.rollout)
}

export function stopApplicationRollout(tenant: string, app: string, expectedGeneration: number): Promise<void> {
  return request<void>(
    `/api/v1/apps/${encodeURIComponent(tenant)}/${encodeURIComponent(app)}/rollout`,
    {
      method: 'DELETE',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ expected_generation: expectedGeneration }),
    },
  )
}

export function promoteApplicationCandidate(tenant: string, app: string, expectedVersion: number, expectedGeneration: number): Promise<Snapshot> {
  return request<{ application: Snapshot }>(
    `/api/v1/apps/${encodeURIComponent(tenant)}/${encodeURIComponent(app)}/candidate/promote`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ expected_version: expectedVersion, expected_generation: expectedGeneration }),
    },
  ).then((response) => response.application)
}

export function rollbackApplicationVersion(tenant: string, app: string, version: number): Promise<Snapshot> {
  return request<{ application: Snapshot }>(
    `/api/v1/apps/${encodeURIComponent(tenant)}/${encodeURIComponent(app)}/rollback/${version}`,
    { method: 'POST' },
  ).then((response) => response.application)
}
