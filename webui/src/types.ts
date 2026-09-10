// TypeScript mirrors of the /api/v1/* JSON payloads.

export interface ChannelBinding {
	type: string
	binding_id: string
	credential_ref?: string
	trusted_enterprise_id?: string
	access_policy?: 'member_only' | 'allowlist' | 'public'
	allowlist?: string[]
}

export type AgentStatus = 'draft' | 'active' | 'disabled'

export interface ModelCandidate {
  provider_id: string
  name: string
}

export interface GenerationConfig {
  temperature?: number
  top_p?: number
  max_tokens?: number
  reasoning_effort?: 'low' | 'medium' | 'high' | 'max' | 'xhigh'
  thinking_enabled?: boolean
  thinking_tokens?: number
}

export interface ModelConfig {
  provider_id: string
  name: string
  failover_candidates?: ModelCandidate[]
  generation?: GenerationConfig
}

export interface ToolPolicy {
  allowed?: string[]
  allowed_roles?: Record<string, string[]>
  require_confirmation?: string[]
  http?: HTTPToolConfig[]
  mcp?: MCPToolConfig[]
}

export interface ToolJSONSchema {
  type?: string
  description?: string
  pattern?: string
  required?: string[]
  properties?: Record<string, ToolJSONSchema>
  items?: ToolJSONSchema
  additionalProperties?: boolean | ToolJSONSchema
  default?: unknown
  enum?: unknown[]
  $ref?: string
  $defs?: Record<string, ToolJSONSchema>
}

export interface HTTPToolConfig {
  name: string
  description?: string
  url: string
  credential_ref?: string
  input_schema?: ToolJSONSchema
}

export interface MCPToolConfig {
  name: string
  description?: string
  transport?: 'streamable' | 'sse'
  url: string
  credential_ref?: string
}

export interface ToolInfo {
  name: string
  description?: string
}

export interface TenantModelProviderInfo {
  id: string
  type?: string
  models: ModelInfo[]
}

export type BackendDomain = 'session' | 'memory' | 'knowledge' | 'artifact'

export interface TenantBackendProfile {
  profile_id: string
  display_name: string
  driver: string
  status: 'active' | 'disabled'
  domains: BackendDomain[]
  available: boolean
}

export interface BackendProfile extends TenantBackendProfile {
  connection_ref?: string
  created_at?: string
  updated_at?: string
}

export interface TenantCatalog {
  model_providers: TenantModelProviderInfo[]
  backend_profiles: TenantBackendProfile[]
  channel_credential_refs: string[]
  tool_credential_refs: string[]
  tools: ToolInfo[]
}

export interface BackendProfileRef {
  profile_id: string
}

export interface StoragePolicy {
  session: BackendProfileRef
  memory: BackendProfileRef
  knowledge: BackendProfileRef
  artifact: BackendProfileRef
}

export interface GovernancePolicy {
  max_tool_calls: number
  budget_units: number
  requests_per_minute?: number
  max_concurrent_runs?: number
  token_budget_per_hour?: number
  token_reservation?: number
}

export interface AuditPolicy {
  retention_days: number
}

export interface TenantConfig {
  tenant_id: string
  app_code: string
	status: AgentStatus
  config_version: number
  // System prompt / business context; optional on legacy rows.
  instruction?: string
  model: ModelConfig
  tools: ToolPolicy
  storage: StoragePolicy
  governance: GovernancePolicy
  audit: AuditPolicy
  // Legacy rows may return null; always access via channelsOf().
  channels: ChannelBinding[] | null
}

export interface Snapshot {
  Config: TenantConfig
  Checksum: string
  PublishedAt: string
}

// channelsOf normalizes a possible null to an array so empty-binding bots
// never crash the render path.
export function channelsOf(config: TenantConfig): ChannelBinding[] {
  return config.channels ?? []
}

export interface Claim {
  channel: string
  binding_id: string
  message_id: string
  status: string
  trace_id: string
  updated_at: string
  app_code?: string
  started_at?: string
  ended_at?: string
  failed?: boolean
}

export interface AuditEvent {
  ID: string
  TenantID: string
  TraceID: string
  RequestID?: string
  Channel?: string
  UserID?: string
  SessionID?: string
  AgentName?: string
  ToolName?: string
  Action: string
  Result: string
  Decision?: string
  LatencyMS?: number
  ErrorType?: string
  CostMicros?: number
  Detail: string
  CreatedAt: string
}

export interface OutboxEvent {
  ID: string
  TenantID: string
  AggregateKey: string
  Type: string
  Payload: string // base64-encoded JSON
  CreatedAt: string
  DeliveredAt: string | null
}

export interface ExecutionTrace {
  event_id: string
  trace_id?: string
  status?: 'completed' | 'incomplete' | 'failed' | string
  attempts: number
  claim?: Claim
  outbox?: OutboxEvent[]
  agent_trace?: AgentExecutionTrace
  tool_executions?: ToolExecution[]
}

export interface ToolExecution {
  request_id: string
  tool_call_id: string
  tool_name: string
  status: 'running' | 'completed' | 'failed' | 'outcome_unknown'
  trace_id: string
  error_type?: string
  started_at: string
  completed_at?: string
}

export interface AgentExecutionTraceUsage {
  prompt_tokens: number
  completion_tokens: number
  total_tokens: number
  cached_tokens?: number
  cache_creation_tokens?: number
  cache_read_tokens?: number
  reasoning_tokens?: number
}

export interface AgentExecutionTraceStep {
  step_id: string
  invocation_id?: string
  parent_invocation_id?: string
  agent_name?: string
  branch?: string
  node_id?: string
  node_type?: string
  started_at: string
  ended_at: string
  predecessor_step_ids?: string[]
  applied_surface_ids?: string[]
  usage?: AgentExecutionTraceUsage
  failed?: boolean
}

export interface AgentExecutionTrace {
  status: 'completed' | 'incomplete' | 'failed'
  root_agent_name?: string
  root_invocation_id?: string
  session_id?: string
  started_at: string
  ended_at: string
  usage?: AgentExecutionTraceUsage
  steps: AgentExecutionTraceStep[]
}

export interface SystemInfo {
  version: string
  listen_address: string
  kafka_topic: string
  kafka_brokers: string
  redis_address: string
  model_providers: ModelProviderInfo[]
  channel_credential_refs?: string[]
}

export interface ModelCapabilities {
  reasoning_efforts?: Array<'low' | 'medium' | 'high' | 'max' | 'xhigh'>
  thinking_toggle?: boolean
  thinking_budget?: boolean
  input?: {
    image?: boolean
    audio?: boolean
    file?: boolean
  }
}

export interface RolloutPolicy {
  tenant_id: string
  app_code: string
  generation: number
  stable_version: number
  candidate_version: number
  basis_points: number
  test_user_ids: string[]
  ingresses: string[]
  updated_at: string
}

export interface ModelInfo {
  name: string
  capabilities?: ModelCapabilities
  pricing?: {
    prompt_micros_per_million_tokens: number
    cached_prompt_micros_per_million_tokens?: number
    completion_micros_per_million_tokens: number
  }
  source?: 'configured' | 'discovered'
}

export interface ModelProviderInfo {
  id: string
  type?: string
  base_url?: string
  credential_refs?: string[]
  configured?: boolean
  models?: ModelInfo[]
  sync_error?: string
}

export interface SystemStatus {
  info: SystemInfo
  status: Record<string, string>
  nodes?: ServiceNode[]
}

export interface ChannelBindingStatus {
  channel: 'telegram' | 'wecom' | 'feishu'
  binding_id: string
  state: 'ready' | 'connecting' | 'connected' | 'error' | 'offline'
  owner?: string
  last_changed_at?: string
  last_error?: string
}

export interface ServiceNode {
  node_id: string
  boot_id: string
  role: 'gateway' | 'worker' | 'all'
  state: 'ready' | 'draining' | 'offline'
  build_version: string
  inflight: number
  started_at: string
  last_heartbeat: string
  lease_until: string
}

export interface ChatRequest {
  tenant_id: string
  app_code: string
  conversation_id: string
  text: string
  request_id?: string
}

export interface PlatformSession {
  TenantID: string
  AppCode: string
  SessionKey: string
  SubjectID: string
  OwnerPlatformUserID?: string
  Status: string
  UpdatedAt: string
  ArchivedAt?: string | null
  Conversations?: SessionConversation[]
  Summary?: string
  Preview?: string
}

export interface SessionConversation {
  TenantID: string
  AppCode: string
  SessionKey: string
  Channel: 'web' | 'telegram' | 'wecom' | 'feishu'
  BindingID: string
  ConversationID: string
  ExternalUserID: string
  Scope: 'direct' | 'group'
  StartedAt: string
  UpdatedAt: string
  EndedAt?: string | null
}

export interface SessionChatMessage {
  id: string
  role: 'user' | 'assistant'
  content: string
  time: string
  source?: {
    channel: 'web' | 'telegram' | 'wecom' | 'feishu'
    binding_id: string
    conversation_id: string
    scope: 'direct' | 'group'
    actor_external_user_id: string
    actor_platform_user_id?: string
    trigger_type: 'direct' | 'mention' | 'command'
  }
}

export interface SSEEvent {
  type: 'delta' | 'done' | 'error'
  content?: string
  reply?: string
  message?: string
}

export interface MemoryValue {
  memory: string
  topics?: string[]
  last_updated?: string
  kind?: 'fact' | 'episode'
  event_time?: string
  participants?: string[]
  location?: string
}

export interface MemoryEntry {
  id: string
  app_name: string
  memory: MemoryValue
  user_id: string
  created_at: string
  updated_at: string
  score?: number
}

export interface KnowledgeDocument {
  tenant_id: string
  app_code: string
  document_id: string
  name?: string
  status?: 'ready' | 'indexing' | 'failed'
  total_chunks?: number
  metadata: Record<string, string> | null
  updated_at: string
}

export interface ArtifactRecord {
  filename: string
  version: number
  mime_type: string
  data: Blob
  text?: string
}

// decodeBase64JSON turns a base64-encoded UTF-8 JSON document into an object.
export function decodeBase64JSON<T>(encoded: string): T | null {
  try {
    const bytes = Uint8Array.from(atob(encoded), (char) => char.charCodeAt(0))
    const json = new TextDecoder('utf-8', { fatal: true }).decode(bytes)
    return JSON.parse(json) as T
  } catch {
    return null
  }
}

export function outboxReplyText(payload: string): string {
  const decoded = decodeBase64JSON<{ text?: string }>(payload)
  return typeof decoded?.text === 'string' ? decoded.text : ''
}
