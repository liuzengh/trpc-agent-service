// Shared types mirroring the Go backend API contracts.

export interface TenantChannel {
  type: string;
  binding_id: string;
  enabled: boolean;
}

export interface TenantInfo {
  tenant_id: string;
  version: string;
  enabled: boolean;
  app: { name: string; agent_name: string };
  model: { provider: string; name: string; variant: string; streaming: boolean };
  channels: TenantChannel[];
  data: Record<string, string>;
  active_revision?: string;
  canary_revision?: string;
  rollout_percent?: number;
  generation?: number;
}

export interface TenantListResponse {
  tenants: TenantInfo[];
}

/** POST /admin/v1/tenants/custom-model request body. */
export interface CustomModelRequest {
  api_format: string;
  base_url: string;
  full_url: boolean;
  model_id: string;
  display_name: string;
  api_key_env: string;
}

export interface CustomModelResponse {
  status: string;
  tenant_id: string;
  display_name: string;
  model: { provider: string; name: string; base_url: string };
}

export interface StageTrace {
  name: string;
  start_ms: number;
  duration_ms: number;
}

export interface ToolTrace {
  name: string;
  decision: string;
  reason?: string;
}

export interface ChatResult {
  text: string;
  request_id: string;
  session_id: string;
  duplicate: boolean;
  cache_hit: boolean;
  trace_id?: string;
  prompt_tokens: number;
  completion_tokens: number;
  cost_usd: number;
  latency_ms: number;
  tools?: ToolTrace[];
  timeline?: StageTrace[];
}

export interface ChatRequest {
  message_id: string;
  user_id: string;
  conversation_id: string;
  thread_id?: string;
  scope: 'direct' | 'group';
  text: string;
}

export type MessageRole = 'user' | 'agent';

export interface ChatMessage {
  id: string;
  role: MessageRole;
  text: string;
  createdAt: number;
  /** Only present on agent replies: the raw /v1/chat result. */
  result?: ChatResult;
  /** Request payload sent for this turn (stored on the user row and its reply). */
  request?: ChatRequest;
  error?: string;
}

export interface Conversation {
  id: string;
  title: string;
  scope: 'direct' | 'group';
  createdAt: number;
  updatedAt: number;
  /** Latest time the conversation was opened while replies were visible. */
  lastReadAt?: number;
  messages: ChatMessage[];
}

/** One parsed Prometheus exposition line, e.g. name{labels} value. */
export interface MetricRow {
  name: string;
  labels: Record<string, string>;
  value: number;
}
