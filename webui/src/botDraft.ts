import type { ApplicationPayload } from './api'
import { channelLabel } from './components/channelMeta'
import { isTenantConfigurableTool } from './toolPolicy'
import { channelsOf, type AgentStatus, type GenerationConfig, type ModelCapabilities, type Snapshot, type ToolJSONSchema } from './types'

export interface BindingDraft {
  type: string
  binding_id: string
  credential_ref: string
  access_policy: 'member_only' | 'allowlist' | 'public'
  allowlist: string
}

export interface HTTPToolDraft {
  name: string
  description: string
  url: string
  credential_ref: string
  input_schema: string
  enabled: boolean
  require_confirmation: boolean
}

export interface MCPToolDraft {
  name: string
  description: string
  transport: 'streamable' | 'sse'
  url: string
  credential_ref: string
  allowed_tools: string
  confirmation_tools: string
}

export interface BotDraft {
  app_code: string
  status: AgentStatus
  instruction: string
  provider_id: string
  model_name: string
  session_profile_id: string
  memory_profile_id: string
  knowledge_profile_id: string
  artifact_profile_id: string
  max_tool_calls: number
  budget_units: number
  retention_days: number
  tools_allowed: string[]
  tools_require_confirmation: string[]
  tools_http: HTTPToolDraft[]
  tools_mcp: MCPToolDraft[]
  channels: BindingDraft[]
  custom_temperature: boolean
  temperature: number
  max_tokens: string
  top_p: string
  thinking_enabled: boolean
  reasoning_effort: string
  thinking_tokens: string
  failover_candidates: { provider_id: string; name: string }[]
  max_concurrent_runs: number
  token_budget_per_hour: string
  token_reservation: string
}

export const EMPTY_BOT_DRAFT: BotDraft = {
  app_code: '', status: 'draft', instruction: '',
  provider_id: '', model_name: '',
  session_profile_id: '', memory_profile_id: '', knowledge_profile_id: '', artifact_profile_id: '',
  max_tool_calls: 8, budget_units: 100, retention_days: 90,
  tools_allowed: [],
  tools_require_confirmation: [],
  tools_http: [],
  tools_mcp: [],
  channels: [],
  custom_temperature: false,
  temperature: 0.7,
  max_tokens: '',
  top_p: '',
  thinking_enabled: false,
  reasoning_effort: '',
  thinking_tokens: '',
  failover_candidates: [],
  max_concurrent_runs: 0,
  token_budget_per_hour: '',
  token_reservation: '',
}

export function botDraftFromSnapshot(app: Snapshot | null): BotDraft {
  if (!app) return { ...EMPTY_BOT_DRAFT }

  const model = app.Config.model
  const generation = model?.generation
  const toolPolicy = app.Config.tools
  const customNames = existingCustomToolNames(toolPolicy)
  const allowedNames = toolPolicy?.allowed ?? []
  const confirmationNames = toolPolicy?.require_confirmation ?? []

  return {
    app_code: app.Config.app_code,
    status: app.Config.status,
    instruction: app.Config.instruction ?? '',
    provider_id: model?.provider_id || '',
    model_name: model?.name || '',
    session_profile_id: app.Config.storage.session?.profile_id ?? '',
    memory_profile_id: app.Config.storage.memory?.profile_id ?? '',
    knowledge_profile_id: app.Config.storage.knowledge?.profile_id ?? '',
    artifact_profile_id: app.Config.storage.artifact?.profile_id ?? '',
    max_tool_calls: app.Config.governance.max_tool_calls,
    budget_units: app.Config.governance.budget_units,
    retention_days: app.Config.audit.retention_days,
    tools_allowed: allowedNames.filter((name) => !customNames.has(name) && isTenantConfigurableTool(name)),
    tools_require_confirmation: confirmationNames.filter((name) => !customNames.has(name) && isTenantConfigurableTool(name)),
    tools_http: (toolPolicy?.http ?? []).map((tool) => ({
      name: tool.name,
      description: tool.description ?? '',
      url: tool.url,
      credential_ref: tool.credential_ref ?? '',
      input_schema: schemaDraft(tool.input_schema),
      enabled: allowedNames.includes(tool.name),
      require_confirmation: confirmationNames.includes(tool.name),
    })),
    tools_mcp: (toolPolicy?.mcp ?? []).map((server) => {
      const prefix = `${server.name}_`
      return {
        name: server.name,
        description: server.description ?? '',
        transport: server.transport === 'sse' ? 'sse' : 'streamable',
        url: server.url,
        credential_ref: server.credential_ref ?? '',
        allowed_tools: allowedNames.filter((name) => name.startsWith(prefix)).map((name) => name.slice(prefix.length)).join(', '),
        confirmation_tools: confirmationNames.filter((name) => name.startsWith(prefix)).map((name) => name.slice(prefix.length)).join(', '),
      }
    }),
    channels: channelsOf(app.Config).map((binding) => ({
      type: binding.type,
      binding_id: binding.binding_id,
      credential_ref: binding.credential_ref ?? '',
      access_policy: binding.access_policy ?? 'public',
      allowlist: (binding.allowlist ?? []).join(', '),
    })),
    custom_temperature: generation?.temperature !== undefined,
    temperature: generation?.temperature ?? 0.7,
    max_tokens: generation?.max_tokens !== undefined ? String(generation.max_tokens) : '',
    top_p: generation?.top_p !== undefined ? String(generation.top_p) : '',
    thinking_enabled: Boolean(generation?.thinking_enabled),
    reasoning_effort: generation?.reasoning_effort || '',
    thinking_tokens: generation?.thinking_tokens !== undefined ? String(generation.thinking_tokens) : '',
    failover_candidates: (model?.failover_candidates ?? []).map((candidate) => ({
      provider_id: candidate.provider_id,
      name: candidate.name,
    })),
    max_concurrent_runs: app.Config.governance?.max_concurrent_runs ?? 0,
    token_budget_per_hour: app.Config.governance?.token_budget_per_hour ? String(app.Config.governance.token_budget_per_hour) : '',
    token_reservation: app.Config.governance?.token_reservation ? String(app.Config.governance.token_reservation) : '',
  }
}

export function payloadFromSnapshot(app: Snapshot, status: AgentStatus = app.Config.status): ApplicationPayload {
  return {
    tenant_id: app.Config.tenant_id,
    app_code: app.Config.app_code,
    status,
    instruction: app.Config.instruction ?? '',
    model: app.Config.model,
    tools: app.Config.tools ?? { allowed: [] },
    storage: app.Config.storage,
    governance: app.Config.governance,
    audit: app.Config.audit,
    channels: channelsOf(app.Config).map((binding) => ({
      type: binding.type,
      binding_id: binding.binding_id,
      credential_ref: binding.credential_ref,
      access_policy: binding.access_policy ?? 'public',
      allowlist: binding.allowlist ?? [],
    })),
  }
}

export function applicationPayloadFromDraft({
  draft,
  tenantID,
  mode,
  app,
  capabilities,
}: {
  draft: BotDraft
  tenantID: string
  mode: 'create' | 'edit'
  app: Snapshot | null
  capabilities?: ModelCapabilities
}): ApplicationPayload {
  for (const [label, profileID] of [
    ['会话', draft.session_profile_id],
    ['偏好', draft.memory_profile_id],
    ['知识库', draft.knowledge_profile_id],
    ['文件', draft.artifact_profile_id],
  ] as const) {
    if (!profileID.trim()) throw new Error(`请选择${label}数据后端`)
  }
  const generation = generationFromDraft(draft, capabilities)
  const validCandidates = draft.failover_candidates
    .map((candidate) => ({ provider_id: candidate.provider_id.trim(), name: candidate.name.trim() }))
    .filter((candidate) => candidate.provider_id && candidate.name)

  const httpTools = draft.tools_http.map((tool, index) => {
    const name = tool.name.trim()
    const url = tool.url.trim()
    if (!name || !url) throw new Error(`HTTP 工具 ${index + 1} 需要名称和 URL`)
    let inputSchema: ToolJSONSchema | undefined
    if (tool.input_schema.trim()) {
      try {
        inputSchema = JSON.parse(tool.input_schema) as ToolJSONSchema
      } catch {
        throw new Error(`HTTP 工具 ${name} 的输入 Schema 不是有效 JSON`)
      }
      if (!inputSchema || typeof inputSchema !== 'object' || Array.isArray(inputSchema)) {
        throw new Error(`HTTP 工具 ${name} 的输入 Schema 必须是 JSON 对象`)
      }
    }
    return {
      name,
      description: tool.description.trim() || undefined,
      url,
      credential_ref: tool.credential_ref.trim() || undefined,
      input_schema: inputSchema,
    }
  })

  const mcpTools = draft.tools_mcp.map((server, index) => {
    const name = server.name.trim()
    const url = server.url.trim()
    if (!name || !url) throw new Error(`MCP 服务 ${index + 1} 需要名称和 URL`)
    const confirmations = new Set(commaNames(server.confirmation_tools))
    const allowed = new Set(commaNames(server.allowed_tools))
    for (const toolName of confirmations) {
      if (!allowed.has(toolName)) throw new Error(`MCP 服务 ${name}：审批工具 ${toolName} 必须先加入允许工具`)
    }
    return {
      name,
      description: server.description.trim() || undefined,
      transport: server.transport,
      url,
      credential_ref: server.credential_ref.trim() || undefined,
    }
  })

  const customAllowedTools = [
    ...draft.tools_http.filter((tool) => tool.enabled).map((tool) => tool.name.trim()),
    ...draft.tools_mcp.flatMap((server) => commaNames(server.allowed_tools).map((name) => `${server.name.trim()}_${name}`)),
  ].filter(Boolean)
  const customConfirmationTools = [
    ...draft.tools_http.filter((tool) => tool.enabled && tool.require_confirmation).map((tool) => tool.name.trim()),
    ...draft.tools_mcp.flatMap((server) => commaNames(server.confirmation_tools).map((name) => `${server.name.trim()}_${name}`)),
  ].filter(Boolean)
  const allowedTools = [...new Set([...draft.tools_allowed.filter(isTenantConfigurableTool), ...customAllowedTools])]
  const confirmationTools = [...new Set([...draft.tools_require_confirmation.filter(isTenantConfigurableTool), ...customConfirmationTools])]
  const allowedToolSet = new Set(allowedTools)
  const invalidConfirmationTools = confirmationTools.filter((name) => !allowedToolSet.has(name))
  if (invalidConfirmationTools.length > 0) {
    throw new Error(`需要审批的工具必须先加入允许调用列表：${invalidConfirmationTools.join('、')}`)
  }
  const existingAllowedRoles = app?.Config.tools?.allowed_roles ?? {}
  const allowedRoles = Object.fromEntries(
    Object.entries(existingAllowedRoles).filter(([toolName]) => allowedToolSet.has(toolName)),
  )

  return {
    tenant_id: tenantID.trim(),
    app_code: draft.app_code.trim(),
    status: mode === 'create' ? 'draft' : draft.status,
    instruction: draft.instruction.trim(),
    model: {
      provider_id: draft.provider_id.trim(),
      name: draft.model_name.trim(),
      ...(generation ? { generation } : {}),
      ...(validCandidates.length > 0 ? { failover_candidates: validCandidates } : {}),
    },
    tools: {
      allowed: allowedTools,
      ...(confirmationTools.length > 0 ? { require_confirmation: confirmationTools } : {}),
      ...(Object.keys(allowedRoles).length > 0 ? { allowed_roles: allowedRoles } : {}),
      ...(httpTools.length > 0 ? { http: httpTools } : {}),
      ...(mcpTools.length > 0 ? { mcp: mcpTools } : {}),
    },
    storage: {
      session: { profile_id: draft.session_profile_id.trim() },
      memory: { profile_id: draft.memory_profile_id.trim() },
      knowledge: { profile_id: draft.knowledge_profile_id.trim() },
      artifact: { profile_id: draft.artifact_profile_id.trim() },
    },
    governance: {
      max_tool_calls: draft.max_tool_calls,
      budget_units: draft.budget_units,
      ...(draft.max_concurrent_runs > 0 ? { max_concurrent_runs: draft.max_concurrent_runs } : {}),
      ...(draft.token_budget_per_hour.trim() ? { token_budget_per_hour: Number(draft.token_budget_per_hour.trim()) } : {}),
      ...(draft.token_reservation.trim() ? { token_reservation: Number(draft.token_reservation.trim()) } : {}),
    },
    audit: { retention_days: draft.retention_days },
    channels: draft.channels.map((binding) => {
      if (!binding.binding_id.trim()) throw new Error(`${channelLabel(binding.type)}的机器人账号标识不能为空`)
      if (!binding.credential_ref.trim()) throw new Error(`${channelLabel(binding.type)}需要选择平台凭据`)
      return {
        type: binding.type,
        binding_id: binding.binding_id.trim(),
        credential_ref: binding.credential_ref.trim(),
        access_policy: binding.access_policy,
        allowlist: binding.access_policy === 'allowlist' ? commaNames(binding.allowlist) : [],
      }
    }),
  }
}

function generationFromDraft(draft: BotDraft, capabilities?: ModelCapabilities): GenerationConfig | undefined {
  const supportedEfforts = capabilities?.reasoning_efforts ?? []
  const reasoningEffortAllowed = !capabilities?.thinking_toggle || draft.thinking_enabled
  const reasoningEffort = reasoningEffortAllowed && supportedEfforts.includes(draft.reasoning_effort as NonNullable<GenerationConfig['reasoning_effort']>)
    ? draft.reasoning_effort as NonNullable<GenerationConfig['reasoning_effort']>
    : undefined
  const thinkingEnabled = Boolean(capabilities?.thinking_toggle && draft.thinking_enabled)
  const thinkingTokens = capabilities?.thinking_budget ? draft.thinking_tokens.trim() : ''
  if (!draft.custom_temperature && !draft.max_tokens.trim() && !draft.top_p.trim() && !reasoningEffort && !thinkingEnabled && !thinkingTokens) return undefined

  const generation: GenerationConfig = {}
  if (draft.custom_temperature) generation.temperature = draft.temperature
  if (draft.max_tokens.trim()) generation.max_tokens = Number(draft.max_tokens.trim())
  if (draft.top_p.trim()) generation.top_p = Number(draft.top_p.trim())
  if (reasoningEffort) generation.reasoning_effort = reasoningEffort
  if (thinkingEnabled) generation.thinking_enabled = true
  if (thinkingTokens) generation.thinking_tokens = Number(thinkingTokens)
  return generation
}

function commaNames(raw: string): string[] {
  return [...new Set(raw.split(',').map((value) => value.trim()).filter(Boolean))]
}

function existingCustomToolNames(policy: Snapshot['Config']['tools'] | undefined): Set<string> {
  const names = new Set<string>()
  const allowed = policy?.allowed ?? []
  for (const http of policy?.http ?? []) names.add(http.name)
  for (const mcp of policy?.mcp ?? []) {
    const prefix = `${mcp.name}_`
    for (const name of [...allowed, ...(policy?.require_confirmation ?? [])]) {
      if (name.startsWith(prefix)) names.add(name)
    }
  }
  return names
}

function schemaDraft(schema: ToolJSONSchema | undefined): string {
  return schema ? JSON.stringify(schema, null, 2) : ''
}
