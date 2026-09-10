import api from './index'

export interface Agent {
  id: string
  tenant_id: string
  name: string
  description?: string
  status: 'draft' | 'published' | 'disabled'
  current_version: number
}

export interface RuntimeProfile {
  system_prompt: string
  endpoint_id: string
  tool_ids?: string[]
  kb_ids?: string[]
  skill_ids?: string[]
  approval_tool_ids?: string[] // tool calls that must be human-approved at runtime
}

export interface VersionInfo {
  version: number
  status: string
}

export async function listAgents(tenantId = ''): Promise<Agent[]> {
  const { data } = await api.get<Agent[]>('/agents', { params: { tenant_id: tenantId } })
  return data
}

export async function createAgent(a: Agent): Promise<Agent> {
  const { data } = await api.post<Agent>('/agents', a)
  return data
}

export async function updateAgent(a: Agent): Promise<Agent> {
  const { data } = await api.put<Agent>(`/agents/${a.id}`, a)
  return data
}

export async function deleteAgent(id: string): Promise<void> {
  await api.delete(`/agents/${id}`)
}

export async function publishAgent(id: string, p: RuntimeProfile): Promise<{ version: number }> {
  const { data } = await api.post<{ version: number }>(`/agents/${id}/publish`, p)
  return data
}

export async function rollbackAgent(id: string, version: number): Promise<void> {
  await api.post(`/agents/${id}/rollback`, { version })
}

export async function listVersions(id: string): Promise<VersionInfo[]> {
  const { data } = await api.get<VersionInfo[]>(`/agents/${id}/versions`)
  return data
}

export async function getProfile(id: string): Promise<RuntimeProfile> {
  const { data } = await api.get<RuntimeProfile>(`/agents/${id}/profile`)
  return data
}
