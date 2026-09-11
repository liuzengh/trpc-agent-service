import api from './index'

export interface ToolDef {
  id: string
  name: string
  description: string
  risk_level: string
}

export async function listTools(): Promise<ToolDef[]> {
  const { data } = await api.get<ToolDef[]>('/tools')
  return data
}

export async function listGrants(toolId: string): Promise<string[]> {
  const { data } = await api.get<string[]>(`/tools/${toolId}/grants`)
  return data
}

export async function grantTool(toolId: string, agentId: string): Promise<void> {
  await api.put(`/tools/${toolId}/grants/${agentId}`)
}

export async function revokeTool(toolId: string, agentId: string): Promise<void> {
  await api.delete(`/tools/${toolId}/grants/${agentId}`)
}
