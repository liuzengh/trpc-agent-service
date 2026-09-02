import axios from 'axios'

export interface ToolDef {
  id: string
  name: string
  description: string
  risk_level: string
}

const baseURL = import.meta.env.VITE_API_BASE ?? 'http://localhost:8080'

const client = axios.create({ baseURL })

export async function listTools(): Promise<ToolDef[]> {
  const { data } = await client.get<ToolDef[]>('/tools')
  return data
}

export async function listGrants(toolId: string): Promise<string[]> {
  const { data } = await client.get<string[]>(`/tools/${toolId}/grants`)
  return data
}

export async function grantTool(toolId: string, agentId: string): Promise<void> {
  await client.put(`/tools/${toolId}/grants/${agentId}`)
}

export async function revokeTool(toolId: string, agentId: string): Promise<void> {
  await client.delete(`/tools/${toolId}/grants/${agentId}`)
}
