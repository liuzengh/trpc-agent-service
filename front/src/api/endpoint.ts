import axios from 'axios'

export interface Endpoint {
  id: string
  scope: 'global' | 'tenant'
  tenant_id?: string
  name: string
  provider: string
  base_url: string
  model_name: string
  api_key?: string
}

const baseURL = import.meta.env.VITE_API_BASE ?? 'http://localhost:8080'

const client = axios.create({ baseURL })

export async function listEndpoints(tenantId = ''): Promise<Endpoint[]> {
  const { data } = await client.get<Endpoint[]>('/endpoints', { params: { tenant_id: tenantId } })
  return data
}

export async function createEndpoint(e: Endpoint): Promise<Endpoint> {
  const { data } = await client.post<Endpoint>('/endpoints', e)
  return data
}

export async function updateEndpoint(e: Endpoint): Promise<Endpoint> {
  const { data } = await client.put<Endpoint>(`/endpoints/${e.id}`, e)
  return data
}

export async function deleteEndpoint(id: string): Promise<void> {
  await client.delete(`/endpoints/${id}`)
}
