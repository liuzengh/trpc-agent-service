import api from './index'

export interface Endpoint {
  id: string
  scope: 'global' | 'tenant'
  tenant_id?: string
  name: string
  provider: string
  type?: 'chat' | 'embedding'
  base_url: string
  model_name: string
  api_key?: string
  api_key_ref?: string
}

export async function listEndpoints(tenantId = ''): Promise<Endpoint[]> {
  const { data } = await api.get<Endpoint[]>('/endpoints', { params: { tenant_id: tenantId } })
  return data
}

export async function createEndpoint(e: Endpoint): Promise<Endpoint> {
  const { data } = await api.post<Endpoint>('/endpoints', e)
  return data
}

export async function updateEndpoint(e: Endpoint): Promise<Endpoint> {
  const { data } = await api.put<Endpoint>(`/endpoints/${e.id}`, e)
  return data
}

export async function deleteEndpoint(id: string): Promise<void> {
  await api.delete(`/endpoints/${id}`)
}
