import axios from 'axios'

export interface Tenant {
  id: string
  name: string
  status: 'active' | 'disabled'
}

const baseURL = import.meta.env.VITE_API_BASE ?? 'http://localhost:8080'

const client = axios.create({ baseURL })

export async function listTenants(): Promise<Tenant[]> {
  const { data } = await client.get<Tenant[]>('/tenants')
  return data
}

export async function createTenant(t: Tenant): Promise<Tenant> {
  const { data } = await client.post<Tenant>('/tenants', t)
  return data
}

export async function updateTenant(t: Tenant): Promise<Tenant> {
  const { data } = await client.put<Tenant>(`/tenants/${t.id}`, t)
  return data
}

export async function deleteTenant(id: string): Promise<void> {
  await client.delete(`/tenants/${id}`)
}
