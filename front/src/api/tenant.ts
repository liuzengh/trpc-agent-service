import api from './index'

export interface TenantQuota {
  /** per-tenant token budget; 0 / absent = unlimited */
  token_quota?: number
}

export interface TenantAuditPolicy {
  /** sensitive-data redaction; absent = on (default) */
  redact?: boolean
  /** IM user allow-list; empty = everyone allowed */
  im_allow_users?: string[]
  /** static-tool whitelist; empty = unrestricted */
  tool_whitelist?: string[]
  /** tool ids always requiring human approval */
  force_approval_tools?: string[]
}

export interface Tenant {
  id: string
  name: string
  status: 'active' | 'disabled'
  quota?: TenantQuota
  audit_policy?: TenantAuditPolicy
}

export async function listTenants(): Promise<Tenant[]> {
  const { data } = await api.get<Tenant[]>('/tenants')
  return data
}

export async function createTenant(t: Tenant): Promise<Tenant> {
  const { data } = await api.post<Tenant>('/tenants', t)
  return data
}

export async function updateTenant(t: Tenant): Promise<Tenant> {
  const { data } = await api.put<Tenant>(`/tenants/${t.id}`, t)
  return data
}

export async function deleteTenant(id: string): Promise<void> {
  await api.delete(`/tenants/${id}`)
}

/** A recorded tenant configuration snapshot (see 011_tenant_config_versions). */
export interface TenantConfigVersion {
  version: number
  /** full tenant state at that version */
  config?: Tenant
  created_at: string
}

/** Lists the tenant's configuration history, newest first. */
export async function listConfigVersions(id: string): Promise<TenantConfigVersion[]> {
  const { data } = await api.get<TenantConfigVersion[]>(`/tenants/${id}/config-versions`)
  return data
}

/** Restores a tenant to a recorded configuration version; returns the restored tenant. */
export async function rollbackConfig(id: string, version: number): Promise<Tenant> {
  const { data } = await api.post<Tenant>(`/tenants/${id}/config-rollback`, { version })
  return data
}
