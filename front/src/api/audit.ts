import axios from 'axios'

export interface AuditLog {
  audit_id: string
  tenant_id: string
  channel: string
  user_id: string
  session_id: string
  agent_name?: string
  tool_name?: string
  decision?: string
  latency_ms?: number
  error_type?: string
  cost?: number
  trace_id: string
  created_at: string
}

const baseURL = import.meta.env.VITE_API_BASE ?? 'http://localhost:8080'

const client = axios.create({ baseURL })

export async function listAudit(tenantId = '', limit = 200): Promise<AuditLog[]> {
  const { data } = await client.get<AuditLog[]>('/audit', {
    params: { tenant_id: tenantId, limit },
  })
  return data
}
