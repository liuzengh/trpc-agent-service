import api from './index'

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

/** 资产变更行的 channel 值（后端 web.AuditSourceAsset）。 */
export const AUDIT_SOURCE_ASSET = 'console'

/** 资产变更行的 agent_name 取值；Agent 运行行该字段为空。 */
export type AuditAssetKind = 'agent' | 'kb' | 'skill' | 'binding' | 'endpoint'

export interface AuditFilters {
  tenant_id?: string
  /** 按来源：console=资产变更（谁改了哪个资产），留空/其它=Agent 运行 */
  channel?: string
  /** 资产种类（仅资产变更行有值） */
  kind?: string
  /** executed | deny | failed | allow | approve */
  decision?: string
  /** 操作者（成员 id），用于「这个成员改过什么」的追溯 */
  user_id?: string
  limit?: number
}

export async function listAudit(filters: AuditFilters = {}): Promise<AuditLog[]> {
  const { data } = await api.get<AuditLog[]>('/audit', {
    params: { limit: 200, ...filters },
  })
  return data
}

/** 该行是否为资产变更（而非 Agent 运行）。 */
export function isAssetChange(row: AuditLog): boolean {
  return row.channel === AUDIT_SOURCE_ASSET && !!row.session_id?.startsWith('asset:')
}
