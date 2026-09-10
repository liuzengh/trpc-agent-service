import api from './index'

export interface UsageSummary {
  dimension: string
  total: number
  count: number
}

export interface SkillMeta {
  skill_id: string
  code: string
  name: string
  version: number
}

/** meta payload of a usage row: which tools/skills were involved this turn. */
export interface UsageMeta {
  tools?: string[]
  calls?: Record<string, number>
  /** newer rows carry snapshots; older rows carried a plain skill-id list */
  skills?: Array<SkillMeta | string>
}

export interface UsageRow {
  record_id: string
  tenant_id: string
  agent_id?: string
  dimension: string
  amount: number
  meta?: UsageMeta
  created_at: string
}

export interface UsageResponse {
  summary: UsageSummary[]
  rows: UsageRow[]
}

export interface UsageQuery {
  tenant_id?: string
  agent_id?: string
  dimension?: string
  from?: string
  to?: string
}

export async function getUsage(q: UsageQuery = {}): Promise<UsageResponse> {
  const { data } = await api.get<UsageResponse>('/usage', { params: q })
  return data
}
