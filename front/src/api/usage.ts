import axios from 'axios'

export interface UsageSummary {
  dimension: string
  total: number
  count: number
}

export interface UsageRow {
  record_id: string
  tenant_id: string
  agent_id?: string
  dimension: string
  amount: number
  meta?: string
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

const baseURL = import.meta.env.VITE_API_BASE ?? 'http://localhost:8080'
const client = axios.create({ baseURL })

export async function getUsage(q: UsageQuery = {}): Promise<UsageResponse> {
  const { data } = await client.get<UsageResponse>('/usage', { params: q })
  return data
}
