import api from './index'

export interface LedgerSession {
  session_id: string
  tenant_id: string
  agent_id: string
  member_id: string
  channel: string
  last_message_at?: string
  updated_at: string
}

export interface LedgerMessage {
  id: number
  message_id: string
  session_id: string
  agent_id: string
  member_id: string
  role: 'USER' | 'ASSISTANT'
  content: string
  status: string
  turn_id: string
  turn_timestamp: number
  created_at: string
}

export async function listSessions(tenantId = '', memberId = '', limit = 20): Promise<LedgerSession[]> {
  const { data } = await api.get<LedgerSession[]>('/sessions', {
    params: { tenant_id: tenantId, member_id: memberId, limit },
  })
  return data
}

export async function listMessages(sessionId: string, beforeTurn = 0, limit = 50): Promise<LedgerMessage[]> {
  const { data } = await api.get<LedgerMessage[]>(`/sessions/${sessionId}/messages`, {
    params: { before_turn: beforeTurn || undefined, limit },
  })
  return data
}
