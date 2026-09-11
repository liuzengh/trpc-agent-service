import api from './index'

export type MemberRole = 'owner' | 'admin' | 'member'

export interface Member {
  tenant_id: string
  user_id: string
  role: MemberRole
  created_at: string
}

export interface CreateMemberInput {
  user_id: string
  password: string
  role: MemberRole
  /** Only the platform owner may choose the tenant; an admin's value is ignored
   * server-side and the member always lands in the admin's own tenant. */
  tenant_id?: string
}

export async function listMembers(): Promise<Member[]> {
  const { data } = await api.get<Member[]>('/members')
  return data
}

export async function createMember(input: CreateMemberInput): Promise<Member> {
  const { data } = await api.post<Member>('/members', input)
  return data
}

export async function updateMemberRole(userId: string, role: MemberRole): Promise<void> {
  await api.put(`/members/${encodeURIComponent(userId)}`, { role })
}

export async function deleteMember(userId: string): Promise<void> {
  await api.delete(`/members/${encodeURIComponent(userId)}`)
}
