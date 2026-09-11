import api from './index'
import type { AssetVisibility } from './asset'

export type SkillScope = 'global' | 'tenant'
export type SkillStatus = 'draft' | 'published' | 'disabled'

export interface Skill {
  skill_id: string
  scope: SkillScope
  owner_tenant_id?: string
  code: string
  name: string
  description?: string
  current_version: number
  status: SkillStatus
  /** 作者（成员 id）；作者可编辑/删除并决定是否共享 */
  created_by?: string
  /** private=仅作者与租户管理员可见；shared=租户内共享只读 */
  visibility?: AssetVisibility
  created_at: string
  updated_at: string
}

export interface SkillInput {
  code: string
  name: string
  description?: string
  scope: SkillScope
  owner_tenant_id?: string
  visibility?: AssetVisibility
}

export interface SkillVersion {
  skill_id: string
  version: number
  content_md: string
  checksum: string
  prompt_template?: string
  executor_type: string
  timeout_seconds: number
  status: SkillStatus
  published_at?: string
}

export interface VersionInput {
  version: number
  content_md: string
  prompt_template?: string
}

export async function listSkills(tenantId = ''): Promise<Skill[]> {
  const { data } = await api.get<Skill[]>('/skills', { params: { tenant_id: tenantId } })
  return data
}

export async function getSkill(id: string): Promise<Skill> {
  const { data } = await api.get<Skill>(`/skills/${id}`)
  return data
}

export async function createSkill(s: SkillInput): Promise<Skill> {
  const { data } = await api.post<Skill>('/skills', s)
  return data
}

export async function updateSkill(id: string, patch: Partial<SkillInput> & { status?: SkillStatus }): Promise<Skill> {
  const { data } = await api.put<Skill>(`/skills/${id}`, patch)
  return data
}

export async function deleteSkill(id: string): Promise<void> {
  await api.delete(`/skills/${id}`)
}

export async function createVersion(skillId: string, v: VersionInput): Promise<SkillVersion> {
  const { data } = await api.post<SkillVersion>(`/skills/${skillId}/versions`, v)
  return data
}

export async function publishVersion(skillId: string, version: number): Promise<void> {
  await api.post(`/skills/${skillId}/versions/${version}/publish`)
}

export async function listVersions(skillId: string): Promise<SkillVersion[]> {
  const { data } = await api.get<SkillVersion[]>(`/skills/${skillId}/versions`)
  return data
}
