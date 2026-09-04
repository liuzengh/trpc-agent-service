import axios from 'axios'

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
  created_at: string
  updated_at: string
}

export interface SkillInput {
  code: string
  name: string
  description?: string
  scope: SkillScope
  owner_tenant_id?: string
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

const baseURL = import.meta.env.VITE_API_BASE ?? 'http://localhost:8080'
const client = axios.create({ baseURL })

export async function listSkills(tenantId = ''): Promise<Skill[]> {
  const { data } = await client.get<Skill[]>('/skills', { params: { tenant_id: tenantId } })
  return data
}

export async function getSkill(id: string): Promise<Skill> {
  const { data } = await client.get<Skill>(`/skills/${id}`)
  return data
}

export async function createSkill(s: SkillInput): Promise<Skill> {
  const { data } = await client.post<Skill>('/skills', s)
  return data
}

export async function updateSkill(id: string, patch: Partial<SkillInput> & { status?: SkillStatus }): Promise<Skill> {
  const { data } = await client.put<Skill>(`/skills/${id}`, patch)
  return data
}

export async function deleteSkill(id: string): Promise<void> {
  await client.delete(`/skills/${id}`)
}

export async function createVersion(skillId: string, v: VersionInput): Promise<SkillVersion> {
  const { data } = await client.post<SkillVersion>(`/skills/${skillId}/versions`, v)
  return data
}

export async function publishVersion(skillId: string, version: number): Promise<void> {
  await client.post(`/skills/${skillId}/versions/${version}/publish`)
}

export async function listVersions(skillId: string): Promise<SkillVersion[]> {
  const { data } = await client.get<SkillVersion[]>(`/skills/${skillId}/versions`)
  return data
}
