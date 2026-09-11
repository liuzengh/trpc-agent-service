import { beforeEach, describe, expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { useSkillStore } from './skill'
import * as api from '../api/skill'

vi.mock('../api/skill', () => ({
  listSkills: vi.fn(),
  createSkill: vi.fn(),
  updateSkill: vi.fn(),
  deleteSkill: vi.fn(),
  createVersion: vi.fn(),
  publishVersion: vi.fn(),
  listVersions: vi.fn(),
}))

const published = {
  skill_id: 'sk-1',
  scope: 'tenant' as const,
  owner_tenant_id: 'acme',
  code: 'triage',
  name: '分诊',
  current_version: 2,
  status: 'published' as const,
  created_at: '2026-09-02T00:00:00Z',
  updated_at: '2026-09-02T00:00:00Z',
}

describe('useSkillStore', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    vi.clearAllMocks()
  })

  it('fetch populates skills from the api', async () => {
    vi.mocked(api.listSkills).mockResolvedValue([published])
    const store = useSkillStore()
    await store.fetch()
    expect(store.skills).toHaveLength(1)
    expect(store.skills[0]?.code).toBe('triage')
  })

  it('create appends the new skill', async () => {
    vi.mocked(api.createSkill).mockResolvedValue({ ...published, current_version: 0, status: 'draft' })
    const store = useSkillStore()
    await store.create({ code: 'triage', name: '分诊', scope: 'tenant', owner_tenant_id: 'acme' })
    expect(store.skills).toHaveLength(1)
    expect(api.createSkill).toHaveBeenCalledWith({ code: 'triage', name: '分诊', scope: 'tenant', owner_tenant_id: 'acme' })
  })

  it('update replaces the skill in place', async () => {
    vi.mocked(api.updateSkill).mockResolvedValue({ ...published, name: '智能分诊' })
    const store = useSkillStore()
    store.skills = [{ ...published }]
    await store.update('sk-1', { name: '智能分诊' })
    expect(store.skills[0]?.name).toBe('智能分诊')
  })

  it('remove deletes via the api', async () => {
    vi.mocked(api.deleteSkill).mockResolvedValue()
    const store = useSkillStore()
    store.skills = [{ ...published }]
    await store.remove('sk-1')
    expect(store.skills).toHaveLength(0)
    expect(api.deleteSkill).toHaveBeenCalledWith('sk-1')
  })

  it('delegates version management to the api', async () => {
    vi.mocked(api.listVersions).mockResolvedValue([])
    vi.mocked(api.createVersion).mockResolvedValue({
      skill_id: 'sk-1',
      version: 3,
      content_md: '# x',
      checksum: 'abc',
      executor_type: 'inline',
      timeout_seconds: 30,
      status: 'draft',
    })
    vi.mocked(api.publishVersion).mockResolvedValue()
    const store = useSkillStore()
    await store.listVersions('sk-1')
    expect(api.listVersions).toHaveBeenCalledWith('sk-1')
    await store.addVersion('sk-1', { version: 3, content_md: '# x' })
    expect(api.createVersion).toHaveBeenCalledWith('sk-1', { version: 3, content_md: '# x' })
    await store.publish('sk-1', 3)
    expect(api.publishVersion).toHaveBeenCalledWith('sk-1', 3)
  })
})
