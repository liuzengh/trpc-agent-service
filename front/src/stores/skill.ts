import { defineStore } from 'pinia'
import * as api from '../api/skill'
import type { Skill, SkillVersion, VersionInput } from '../api/skill'
import { useTenantStore } from './tenant'

export const useSkillStore = defineStore('skill', {
  state: () => ({
    skills: [] as Skill[],
    loading: false,
    error: '',
  }),
  actions: {
    async fetch() {
      this.loading = true
      this.error = ''
      try {
        const tenantStore = useTenantStore()
        this.skills = await api.listSkills(tenantStore.currentTenantId)
      } catch (e) {
        this.error = String(e)
      } finally {
        this.loading = false
      }
    },
    async create(s: api.SkillInput) {
      const created = await api.createSkill(s)
      this.skills.push(created)
    },
    async update(id: string, patch: Partial<api.SkillInput> & { status?: api.SkillStatus }) {
      const updated = await api.updateSkill(id, patch)
      const i = this.skills.findIndex((x) => x.skill_id === id)
      if (i >= 0) this.skills[i] = updated
    },
    async remove(id: string) {
      await api.deleteSkill(id)
      this.skills = this.skills.filter((x) => x.skill_id !== id)
    },
    async listVersions(skillId: string): Promise<SkillVersion[]> {
      return api.listVersions(skillId)
    },
    async addVersion(skillId: string, v: VersionInput): Promise<SkillVersion> {
      return api.createVersion(skillId, v)
    },
    async publish(skillId: string, version: number) {
      await api.publishVersion(skillId, version)
      const i = this.skills.findIndex((x) => x.skill_id === skillId)
      if (i >= 0 && this.skills[i].current_version === 0) {
        // refresh whole list so current_version/status columns update
        await this.fetch()
      }
    },
  },
})
