import { defineStore } from 'pinia'
import * as api from '../api/agent'
import type { Agent, RuntimeProfile } from '../api/agent'

export const useAgentStore = defineStore('agent', {
  state: () => ({
    agents: [] as Agent[],
    loading: false,
    error: '',
  }),
  actions: {
    async fetch(tenantId = '') {
      this.loading = true
      this.error = ''
      try {
        this.agents = await api.listAgents(tenantId)
      } catch (e) {
        this.error = String(e)
      } finally {
        this.loading = false
      }
    },
    async create(a: Agent) {
      const created = await api.createAgent(a)
      this.agents.push(created)
    },
    async update(a: Agent) {
      await api.updateAgent(a)
      const i = this.agents.findIndex((x) => x.id === a.id)
      if (i >= 0) this.agents[i] = { ...this.agents[i], ...a }
    },
    async remove(id: string) {
      await api.deleteAgent(id)
      this.agents = this.agents.filter((x) => x.id !== id)
    },
    async publish(id: string, p: RuntimeProfile): Promise<number> {
      const { version } = await api.publishAgent(id, p)
      const i = this.agents.findIndex((x) => x.id === id)
      if (i >= 0) {
        this.agents[i].status = 'published'
        this.agents[i].current_version = version
      }
      return version
    },
    async rollback(id: string, version: number) {
      await api.rollbackAgent(id, version)
      const i = this.agents.findIndex((x) => x.id === id)
      if (i >= 0) this.agents[i].current_version = version
    },
  },
})
