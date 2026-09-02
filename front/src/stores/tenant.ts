import { defineStore } from 'pinia'
import * as api from '../api/tenant'

export interface Tenant {
  id: string
  name: string
  status: 'active' | 'disabled'
}

export const useTenantStore = defineStore('tenant', {
  state: () => ({
    tenants: [] as Tenant[],
    loading: false,
    error: '',
  }),
  actions: {
    async fetch() {
      this.loading = true
      this.error = ''
      try {
        this.tenants = await api.listTenants()
      } catch (e) {
        this.error = String(e)
      } finally {
        this.loading = false
      }
    },
    async create(t: Tenant) {
      const created = await api.createTenant(t)
      this.tenants.push(created)
    },
    async update(t: Tenant) {
      await api.updateTenant(t)
      const i = this.tenants.findIndex((x) => x.id === t.id)
      if (i >= 0) this.tenants[i] = { ...t }
    },
    async remove(id: string) {
      await api.deleteTenant(id)
      this.tenants = this.tenants.filter((x) => x.id !== id)
    },
  },
})
