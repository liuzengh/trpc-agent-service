import { defineStore } from 'pinia'
import * as api from '../api/tenant'

export interface Tenant {
  id: string
  name: string
  status: 'active' | 'disabled'
}

const STORAGE_KEY = 'currentTenantId'

function loadTenantId(): string {
  if (typeof localStorage !== 'undefined') return localStorage.getItem(STORAGE_KEY) || ''
  return ''
}

function saveTenantId(id: string): void {
  if (typeof localStorage !== 'undefined') localStorage.setItem(STORAGE_KEY, id)
}

export const useTenantStore = defineStore('tenant', {
  state: () => ({
    tenants: [] as Tenant[],
    currentTenantId: loadTenantId(),
    loading: false,
    error: '',
  }),
  getters: {
    currentTenant(state): Tenant | undefined {
      return state.tenants.find((t) => t.id === state.currentTenantId)
    },
  },
  actions: {
    async fetch() {
      this.loading = true
      this.error = ''
      try {
        this.tenants = await api.listTenants()
        // Default the selection to the first tenant so tenant-scoped queries
        // (skills/kbs/agent mounts) carry a real tenant_id.
        if (!this.currentTenantId && this.tenants.length > 0) {
          this.currentTenantId = this.tenants[0].id
        }
      } catch (e) {
        this.error = String(e)
      } finally {
        this.loading = false
      }
    },
    setCurrentTenant(id: string) {
      this.currentTenantId = id
      saveTenantId(id)
    },
    async create(t: Tenant) {
      const created = await api.createTenant(t)
      this.tenants.push(created)
      if (!this.currentTenantId) this.setCurrentTenant(created.id)
    },
    async update(t: Tenant) {
      await api.updateTenant(t)
      const i = this.tenants.findIndex((x) => x.id === t.id)
      if (i >= 0) this.tenants[i] = { ...t }
    },
    async remove(id: string) {
      await api.deleteTenant(id)
      this.tenants = this.tenants.filter((x) => x.id !== id)
      if (this.currentTenantId === id) {
        this.setCurrentTenant(this.tenants[0]?.id ?? '')
      }
    },
  },
})
