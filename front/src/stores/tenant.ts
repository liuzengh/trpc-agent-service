import { defineStore } from 'pinia'
import * as api from '../api/tenant'
import { useAuthStore } from './auth'

// Re-export the shared tenant types (governance fields included) so views can
// import Tenant from one place without duplicating the shape.
export type { Tenant, TenantQuota, TenantAuditPolicy } from '../api/tenant'
import type { Tenant } from '../api/tenant'

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
    // 非 owner 只能用自己的租户：localStorage 里可能残留上一个登录者的选择，
    // 一旦沿用它会让带 tenant_id 的列表请求落到别的租户（后端会强制改回，
    // 但界面会出现「查不到东西」的假象），因此这里总是以登录态为准。
    syncToSession() {
      const auth = useAuthStore()
      if (auth.userRole === 'owner') return
      const own = auth.tenantId
      if (own && this.currentTenantId !== own) {
        this.currentTenantId = own
        saveTenantId(own)
      }
    },
    async fetch() {
      this.loading = true
      this.error = ''
      try {
        this.syncToSession()
        // 后端对非 owner 只返回其所属租户，owner 返回全部。
        this.tenants = await api.listTenants()
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
      // Trust the backend's normalized copy over the local payload (e.g.
      // nil/null JSON fields are omitted server-side and rehydrated on read).
      const updated = await api.updateTenant(t)
      const i = this.tenants.findIndex((x) => x.id === t.id)
      if (i >= 0) this.tenants[i] = updated
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
