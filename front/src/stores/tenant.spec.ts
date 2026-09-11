import { beforeEach, describe, expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { useAuthStore } from './auth'
import { useTenantStore } from './tenant'
import * as api from '../api/tenant'

vi.mock('../api/tenant', () => ({
  listTenants: vi.fn(),
  createTenant: vi.fn(),
  updateTenant: vi.fn(),
  deleteTenant: vi.fn(),
}))

describe('useTenantStore', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    const values = new Map<string, string>()
    Object.defineProperty(globalThis, 'localStorage', {
      configurable: true,
      value: {
        getItem: (key: string) => values.get(key) ?? null,
        setItem: (key: string, value: string) => values.set(key, value),
        removeItem: (key: string) => values.delete(key),
        clear: () => values.clear(),
      },
    })
    localStorage.clear()
    vi.clearAllMocks()
  })

  it('starts empty', () => {
    const store = useTenantStore()
    expect(store.tenants).toHaveLength(0)
  })

  it('fetch populates tenants from the api', async () => {
    vi.mocked(api.listTenants).mockResolvedValue([{ id: 't1', name: 'acme', status: 'active' }])
    const store = useTenantStore()
    await store.fetch()
    expect(store.tenants).toHaveLength(1)
    expect(store.tenants[0]?.name).toBe('acme')
  })

  it('remove deletes via the api', async () => {
    vi.mocked(api.listTenants).mockResolvedValue([{ id: 't1', name: 'acme', status: 'active' }])
    vi.mocked(api.deleteTenant).mockResolvedValue()
    const store = useTenantStore()
    await store.fetch()
    await store.remove('t1')
    expect(store.tenants).toHaveLength(0)
    expect(api.deleteTenant).toHaveBeenCalledWith('t1')
  })

  // 非 owner 的租户选择必须来自登录态，而不是 localStorage 里可能残留的上一个
  // 登录者的选择——否则带 tenant_id 的列表请求会打到别的租户。
  function signIn(tenantId: string, role: string) {
    localStorage.setItem('auth_token', 'jwt')
    localStorage.setItem('auth_user', JSON.stringify({ tenant_id: tenantId, user_id: 'u', role }))
    return useAuthStore()
  }

  it('pins a member to the tenant in the session, ignoring a stale stored choice', async () => {
    localStorage.setItem('currentTenantId', 'someone-elses-tenant')
    signIn('tenant-a', 'member')
    vi.mocked(api.listTenants).mockResolvedValue([{ id: 'tenant-a', name: 'a' }] as never)

    const store = useTenantStore()
    await store.fetch()

    expect(store.currentTenantId).toBe('tenant-a')
    expect(localStorage.getItem('currentTenantId')).toBe('tenant-a')
  })

  it('pins an admin to its own tenant too', async () => {
    localStorage.setItem('currentTenantId', 'tenant-b')
    signIn('tenant-a', 'admin')
    vi.mocked(api.listTenants).mockResolvedValue([{ id: 'tenant-a', name: 'a' }] as never)

    const store = useTenantStore()
    await store.fetch()

    expect(store.currentTenantId).toBe('tenant-a')
  })

  it('lets the owner keep an explicit cross-tenant selection', async () => {
    localStorage.setItem('currentTenantId', 'tenant-b')
    signIn('tenant-a', 'owner')
    vi.mocked(api.listTenants).mockResolvedValue([
      { id: 'tenant-a', name: 'a' },
      { id: 'tenant-b', name: 'b' },
    ] as never)

    const store = useTenantStore()
    await store.fetch()

    expect(store.currentTenantId).toBe('tenant-b')
  })
})
