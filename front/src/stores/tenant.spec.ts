import { beforeEach, describe, expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
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
})
