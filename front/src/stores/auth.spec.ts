import { beforeEach, describe, expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import api from '../api'
import { useAuthStore } from './auth'

vi.mock('../api', () => ({
  default: {
    get: vi.fn(),
    post: vi.fn(),
  },
}))

describe('auth store', () => {
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

  it('uses the member returned by login and persists its tenant', async () => {
    vi.mocked(api.post).mockResolvedValue({
      data: {
        token: 'jwt',
        member: { tenant_id: 'tenant-a', user_id: 'alice', role: 'member' },
      },
    } as never)

    const store = useAuthStore()
    const result = await store.login('alice', 'secret')

    expect(result.success).toBe(true)
    expect(store.tenantId).toBe('tenant-a')
    expect(store.userInfo?.user_id).toBe('alice')
    expect(JSON.parse(localStorage.getItem('auth_user') || '{}').tenant_id).toBe('tenant-a')
    expect(api.post).toHaveBeenCalledWith('/auth/login', {
      user_id: 'alice',
      password: 'secret',
    })
  })

  it('clears an invalid persisted session during startup validation', async () => {
    localStorage.setItem('auth_token', 'stale-token')
    localStorage.setItem('auth_user', JSON.stringify({
      tenant_id: 'tenant-a',
      user_id: 'alice',
      role: 'member',
    }))
    vi.mocked(api.get).mockRejectedValue({ response: { status: 401 } })

    const store = useAuthStore()
    const result = await store.ensureSession()

    expect(result).toBe(false)
    expect(store.isAuthenticated).toBe(false)
    expect(localStorage.getItem('auth_token')).toBeNull()
    expect(api.get).toHaveBeenCalledWith('/auth/me')
  })

  // A transient network/CORS failure is not a verdict on the token: logging the
  // user out would be worse than showing an offline state.
  it('keeps the session when validation fails for non-auth reasons', async () => {
    localStorage.setItem('auth_token', 'valid-token')
    localStorage.setItem('auth_user', JSON.stringify({
      tenant_id: 'tenant-a',
      user_id: 'alice',
      role: 'member',
    }))
    vi.mocked(api.get).mockRejectedValue(new Error('Network Error'))

    const store = useAuthStore()
    const result = await store.ensureSession()

    expect(result).toBe(false)
    expect(store.isAuthenticated).toBe(true)
    expect(localStorage.getItem('auth_token')).toBe('valid-token')
  })

  it('validates a persisted session once and reuses the result', async () => {
    localStorage.setItem('auth_token', 'valid-token')
    vi.mocked(api.get).mockResolvedValue({
      data: { tenant_id: 'tenant-a', user_id: 'alice', role: 'owner' },
    } as never)

    const store = useAuthStore()
    await store.ensureSession()
    await store.ensureSession()

    expect(api.get).toHaveBeenCalledTimes(1)
    expect(store.userRole).toBe('owner')
  })
})
