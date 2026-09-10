import { beforeEach, describe, expect, it, vi } from 'vitest'
import { apiClient, setUnauthorizedHandler } from './index'

// The request interceptor reads the token from localStorage, so the node test
// environment needs a stub (mirrors stores/auth.spec.ts).
beforeEach(() => {
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
  setUnauthorizedHandler(null)
})

function failWith(status: number, url: string) {
  apiClient.defaults.adapter = async (config) => {
    const error: any = new Error(`Request failed with status code ${status}`)
    error.config = { ...config, url }
    error.response = { status, data: {}, headers: {}, config: { ...config, url } }
    error.isAxiosError = true
    throw error
  }
}

describe('api 401 handling', () => {
  it('hands a business-endpoint 401 to the registered handler', async () => {
    const handler = vi.fn()
    setUnauthorizedHandler(handler)
    failWith(401, '/tenants')

    await expect(apiClient.get('/tenants')).rejects.toBeTruthy()
    expect(handler).toHaveBeenCalledTimes(1)
  })

  // Regression guard: routing /auth/* 401s through the handler is what turns the
  // login flow into a redirect fight (the guard and the handler would each send
  // the user somewhere). The store/guard own that path.
  it('leaves /auth/* 401s to the auth store and router guard', async () => {
    const handler = vi.fn()
    setUnauthorizedHandler(handler)
    failWith(401, '/auth/me')

    await expect(apiClient.get('/auth/me')).rejects.toBeTruthy()
    expect(handler).not.toHaveBeenCalled()
  })

  it('ignores failures that are not 401', async () => {
    const handler = vi.fn()
    setUnauthorizedHandler(handler)
    failWith(500, '/tenants')

    await expect(apiClient.get('/tenants')).rejects.toBeTruthy()
    expect(handler).not.toHaveBeenCalled()
  })

  it('rejects normally when no handler is registered', async () => {
    failWith(401, '/tenants')

    await expect(apiClient.get('/tenants')).rejects.toBeTruthy()
  })
})
