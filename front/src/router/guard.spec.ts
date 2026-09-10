import { describe, expect, it, vi } from 'vitest'
import type { LocationQueryRaw, RouteLocationNormalized } from 'vue-router'
import { LOGIN_PATH, routes } from './routes'
import { resolveNavigation, safeRedirect, type GuardAuth } from './guard'

type GuardTarget = Pick<RouteLocationNormalized, 'path' | 'fullPath' | 'meta' | 'query'>

function routeTo(path: string, query: LocationQueryRaw = {}): GuardTarget {
  const record = routes.find((r) => r.path === path)
  if (!record) throw new Error(`unknown route ${path}`)
  return { path, fullPath: path, meta: record.meta ?? {}, query } as GuardTarget
}

function auth(overrides: Partial<GuardAuth> = {}): GuardAuth {
  return {
    isAuthenticated: true,
    hasPermission: () => true,
    logout: vi.fn(),
    ...overrides,
  }
}

describe('router guard', () => {
  it('sends an anonymous visitor to /login and remembers the target', () => {
    const decision = resolveNavigation(
      routeTo('/agents'),
      routes,
      auth({ isAuthenticated: false }),
    )

    expect(decision).toEqual({ path: LOGIN_PATH, query: { redirect: '/agents' } })
  })

  it('sends an anonymous visitor hitting / to /login without a redirect query', () => {
    const decision = resolveNavigation(routeTo('/'), routes, auth({ isAuthenticated: false }))

    expect(decision).toEqual({ path: LOGIN_PATH })
  })

  it('bounces a signed-in user off /login to the home page', () => {
    expect(resolveNavigation(routeTo(LOGIN_PATH), routes, auth())).toBe('/')
  })

  it('honours ?redirect= after a signed-in user lands on /login', () => {
    const decision = resolveNavigation(
      routeTo(LOGIN_PATH, { redirect: '/agents' }),
      routes,
      auth(),
    )

    expect(decision).toBe('/agents')
  })

  // Regression: /login?redirect=/login used to redirect to /login again, which
  // vue-router resolves as an endless loop (the page flickers forever).
  it('never redirects /login back to itself', () => {
    for (const raw of ['/login', '/login?redirect=/agents', '/login/foo', 'https://evil.test']) {
      expect(safeRedirect(raw)).toBe('/')
      expect(resolveNavigation(routeTo(LOGIN_PATH, { redirect: raw }), routes, auth())).toBe('/')
    }
  })

  it('leaves an authorised visitor untouched', () => {
    expect(resolveNavigation(routeTo('/agents'), routes, auth())).toBe(true)
  })

  it('falls back to the first reachable page when a permission is missing', () => {
    const decision = resolveNavigation(
      routeTo('/'),
      routes,
      auth({ hasPermission: (permission) => permission === 'agent:read' }),
    )

    expect(decision).toBe('/agents')
  })

  // Regression: when a role could reach no page the old guard redirected to
  // /login while still authenticated, so the login guard sent it back to '/'
  // and the app oscillated between the two routes forever.
  it('ends the session instead of looping when no page is reachable', () => {
    const logout = vi.fn()
    const decision = resolveNavigation(
      routeTo('/'),
      routes,
      auth({ hasPermission: () => false, logout }),
    )

    expect(decision).toBe(LOGIN_PATH)
    expect(logout).toHaveBeenCalledTimes(1)
  })

  it('never bounces a denied route to itself or back to /login', () => {
    const member = auth({
      hasPermission: (permission) => ['tenant:read', 'agent:read', 'chat'].includes(permission),
    })

    for (const record of routes) {
      if (record.path === LOGIN_PATH) continue
      if (!record.meta?.permission || member.hasPermission(record.meta.permission as string)) {
        continue
      }
      const decision = resolveNavigation(routeTo(record.path), routes, member)
      expect(decision).not.toBe(record.path)
      expect(decision).not.toBe(LOGIN_PATH)
    }
  })
})
