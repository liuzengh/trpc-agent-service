/*
 * 路由守卫策略
 *
 * 从 router 中抽出成纯函数，便于单元测试（不需要 DOM / 真实 router）。
 * 重点是保证认证失败与权限不足时「收敛」到登录页或可访问页，
 * 绝不会出现 /login ↔ 受保护页 之间的无限重定向（表现为页面反复闪烁）。
 */

import type { RouteLocationNormalized, RouteRecordRaw } from 'vue-router'
import { LOGIN_PATH } from './routes'

export interface GuardAuth {
  isAuthenticated: boolean
  hasPermission: (permission: string) => boolean
  logout: () => void
}

export type GuardDecision = true | string | { path: string; query?: Record<string, string> }

// Only in-app absolute paths are accepted as a post-login target; anything else
// (external URL, empty, /login itself) falls back to the home page so the login
// page can never redirect to itself.
export function safeRedirect(raw: unknown): string {
  if (typeof raw !== 'string') return '/'
  if (!raw.startsWith('/') || raw.startsWith('//')) return '/'
  if (raw === LOGIN_PATH || raw.startsWith(`${LOGIN_PATH}?`) || raw.startsWith(`${LOGIN_PATH}/`)) {
    return '/'
  }
  return raw
}

export function resolveNavigation(
  to: Pick<RouteLocationNormalized, 'path' | 'fullPath' | 'meta' | 'query'>,
  routes: RouteRecordRaw[],
  auth: GuardAuth,
): GuardDecision {
  // Public routes: only /login needs the "already signed in" bounce.
  if (to.meta.requiresAuth === false) {
    if (to.path !== LOGIN_PATH || !auth.isAuthenticated) return true
    return safeRedirect(to.query.redirect)
  }

  if (!auth.isAuthenticated) {
    return to.fullPath === '/'
      ? { path: LOGIN_PATH }
      : { path: LOGIN_PATH, query: { redirect: to.fullPath } }
  }

  const permission = to.meta.permission as string | undefined
  if (permission && !auth.hasPermission(permission)) {
    const fallback = routes.find((route) => {
      const routePermission = route.meta?.permission as string | undefined
      return (
        route.path !== LOGIN_PATH &&
        route.path !== to.path &&
        (!routePermission || auth.hasPermission(routePermission))
      )
    })
    if (fallback) return fallback.path
    // This role can reach no page at all. Drop the session and land on /login:
    // redirecting while still "authenticated" would send the guard straight back
    // here, looping forever.
    auth.logout()
    return LOGIN_PATH
  }

  return true
}
