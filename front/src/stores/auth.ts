/*
 * Auth Store
 *
 * 管理用户认证状态和权限信息
 * 职责：登录、登出、权限检查、token 管理
 */

import { computed, ref } from 'vue'
import { defineStore } from 'pinia'
import api from '../api'

export interface AuthUser {
  tenant_id: string
  user_id: string
  role: string
}

// 从 localStorage 恢复状态
function getStoredAuth(): { token: string; user: AuthUser | null } {
  const token = localStorage.getItem('auth_token') || ''
  const userStr = localStorage.getItem('auth_user')
  let user = null
  if (userStr) {
    try {
      user = JSON.parse(userStr)
    } catch {
      user = null
    }
  }
  return { token, user: user as AuthUser | null }
}

// 存储状态到 localStorage
function setStoredAuth(token: string, user: AuthUser) {
  localStorage.setItem('auth_token', token)
  localStorage.setItem('auth_user', JSON.stringify(user))
}

// 清除存储
function clearStoredAuth() {
  localStorage.removeItem('auth_token')
  localStorage.removeItem('auth_user')
}

export const useAuthStore = defineStore('auth', () => {
  const initialAuth = getStoredAuth()
  const token = ref(initialAuth.token)
  const user = ref<AuthUser | null>(initialAuth.user)
  const sessionValidated = ref(false)
  const loggingIn = ref(false)

  const login = async (userId: string, password: string) => {
    loggingIn.value = true
    try {
      const response = await api.post('/auth/login', {
        user_id: userId,
        password
      })

      if (response.data.token && response.data.member) {
        token.value = response.data.token
        user.value = response.data.member
        sessionValidated.value = true
        setStoredAuth(token.value, user.value)
        loggingIn.value = false
        return { success: true }
      }
      loggingIn.value = false
      return { success: false, error: '登录失败' }
    } catch (error: any) {
      loggingIn.value = false
      return {
        success: false,
        error: error.response?.data?.error || '登录失败'
      }
    }
  }

  // 登出
  const logout = () => {
    token.value = ''
    user.value = null
    sessionValidated.value = false
    loggingIn.value = false
    clearStoredAuth()
  }

  const ensureSession = async () => {
    if (!token.value) return false
    if (sessionValidated.value) return true
    // During login, don't let the router guard race with the login flow.
    if (loggingIn.value) return false
    try {
      const response = await api.get('/auth/me')
      if (!response.data?.user_id || !response.data?.tenant_id) {
        // Malformed session payload: treat it as invalid rather than leaving a
        // half-populated store behind (the guard would bounce on every reload).
        logout()
        return false
      }
      user.value = response.data
      sessionValidated.value = true
      setStoredAuth(token.value, user.value)
      return true
    } catch (error: any) {
      // The server explicitly rejected the token (expired / rotated secret /
      // deleted member). Clear it so the guard sends the user to /login instead
      // of bouncing forever between the protected route and the login page.
      // Network/CORS errors are NOT a verdict on the token, so the session is
      // kept and a transient outage never logs the user out.
      const status = error?.response?.status
      if (status === 401 || status === 403) {
        logout()
      }
      return false
    }
  }

  // 注册
  const register = async (_tenantId: string, userId: string, password: string, role: string = 'member') => {
    try {
      await api.post('/auth/register', {
        user_id: userId,
        password: password,
        role: role
      })
      return { success: true }
    } catch (error: any) {
      return {
        success: false,
        error: error.response?.data?.error || '注册失败'
      }
    }
  }

  // 检查是否有某个权限
  const hasPermission = (permission: string) => {
    const ROLE_PERMISSIONS = {
      owner: [
        'tenant:manage', 'tenant:read',
        'agent:create', 'agent:read', 'agent:update', 'agent:delete',
        'tool:manage', 'kb:manage', 'skill:manage', 'channel:manage',
        'secret:manage', 'audit:read', 'chat', 'dlq:manage'
      ],
      admin: [
        'tenant:read',
        'agent:create', 'agent:read', 'agent:update',
        'tool:manage', 'kb:manage', 'skill:manage', 'channel:manage',
        'audit:read', 'chat', 'dlq:manage'
      ],
      member: [
        'tenant:read',
        'agent:read', 'chat'
      ]
    }
    return ROLE_PERMISSIONS[user.value?.role || '']?.includes(permission) ?? false
  }

  const isAuthenticated = computed(() => !!token.value)
  const userInfo = computed(() => user.value)
  const userRole = computed(() => user.value?.role || '')
  const tenantId = computed(() => user.value?.tenant_id || '')
  const isLoggingIn = computed(() => loggingIn.value)

  return {
    token,
    user,
    isAuthenticated,
    userInfo,
    userRole,
    tenantId,
    isLoggingIn,
    login,
    logout,
    ensureSession,
    register,
    hasPermission,
  }
})
