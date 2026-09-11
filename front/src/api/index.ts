/*
 * API 请求模块
 *
 * 配置 axios 实例，自动携带 JWT token；会话中途失效（401）时把决定权交给
 * 注册进来的处理器（见 main.ts），本模块自身不做任何跳转与状态写入。
 */

import axios from 'axios'

const baseURL = import.meta.env.VITE_API_BASE ?? 'http://localhost:8080'
export const apiClient = axios.create({
  baseURL: baseURL,
  headers: {
    'Content-Type': 'application/json'
  }
})

// 请求拦截器 - 自动添加 JWT token
apiClient.interceptors.request.use(config => {
  const token = localStorage.getItem('auth_token')
  if (token) {
    config.headers.Authorization = `Bearer ${token}`
  }
  return config
})

/**
 * 会话失效处理器：任何**业务接口**（非 /auth/*）返回 401 时被调用一次，
 * 由调用方决定收尾动作（结束会话 + 回到登录页）。
 *
 * 用回调注册而不是直接 import store/router，避免 api → store → api 的循环依赖。
 */
export type UnauthorizedHandler = (url: string) => void

let unauthorizedHandler: UnauthorizedHandler | null = null

export function setUnauthorizedHandler(handler: UnauthorizedHandler | null): void {
  unauthorizedHandler = handler
}

// 响应拦截器 - 处理 401 未授权
//
// /auth/* 一律不碰：登录、注册、会话校验（GET /auth/me）的 401 属于登录流程
// 本身，由路由守卫与 auth store 处理。历史教训：在这里用 window.location 硬跳
// 转/复位登录状态，会与登录页的初始化请求形成「整页重载死循环」（页面闪烁）。
apiClient.interceptors.response.use(
  response => response,
  error => {
    const url = error?.config?.url || ''
    if (error?.response?.status === 401 && !url.startsWith('/auth/')) {
      unauthorizedHandler?.(url)
    }
    return Promise.reject(error)
  }
)

export default apiClient
