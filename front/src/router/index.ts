import { createRouter, createWebHistory } from 'vue-router'
import { useAuthStore } from '../stores/auth'
import { resolveNavigation } from './guard'
import { routes } from './routes'

const router = createRouter({
  history: createWebHistory(),
  routes,
})

// 路由守卫：认证 + 粗粒度权限。
// 具体判定规则见 ./guard.ts（纯函数，单测覆盖「不产生重定向死循环」）。
router.beforeEach(async (to) => {
  const authStore = useAuthStore()
  const authenticated = await authStore.ensureSession()

  return resolveNavigation(to, routes, {
    isAuthenticated: authenticated,
    hasPermission: authStore.hasPermission,
    logout: authStore.logout,
  })
})

export default router
