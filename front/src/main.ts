import { createApp } from 'vue'
import { createPinia } from 'pinia'
import ElementPlus from 'element-plus'
import 'element-plus/dist/index.css'
import App from './App.vue'
import router from './router'
import { setUnauthorizedHandler } from './api'
import { useAuthStore } from './stores/auth'
import { LOGIN_PATH } from './router/routes'

const app = createApp(App)
app.use(createPinia())
app.use(router)
app.use(ElementPlus)

// 会话中途失效：任意业务接口返回 401（token 过期 / 密钥轮换 / 成员被删）时，
// 结束会话并回到登录页，并记住当前页以便重新登录后原路返回。
// 注册在这里是因为 api 层不能依赖 store 与 router（会形成循环依赖）。
setUnauthorizedHandler(() => {
  const authStore = useAuthStore()
  // 重复/并发的 401 只会生效一次：logout() 之后 isAuthenticated 即为 false。
  if (!authStore.isAuthenticated) return
  const current = router.currentRoute.value
  if (current.path === LOGIN_PATH) return

  const redirect = current.fullPath
  authStore.logout()
  router.replace(redirect === '/' ? { path: LOGIN_PATH } : { path: LOGIN_PATH, query: { redirect } })
})

app.mount('#app')
