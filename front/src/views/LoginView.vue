<template>
  <div class="login-container">
    <div class="login-box">
      <h2 class="login-title">Agent 平台登录</h2>
      <el-form :model="form" :rules="rules" ref="formRef" label-width="0">
        <el-form-item prop="userId">
          <el-input
            v-model="form.userId"
            placeholder="用户名"
            prefix-icon="User"
          />
        </el-form-item>
        <el-form-item prop="password">
          <el-input
            v-model="form.password"
            type="password"
            placeholder="密码"
            prefix-icon="Lock"
            @keyup.enter="handleLogin"
          />
        </el-form-item>
        <el-form-item>
          <el-button
            type="primary"
            :loading="loading"
            style="width: 100%"
            @click="handleLogin"
          >
            登录
          </el-button>
        </el-form-item>
      </el-form>
      <div class="login-footer">
        <span>演示账号：admin / admin123</span>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, reactive } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useAuthStore } from '../stores/auth'
import { useTenantStore } from '../stores/tenant'
import { safeRedirect } from '../router/guard'
import { ElMessage } from 'element-plus'

const router = useRouter()
const route = useRoute()
const authStore = useAuthStore()
const tenantStore = useTenantStore()

const formRef = ref()
const loading = ref(false)

const form = reactive({
  userId: 'admin',
  password: 'admin123'
})

const rules = {
  userId: [{ required: true, message: '请输入用户名', trigger: 'blur' }],
  password: [{ required: true, message: '请输入密码', trigger: 'blur' }]
}

const handleLogin = async () => {
  if (!formRef.value) return

  loading.value = true
  try {
    await formRef.value.validate()
    const result = await authStore.login(form.userId, form.password)
    if (!result.success) {
      ElMessage.error(result.error || '登录失败')
      return
    }

    // The member response is the source of truth for tenant selection. A
    // tenant-list failure must not prevent a successful authentication from
    // reaching the application.
    if (authStore.tenantId) {
      tenantStore.setCurrentTenant(authStore.tenantId)
    }
    void tenantStore.fetch()
    ElMessage.success('登录成功')
    // safeRedirect keeps a hand-crafted ?redirect= from sending us back to the
    // login page (which would bounce again) or to an external URL.
    await router.replace(safeRedirect(route.query.redirect))
  } catch (error: any) {
    ElMessage.error(error?.response?.data?.error || '登录失败')
  } finally {
    loading.value = false
  }
}
</script>

<style scoped>
.login-container {
  display: flex;
  justify-content: center;
  align-items: center;
  min-height: 100vh;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
}

.login-box {
  width: 400px;
  padding: 40px;
  background: white;
  border-radius: 8px;
  box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
}

.login-title {
  text-align: center;
  margin-bottom: 30px;
  color: #333;
  font-size: 24px;
}

.login-footer {
  text-align: center;
  margin-top: 20px;
  color: #999;
  font-size: 12px;
}
</style>
