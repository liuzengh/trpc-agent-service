<script setup lang="ts">
import { onMounted, watch, computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useTenantStore } from './stores/tenant'
import { useSkillStore } from './stores/skill'
import { useKBStore } from './stores/kb'
import { useAgentStore } from './stores/agent'
import { useEndpointStore } from './stores/endpoint'
import { useAuthStore } from './stores/auth'

const route = useRoute()
const router = useRouter()
const tenantStore = useTenantStore()
const skillStore = useSkillStore()
const kbStore = useKBStore()
const agentStore = useAgentStore()
const endpointStore = useEndpointStore()
const authStore = useAuthStore()

const isLoginPage = computed(() => route.path === '/login')
// 只有平台级 owner 需要跨租户切换；admin/member 固定属于自己的租户。
const isOwner = computed(() => authStore.userRole === 'owner')

onMounted(() => {
  // Gate on the session, not on the route path: the router resolves the initial
  // route asynchronously, so the login page can still briefly see a stale '/'
  // and fire an unauthenticated request (a spurious 401 on every visit).
  if (authStore.isAuthenticated) {
    tenantStore.fetch()
  }
})

// Switching tenants re-fetches tenant-scoped collections.
// Permission-aware on purpose: each store mirrors the permission its page route
// declares, so a role that cannot read a collection never fires a request that
// the backend would reject (and the member/agent scoping stays coherent).
watch(
  () => tenantStore.currentTenantId,
  (id) => {
    if (!id) return
    if (authStore.hasPermission('skill:manage')) skillStore.fetch()
    if (authStore.hasPermission('kb:manage')) kbStore.fetch()
    if (authStore.hasPermission('agent:read')) agentStore.fetch()
    if (authStore.hasPermission('endpoint:manage')) endpointStore.fetch()
  },
)

// 登出
const handleLogout = () => {
  authStore.logout()
  router.push('/login')
}

// 导航菜单配置（根据权限过滤）
const navItems = computed(() => {
  const items = [
    { path: '/', label: '租户管理', icon: '📊', permission: 'tenant:manage' },
    { path: '/endpoints', label: '模型端点', icon: '🔌', permission: 'endpoint:manage' },
    { path: '/agents', label: 'Agent 配置', icon: '🤖', permission: 'agent:read' },
    { path: '/tools', label: '工具目录', icon: '🔧', permission: 'tool:manage' },
    { path: '/kbs', label: '知识库', icon: '📚', permission: 'kb:manage' },
    { path: '/skills', label: 'Skill 资产', icon: '⚡', permission: 'skill:manage' },
    { path: '/chat', label: 'Agent 对话', icon: '💬', permission: 'chat' },
    { path: '/history', label: '会话历史', icon: '📜', permission: 'agent:read' },
    { path: '/channels', label: 'IM 通道', icon: '🔗', permission: 'channel:manage' },
    { path: '/secrets', label: '密钥管理', icon: '🔐', permission: 'secret:manage' },
    { path: '/audit', label: '审计日志', icon: '📝', permission: 'audit:read' },
    { path: '/usage', label: '用量计量', icon: '📈', permission: 'usage:read' },
    { path: '/users', label: '用户管理', icon: '👥', permission: 'member:manage' },
  ]

  // 根据权限过滤
  return items.filter(item => {
    if (!item.permission) return true
    return authStore.hasPermission(item.permission)
  })
})
</script>

<template>
  <!-- 登录页：无布局 -->
  <div v-if="isLoginPage">
    <router-view />
  </div>

  <!-- 管理页：带侧边栏布局 -->
  <div v-else class="layout">
    <aside class="sidebar">
      <div class="logo">Agent 平台</div>
      <div v-if="isOwner" class="tenant-picker">
        <el-select
          :model-value="tenantStore.currentTenantId"
          placeholder="选择租户"
          size="small"
          style="width: 100%"
          @change="tenantStore.setCurrentTenant"
        >
          <el-option v-for="t in tenantStore.tenants" :key="t.id" :label="t.name" :value="t.id" />
        </el-select>
      </div>
      <div class="nav-items">
        <router-link
          v-for="item in navItems"
          :key="item.path"
          :to="item.path"
          class="nav"
          :class="{ active: route.path === item.path }"
        >
          <span class="nav-icon">{{ item.icon }}</span>
          <span class="nav-label">{{ item.label }}</span>
        </router-link>
      </div>
      <div class="sidebar-footer">
        <div class="user-info">
          <span class="user-role">{{ authStore.userRole }}</span>
          <span class="user-name">{{ authStore.userInfo?.user_id }}</span>
        </div>
        <el-button type="text" @click="handleLogout" class="logout-btn">
          退出登录
        </el-button>
      </div>
    </aside>
    <section class="content">
      <router-view />
    </section>
  </div>
</template>

<style>
body {
  margin: 0;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
}
.layout {
  display: flex;
  min-height: 100vh;
}
.sidebar {
  width: 220px;
  background: #1f2d3d;
  color: #fff;
  padding-top: 16px;
  flex-shrink: 0;
  display: flex;
  flex-direction: column;
}
.logo {
  padding: 0 20px 16px;
  font-size: 16px;
  font-weight: 600;
}
.tenant-picker {
  padding: 0 12px 12px;
}
.nav-items {
  flex: 1;
  overflow-y: auto;
}
.nav {
  display: flex;
  align-items: center;
  padding: 12px 20px;
  color: #c0c4cc;
  text-decoration: none;
  font-size: 14px;
  gap: 8px;
}
.nav:hover {
  color: #fff;
  background: rgba(255, 255, 255, 0.08);
}
.nav.active {
  color: #fff;
  background: #409eff;
}
.nav-icon {
  font-size: 16px;
}
.sidebar-footer {
  padding: 16px;
  border-top: 1px solid rgba(255, 255, 255, 0.1);
}
.user-info {
  display: flex;
  flex-direction: column;
  margin-bottom: 8px;
}
.user-role {
  font-size: 12px;
  color: #909399;
  text-transform: uppercase;
}
.user-name {
  font-size: 14px;
  color: #fff;
}
.logout-btn {
  width: 100%;
  color: #c0c4cc;
}
.logout-btn:hover {
  color: #fff;
}
.content {
  flex: 1;
  background: #f5f7fa;
  min-width: 0;
}
</style>
