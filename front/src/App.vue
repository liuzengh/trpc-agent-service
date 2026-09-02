<script setup lang="ts">
import { onMounted, watch } from 'vue'
import { useRoute } from 'vue-router'
import { useTenantStore } from './stores/tenant'
import { useSkillStore } from './stores/skill'
import { useKBStore } from './stores/kb'
import { useAgentStore } from './stores/agent'

const route = useRoute()
const tenantStore = useTenantStore()
const skillStore = useSkillStore()
const kbStore = useKBStore()
const agentStore = useAgentStore()

onMounted(() => tenantStore.fetch())

// Switching tenants re-fetches tenant-scoped collections so skills, KBs and
// agent mounts always reflect the current tenant.
watch(
  () => tenantStore.currentTenantId,
  (id) => {
    if (!id) return
    skillStore.fetch()
    kbStore.fetch()
    agentStore.fetch()
  },
)
</script>

<template>
  <div class="layout">
    <aside class="sidebar">
      <div class="logo">Agent 平台</div>
      <div class="tenant-picker">
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
      <router-link to="/" class="nav" :class="{ active: route.path === '/' }">租户管理</router-link>
      <router-link to="/endpoints" class="nav" :class="{ active: route.path === '/endpoints' }">模型端点</router-link>
      <router-link to="/agents" class="nav" :class="{ active: route.path === '/agents' }">Agent 配置</router-link>
      <router-link to="/tools" class="nav" :class="{ active: route.path === '/tools' }">工具目录</router-link>
      <router-link to="/kbs" class="nav" :class="{ active: route.path === '/kbs' }">知识库</router-link>
      <router-link to="/skills" class="nav" :class="{ active: route.path === '/skills' }">Skill 资产</router-link>
      <router-link to="/chat" class="nav" :class="{ active: route.path === '/chat' }">Agent 对话</router-link>
      <router-link to="/history" class="nav" :class="{ active: route.path === '/history' }">会话历史</router-link>
      <router-link to="/channels" class="nav" :class="{ active: route.path === '/channels' }">IM 通道</router-link>
      <router-link to="/audit" class="nav" :class="{ active: route.path === '/audit' }">审计日志</router-link>
      <router-link to="/usage" class="nav" :class="{ active: route.path === '/usage' }">用量计量</router-link>
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
  width: 200px;
  background: #1f2d3d;
  color: #fff;
  padding-top: 16px;
  flex-shrink: 0;
}
.logo {
  padding: 0 20px 16px;
  font-size: 16px;
  font-weight: 600;
}
.tenant-picker {
  padding: 0 12px 12px;
}
.nav {
  display: block;
  padding: 12px 20px;
  color: #c0c4cc;
  text-decoration: none;
  font-size: 14px;
}
.nav:hover {
  color: #fff;
  background: rgba(255, 255, 255, 0.08);
}
.nav.active {
  color: #fff;
  background: #409eff;
}
.content {
  flex: 1;
  background: #f5f7fa;
  min-width: 0;
}
</style>
