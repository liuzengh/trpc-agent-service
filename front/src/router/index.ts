import { createRouter, createWebHistory } from 'vue-router'

const routes = [
  {
    path: '/',
    name: 'tenants',
    component: () => import('../views/TenantListView.vue'),
  },
  {
    path: '/agents',
    name: 'agents',
    component: () => import('../views/AgentListView.vue'),
  },
  {
    path: '/endpoints',
    name: 'endpoints',
    component: () => import('../views/EndpointListView.vue'),
  },
  {
    path: '/tools',
    name: 'tools',
    component: () => import('../views/ToolListView.vue'),
  },
  {
    path: '/kbs',
    name: 'kbs',
    component: () => import('../views/KnowledgeBaseListView.vue'),
  },
  {
    path: '/skills',
    name: 'skills',
    component: () => import('../views/SkillListView.vue'),
  },
  {
    path: '/chat',
    name: 'chat',
    component: () => import('../views/ChatView.vue'),
  },
  {
    path: '/history',
    name: 'history',
    component: () => import('../views/SessionHistoryView.vue'),
  },
  {
    path: '/channels',
    name: 'channels',
    component: () => import('../views/ChannelListView.vue'),
  },
  {
    path: '/secrets',
    name: 'secrets',
    component: () => import('../views/SecretListView.vue'),
  },
  {
    path: '/audit',
    name: 'audit',
    component: () => import('../views/AuditListView.vue'),
  },
  {
    path: '/usage',
    name: 'usage',
    component: () => import('../views/UsageView.vue'),
  },
]

export default createRouter({
  history: createWebHistory(),
  routes,
})
