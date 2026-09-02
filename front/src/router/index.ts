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
    path: '/audit',
    name: 'audit',
    component: () => import('../views/AuditListView.vue'),
  },
]

export default createRouter({
  history: createWebHistory(),
  routes,
})
