/*
 * 路由表
 *
 * 单独成模块，使路由守卫策略可以在没有 DOM 的单元测试中直接引用真实路由。
 */

import type { RouteRecordRaw } from 'vue-router'

export const LOGIN_PATH = '/login'

export const routes: RouteRecordRaw[] = [
  {
    path: LOGIN_PATH,
    name: 'login',
    component: () => import('../views/LoginView.vue'),
    meta: { requiresAuth: false }
  },
  {
    path: '/',
    name: 'tenants',
    component: () => import('../views/TenantListView.vue'),
    meta: { requiresAuth: true, permission: 'tenant:manage' }
  },
  {
    path: '/agents',
    name: 'agents',
    component: () => import('../views/AgentListView.vue'),
    meta: { requiresAuth: true, permission: 'agent:read' }
  },
  {
    path: '/endpoints',
    name: 'endpoints',
    component: () => import('../views/EndpointListView.vue'),
    meta: { requiresAuth: true, permission: 'agent:read' }
  },
  {
    path: '/tools',
    name: 'tools',
    component: () => import('../views/ToolListView.vue'),
    meta: { requiresAuth: true, permission: 'tool:manage' }
  },
  {
    path: '/kbs',
    name: 'kbs',
    component: () => import('../views/KnowledgeBaseListView.vue'),
    meta: { requiresAuth: true, permission: 'kb:manage' }
  },
  {
    path: '/skills',
    name: 'skills',
    component: () => import('../views/SkillListView.vue'),
    meta: { requiresAuth: true, permission: 'skill:manage' }
  },
  {
    path: '/chat',
    name: 'chat',
    component: () => import('../views/ChatView.vue'),
    meta: { requiresAuth: true, permission: 'chat' }
  },
  {
    path: '/history',
    name: 'history',
    component: () => import('../views/SessionHistoryView.vue'),
    meta: { requiresAuth: true, permission: 'agent:read' }
  },
  {
    path: '/channels',
    name: 'channels',
    component: () => import('../views/ChannelListView.vue'),
    meta: { requiresAuth: true, permission: 'channel:manage' }
  },
  {
    path: '/secrets',
    name: 'secrets',
    component: () => import('../views/SecretListView.vue'),
    meta: { requiresAuth: true, permission: 'secret:manage' }
  },
  {
    path: '/audit',
    name: 'audit',
    component: () => import('../views/AuditListView.vue'),
    meta: { requiresAuth: true, permission: 'audit:read' }
  },
  {
    path: '/usage',
    name: 'usage',
    component: () => import('../views/UsageView.vue'),
    meta: { requiresAuth: true, permission: 'audit:read' }
  },
  {
    path: '/users',
    name: 'users',
    component: () => import('../views/UserManagementView.vue'),
    meta: { requiresAuth: true, permission: 'tenant:manage' }
  },
]
