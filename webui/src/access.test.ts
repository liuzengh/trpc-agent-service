import { describe, expect, it } from 'vitest'

import { canManageTenant, tenantRole, tenantRoleLabel } from './access'
import type { SessionUser } from './api'

function user(role: 'member' | 'admin'): SessionUser {
  return {
    platform_user_id: 'u1',
    role,
    is_system_admin: false,
    tenants: [{ tenant_id: 'tenant-a', display_name: 'TrailForge', role }],
  }
}

describe('console access', () => {
  it('keeps ordinary members out of tenant management', () => {
    expect(tenantRole(user('member'), 'tenant-a')).toBe('member')
    expect(canManageTenant(user('member'), 'tenant-a')).toBe(false)
    expect(tenantRoleLabel(user('member'), 'tenant-a')).toBe('租户成员')
  })

	it('allows only tenant admins to manage their tenant', () => {
		expect(canManageTenant(user('admin'), 'tenant-a')).toBe(true)
  })

  it('does not reuse a role across tenant memberships', () => {
    expect(tenantRole(user('admin'), 'tenant-b')).toBe('')
    expect(canManageTenant(user('admin'), 'tenant-b')).toBe(false)
  })

	it('does not turn a system administrator into a tenant administrator', () => {
		const admin: SessionUser = { platform_user_id: 'root', role: '', is_system_admin: true, tenants: [] }
		expect(canManageTenant(admin, 'tenant-new')).toBe(false)
		expect(tenantRoleLabel(admin, 'tenant-new')).toBe('未加入租户')
	})
})
