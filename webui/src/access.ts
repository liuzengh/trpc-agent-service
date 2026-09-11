import type { SessionUser } from './api'

export type TenantRole = 'member' | 'admin'

export function tenantRole(user: SessionUser | null | undefined, tenantID: string): TenantRole | '' {
	if (!user || !tenantID) return ''
	const membership = user.tenants?.find((entry) => entry.tenant_id === tenantID)
	return membership?.role === 'admin' || membership?.role === 'member'
		? membership.role
		: ''
}

export function canManageTenant(user: SessionUser | null | undefined, tenantID: string): boolean {
	return tenantRole(user, tenantID) === 'admin'
}

export function canAdminTenant(user: SessionUser | null | undefined, tenantID: string): boolean {
	return tenantRole(user, tenantID) === 'admin'
}

export function canManageMembers(user: SessionUser | null | undefined, tenantID: string): boolean {
  return canAdminTenant(user, tenantID)
}

export function tenantRoleLabel(user: SessionUser | null | undefined, tenantID: string): string {
	switch (tenantRole(user, tenantID)) {
		case 'admin': return '租户管理员'
		case 'member': return '租户成员'
		default: return '未加入租户'
	}
}
