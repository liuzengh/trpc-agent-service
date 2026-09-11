// Admin API: tenant registry access. The GET also doubles as the login token
// verification because it is the cheapest authenticated endpoint.
import { apiJSON } from './client';
import type { TenantListResponse } from '../types';

export function listTenants(): Promise<TenantListResponse> {
  return apiJSON<TenantListResponse>('/admin/v1/tenants');
}

export function reloadConfig(): Promise<{ status: string; releases?: unknown[] }> {
  return apiJSON<{ status: string }>('/admin/v1/reload', { method: 'POST' });
}

export function rollbackTenant(tenant: string, body?: { target_revision?: string; reason?: string }): Promise<{ status: string; release?: unknown }> {
  return apiJSON<{ status: string; release?: unknown }>(
    `/admin/v1/tenants/${encodeURIComponent(tenant)}/rollback`,
    { method: 'POST', body: body ? JSON.stringify(body) : undefined },
  );
}

export function listTenantRevisions(tenant: string): Promise<{ revisions: unknown[] }> {
  return apiJSON<{ revisions: unknown[] }>(`/admin/v1/tenants/${encodeURIComponent(tenant)}/revisions`);
}

export function getTenantRelease(tenant: string, release: string): Promise<{ release: unknown }> {
  return apiJSON<{ release: unknown }>(
    `/admin/v1/tenants/${encodeURIComponent(tenant)}/releases/${encodeURIComponent(release)}`,
  );
}

export function listConfigNodes(): Promise<{ nodes: unknown[] }> {
  return apiJSON<{ nodes: unknown[] }>('/admin/v1/config/nodes');
}
