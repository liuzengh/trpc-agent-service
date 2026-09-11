// Auth session: admin token + selected tenant + mock user id. Persisted to
// localStorage only when the user opted in ("保持登录").
import { create } from 'zustand';
import type { TenantInfo } from '../types';

const STORAGE_KEY = 'im-console.auth.v1';

interface PersistedAuth {
  token: string;
  tenant: TenantInfo | null;
  userId: string;
}

interface AuthState extends PersistedAuth {
  login: (token: string, tenant: TenantInfo, userId: string, keep: boolean) => void;
  logout: () => void;
  setTenant: (tenant: TenantInfo) => void;
}

function isTenantInfo(value: unknown): value is TenantInfo {
  if (!value || typeof value !== 'object') return false;
  const tenant = value as Partial<TenantInfo>;
  return (
    typeof tenant.tenant_id === 'string' &&
    Boolean(tenant.app && typeof tenant.app.name === 'string') &&
    Boolean(tenant.model && typeof tenant.model.provider === 'string' && typeof tenant.model.name === 'string') &&
    Array.isArray(tenant.channels)
  );
}

function loadPersisted(): PersistedAuth {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return { token: '', tenant: null, userId: '' };
    const parsed = JSON.parse(raw) as Partial<PersistedAuth>;
    if (
      typeof parsed.token === 'string' &&
      typeof parsed.userId === 'string' &&
      (parsed.tenant === null || isTenantInfo(parsed.tenant))
    ) {
      return { token: parsed.token, tenant: parsed.tenant, userId: parsed.userId };
    }
  } catch {
    /* corrupted storage: fall through to empty session */
  }
  return { token: '', tenant: null, userId: '' };
}

function persist(state: PersistedAuth) {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
  } catch {
    /* Storage may be disabled or full; the in-memory login still works. */
  }
}

function removePersisted() {
  try {
    localStorage.removeItem(STORAGE_KEY);
  } catch {
    /* Storage may be disabled. */
  }
}

function hasPersisted(): boolean {
  try {
    return Boolean(localStorage.getItem(STORAGE_KEY));
  } catch {
    return false;
  }
}

export const useAuthStore = create<AuthState>((set, get) => ({
  ...loadPersisted(),

  login: (token, tenant, userId, keep) => {
    if (keep) persist({ token, tenant, userId });
    else removePersisted();
    set({ token, tenant, userId });
  },

  logout: () => {
    removePersisted();
    set({ token: '', tenant: null, userId: '' });
  },

  setTenant: (tenant) => {
    set({ tenant });
    const { token, userId } = get();
    if (token && hasPersisted()) {
      persist({ token, tenant, userId });
    }
  },
}));

export function isAuthenticated(): boolean {
  const { token, tenant, userId } = useAuthStore.getState();
  return Boolean(token && tenant && userId);
}

export function isAuthPersisted(): boolean {
  return hasPersisted();
}
