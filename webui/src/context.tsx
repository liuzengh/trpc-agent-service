import { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getApps, getTenants, setActiveTenantHeader, type SessionUser, type TenantSummary } from './api'
import type { Snapshot } from './types'

interface AppContextValue {
  user: SessionUser | null
  apps: Snapshot[]
  appsLoading: boolean
  appsError: string
  tenants: string[]
  tenantSummaries: TenantSummary[]
  tenantsLoading: boolean
  tenant: string
  setTenant: (tenant: string) => void
  refresh: () => Promise<void>
  activeAppKey: string
  setActiveAppKey: (key: string) => void
  refreshUser: () => Promise<void>
}

type AppContextHotData = {
  appContext?: ReturnType<typeof createContext<AppContextValue | null>>
}

// Vite can keep a lazily loaded page from the previous HMR graph while this
// module is replaced. Persist the context token across HMR updates so provider
// and consumers never end up holding two different Context objects.
const hotData = import.meta.hot?.data as AppContextHotData | undefined
const AppContext = hotData?.appContext ?? createContext<AppContextValue | null>(null)
if (import.meta.hot) {
  ;(import.meta.hot.data as AppContextHotData).appContext = AppContext
}

export const STORAGE_KEY_ACTIVE_APP = 'trpc_agent_active_app'
export const STORAGE_KEY_ACTIVE_TENANT = 'trpc_agent_active_tenant'

export function getInitialTenant(user?: SessionUser | null): string {
  if (typeof window !== 'undefined') {
    const searchParams = new URLSearchParams(window.location.search)
    const urlTenant = searchParams.get('tenant')
    if (urlTenant) return urlTenant
    const urlApp = searchParams.get('app')
    if (urlApp && urlApp.includes('/')) {
      return urlApp.split('/')[0]
    }
    try {
      const stored = localStorage.getItem(STORAGE_KEY_ACTIVE_TENANT)
      if (stored) return stored
    } catch {}
  }
  return user?.active_tenant_id || user?.tenants?.[0]?.tenant_id || ''
}

export function getInitialActiveAppKey(): string {
  if (typeof window !== 'undefined') {
    const searchParams = new URLSearchParams(window.location.search)
    const urlApp = searchParams.get('app')
    if (urlApp) return urlApp
    try {
      const stored = localStorage.getItem(STORAGE_KEY_ACTIVE_APP)
      if (stored) return stored
    } catch {}
  }
  return ''
}

export function AppProvider({ children, user, refreshUser }: { children: React.ReactNode; user?: SessionUser | null; refreshUser?: () => Promise<void> }) {
  const [tenant, setTenantState] = useState(() => {
    const initial = getInitialTenant(user)
    if (initial) setActiveTenantHeader(initial)
    return initial
  })
  const [requestedActiveAppKey, setRequestedActiveAppKey] = useState(getInitialActiveAppKey)

  const {
    data: apps = [],
    error: appsQueryError,
    isLoading: appsLoading,
    refetch: refetchApps,
  } = useQuery({
    queryKey: ['console', 'apps'],
    queryFn: ({ signal }) => getApps(signal),
  })
  const {
    data: tenantSummaries = [],
    error: tenantsQueryError,
    isLoading: tenantsLoading,
    refetch: refetchTenants,
  } = useQuery({
    queryKey: ['console', 'tenants'],
    queryFn: ({ signal }) => getTenants(signal),
  })

  const setTenant = useCallback((nextTenant: string) => {
    setTenantState(nextTenant)
    setActiveTenantHeader(nextTenant)
    try {
      if (nextTenant) {
        localStorage.setItem(STORAGE_KEY_ACTIVE_TENANT, nextTenant)
      } else {
        localStorage.removeItem(STORAGE_KEY_ACTIVE_TENANT)
      }
    } catch {}
  }, [])

  const tenants = useMemo(() => {
    return tenantSummaries.map((entry) => entry.tenant_id)
  }, [tenantSummaries])

  const queryError = appsQueryError ?? tenantsQueryError
  const appsError = queryError instanceof Error ? queryError.message : ''

  useEffect(() => {
    if (tenantSummaries.length === 0) return
    const known = new Set(tenantSummaries.map((entry) => entry.tenant_id))
    const nextTenant = tenant && known.has(tenant)
      ? tenant
      : user?.active_tenant_id && known.has(user.active_tenant_id)
        ? user.active_tenant_id
        : tenantSummaries[0]?.tenant_id ?? ''
    if (nextTenant !== tenant) setTenantState(nextTenant)
    setActiveTenantHeader(nextTenant)
    if (nextTenant) {
      try {
        localStorage.setItem(STORAGE_KEY_ACTIVE_TENANT, nextTenant)
      } catch {}
    }
  }, [tenant, tenantSummaries, user])

  const refresh = useCallback(async () => {
    const [appsResult, tenantsResult] = await Promise.all([refetchApps(), refetchTenants()])
    const error = appsResult.error ?? tenantsResult.error
    if (error) throw error
  }, [refetchApps, refetchTenants])

  const activeAppKey = useMemo(() => {
    if (appsLoading && requestedActiveAppKey) {
      return requestedActiveAppKey
    }
    return resolveActiveAppKey(apps, tenant, requestedActiveAppKey)
  }, [apps, appsLoading, requestedActiveAppKey, tenant])

  useEffect(() => {
    if (appsLoading) return
    if (apps.length === 0) return
    if (activeAppKey && requestedActiveAppKey !== activeAppKey) {
      setRequestedActiveAppKey(activeAppKey)
    }
    if (activeAppKey) {
      try {
        localStorage.setItem(STORAGE_KEY_ACTIVE_APP, activeAppKey)
      } catch {}
    }
  }, [activeAppKey, apps.length, appsLoading, requestedActiveAppKey])

  const setActiveAppKey = useCallback((key: string) => {
    setRequestedActiveAppKey(key)
    try {
      if (key) {
        localStorage.setItem(STORAGE_KEY_ACTIVE_APP, key)
      } else {
        localStorage.removeItem(STORAGE_KEY_ACTIVE_APP)
      }
    } catch {}
  }, [])

  const value = useMemo<AppContextValue>(
    () => ({ user: user ?? null, apps, appsLoading, appsError, tenants, tenantSummaries, tenantsLoading, tenant, setTenant, refresh, activeAppKey, setActiveAppKey, refreshUser: refreshUser ?? (async () => {}) }),
    [user, apps, appsLoading, appsError, tenants, tenantSummaries, tenantsLoading, tenant, setTenant, refresh, activeAppKey, refreshUser],
  )
  return <AppContext.Provider value={value}>{children}</AppContext.Provider>
}

export function resolveActiveAppKey(apps: Snapshot[], tenant: string, requestedKey: string): string {
  const tenantApps = apps.filter((app) => app.Config.tenant_id === tenant && app.Config.status === 'active')
  if (requestedKey) {
    const exact = tenantApps.find((app) => `${app.Config.tenant_id}/${app.Config.app_code}` === requestedKey)
    if (exact) return `${exact.Config.tenant_id}/${exact.Config.app_code}`

    if (!requestedKey.includes('/')) {
      const codeMatch = tenantApps.find((app) => app.Config.app_code === requestedKey)
      if (codeMatch) return `${codeMatch.Config.tenant_id}/${codeMatch.Config.app_code}`
    }
  }
  const first = tenantApps[0]
  return first ? `${first.Config.tenant_id}/${first.Config.app_code}` : ''
}

export function useAppContext(): AppContextValue {
  const value = useContext(AppContext)
  if (!value) throw new Error('useAppContext must be used inside AppProvider')
  return value
}
