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

export function AppProvider({ children, user, refreshUser }: { children: React.ReactNode; user?: SessionUser | null; refreshUser?: () => Promise<void> }) {
  const [tenant, setTenantState] = useState('')
  const [requestedActiveAppKey, setRequestedActiveAppKey] = useState('')

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
  }, [])

  const tenants = useMemo(() => {
    return tenantSummaries.map((entry) => entry.tenant_id)
  }, [tenantSummaries])

  const queryError = appsQueryError ?? tenantsQueryError
  const appsError = queryError instanceof Error ? queryError.message : ''

  useEffect(() => {
    const known = new Set(tenantSummaries.map((entry) => entry.tenant_id))
    const nextTenant = tenant && known.has(tenant)
      ? tenant
      : user?.active_tenant_id && known.has(user.active_tenant_id)
        ? user.active_tenant_id
        : tenantSummaries[0]?.tenant_id ?? ''
    if (nextTenant !== tenant) setTenantState(nextTenant)
    setActiveTenantHeader(nextTenant)
  }, [tenant, tenantSummaries, user])

  const refresh = useCallback(async () => {
    const [appsResult, tenantsResult] = await Promise.all([refetchApps(), refetchTenants()])
    const error = appsResult.error ?? tenantsResult.error
    if (error) throw error
  }, [refetchApps, refetchTenants])

  const activeAppKey = useMemo(
    () => resolveActiveAppKey(apps, tenant, requestedActiveAppKey),
    [apps, requestedActiveAppKey, tenant],
  )

  useEffect(() => {
    if (requestedActiveAppKey !== activeAppKey) setRequestedActiveAppKey(activeAppKey)
  }, [activeAppKey, requestedActiveAppKey])

  const setActiveAppKey = useCallback((key: string) => {
    setRequestedActiveAppKey(key)
  }, [])

  const value = useMemo<AppContextValue>(
    () => ({ user: user ?? null, apps, appsLoading, appsError, tenants, tenantSummaries, tenantsLoading, tenant, setTenant, refresh, activeAppKey, setActiveAppKey, refreshUser: refreshUser ?? (async () => {}) }),
    [user, apps, appsLoading, appsError, tenants, tenantSummaries, tenantsLoading, tenant, setTenant, refresh, activeAppKey, refreshUser],
  )
  return <AppContext.Provider value={value}>{children}</AppContext.Provider>
}

export function resolveActiveAppKey(apps: Snapshot[], tenant: string, requestedKey: string): string {
  const tenantApps = apps.filter((app) => app.Config.tenant_id === tenant && app.Config.status === 'active')
  if (tenantApps.some((app) => `${app.Config.tenant_id}/${app.Config.app_code}` === requestedKey)) return requestedKey
  const first = tenantApps[0]
  return first ? `${first.Config.tenant_id}/${first.Config.app_code}` : ''
}

export function useAppContext(): AppContextValue {
  const value = useContext(AppContext)
  if (!value) throw new Error('useAppContext must be used inside AppProvider')
  return value
}
