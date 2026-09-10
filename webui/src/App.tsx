import { lazy, Suspense, useCallback, useEffect, useRef, useState } from 'react'
import * as Tabs from '@radix-ui/react-tabs'
import { useQuery } from '@tanstack/react-query'
import {
  beginLogin,
  changeLocalPassword,
  getLoginProviders,
  getMe,
  loginLocal,
  registerLocal,
  postLogout,
  setUnauthorizedHandler,
  type LoginProvider,
  type SessionUser,
} from './api'
import { AppProvider, useAppContext } from './context'
import { useDismissibleLayer } from './hooks/useDismissibleLayer'
import { ChatWorkspaceProvider } from './chatState'
import { ChatPage } from './pages/ChatPage'
import { ChannelBrandIcon } from './components/ChannelBrand'
import { FeedbackBanner } from './components/FeedbackBanner'
import { RefreshButton } from './components/RefreshButton'
import { FeishuQRPanel } from './components/FeishuQRPanel'
import {
  AccountIcon,
  ActivityIcon,
  BotIcon,
  ChatIcon,
  CpuIcon,
  DatabaseIcon,
  ChevronDownIcon,
  LogoutIcon,
  ServerIcon,
  SettingsIcon,
  ShieldIcon,
  SparklesIcon,
} from './components/Icons'
import { SelectControl } from './components/SelectControl'
import { PageErrorBoundary } from './components/PageErrorBoundary'
import { canManageMembers, canManageTenant, tenantRole, tenantRoleLabel } from './access'

const BotsPage = lazy(() => import('./pages/BotsPage').then((module) => ({ default: module.BotsPage })))
const ExecutionsPage = lazy(() => import('./pages/ExecutionsPage').then((module) => ({ default: module.ExecutionsPage })))
const PlatformDataPage = lazy(() => import('./pages/PlatformDataPage').then((module) => ({ default: module.PlatformDataPage })))
const PreferencesPage = lazy(() => import('./pages/PreferencesPage').then((module) => ({ default: module.PreferencesPage })))
const SystemPage = lazy(() => import('./pages/SystemPage').then((module) => ({ default: module.SystemPage })))
const LoginSettingsPage = lazy(() => import('./pages/LoginSettingsPage').then((module) => ({ default: module.LoginSettingsPage })))
const ModelsPage = lazy(() => import('./pages/ModelsPage').then((module) => ({ default: module.ModelsPage })))
const BackendProfilesPage = lazy(() => import('./pages/BackendProfilesPage').then((module) => ({ default: module.BackendProfilesPage })))
const AccountPage = lazy(() => import('./pages/AccountPage').then((module) => ({ default: module.AccountPage })))
const MembersPage = lazy(() => import('./pages/MembersPage').then((module) => ({ default: module.MembersPage })))
const UsersPage = lazy(() => import('./pages/UsersPage').then((module) => ({ default: module.UsersPage })))
const TenantsPage = lazy(() => import('./pages/TenantsPage').then((module) => ({ default: module.TenantsPage })))

type Tab = 'chat' | 'preferences' | 'account' | 'bots' | 'members' | 'tenants' | 'users' | 'executions' | 'data' | 'models' | 'backends' | 'login' | 'system'

type NavScope = 'workspace' | 'tenant' | 'members' | 'system'
type NavItem = { id: Tab; label: string; memberLabel?: string; icon: (props: { size?: number }) => JSX.Element; scope: NavScope }

const NAV: { section: string; items: NavItem[] }[] = [
  {
    section: '工作区',
    items: [
      { id: 'chat', label: '对话', icon: ChatIcon, scope: 'workspace' },
      { id: 'preferences', label: '我的偏好', icon: SparklesIcon, scope: 'workspace' },
    ],
  },
  {
    section: '租户管理',
    items: [
      { id: 'bots', label: '机器人', icon: BotIcon, scope: 'tenant' },
      { id: 'members', label: '成员', icon: AccountIcon, scope: 'members' },
      { id: 'data', label: '知识库', icon: DatabaseIcon, scope: 'tenant' },
      { id: 'executions', label: '执行记录', icon: ActivityIcon, scope: 'tenant' },
    ],
  },
  {
    section: '系统管理',
    items: [
      { id: 'tenants', label: '租户', icon: ShieldIcon, scope: 'system' },
      { id: 'users', label: '用户', icon: AccountIcon, scope: 'system' },
      { id: 'models', label: '模型资产', icon: CpuIcon, scope: 'system' },
      { id: 'backends', label: '数据后端', icon: DatabaseIcon, scope: 'system' },
      { id: 'login', label: '登录设置', icon: SettingsIcon, scope: 'system' },
      { id: 'system', label: '系统状态', icon: ServerIcon, scope: 'system' },
    ],
  },
]

const TAB_META: Record<Tab, { section: string; title: string }> = {
  chat: { section: '工作区', title: '对话' },
  preferences: { section: '工作区', title: '我的偏好' },
  account: { section: '账号', title: '账号设置' },
  bots: { section: '租户管理', title: '机器人' },
  members: { section: '租户管理', title: '成员' },
  tenants: { section: '系统管理', title: '租户' },
  users: { section: '系统管理', title: '用户' },
  data: { section: '租户管理', title: '知识库' },
  executions: { section: '租户管理', title: '执行记录' },
  models: { section: '系统管理', title: '模型资产' },
  backends: { section: '系统管理', title: '数据后端' },
  login: { section: '系统管理', title: '登录设置' },
  system: { section: '系统管理', title: '系统状态' },
}

const APP_SCOPED_TABS = new Set<Tab>(['chat', 'data', 'executions'])
const APP_SWITCHER_TABS = new Set<Tab>(['chat', 'preferences', 'data', 'executions'])

function initialTab(): Tab {
  const requested = new URLSearchParams(window.location.search).get('tab') as Tab | null
  if (requested === 'account' || requested === 'bots') return requested
  if (requested && NAV.some((entry) => entry.items.some((item) => item.id === requested))) return requested
  return 'chat'
}

function visibleNavigation(user: SessionUser, tenant: string) {
  const tenantManager = canManageTenant(user, tenant)
  const tenantAccess = Boolean(tenant) && tenantRole(user, tenant) !== ''
  return NAV.map((group) => ({
    ...group,
    items: group.items.filter((item) => {
      if (item.scope === 'workspace') return tenantAccess
      if (item.scope === 'tenant') return tenantManager
      if (item.scope === 'members') return Boolean(tenant) && canManageMembers(user, tenant)
      return user.is_system_admin
    }),
  })).filter((group) => group.items.length > 0)
}

// LOGIN_ERRORS 映射服务端回调失败原因 → 中文提示。
const LOGIN_ERRORS: Record<string, string> = {
  invalid_state: '登录状态已失效，请重新发起登录',
  exchange_failed: '企业账号授权失败，请重试',
  provider_unavailable: '登录方式暂不可用，请重新选择',
  account_suspended: '当前企业账号已被停用，请联系管理员',
  access_denied: '已取消授权，如需登录请重新扫码',
  qr_expired: '二维码已失效，请刷新后重新扫码',
  qr_failed: '二维码登录失败，请改用跳转登录',
  sdk_unavailable: '二维码组件加载失败，请改用跳转登录',
}

export default function App() {
  const [user, setUser] = useState<SessionUser | null>(null)
  const [checking, setChecking] = useState(true)
  const [tab, setTab] = useState<Tab>(initialTab)
  const userRef = useRef(user)
  userRef.current = user

  // 401 全局拦截：曾认为已登录 → 视为会话过期回登录页；否则静默（初始探测）。
  useEffect(() => {
    setUnauthorizedHandler(() => {
      if (userRef.current) {
        window.location.replace('/console/?expired=1')
        return
      }
      setUser(null)
    })
    return () => setUnauthorizedHandler(null)
  }, [])

  // 启动时校验会话：服务端 session 有效则进入控制台。
  useEffect(() => {
    let alive = true
    getMe()
      .then((me) => {
        if (!alive) return
        setUser(me)
      })
      .catch(() => {
        if (!alive) return
        setUser(null)
      })
      .finally(() => {
        if (alive) setChecking(false)
      })
    return () => {
      alive = false
    }
  }, [])

  const logout = useCallback(async () => {
    try {
      await postLogout()
    } catch {
      // 服务端已无会话时也正常清本地状态。
    }
    setUser(null)
  }, [])

  if (checking) {
    return (
      <div className="login-screen">
        <div className="login-card login-card-plain">
          <div className="login-brand">
            <span className="logo-mark large">
              <SparklesIcon size={20} />
            </span>
            <h1>Agent 平台</h1>
            <p>正在验证登录状态…</p>
          </div>
        </div>
      </div>
    )
  }

  if (!user) {
    return <LoginScreen />
  }

  if (user.must_change_password) {
    return <ForcedPasswordChangeScreen onChanged={setUser} />
  }

  return (
    <AppProvider user={user} refreshUser={async () => setUser(await getMe())}>
      <ChatWorkspaceProvider>
        <Shell tab={tab} setTab={setTab} user={user} onLogout={logout} />
      </ChatWorkspaceProvider>
    </AppProvider>
  )
}

function Shell({
  tab,
  setTab,
  user,
  onLogout,
}: {
  tab: Tab
  setTab: (tab: Tab) => void
  user: SessionUser
  onLogout: () => void
}) {
  const { tenant, tenantsLoading, activeAppKey, setActiveAppKey } = useAppContext()
  const navigationTenant = tenant || user.active_tenant_id || user.tenants?.[0]?.tenant_id || ''
  const navigation = visibleNavigation(user, navigationTenant)
  const tenantManager = canManageTenant(user, navigationTenant)
  const accountMenuRef = useDismissibleLayer<HTMLDetailsElement>({
    onDismiss: (menu) => menu.removeAttribute('open'),
    restoreFocus: (menu) => menu.querySelector<HTMLElement>('summary')?.focus(),
  })
  const fallbackTab = navigation[0]?.items[0]?.id ?? 'account'
  const activeTab = tenantsLoading || tab === 'account' || (tab === 'bots' && tenantManager) || navigation.some((group) => group.items.some((item) => item.id === tab)) ? tab : fallbackTab

  useEffect(() => {
    if (!tenantsLoading && activeTab !== tab) {
      setTab(fallbackTab)
    }
  }, [activeTab, fallbackTab, setTab, tab, tenantsLoading])

  const itemLabel = (item: NavItem) => !tenantManager && item.memberLabel ? item.memberLabel : item.label
  return (
    <div className="shell">
      <aside className="sidebar">
        <div className="sidebar-brand">
          <span className="logo-mark">
            <SparklesIcon size={16} />
          </span>
          <div>
            <div className="logo-title">Agent 平台</div>
          </div>
        </div>
        <nav className="sidebar-nav" aria-label="主导航">
          <Tabs.Root value={activeTab} orientation="vertical" onValueChange={(value) => setTab(value as Tab)}>
            <Tabs.List className="sidebar-nav-list">
              {navigation.map((group) => (
                <div key={group.section} className="nav-group">
                  <div className="nav-section">{group.section}</div>
                  {group.items.map((item) => {
                    const Icon = item.icon
                    return (
                      <Tabs.Trigger
                        key={item.id}
                        value={item.id}
                        aria-current={activeTab === item.id ? 'page' : undefined}
                        className={activeTab === item.id ? 'nav-item active' : 'nav-item'}
                      >
                        <Icon size={18} />
                        <span>{itemLabel(item)}</span>
                      </Tabs.Trigger>
                    )
                  })}
                </div>
              ))}
            </Tabs.List>
          </Tabs.Root>
        </nav>
        <div className="sidebar-footer">
          <details className="sidebar-account" ref={accountMenuRef}>
            <summary className="sidebar-user" aria-label="打开账号菜单">
              <span className="avatar avatar-sm">P</span>
              <span className="sidebar-user-name" title={user.email || `平台用户 ${user.platform_user_id}`}>
                {user.display_name || user.email || '平台用户'}
              </span>
              <span className="sidebar-user-role" title={tenantRoleLabel(user, tenant)}>
                <ShieldIcon size={12} />
                {tenantRoleLabel(user, tenant)}
              </span>
              <ChevronDownIcon className="sidebar-account-chevron" size={14} />
            </summary>
            <div className="sidebar-account-menu">
              <button
                type="button"
                className="sidebar-account-action"
                onClick={() => {
                  setTab('account')
                  accountMenuRef.current?.removeAttribute('open')
                }}
              >
                <SettingsIcon size={16} /> 账号设置
              </button>
              <button type="button" className="sidebar-account-action danger" onClick={onLogout}>
                <LogoutIcon size={16} /> 退出登录
              </button>
            </div>
          </details>
        </div>
      </aside>

      <div className="main">
        <Header
          section={TAB_META[activeTab].section}
          title={TAB_META[activeTab].title}
          showAppSwitcher={APP_SWITCHER_TABS.has(activeTab)}
          showAppSettings={APP_SCOPED_TABS.has(activeTab) && tenantManager}
          onOpenAppSettings={() => setTab('bots')}
          onOpenAccount={() => setTab('account')}
        />
        <main className={`content${activeTab === 'chat' ? ' content-chat' : ''}`}>
          <div hidden={activeTab !== 'chat'}>
            <PageErrorBoundary resetKey={`chat:${activeAppKey}`}>
              <ChatPage />
            </PageErrorBoundary>
          </div>
          {activeTab !== 'chat' && (
            <PageErrorBoundary resetKey={activeTab}>
              <Suspense fallback={<div className="empty-block">正在加载页面…</div>}>
                {activeTab === 'bots' && (
                  <BotsPage
                    onOpenChat={(app) => {
                      setActiveAppKey(`${app.Config.tenant_id}/${app.Config.app_code}`)
                      setTab('chat')
                    }}
                  />
                )}
                {activeTab === 'preferences' && <PreferencesPage />}
                {activeTab === 'account' && <AccountPage />}
                {activeTab === 'members' && <MembersPage />}
                {activeTab === 'tenants' && <TenantsPage />}
                {activeTab === 'users' && <UsersPage />}
                {activeTab === 'executions' && <ExecutionsPage />}
                {activeTab === 'data' && <PlatformDataPage />}
                {activeTab === 'models' && <ModelsPage />}
                {activeTab === 'backends' && <BackendProfilesPage />}
                {activeTab === 'login' && <LoginSettingsPage />}
                {activeTab === 'system' && <SystemPage />}
              </Suspense>
            </PageErrorBoundary>
          )}
        </main>
      </div>

      {/* 移动端底部导航（≤900px 时替代侧栏） */}
      <nav className="mobile-nav" aria-label="移动导航">
        <Tabs.Root value={activeTab} onValueChange={(value) => setTab(value as Tab)}>
          <Tabs.List className="mobile-nav-list">
            {navigation.flatMap((group) => group.items).map((item) => {
              const Icon = item.icon
              return (
                <Tabs.Trigger
                  key={item.id}
                  value={item.id}
                  aria-current={activeTab === item.id ? 'page' : undefined}
                  className={activeTab === item.id ? 'mobile-nav-item active' : 'mobile-nav-item'}
                >
                  <Icon size={20} />
                  <span>{itemLabel(item)}</span>
                </Tabs.Trigger>
              )
            })}
          </Tabs.List>
        </Tabs.Root>
      </nav>
    </div>
  )
}

function Header({
  section,
  title,
  showAppSwitcher,
  showAppSettings,
  onOpenAppSettings,
  onOpenAccount,
}: {
  section: string
  title: string
  showAppSwitcher: boolean
  showAppSettings: boolean
  onOpenAppSettings: () => void
  onOpenAccount: () => void
}) {
  const { apps, tenant, tenantSummaries, setTenant, activeAppKey, setActiveAppKey } = useAppContext()
  const activeApps = apps.filter((app) => app.Config.status === 'active')
  const tenantApps = activeApps.filter((app) => app.Config.tenant_id === tenant)
  const selectedApp = activeApps.find((app) => `${app.Config.tenant_id}/${app.Config.app_code}` === activeAppKey && app.Config.tenant_id === tenant) ?? tenantApps[0]
  const tenantNames = new Map(tenantSummaries.map((entry) => [entry.tenant_id, entry.display_name || entry.tenant_id]))
  const appGroups = [...new Set(activeApps.map((app) => app.Config.tenant_id))].map((tenantID) => ({
    id: tenantID,
    label: tenantNames.get(tenantID) || tenantID,
    options: activeApps
      .filter((app) => app.Config.tenant_id === tenantID)
      .map((app) => ({
        value: `${app.Config.tenant_id}/${app.Config.app_code}`,
        label: app.Config.app_code,
        leading: <BotIcon size={16} />,
      })),
  }))

  const selectApp = (key: string) => {
    const separator = key.indexOf('/')
    if (separator <= 0) return
    const nextTenant = key.slice(0, separator)
    if (nextTenant !== tenant) setTenant(nextTenant)
    setActiveAppKey(key)
  }

  return (
    <header className="header">
      <div className="page-breadcrumb"><span>{section}</span><span aria-hidden="true">/</span><strong>{title}</strong></div>
      <div className="header-right">
        {showAppSwitcher && (
          <div className="app-select">
            <label className="sr-only" htmlFor="app-switcher">当前机器人</label>
            <SelectControl
              id="app-switcher"
              ariaLabel="当前机器人"
              value={selectedApp ? `${selectedApp.Config.tenant_id}/${selectedApp.Config.app_code}` : ''}
              onValueChange={selectApp}
              disabled={activeApps.length === 0}
              placeholder="暂无机器人"
              groups={appGroups.length > 1 ? appGroups : []}
              options={appGroups.length > 1 ? [] : appGroups[0]?.options ?? []}
              shellClassName="app-switcher-shell"
              leading={<BotIcon size={17} />}
              valueLabel={selectedApp?.Config.app_code}
            />
          </div>
        )}
        {showAppSettings && (
          <button type="button" className="header-app-settings" onClick={onOpenAppSettings} aria-label="机器人设置" title="机器人设置">
            <SettingsIcon size={17} />
          </button>
        )}
        <button type="button" className="header-account-button" onClick={onOpenAccount} aria-label="账号设置" title="账号设置">
          <AccountIcon size={18} />
        </button>
      </div>
    </header>
  )
}

function LoginScreen() {
  const [loadError, setLoadError] = useState('')
  const [busy, setBusy] = useState('')
  const [localUsername, setLocalUsername] = useState('')
  const [localPassword, setLocalPassword] = useState('')
  const [localPasswordConfirm, setLocalPasswordConfirm] = useState('')
  const [localDisplayName, setLocalDisplayName] = useState('')
  const [localEmail, setLocalEmail] = useState('')
  const [localMode, setLocalMode] = useState<'login' | 'register'>('login')

  const providersQuery = useQuery({
    queryKey: ['console', 'login-providers'],
    queryFn: ({ signal }) => getLoginProviders(signal),
    staleTime: 60_000,
  })
  const providers = providersQuery.data ?? []
  const providersError = providersQuery.error instanceof Error ? providersQuery.error.message : ''
  const visibleLoadError = loadError || (providersError ? '无法获取登录配置，请确认服务已启动' : '')

  const params = new URLSearchParams(window.location.search)
  const loginError = params.get('login_error')
  const expired = params.get('expired') === '1'
  const localProvider = providers.find((provider) => provider.type === 'local')
  const enterpriseProviders = providers.filter((provider) => provider.type !== 'local' && provider.type !== 'mock' && provider.enabled)
  const mockProviders = providers.filter((provider) => provider.type === 'mock' && provider.enabled)
  const localEnabled = Boolean(localProvider?.enabled)
  const localRegistrationEnabled = Boolean(localProvider?.registration_enabled)

  const start = async (provider: LoginProvider) => {
    if (!provider.enabled || !provider.provider_id) return
    setBusy(provider.provider_id)
    setLoadError('')
    try {
      const login = await beginLogin(provider.provider_id)
      window.location.href = login.auth_url
    } catch (error) {
      setBusy('')
      setLoadError((error as Error).message)
    }
  }

  const submitLocal = async (event: React.FormEvent) => {
    event.preventDefault()
    if (!localProvider?.enabled || !localUsername.trim() || !localPassword) return
    setBusy('local')
    setLoadError('')
    try {
      await loginLocal(localUsername.trim(), localPassword)
      window.location.reload()
    } catch {
      setBusy('')
      setLoadError('用户名或密码错误')
    }
  }

  const submitLocalRegistration = async (event: React.FormEvent) => {
		event.preventDefault()
		if (!localRegistrationEnabled || !localUsername.trim() || !localPassword) return
		if (localPassword !== localPasswordConfirm) {
			setLoadError('两次输入的密码不一致')
			return
		}
		setBusy('local-register')
		setLoadError('')
		try {
			await registerLocal(localUsername.trim(), localDisplayName.trim(), localEmail.trim(), localPassword)
			window.location.reload()
		} catch (error) {
			setBusy('')
			setLoadError((error as Error).message)
		}
	}

  return (
    <div className="login-screen">
      <div className="login-card">
        <div className="login-brand">
          <span className="login-brand-mark">
            <SparklesIcon size={20} />
          </span>
          <div className="login-brand-copy">
            <span className="login-brand-eyebrow">TRPC AGENT SERVICE</span>
            <h1>登录工作台</h1>
            <p>使用你的平台账号继续。</p>
          </div>
        </div>

        {expired && (
          <FeedbackBanner tone="error">
            <ShieldIcon size={15} /> 会话已过期，请重新登录
          </FeedbackBanner>
        )}
        {loginError && LOGIN_ERRORS[loginError] && (
          <FeedbackBanner tone="error">
            <ShieldIcon size={15} /> {LOGIN_ERRORS[loginError]}
          </FeedbackBanner>
        )}
        {visibleLoadError && (
          <FeedbackBanner tone="error">
            <ShieldIcon size={15} /> <span>{visibleLoadError}</span>
            {providersError && (
              <RefreshButton onClick={() => void providersQuery.refetch()} loading={providersQuery.isFetching} label="重新读取登录方式" />
            )}
          </FeedbackBanner>
        )}

        <div className="login-provider-list">
          {providersQuery.isLoading && <div className="login-provider-loading">正在读取登录方式…</div>}
          {localEnabled && localMode === 'login' && (
            <form id="local-login-form" className="login-local-form" onSubmit={(event) => void submitLocal(event)}>
              <div className="login-local-head">
                <div className="login-provider-copy"><strong>{localProvider?.display_name || '本地账号'}</strong><small>使用用户名和密码登录</small></div>
              </div>
              <input autoComplete="username" value={localUsername} onChange={(event) => setLocalUsername(event.target.value)} placeholder="用户名" aria-label="本地用户名" />
              <input type="password" autoComplete="current-password" value={localPassword} onChange={(event) => setLocalPassword(event.target.value)} placeholder="密码" aria-label="本地密码" />
              <button type="submit" className="primary" disabled={busy !== '' || !localUsername.trim() || !localPassword}>{busy === 'local' ? '登录中…' : '登录'}</button>
				{localRegistrationEnabled && <button type="button" className="text-button login-local-register-toggle" disabled={busy !== ''} onClick={() => { setLocalMode('register'); setLoadError('') }}>注册本地账号</button>}
            </form>
          )}

			{localEnabled && localMode === 'register' && (
				<form id="local-login-form" className="login-local-form" onSubmit={(event) => void submitLocalRegistration(event)}>
					<div className="login-local-head">
						<div className="login-provider-copy"><strong>注册本地账号</strong><small>注册后只创建平台身份，不自动加入任何租户</small></div>
						<button type="button" className="text-button" onClick={() => { setLocalMode('login'); setLoadError('') }} disabled={busy !== ''}>返回登录</button>
					</div>
					<input autoComplete="username" value={localUsername} onChange={(event) => setLocalUsername(event.target.value)} placeholder="用户名（3–64 个字符）" aria-label="注册用户名" />
					<input value={localDisplayName} onChange={(event) => setLocalDisplayName(event.target.value)} placeholder="显示名称（可选）" aria-label="显示名称" />
					<input type="email" value={localEmail} onChange={(event) => setLocalEmail(event.target.value)} placeholder="邮箱（可选）" aria-label="邮箱" />
					<input type="password" autoComplete="new-password" value={localPassword} onChange={(event) => setLocalPassword(event.target.value)} placeholder="密码（至少 12 位）" aria-label="注册密码" />
					<input type="password" autoComplete="new-password" value={localPasswordConfirm} onChange={(event) => setLocalPasswordConfirm(event.target.value)} placeholder="再次输入密码" aria-label="确认注册密码" />
					<button type="submit" className="primary" disabled={busy !== '' || !localUsername.trim() || localPassword.length < 12 || !localPasswordConfirm}>{busy === 'local-register' ? '注册中…' : '注册并登录'}</button>
				</form>
			)}

          {enterpriseProviders.length > 0 && localMode === 'login' && (
            <div className="login-enterprise-block">
              <div className="login-enterprise-divider"><span>或使用企业账号</span></div>
              <div className="login-enterprise-list">
                {enterpriseProviders.map((provider) =>
                  provider.qr_enabled && provider.provider_id ? (
                    <FeishuQRPanel
                      key={`qr-${provider.provider_id}`}
                      provider={provider}
                      busy={busy !== ''}
                      onFallback={() => void start(provider)}
                    />
                  ) : (
                    <button
                      key={provider.provider_id || provider.type}
                      type="button"
                      className="login-enterprise-provider"
                      disabled={busy !== ''}
                      aria-label={`使用 ${provider.display_name} 登录`}
                      title={`使用 ${provider.display_name} 登录`}
                      onClick={() => void start(provider)}
                    >
                      <span className={`login-enterprise-icon login-provider-${provider.type}`}>
                        {provider.type === 'oidc'
                          ? <ShieldIcon size={18} />
                          : <ChannelBrandIcon channel={provider.type} size={20} />}
                      </span>
                      <span>{busy === provider.provider_id ? '跳转中…' : provider.display_name}</span>
                    </button>
                  ),
                )}
              </div>
            </div>
          )}

          {mockProviders.length > 0 && localMode === 'login' && (
            <div className="login-development-access">
              <span>开发测试</span>
              {mockProviders.map((provider) => (
                <button key={provider.provider_id || provider.type} type="button" className="text-button" disabled={busy !== ''} onClick={() => void start(provider)}>
                  {busy === provider.provider_id ? '正在进入…' : '使用 Mock 登录'}
                </button>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

function ForcedPasswordChangeScreen({ onChanged }: { onChanged: (user: SessionUser) => void }) {
  const [currentPassword, setCurrentPassword] = useState('')
  const [newPassword, setNewPassword] = useState('')
  const [confirmPassword, setConfirmPassword] = useState('')
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)

  const submit = async (event: React.FormEvent) => {
    event.preventDefault()
    setError('')
    if (newPassword !== confirmPassword) {
      setError('两次输入的新密码不一致')
      return
    }
    setBusy(true)
    try {
      await changeLocalPassword(currentPassword, newPassword)
      onChanged(await getMe())
    } catch (caught) {
      setError((caught as Error).message)
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="login-screen">
      <form className="login-card login-password-change" onSubmit={(event) => void submit(event)}>
        <div className="login-brand">
          <span className="logo-mark large"><ShieldIcon size={20} /></span>
          <h1>修改临时密码</h1>
          <p>首次登录必须设置只有你知道的新密码，完成后才能进入平台。</p>
        </div>
        {error && <FeedbackBanner tone="error">{error}</FeedbackBanner>}
        <input type="password" autoComplete="current-password" value={currentPassword} onChange={(event) => setCurrentPassword(event.target.value)} placeholder="当前临时密码" aria-label="当前临时密码" />
        <input type="password" autoComplete="new-password" value={newPassword} onChange={(event) => setNewPassword(event.target.value)} placeholder="新密码" aria-label="新密码" />
        <input type="password" autoComplete="new-password" value={confirmPassword} onChange={(event) => setConfirmPassword(event.target.value)} placeholder="再次输入新密码" aria-label="确认新密码" />
        <button type="submit" className="primary" disabled={busy || !currentPassword || !newPassword || !confirmPassword}>{busy ? '正在修改…' : '修改密码并继续'}</button>
      </form>
    </div>
  )
}
