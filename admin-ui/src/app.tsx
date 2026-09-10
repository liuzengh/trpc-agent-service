import { createContext, useContext, useEffect, useState } from 'react'
import {
  Alert,
  Button,
  Card,
  Dialog,
  Drawer,
  Progress,
  Table as TDesignTable,
  Tag,
  Tabs,
} from 'tdesign-react'
import {
  BrowserRouter,
  Link,
  Navigate,
  Route,
  Routes,
  useLocation,
  useNavigate,
  useParams,
  useSearchParams,
} from 'react-router-dom'
import {
  AgentApp,
  AppConfig,
  AppConfigView,
  Approval,
  AuditEvent,
  BackendRef,
  Binding,
  Execution,
  get,
  getToken,
  Migration,
  Operations,
  post,
  RecentError,
  saveToken,
  SessionInfo,
  Tenant,
  clearToken,
} from './api'

const navItems = [
  ['总览', '/'],
  ['租户', '/tenants'],
  ['Agent 应用', '/agent-apps'],
  ['配置版本', '/config-versions'],
  ['后端', '/backends'],
  ['通道', '/channels'],
  ['执行记录', '/executions'],
  ['审批', '/approvals'],
  ['数据迁移', '/migrations'],
  ['审计日志', '/audit'],
  ['运行状态', '/operations'],
] as const

const AdminSessionContext = createContext<SessionInfo | null>(null)

type Columns = { colKey: string; title: string; cell?: (h: unknown, row: any) => React.ReactNode }[]

// TDesign passes one context object to cell renderers. Keep the page column
// definitions small and adapt that framework detail in one place.
function Table(props: any) {
  const columns = (props.columns ?? []).map((column: any) => ({
    ...column,
    cell: typeof column.cell === 'function' ? (context: any) => column.cell(undefined, context.row) : column.cell,
  }))
  return <TDesignTable {...props} columns={columns} empty={props.empty ?? <EmptyState message="暂无数据" />} />
}

export function AdminApp() {
  const [session, setSession] = useState<SessionInfo | null>(null)
  const [checking, setChecking] = useState(Boolean(getToken()))

  useEffect(() => {
    if (!getToken()) return
    get<SessionInfo>('/admin/v1/session')
      .then(setSession)
      .catch(() => {
        clearToken()
        setSession(null)
      })
      .finally(() => setChecking(false))
  }, [])

  if (checking) return <div className="center-page">正在检查管理员会话…</div>
  if (!session) return <Login onLogin={setSession} />
  return (
    <BrowserRouter>
      <Shell session={session} onLogout={() => { clearToken(); setSession(null) }} />
    </BrowserRouter>
  )
}

function Login({ onLogin }: { onLogin: (session: SessionInfo) => void }) {
  const [token, setToken] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  async function submit(event: React.FormEvent) {
    event.preventDefault()
    if (!token.trim()) { setError('管理员 Token 不能为空'); return }
    setBusy(true); setError(''); saveToken(token.trim())
    try {
      onLogin(await get<SessionInfo>('/admin/v1/session'))
    } catch (cause) {
      clearToken()
      setError(cause instanceof Error ? cause.message : '登录失败')
    } finally { setBusy(false) }
  }
  return (
    <main className="center-page login-page">
      <Card title="Agent Platform Admin" bordered>
        <p className="muted">使用控制面的 Admin Token 登录。</p>
        <form onSubmit={submit} className="stack-form">
          <label>Admin Token<input data-testid="admin-token" type="password" autoComplete="off" value={token} onChange={e => setToken(e.target.value)} /></label>
          {error && <Alert theme="error" message={error} />}
          <Button type="submit" theme="primary" loading={busy} block>登录</Button>
        </form>
      </Card>
    </main>
  )
}

function Shell({ session, onLogout }: { session: SessionInfo; onLogout: () => void }) {
  const location = useLocation()
  const navigate = useNavigate()
  const active = location.pathname === '/' ? '/' : `/${location.pathname.split('/')[1]}`
  return (
    <AdminSessionContext.Provider value={session}>
      <div className="app-shell">
      <header className="topbar">
        <div className="brand">Agent Platform <span>Admin</span></div>
        <div className="topbar-actions"><span className="role-pill">{adminRoleLabel(session.role)}</span><span className="actor">{display(session.actor_id)}</span><Button variant="text" onClick={onLogout}>退出登录</Button></div>
      </header>
      <aside className="sidebar">
        <nav aria-label="Admin navigation">
          {navItems.map(([label, path]) => <button key={path} className={active === path ? 'nav-item active' : 'nav-item'} onClick={() => navigate(path === '/' ? '/' : `${path}${location.search}`)}>{label}</button>)}
        </nav>
      </aside>
      <main className="main-content"><Routes>
        <Route path="/" element={<OverviewPage />} />
        <Route path="/tenants" element={<TenantsPage />} />
        <Route path="/tenants/:tenantId" element={<TenantDetailPage />} />
        <Route path="/agent-apps" element={<AgentAppsPage />} />
        <Route path="/agent-apps/:tenantId/:appId" element={<AgentAppDetailPage />} />
        <Route path="/config-versions" element={<ConfigVersionsPage />} />
        <Route path="/backends" element={<BackendsPage />} />
        <Route path="/channels" element={<ChannelsPage />} />
        <Route path="/executions" element={<ExecutionsPage />} />
        <Route path="/approvals" element={<ApprovalsPage />} />
        <Route path="/migrations" element={<MigrationsPage />} />
        <Route path="/audit" element={<AuditPage />} />
        <Route path="/operations" element={<OperationsPage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes></main>
      </div>
    </AdminSessionContext.Provider>
  )
}

function useCanMutate() {
  return useContext(AdminSessionContext)?.role !== 'auditor'
}

function useData<T>(path: string, enabled = true) {
  const [state, setState] = useState<{ data?: T; loading: boolean; error?: string }>({ loading: enabled })
  useEffect(() => {
    let live = true
    if (!enabled) { setState({ loading: false }); return () => { live = false } }
    setState({ loading: true })
    get<T>(path).then(data => live && setState({ data, loading: false })).catch(error => live && setState({ loading: false, error: error instanceof Error ? error.message : 'Request failed' }))
    return () => { live = false }
  }, [path, enabled])
  return state
}

function Page({ title, subtitle, children, actions }: { title: string; subtitle?: string; children: React.ReactNode; actions?: React.ReactNode }) {
  return <section className="page"><div className="page-heading"><div><h1>{title}</h1>{subtitle && <p className="muted">{subtitle}</p>}</div>{actions}</div>{children}</section>
}

const statusLabels: Record<string, string> = {
  READY: '就绪',
  NOT_READY: '未就绪',
  RUNNING: '运行中',
  PENDING: '等待中',
  QUEUED: '排队中',
  FAILED: '失败',
  SUCCEEDED: '成功',
  SUSPENDED: '已暂停',
  UNKNOWN: '未知',
  ACTIVE: '启用',
  APPROVED: '已批准',
  DENIED: '已拒绝',
  ALLOW: '允许',
  ASK: '需审批',
  EXPIRED: '已过期',
  DEGRADED: '降级',
  UNCERTAIN: '待确认',
  PAUSED: '已暂停',
  COPYING: '复制中',
  VERIFYING: '校验中',
  RETRYING: '重试中',
  CANCELLED: '已取消',
  CANCELED: '已取消',
  DRAINING: '排空中',
  PUBLISHED: '已发布',
  DISABLED: '未启用',
  CONFIGURED: '已配置',
  UNAVAILABLE: '暂不可用',
}

function statusKey(status: unknown) {
  if (typeof status !== 'string' || !status.trim()) return 'UNKNOWN'
  return status.trim().toUpperCase().replace(/[\s-]+/g, '_')
}

function statusText(status: unknown) { return statusLabels[statusKey(status)] ?? '未知' }

function StatusTag({ status }: { status?: string }) {
  const value = statusKey(status)
  const theme = value === 'READY' || value === 'ACTIVE' || value === 'SUCCEEDED' || value === 'APPROVED'
    ? 'success'
    : value === 'DEGRADED' || value === 'UNCERTAIN' || value === 'PAUSED' || value === 'RUNNING' || value === 'COPYING' || value === 'VERIFYING' || value === 'RETRYING' || value === 'PENDING' || value === 'QUEUED'
      ? 'warning'
      : value === 'FAILED' || value === 'DENIED' || value === 'EXPIRED' || value === 'NOT_READY' || value === 'SUSPENDED' || value === 'CANCELLED' || value === 'CANCELED'
        ? 'danger'
        : 'default'
  return <Tag theme={theme as any} variant="light-outline">{statusText(value)}</Tag>
}

function adminRoleLabel(role?: string) {
  return ({ system_admin: '系统管理员', operator: '运营人员', auditor: '审计人员' } as Record<string, string>)[role ?? ''] ?? '管理员'
}

function display(value: unknown, fallback = '—') {
  if (value === null || value === undefined || value === '') return fallback
  if (typeof value === 'number' && !Number.isFinite(value)) return fallback
  const text = String(value).trim()
  return text || fallback
}

const jaegerBaseURL = ((import.meta.env.VITE_JAEGER_URL as string | undefined) ?? '').replace(/\/$/, '')

function TraceLink({ traceID, label }: { traceID?: string; label?: string }) {
  if (!traceID) return <span>—</span>
  if (!jaegerBaseURL) return <span>{label ?? traceID}</span>
  return <a href={`${jaegerBaseURL}/trace/${encodeURIComponent(traceID)}`} target="_blank" rel="noreferrer">{label ?? traceID}</a>
}

function numberValue(value: unknown): number | undefined {
  const number = typeof value === 'number' ? value : typeof value === 'string' && value.trim() ? Number(value) : NaN
  return Number.isFinite(number) ? number : undefined
}

function countText(value: unknown) {
  const number = numberValue(value)
  return number === undefined ? '—' : String(Math.trunc(number))
}

function percentText(value: unknown, fallback = '—') {
  const number = numberValue(value)
  return number === undefined ? fallback : `${Math.round(number * 100)}%`
}

function ratioPercent(numerator: unknown, denominator: unknown) {
  const top = numberValue(numerator)
  const bottom = numberValue(denominator)
  if (top === undefined || bottom === undefined || bottom <= 0) return undefined
  return Math.min(100, Math.max(0, Math.round((top / bottom) * 100)))
}

function workerCapacityText(value: unknown) {
  const number = numberValue(value)
  return number === undefined || number <= 0 ? '尚未评估' : `${countText(number)} 个并发执行 / Worker`
}

function workerUtilizationText(value: unknown, capacity: unknown) {
  const base = numberValue(capacity)
  return base === undefined || base <= 0 ? '暂无容量基准' : percentText(value)
}

function migrationProgressText(active: unknown, progress: unknown) {
  const activeCount = numberValue(active)
  return activeCount !== undefined && activeCount <= 0 ? '无进行中的迁移' : percentText(progress)
}

function EmptyState({ message = '暂无数据' }: { message?: string }) { return <div className="empty-state">{message}</div> }
function ErrorState({ error }: { error?: string }) { return error ? <Alert theme="error" message={`读取失败：${display(error, '未知错误')}`} /> : null }
function LoadingState() { return <div className="loading-line">加载中…</div> }
function ScopeBar({ requiredApp = false }: { requiredApp?: boolean }) {
  const [params, setParams] = useSearchParams()
  const [tenant, setTenant] = useState(params.get('tenant_id') ?? '')
  const [app, setApp] = useState(params.get('app_id') ?? '')
  function apply(event: React.FormEvent) { event.preventDefault(); const next = new URLSearchParams(params); tenant ? next.set('tenant_id', tenant) : next.delete('tenant_id'); app ? next.set('app_id', app) : next.delete('app_id'); setParams(next) }
  return <form className="scope-bar" onSubmit={apply}><label>租户<input value={tenant} onChange={e => setTenant(e.target.value)} placeholder="租户 ID" /></label><label>应用<input value={app} onChange={e => setApp(e.target.value)} placeholder={requiredApp ? '应用 ID' : '全部应用'} /></label><Button type="submit" theme="primary" variant="outline">应用范围</Button></form>
}
function NeedScope({ app = false }: { app?: boolean }) { return <Alert theme="warning" message={`请先在上方范围栏选择租户${app ? '和应用' : ''}，再查看此页面。`} /> }
function qs(params: URLSearchParams, keys: string[]) { const value = new URLSearchParams(); keys.forEach(key => { const item = params.get(key); if (item) value.set(key, item) }); return value.toString() }
function date(value?: string) { if (!value) return '—'; const parsed = new Date(value); return Number.isNaN(parsed.getTime()) ? '—' : parsed.toLocaleString() }
function safeOptions(options?: Record<string, string>) { return Object.entries(options ?? {}).filter(([key]) => !/(secret|token|password|passwd|key|credential|dsn|authorization)/i.test(key)) }
function configDiff(from: AppConfig | undefined, to: AppConfig): string {
  if (!from) return `没有可用的 active 基线。目标 ${to.version} 是新的不可变 ConfigVersion。`
  const keys = Array.from(new Set([...Object.keys(from), ...Object.keys(to)])).sort()
  const changes = keys.filter(key => JSON.stringify(from[key as keyof AppConfig]) !== JSON.stringify(to[key as keyof AppConfig])).map(key => ({ field: key, from: from[key as keyof AppConfig] ?? null, to: to[key as keyof AppConfig] ?? null }))
  return JSON.stringify({ from: from.version, to: to.version, changes }, null, 2)
}

function executionStatus(value: Execution) {
  if (value.status === 'PENDING') return value.attempt > 0 ? 'RETRYING' : 'QUEUED'
  if (value.status === 'CANCELED') return 'CANCELLED'
  return value.status
}

function executionDuration(value: Execution) {
  const duration = numberValue(value.duration_ms)
  if (duration !== undefined && duration > 0) return `${duration} ms`
  if (!value.started_at) return '—'
  const end = value.finished_at ? new Date(value.finished_at) : new Date()
  const start = new Date(value.started_at)
  const milliseconds = end.getTime() - start.getTime()
  return Number.isFinite(milliseconds) && milliseconds > 0 ? `${milliseconds} ms` : '—'
}

function useDangerousAction() {
  const [pending, setPending] = useState<{ title: string; impact: string; action: () => Promise<void> }>()
  const [busy, setBusy] = useState(false)
  const dialog = <Dialog visible={Boolean(pending)} header={pending?.title} onClose={() => !busy && setPending(undefined)} onCancel={() => setPending(undefined)} onConfirm={async () => { if (!pending) return; setBusy(true); try { await pending.action() } finally { setBusy(false); setPending(undefined) } }} confirmBtn={{ content: '确认', loading: busy }} cancelBtn="取消"><p>{pending?.impact}</p></Dialog>
  return { ask: (title: string, impact: string, action: () => Promise<void>) => setPending({ title, impact, action }), dialog }
}

function OverviewPage() {
  const state = useData<Operations>('/admin/v1/overview')
  if (state.loading) return <Page title="总览"><LoadingState /></Page>
  return <Page title="总览" subtitle="当前控制面健康摘要；详细指标请结合 Prometheus 查看。">
    <ErrorState error={state.error} />
    {state.data && <>
      <div className="summary-grid">
        {[
          ['Gateway 状态', statusText(state.data.gateway_readiness)],
          ['Worker 数量', countText(state.data.worker_count)],
          ['执行中', countText(state.data.active_executions)],
          ['任务积压', countText(state.data.queue_backlog)],
          ['重试积压', countText(state.data.retry_backlog)],
          ['待处理审批', countText(state.data.pending_approvals)],
          ['进行中迁移', countText(state.data.active_migrations)],
          ['IM 通道', statusText(state.data.channel_readiness)],
          ['回复积压', countText(state.data.reply_backlog)],
        ].map(([label, value]) => <div className="summary-row" key={label}><span>{label}</span><strong>{value}</strong></div>)}
      </div>
      <Card title="近期运行信号" className="panel">
        <div className="status-line"><span>Gateway</span><StatusTag status={state.data.gateway_readiness} /><span>Worker</span><StatusTag status={state.data.worker_readiness} /><span>IM 通道</span><StatusTag status={state.data.channel_readiness} /><span>卡住的迁移</span><StatusTag status={state.data.stuck_migrations ? 'DEGRADED' : 'READY'} /></div>
        <p className="muted">更新时间：{date(state.data.generated_at)}。详细 Trace 和指标请使用 Jaeger、Prometheus。</p>
      </Card>
      <Card title="近期错误" className="panel">
        {state.data.recent_errors?.length ? <Table rowKey="trace_id" columns={[{ colKey: 'occurred_at', title: '时间', cell: (_: unknown, row: RecentError) => date(row.occurred_at) }, { colKey: 'scope', title: '租户 / 应用', cell: (_: unknown, row: RecentError) => `${display(row.tenant_id)}/${display(row.app_id)}` }, { colKey: 'event_type', title: '事件' }, { colKey: 'error_type', title: '错误类型' }, { colKey: 'trace_id', title: 'Trace', cell: (_: unknown, row: RecentError) => <TraceLink traceID={row.trace_id} /> }] as any} data={state.data.recent_errors} bordered stripe /> : <EmptyState message="暂无错误" />}
      </Card>
    </>}
  </Page>
}

function TenantsPage() {
  const [params] = useSearchParams()
  const state = useData<{ tenants: Tenant[] }>(`/admin/v1/tenants?${qs(params, ['tenant_id'])}`)
  const [counts, setCounts] = useState<Record<string, number>>({})
  useEffect(() => { if (!state.data) return; Promise.allSettled(state.data.tenants.map(async tenant => [tenant.tenant_id, (await get<{ apps: AgentApp[] }>(`/admin/v1/apps?tenant_id=${encodeURIComponent(tenant.tenant_id)}`)).apps.length] as const)).then(results => { const next: Record<string, number> = {}; results.forEach(result => { if (result.status === 'fulfilled') next[result.value[0]] = result.value[1] }); setCounts(next) }) }, [state.data])
  const columns: Columns = [{ colKey: 'tenant_id', title: '租户', cell: (_, row) => <Link to={`/tenants/${encodeURIComponent(row.tenant_id)}`}>{display(row.tenant_id)}</Link> }, { colKey: 'name', title: '名称', cell: (_, row) => display(row.name) }, { colKey: 'status', title: '状态', cell: (_, row) => <StatusTag status={row.status} /> }, { colKey: 'apps', title: 'Agent 应用', cell: (_, row) => countText(row.agent_app_count ?? counts[row.tenant_id]) }, { colKey: 'anomaly', title: '当前异常', cell: (_, row) => <StatusTag status={row.anomaly_status} /> }, { colKey: 'updated_at', title: '更新时间', cell: (_, row) => date(row.updated_at) }]
  return <Page title="租户" subtitle="租户范围与策略信息；页面不会返回 Secret。"><ScopeBar />{state.loading && <LoadingState />}<ErrorState error={state.error} />{state.data && <Table rowKey="tenant_id" columns={columns as any} data={state.data.tenants ?? []} bordered stripe />}</Page>
}

function TenantDetailPage() {
  const { tenantId = '' } = useParams(); const id = decodeURIComponent(tenantId)
  const tenantState = useData<{ tenants: Tenant[] }>(`/admin/v1/tenants?tenant_id=${encodeURIComponent(id)}`)
  const apps = useData<{ apps: AgentApp[] }>(`/admin/v1/apps?tenant_id=${encodeURIComponent(id)}`)
  const bindings = useData<{ bindings: Binding[] }>(`/admin/v1/channel-bindings?tenant_id=${encodeURIComponent(id)}`)
  const tenant = tenantState.data?.tenants[0]
  const backendOverview = apps.data?.apps.flatMap(app => (app.backend_summary ?? []).map(backend => ({ row_key: `${app.app_id}/${backend.provider}/${backend.name}`, app_id: app.app_id, ...backend }))) ?? []
  const channelOverview = apps.data?.apps.flatMap(app => (app.channel_summary ?? []).map(channel => ({ row_key: `${app.app_id}/${channel.binding_id}`, app_id: app.app_id, ...channel }))) ?? []
  return <Page title={`租户 · ${display(id)}`} actions={<Link to="/tenants">返回租户列表</Link>}><ErrorState error={tenantState.error || apps.error || bindings.error} />{tenantState.loading ? <LoadingState /> : tenant ? <><Card title="基本信息" className="panel"><div className="detail-grid"><span>状态</span><StatusTag status={tenant.status} /><span>当前异常</span><StatusTag status={tenant.anomaly_status} /><span>审计策略</span><span>{tenant.audit?.enabled ? '已启用' : '未启用'}</span><span>Agent 应用</span><span>{countText(apps.data?.apps.length)}</span><span>通道绑定</span><span>{countText(bindings.data?.bindings.length)}</span><span>更新时间</span><span>{date(tenant.updated_at)}</span></div></Card><Card title="Agent 应用" className="panel">{apps.data && <Table rowKey="app_id" columns={[{ colKey: 'app_id', title: '应用', cell: (_: unknown, row: AgentApp) => <Link to={`/agent-apps/${id}/${row.app_id}?tenant_id=${encodeURIComponent(id)}&app_id=${encodeURIComponent(row.app_id)}`}>{display(row.app_id)}</Link> }, { colKey: 'status', title: '状态', cell: (_: unknown, row: AgentApp) => <StatusTag status={row.status} /> }, { colKey: 'active_config_version', title: 'Stable 版本', cell: (_: unknown, row: AgentApp) => display(row.active_config_version) }, { colKey: 'canary_config_version', title: 'Canary 版本', cell: (_: unknown, row: AgentApp) => display(row.canary_config_version) }, { colKey: 'backend_summary', title: '后端', cell: (_: unknown, row: AgentApp) => (row.backend_summary ?? []).map(item => `${display(item.provider)}/${display(item.name)}`).join(', ') || '—' }, { colKey: 'channel_summary', title: '通道', cell: (_: unknown, row: AgentApp) => (row.channel_summary ?? []).map(item => display(item.provider)).join(', ') || '—' }] as any} data={apps.data.apps ?? []} bordered stripe />}</Card><Card title="后端 / 通道概览" className="panel"><Tabs><Tabs.TabPanel value="backends" label="后端"><Table rowKey="app_id" columns={[{ colKey: 'app_id', title: '应用' }, { colKey: 'provider', title: 'Provider' }, { colKey: 'name', title: 'Profile' }, { colKey: 'status', title: '状态', cell: (_: unknown, row: { status: string }) => <StatusTag status={row.status} /> }] as any} data={backendOverview} bordered stripe /></Tabs.TabPanel><Tabs.TabPanel value="channels" label="通道"><Table rowKey="binding_id" columns={[{ colKey: 'app_id', title: '应用' }, { colKey: 'provider', title: 'Provider' }, { colKey: 'binding_id', title: '绑定 ID' }, { colKey: 'status', title: '状态', cell: (_: unknown, row: { status: string }) => <StatusTag status={row.status} /> }, { colKey: 'connection_status', title: '连接状态', cell: (_: unknown, row: { connection_status: string }) => <StatusTag status={row.connection_status} /> }] as any} data={channelOverview} bordered stripe /></Tabs.TabPanel></Tabs></Card></> : <Alert theme="warning" message="未找到该租户" />}</Page>
}

function AgentAppsPage() {
  const [params] = useSearchParams(); const tenant = params.get('tenant_id') ?? ''
  const state = useData<{ apps: AgentApp[] }>(`/admin/v1/apps?${qs(params, ['tenant_id', 'app_id'])}`, Boolean(tenant))
  const columns: Columns = [{ colKey: 'app_id', title: '应用', cell: (_, row) => <Link to={`/agent-apps/${row.tenant_id}/${row.app_id}?tenant_id=${row.tenant_id}&app_id=${row.app_id}`}>{display(row.app_id)}</Link> }, { colKey: 'tenant_id', title: '租户', cell: (_, row) => display(row.tenant_id) }, { colKey: 'status', title: '状态', cell: (_, row) => <StatusTag status={row.status} /> }, { colKey: 'active_config_version', title: 'Active / Stable', cell: (_, row) => display(row.active_config_version) }, { colKey: 'canary_config_version', title: 'Canary 版本', cell: (_, row) => display(row.canary_config_version) }, { colKey: 'canary_status', title: 'Canary 状态', cell: (_, row) => <StatusTag status={row.canary_status} /> }, { colKey: 'backend_summary', title: '后端', cell: (_, row: AgentApp) => (row.backend_summary ?? []).map(item => `${display(item.provider)}/${display(item.name)}`).join(', ') || '—' }, { colKey: 'channel_summary', title: '通道', cell: (_, row: AgentApp) => (row.channel_summary ?? []).map(item => `${display(item.provider)}/${display(item.binding_id)}`).join(', ') || '—' }]
  return <Page title="Agent 应用" subtitle="运行状态来自 Admin API。"><ScopeBar />{!tenant ? <NeedScope /> : <>{state.loading && <LoadingState />}<ErrorState error={state.error} />{state.data && <Table rowKey="app_id" columns={columns as any} data={state.data.apps ?? []} bordered stripe />}</>}</Page>
}

function AgentAppDetailPage() {
  const { tenantId = '', appId = '' } = useParams(); const tenant = decodeURIComponent(tenantId); const app = decodeURIComponent(appId)
  const state = useData<{ apps: AgentApp[] }>(`/admin/v1/apps?tenant_id=${encodeURIComponent(tenant)}&app_id=${encodeURIComponent(app)}`)
  const value = state.data?.apps[0]
  return <Page title={`Agent 应用 · ${display(app)}`} actions={<Link to={`/agent-apps?tenant_id=${encodeURIComponent(tenant)}`}>返回应用列表</Link>}><ErrorState error={state.error} />{state.loading ? <LoadingState /> : value ? <><Card title="发布状态" className="panel"><div className="detail-grid"><span>租户</span><span>{display(value.tenant_id)}</span><span>状态</span><StatusTag status={value.status} /><span>Active / Stable</span><span>{display(value.active_config_version)}</span><span>Canary</span><span>{display(value.canary_config_version)} {value.canary_config_version && <StatusTag status={value.canary_status} />}</span><span>更新时间</span><span>{date(value.updated_at)}</span></div></Card><Card title="后端概览" className="panel"><Table rowKey="name" columns={[{ colKey: 'provider', title: 'Provider' }, { colKey: 'name', title: 'Profile' }, { colKey: 'status', title: '状态', cell: (_: unknown, row: { status: string }) => <StatusTag status={row.status} /> }, { colKey: 'secret_ref', title: 'SecretRef', cell: (_: unknown, row: NonNullable<AgentApp['backend_summary']>[number]) => row.secret_ref?.name ? `${row.secret_ref.name}${row.secret_ref.version ? ` / ${row.secret_ref.version}` : ''}` : '—' }] as any} data={value.backend_summary ?? []} bordered stripe /></Card><Card title="通道概览" className="panel"><Table rowKey="binding_id" columns={[{ colKey: 'provider', title: 'Provider' }, { colKey: 'binding_id', title: '绑定 ID' }, { colKey: 'status', title: '状态', cell: (_: unknown, row: { status: string }) => <StatusTag status={row.status} /> }, { colKey: 'connection_status', title: '连接状态', cell: (_: unknown, row: { connection_status: string }) => <StatusTag status={row.connection_status} /> }] as any} data={value.channel_summary ?? []} bordered stripe /></Card><div className="link-strip"><Link to={`/config-versions?tenant_id=${tenant}&app_id=${app}`}>配置版本</Link><Link to={`/backends?tenant_id=${tenant}&app_id=${app}`}>后端</Link><Link to={`/channels?tenant_id=${tenant}&app_id=${app}`}>通道</Link><Link to={`/executions?tenant_id=${tenant}&app_id=${app}`}>执行记录</Link><Link to={`/audit?tenant_id=${tenant}&app_id=${app}`}>审计日志</Link></div></> : <Alert theme="warning" message="未找到该 Agent 应用" />}</Page>
}

function ConfigVersionsPage() {
  const [params] = useSearchParams(); const tenant = params.get('tenant_id') ?? ''; const app = params.get('app_id') ?? ''; const [refresh, setRefresh] = useState(0)
  const apps = useData<{ apps: AgentApp[] }>(`/admin/v1/apps?tenant_id=${encodeURIComponent(tenant)}&app_id=${encodeURIComponent(app)}&_refresh=${refresh}`, Boolean(tenant && app))
  const configs = useData<{ configs: AppConfigView[] }>(`/admin/v1/configs?tenant_id=${encodeURIComponent(tenant)}&app_id=${encodeURIComponent(app)}&_refresh=${refresh}`, Boolean(tenant && app))
  const [version, setVersion] = useState(''); const [draft, setDraft] = useState(''); const [canaryVersion, setCanaryVersion] = useState(''); const [canaryPercentage, setCanaryPercentage] = useState(10); const [message, setMessage] = useState(''); const [diffTarget, setDiffTarget] = useState<AppConfigView>()
  const dangerous = useDangerousAction(); const canMutate = useCanMutate()
  const appState = apps.data?.apps[0]
  const configViews = configs.data?.configs ?? []
  const activeConfig = configViews.find(item => item.active)?.config
  const activeVersionView = configViews.find(item => item.active)
  function markChanged(value: string) { setMessage(value); setRefresh(current => current + 1) }
  useEffect(() => { const active = configViews.find(item => item.active) ?? configViews[0]; if (active) { setDraft(JSON.stringify(active.config, null, 2)); setCanaryVersion(appState?.canary_config_version ?? ''); setCanaryPercentage(appState?.canary_percentage || 10) } }, [configs.data, appState?.canary_config_version, appState?.canary_percentage])
  async function publish() { setMessage(''); try { const config = JSON.parse(draft) as AppConfig; config.tenant_id = tenant; config.app_id = app; config.version = version; await post('/admin/v1/configs', { config }); setMessage(`已发布不可变版本 ${version}`) } catch (error) { setMessage(error instanceof Error ? `发布失败：${error.message}` : '发布失败') } }
  if (!tenant || !app) return <Page title="配置版本"><ScopeBar requiredApp /><NeedScope app /></Page>
  const columns: Columns = [{ colKey: 'version', title: '版本', cell: (_, row) => display(row.config?.version) }, { colKey: 'status', title: '状态', cell: (_, row) => <StatusTag status={row.status} /> }, { colKey: 'active', title: 'Stable / Active', cell: (_, row) => row.active ? <StatusTag status="ACTIVE" /> : '—' }, { colKey: 'created_at', title: '创建时间', cell: (_, row) => date(row.created_at) }, { colKey: 'actions', title: '操作', cell: (_, row) => { const rollback = Boolean(activeVersionView && new Date(row.created_at) < new Date(activeVersionView.created_at)); return <div className="table-actions"><Button size="small" variant="outline" onClick={() => setDiffTarget(row)}>详情 / Diff</Button>{canMutate && !row.active && <Button size="small" theme="warning" onClick={() => dangerous.ask(rollback ? '回滚 ConfigVersion' : '激活 ConfigVersion', `租户 ${tenant}，应用 ${app}。当前版本：${display(appState?.active_config_version, '未知')}。目标版本：${display(row.config.version)}。${rollback ? '新请求将回滚到目标版本，已有执行仍保持原版本。' : '新请求将使用目标版本，已有执行仍保持原版本。'}`, async () => { await post(rollback ? '/admin/v1/configs/rollback' : '/admin/v1/configs/activate', { tenant_id: tenant, app_id: app, version: row.config.version }); markChanged(`${rollback ? '已回滚' : '已激活'} ${display(row.config.version)}`) })}>{rollback ? '回滚' : '激活'}</Button>}</div> } }]
  return <Page title="配置版本" subtitle="不可变版本历史。配置变更必须先发布新版本，再执行激活。"><ScopeBar requiredApp /><ErrorState error={apps.error || configs.error} />{message && <Alert theme="info" message={message} />}{configs.loading ? <LoadingState /> : configs.data && <><Card title="发布状态" className="panel"><div className="detail-grid"><span>Stable</span><span>{display(appState?.active_config_version)}</span><span>Canary</span><span>{display(appState?.canary_config_version)} {appState?.canary_config_version && <StatusTag status={appState.canary_status} />}</span></div><div className="inline-form"><label>Canary 目标<select value={canaryVersion} onChange={e => setCanaryVersion(e.target.value)}><option value="">选择已发布版本</option>{configViews.map(item => <option key={item.config.version} value={item.config.version}>{display(item.config.version)}</option>)}</select></label><label>流量比例<input type="number" min="1" max="100" value={canaryPercentage} onChange={e => setCanaryPercentage(Number(e.target.value))} />%</label>{canMutate && <><Button theme="warning" disabled={!canaryVersion} onClick={() => dangerous.ask('启用 Canary', `租户 ${tenant}，应用 ${app}。目标 ${canaryVersion} 将承接 ${canaryPercentage}% 的稳定路由身份。`, async () => { await post('/admin/v1/configs/canary/enable', { tenant_id: tenant, app_id: app, version: canaryVersion, percentage: canaryPercentage }); markChanged(`已启用 Canary ${canaryVersion}`) })}>启用 Canary</Button><Button variant="outline" disabled={!appState?.canary_config_version} onClick={() => dangerous.ask('暂停 Canary', `租户 ${tenant}，应用 ${app}。新请求回到 Stable 版本，候选版本仍保留。`, async () => { await post('/admin/v1/configs/canary/pause', { tenant_id: tenant, app_id: app }); markChanged('Canary 已暂停') })}>暂停</Button><Button variant="outline" disabled={!appState?.canary_config_version} onClick={() => dangerous.ask('提升 Canary', `租户 ${tenant}，应用 ${app}。当前 Stable：${display(appState?.active_config_version, '未知')}。目标：${display(appState?.canary_config_version ?? canaryVersion)}。目标版本将成为 Stable。`, async () => { await post('/admin/v1/configs/canary/promote', { tenant_id: tenant, app_id: app }); markChanged('Canary 已提升为 Stable') })}>提升为 Stable</Button><Button theme="danger" variant="outline" disabled={!appState?.canary_config_version} onClick={() => dangerous.ask('回滚 Canary', `租户 ${tenant}，应用 ${app}。Stable 版本保持 ${display(appState?.active_config_version, '未知')}，并移除候选版本。`, async () => { await post('/admin/v1/configs/canary/rollback', { tenant_id: tenant, app_id: app }); markChanged('Canary 已回滚') })}>回滚</Button></>}</div></Card><Table rowKey="config.version" columns={columns as any} data={configViews} bordered stripe /><Card title="发布新的不可变版本" className="panel"><div className="inline-form"><label>版本<input value={version} onChange={e => setVersion(e.target.value)} /></label><Button theme="primary" disabled={!canMutate || !version.trim()} onClick={publish}>发布新版本</Button></div><textarea className="code-editor" value={draft} onChange={e => setDraft(e.target.value)} aria-label="不可变配置草稿" /></Card></>}{diffTarget && <Dialog visible header={`Config ${display(diffTarget.config.version)}`} onClose={() => setDiffTarget(undefined)} onConfirm={() => setDiffTarget(undefined)} confirmBtn="关闭" cancelBtn={false}><Tabs><Tabs.TabPanel value="detail" label="版本详情"><pre className="code-block">{JSON.stringify(diffTarget.config, null, 2)}</pre></Tabs.TabPanel><Tabs.TabPanel value="diff" label="差异对比"><p className="muted">与当前不可变版本对比；Secret 字段仅显示引用。</p><pre className="code-block">{configDiff(activeConfig, diffTarget.config)}</pre></Tabs.TabPanel></Tabs></Dialog>}{dangerous.dialog}</Page>
}

function BackendsPage() {
  const [params] = useSearchParams(); const tenant = params.get('tenant_id') ?? ''; const app = params.get('app_id') ?? ''
  const state = useData<{ configs: AppConfigView[] }>(`/admin/v1/configs?tenant_id=${encodeURIComponent(tenant)}&app_id=${encodeURIComponent(app)}`, Boolean(tenant && app))
  const operations = useData<Operations>('/admin/v1/operations', Boolean(tenant && app))
  const active = state.data?.configs.find(item => item.active)?.config
  const refs: [string, BackendRef | undefined][] = [['Session', active?.backend_config.session], ['Memory', active?.backend_config.memory], ['Knowledge', active?.backend_config.knowledge], ['Artifact', active?.backend_config.artifact]]
  return <Page title="后端" subtitle="展示当前不可变 ConfigVersion 中的后端配置；就绪状态来自运行状态接口。"><ScopeBar requiredApp />{!tenant || !app ? <NeedScope app /> : <>{state.loading && <LoadingState />}<ErrorState error={state.error || operations.error} />{active && <div className="backend-grid">{refs.map(([name, ref]) => { const readiness = operations.data?.backends?.find(item => item.provider === ref?.provider)?.status ?? 'UNKNOWN'; const options = safeOptions(ref?.options); return <Card title={name} key={name} className="backend-card">{ref && ref.provider ? <div className="detail-grid"><span>Provider</span><span>{display(ref.provider)}</span><span>后端 / Profile</span><span>{display(ref.name, 'default')}</span><span>状态</span><StatusTag status={readiness} /><span>ConfigVersion</span><span>{display(active.version)}</span><span>SecretRef</span><span>{ref.secret_ref?.name || '—'}{ref.secret_ref?.version ? ` / ${ref.secret_ref.version}` : ''}</span><span>Endpoint 摘要</span><span>{options.length ? options.map(([key, value]) => <code key={key}>{key}={value} </code>) : '—'}</span></div> : <span className="muted">未配置</span>}</Card> })}</div>}</>}</Page>
}

function ChannelsPage() {
  const [params] = useSearchParams(); const tenant = params.get('tenant_id') ?? ''; const app = params.get('app_id') ?? ''
  const state = useData<{ bindings: Binding[] }>(`/admin/v1/channel-bindings?${qs(params, ['tenant_id', 'app_id'])}`, Boolean(tenant))
  const [message, setMessage] = useState(''); const [selected, setSelected] = useState<Binding>(); const dangerous = useDangerousAction(); const canMutate = useCanMutate()
  async function change(binding: Binding, action: 'enable' | 'suspend') { await post(`/admin/v1/channel-bindings/${action}`, { tenant_id: binding.tenant_id, app_id: binding.app_id, binding_id: binding.binding_id }); setMessage(`${display(binding.binding_id)}：${action === 'suspend' ? '已提交暂停' : '已提交启用'}`) }
  const columns: Columns = [{ colKey: 'channel', title: 'Provider', cell: (_, row) => display(row.channel) }, { colKey: 'binding_id', title: '绑定 ID', cell: (_, row) => display(row.binding_id) }, { colKey: 'binding_revision', title: '修订版', cell: (_, row) => display(row.binding_revision) }, { colKey: 'status', title: '状态', cell: (_, row) => <StatusTag status={row.status} /> }, { colKey: 'connection_status', title: '连接状态', cell: (_, row) => <StatusTag status={row.connection_status} /> }, { colKey: 'last_connected_at', title: '最后连接', cell: (_, row) => date(row.last_connected_at) }, { colKey: 'last_error', title: '最近错误', cell: (_, row) => display(row.last_error) }, { colKey: 'scope', title: '租户 / 应用', cell: (_, row) => `${display(row.tenant_id)}/${display(row.app_id)}` }, { colKey: 'actions', title: '操作', cell: (_, row) => <div className="table-actions"><Button size="small" variant="outline" onClick={() => setSelected(row)}>详情</Button>{canMutate && (row.status === 'ACTIVE' ? <Button size="small" theme="danger" variant="outline" onClick={() => dangerous.ask('暂停通道绑定', `租户 ${display(row.tenant_id)}，应用 ${display(row.app_id)}，绑定 ${display(row.binding_id)}。暂停后将停止入站消息并释放长连接。`, () => change(row, 'suspend'))}>暂停</Button> : <Button size="small" theme="primary" variant="outline" onClick={() => dangerous.ask('启用通道绑定', `租户 ${display(row.tenant_id)}，应用 ${display(row.app_id)}，绑定 ${display(row.binding_id)}。适配器可能使用已保存的 SecretRef 重新连接。`, () => change(row, 'enable'))}>启用</Button>)}</div> }]
  return <Page title="通道" subtitle="Feishu 和 WeCom 绑定；页面只展示 SecretRef 元数据。"><ScopeBar />{!tenant ? <NeedScope /> : <>{message && <Alert theme="info" message={message} />}{state.loading && <LoadingState />}<ErrorState error={state.error} />{state.data && <Table rowKey="binding_id" columns={columns as any} data={state.data.bindings ?? []} bordered stripe />}{selected && <Drawer visible header={`通道 ${display(selected.binding_id)}`} onClose={() => setSelected(undefined)}><div className="detail-grid"><span>Provider</span><span>{display(selected.channel)}</span><span>租户 / 应用</span><span>{display(selected.tenant_id)}/{display(selected.app_id)}</span><span>绑定 ID</span><span>{display(selected.binding_id)}</span><span>修订版</span><span>{display(selected.binding_revision)}</span><span>状态</span><StatusTag status={selected.status} /><span>连接状态</span><StatusTag status={selected.connection_status} /><span>SecretRef</span><span>{selected.secret_ref?.name || '—'}{selected.secret_ref?.version ? ` / ${selected.secret_ref.version}` : ''}</span><span>最后连接</span><span>{date(selected.last_connected_at)}</span><span>最近错误</span><span>{display(selected.last_error)}</span></div></Drawer>}</>}{dangerous.dialog}</Page>
}

function ExecutionsPage() {
  const [params] = useSearchParams(); const tenant = params.get('tenant_id') ?? ''
  const state = useData<{ executions: Execution[] }>(`/admin/v1/executions?${qs(params, ['tenant_id', 'app_id'])}`, Boolean(tenant)); const [selected, setSelected] = useState<Execution>()
  const columns: Columns = [{ colKey: 'request_id', title: '执行 ID', cell: (_, row) => display(row.request_id) }, { colKey: 'scope', title: '租户 / 应用', cell: (_, row) => `${display(row.tenant_id)}/${display(row.app_id)}` }, { colKey: 'session_id', title: 'Session', cell: (_, row) => display(row.session_id) }, { colKey: 'config_version', title: 'ConfigVersion', cell: (_, row) => display(row.config_version) }, { colKey: 'status', title: '状态', cell: (_, row) => <StatusTag status={executionStatus(row)} /> }, { colKey: 'lease_owner', title: 'Worker', cell: (_, row) => display(row.lease_owner) }, { colKey: 'attempt', title: '重试次数', cell: (_, row) => countText(row.attempt) }, { colKey: 'created_at', title: '开始时间', cell: (_, row) => date(row.started_at || row.created_at) }, { colKey: 'duration', title: '耗时', cell: (_, row) => executionDuration(row) }, { colKey: 'trace', title: 'Trace', cell: (_, row) => <TraceLink traceID={row.trace_id} label="Jaeger" /> }, { colKey: 'details', title: '详情', cell: (_, row) => <Button size="small" variant="outline" onClick={() => setSelected(row)}>打开</Button> }]
  return <Page title="执行记录" subtitle="页面不会展示 Payload、Prompt、Completion、原始工具参数或 Lease Token。"><ScopeBar />{!tenant ? <NeedScope /> : <>{state.loading && <LoadingState />}<ErrorState error={state.error} />{state.data && <Table rowKey="request_id" columns={columns as any} data={state.data.executions ?? []} bordered stripe />}{selected && <Drawer visible header={`执行记录 ${display(selected.request_id)}`} onClose={() => setSelected(undefined)}><div className="detail-grid"><span>生命周期</span><span><StatusTag status={executionStatus(selected)} /> 创建于 {date(selected.created_at)} / 完成于 {date(selected.finished_at)} / 耗时 {executionDuration(selected)}</span><span>Worker 所有权</span><span>{display(selected.lease_owner)}，有效至 {date(selected.lease_until)}</span><span>Lease / 重试</span><span>第 {countText(selected.attempt)} 次，下一次重试：{date(selected.next_attempt_at)}</span><span>ConfigVersion</span><span>{display(selected.config_version)}</span><span>工具调用</span><span>{selected.tool_calls?.length ? selected.tool_calls.map(call => `${display(call.tool_name)} · ${display(call.event_type)} · ${statusText(call.decision)} ×${countText(call.count)}`).join('；') : '暂无工具审计事件'}</span><span>错误类型 / 摘要</span><span>{display(selected.error_type)}{selected.last_error ? ` / ${selected.last_error}` : ''}</span><span>Trace</span><TraceLink traceID={selected.trace_id} /></div></Drawer>}</>}</Page>
}

function ApprovalsPage() {
  const [params] = useSearchParams(); const tenant = params.get('tenant_id') ?? ''; const app = params.get('app_id') ?? ''
  const [status, setStatus] = useState('PENDING'); const [message, setMessage] = useState(''); const [selected, setSelected] = useState<Approval>(); const dangerous = useDangerousAction(); const canMutate = useCanMutate()
  const query = new URLSearchParams({ tenant_id: tenant, app_id: app, status, limit: '100' }).toString(); const state = useData<{ approvals: Approval[] }>(`/admin/v1/approvals?${query}`, Boolean(tenant && app))
  async function decide(record: Approval, decision: 'approve' | 'deny') { await post(`/admin/v1/approvals/${encodeURIComponent(record.approval_id)}/${decision}`, { tenant_id: record.tenant_id, app_id: record.app_id }); setMessage(`${display(record.approval_id)}：${decision === 'approve' ? '已批准' : '已拒绝'}`) }
  const columns: Columns = [{ colKey: 'status', title: '状态', cell: (_, row) => <StatusTag status={row.status} /> }, { colKey: 'approval_id', title: '审批 ID', cell: (_, row) => display(row.approval_id) }, { colKey: 'request_id', title: '执行 ID', cell: (_, row) => display(row.request_id) }, { colKey: 'config_version', title: 'ConfigVersion', cell: (_, row) => display(row.config_version) }, { colKey: 'tool_name', title: '工具', cell: (_, row) => display(row.tool_name) }, { colKey: 'expires_at', title: '过期时间', cell: (_, row) => date(row.expires_at) }, { colKey: 'actions', title: '操作', cell: (_, row) => <div className="table-actions"><Button size="small" variant="outline" onClick={() => setSelected(row)}>详情</Button>{canMutate && row.status === 'PENDING' && <><Button size="small" theme="primary" onClick={() => dangerous.ask('批准工具请求', `租户 ${display(row.tenant_id)}，应用 ${display(row.app_id)}，执行 ${display(row.request_id)}，ConfigVersion ${display(row.config_version)}，工具 ${display(row.tool_name)}。参数摘要：${display(row.argument_digest)}。`, () => decide(row, 'approve'))}>批准</Button><Button size="small" theme="danger" variant="outline" onClick={() => dangerous.ask('拒绝工具请求', `租户 ${display(row.tenant_id)}，应用 ${display(row.app_id)}，执行 ${display(row.request_id)}，工具 ${display(row.tool_name)}。后端仍是最终授权边界。`, () => decide(row, 'deny'))}>拒绝</Button></>}</div> }]
  return <Page title="审批" subtitle="后端授权是最终边界；此页面只提交明确的审批决定。"><ScopeBar requiredApp />{!tenant || !app ? <NeedScope app /> : <>{message && <Alert theme="info" message={message} />}{<div className="filter-row"><label>状态<select value={status} onChange={e => setStatus(e.target.value)}><option value="PENDING">等待中</option><option value="APPROVED">已批准</option><option value="DENIED">已拒绝</option><option value="EXPIRED">已过期</option></select></label></div>}{state.loading && <LoadingState />}<ErrorState error={state.error} />{state.data && <Table rowKey="approval_id" columns={columns as any} data={state.data.approvals ?? []} bordered stripe />}{selected && <Dialog visible header="审批安全摘要" onClose={() => setSelected(undefined)} onConfirm={() => setSelected(undefined)} confirmBtn="关闭" cancelBtn={false}><div className="detail-grid"><span>租户 / 应用</span><span>{display(selected.tenant_id)}/{display(selected.app_id)}</span><span>执行 ID</span><span>{display(selected.request_id)}</span><span>ConfigVersion</span><span>{display(selected.config_version)}</span><span>工具</span><span>{display(selected.tool_name)}</span><span>请求摘要</span><span>{display(selected.argument_digest)}</span><span>过期时间</span><span>{date(selected.expires_at)}</span></div></Dialog>}</>}{dangerous.dialog}</Page>
}

function MigrationsPage() {
  const [params] = useSearchParams(); const tenant = params.get('tenant_id') ?? ''; const app = params.get('app_id') ?? ''; const state = useData<{ migrations: Migration[] }>(`/admin/v1/data-migrations?${qs(params, ['tenant_id', 'app_id'])}`, Boolean(tenant)); const [message, setMessage] = useState(''); const [selected, setSelected] = useState<Migration>(); const dangerous = useDangerousAction(); const canMutate = useCanMutate()
  async function begin(row: Migration) { await post('/admin/v1/data-migrations/begin', { tenant_id: row.tenant_id, app_id: row.app_id, migration_id: row.migration_id, owner: '', drain_deadline: new Date(Date.now() + 15 * 60_000).toISOString(), lease_duration: '30s' }); setMessage(`${display(row.migration_id)}：已提交开始迁移`) }
  const columns: Columns = [{ colKey: 'migration_id', title: '迁移 ID', cell: (_, row) => display(row.migration_id) }, { colKey: 'scope', title: '租户 / 应用', cell: (_, row) => `${display(row.tenant_id)}/${display(row.app_id)}` }, { colKey: 'domain', title: '迁移域', cell: (_, row) => statusKey(row.domain) === 'KNOWLEDGE' ? '知识库' : 'Session' }, { colKey: 'source', title: '来源', cell: (_, row) => row.source_config_version === row.target_config_version ? '—' : `${statusKey(row.domain) === 'KNOWLEDGE' ? 'Qdrant' : 'Redis'} (${display(row.source_config_version)})` }, { colKey: 'target', title: '目标', cell: (_, row) => `${statusKey(row.domain) === 'KNOWLEDGE' ? 'Qdrant' : 'PostgreSQL'} (${display(row.target_config_version)})` }, { colKey: 'status', title: '阶段', cell: (_, row) => <StatusTag status={row.status} /> }, { colKey: 'progress', title: '进度', cell: (_, row) => { const progress = ratioPercent(row.verify_progress, row.total_sessions); return <div className="progress-cell">{progress === undefined ? <span className="progress-empty">—</span> : <Progress percentage={progress} />}<span>{countText(row.copy_progress)} 已复制 / {countText(row.verify_progress)} 已校验，共 {countText(row.total_sessions)} 条</span></div> } }, { colKey: 'checkpoint', title: '检查点', cell: (_, row) => `${countText(row.copy_progress)}/${countText(row.verify_progress)}` }, { colKey: 'owner', title: '当前所有者', cell: (_, row) => display(row.lease_owner) }, { colKey: 'error', title: '最近错误', cell: (_, row) => display(row.failure_reason) }, { colKey: 'updated_at', title: '更新时间', cell: (_, row) => date(row.updated_at) }, { colKey: 'actions', title: '操作', cell: (_, row) => <div className="table-actions"><Button size="small" variant="outline" onClick={() => setSelected(row)}>详情</Button>{canMutate && row.status === 'PENDING' && <Button size="small" theme="warning" onClick={() => dangerous.ask('开始迁移', `租户 ${display(row.tenant_id)}，应用 ${display(row.app_id)}。${statusKey(row.domain) === 'KNOWLEDGE' ? '知识库向量将从 Qdrant 源迁移到 Qdrant 目标，复制和校验由后端状态机控制。' : 'Session 数据将从 Redis 迁移到 PostgreSQL，进入排空阶段后再执行复制和校验。'}`, () => begin(row))}>开始</Button>}</div> }]
  return <Page title="数据迁移" subtitle="此页面只提交状态机命令，不直接修改迁移状态。"><ScopeBar />{!tenant ? <NeedScope /> : <>{message && <Alert theme="info" message={message} />}{state.loading && <LoadingState />}<ErrorState error={state.error} />{state.data && <Table rowKey="migration_id" columns={columns as any} data={state.data.migrations ?? []} bordered stripe />}{selected && <Drawer visible header={`迁移 ${display(selected.migration_id)}`} onClose={() => setSelected(undefined)}><div className="detail-grid"><span>租户 / 应用</span><span>{display(selected.tenant_id)}/{display(selected.app_id)}</span><span>迁移域</span><span>{statusKey(selected.domain) === 'KNOWLEDGE' ? '知识库' : 'Session'}</span><span>来源 → 目标</span><span>{statusKey(selected.domain) === 'KNOWLEDGE' ? `Qdrant (${display(selected.source_config_version)}) → Qdrant (${display(selected.target_config_version)})` : `Redis (${display(selected.source_config_version)}) → PostgreSQL (${display(selected.target_config_version)})`}</span><span>阶段</span><StatusTag status={selected.status} /><span>进度</span><span>共 {countText(selected.total_sessions)} 条；已复制 {countText(selected.copy_progress)}；已校验 {countText(selected.verify_progress)}；成功 {countText(selected.success_count)}</span><span>检查点</span><span>{countText(selected.copy_progress)}/{countText(selected.verify_progress)} · {date(selected.last_checkpoint_at)}</span><span>当前所有者</span><span>{display(selected.lease_owner)}，有效至 {date(selected.lease_until)}</span><span>重试 / 失败</span><span>{display(selected.last_failure_stage, '暂无失败阶段')} / {display(selected.failure_reason)}</span><span>ConfigVersion 前 / 后</span><span>{display(selected.source_config_version)} / {display(selected.target_config_version)}</span><span>开始时间</span><span>{date(selected.created_at)}</span><span>更新时间</span><span>{date(selected.updated_at)}</span></div></Drawer>}</>}{dangerous.dialog}</Page>
}

function AuditPage() {
  const [params, setParams] = useSearchParams(); const tenant = params.get('tenant_id') ?? ''; const app = params.get('app_id') ?? ''; const [eventType, setEventType] = useState(params.get('event_type') ?? ''); const [toolName, setToolName] = useState(params.get('tool_name') ?? ''); const [traceID, setTraceID] = useState(params.get('trace_id') ?? ''); const [createdAfter, setCreatedAfter] = useState(params.get('created_after') ?? ''); const [createdBefore, setCreatedBefore] = useState(params.get('created_before') ?? ''); const initialPage = Number(params.get('offset') ?? 0); const [page, setPage] = useState(Number.isFinite(initialPage) && initialPage >= 0 ? initialPage : 0); const limit = 25
  const query = new URLSearchParams({ tenant_id: tenant, app_id: app, limit: String(limit), offset: String(page) }); if (eventType) query.set('event_type', eventType); if (toolName) query.set('tool_name', toolName); if (traceID) query.set('trace_id', traceID); if (createdAfter) query.set('created_after', new Date(createdAfter).toISOString()); if (createdBefore) query.set('created_before', new Date(createdBefore).toISOString())
  const state = useData<{ events: AuditEvent[] }>(`/admin/v1/audit-events?${query}`, Boolean(tenant && app))
  function apply(event: React.FormEvent) { event.preventDefault(); setPage(0); const next = new URLSearchParams(params); eventType ? next.set('event_type', eventType) : next.delete('event_type'); toolName ? next.set('tool_name', toolName) : next.delete('tool_name'); traceID ? next.set('trace_id', traceID) : next.delete('trace_id'); createdAfter ? next.set('created_after', new Date(createdAfter).toISOString()) : next.delete('created_after'); createdBefore ? next.set('created_before', new Date(createdBefore).toISOString()) : next.delete('created_before'); next.set('offset', '0'); setParams(next) }
  const columns: Columns = [{ colKey: 'created_at', title: '时间', cell: (_, row) => date(row.created_at) }, { colKey: 'scope', title: '租户 / 应用', cell: (_, row) => `${display(row.tenant_id)}/${display(row.app_id)}` }, { colKey: 'actor', title: '操作者 / 用户', cell: (_, row) => display(row.actor_id || row.user_id) }, { colKey: 'event_type', title: '事件' }, { colKey: 'decision', title: '决策', cell: (_, row) => statusText(row.decision) }, { colKey: 'tool_name', title: '工具', cell: (_, row) => display(row.tool_name) }, { colKey: 'latency', title: '耗时', cell: (_, row) => { const latency = numberValue(row.latency); return latency === undefined ? '—' : `${latency} ns` } }, { colKey: 'error_type', title: '错误类型', cell: (_, row) => display(row.error_type) }, { colKey: 'cost', title: '成本', cell: (_, row) => display(row.cost) }, { colKey: 'trace_id', title: 'Trace', cell: (_, row) => <TraceLink traceID={row.trace_id} /> }]
  return <Page title="审计日志" subtitle="仅展示租户范围内的元数据，按页查询；Prompt、Completion、原始请求和工具参数、Secret 及 Provider 内容均不可见。"><ScopeBar requiredApp />{!tenant || !app ? <NeedScope app /> : <><form className="filter-row" onSubmit={apply}><label>事件类型<input value={eventType} onChange={e => setEventType(e.target.value)} placeholder="execution_started" /></label><label>工具<input value={toolName} onChange={e => setToolName(e.target.value)} placeholder="工具名称" /></label><label>Trace<input value={traceID} onChange={e => setTraceID(e.target.value)} placeholder="Trace ID" /></label><label>开始时间<input type="datetime-local" value={createdAfter} onChange={e => setCreatedAfter(e.target.value)} /></label><label>结束时间<input type="datetime-local" value={createdBefore} onChange={e => setCreatedBefore(e.target.value)} /></label><Button type="submit" variant="outline">筛选</Button></form>{state.loading && <LoadingState />}<ErrorState error={state.error} />{state.data && <><Table rowKey="request_id" columns={columns as any} data={state.data.events ?? []} bordered stripe /><div className="pager"><Button disabled={page === 0} onClick={() => { const next = Math.max(0, page - limit); setPage(next); const copy = new URLSearchParams(params); copy.set('offset', String(next)); setParams(copy) }}>上一页</Button><span>第 {Math.floor(page / limit) + 1} 页</span><Button disabled={!state.data.events.length || state.data.events.length < limit} onClick={() => { const next = page + limit; setPage(next); const copy = new URLSearchParams(params); copy.set('offset', String(next)); setParams(copy) }}>下一页</Button></div></>}</>}</Page>
}

function OperationsPage() {
  const state = useData<Operations>('/admin/v1/operations')
  if (state.loading) return <Page title="运行状态"><LoadingState /></Page>
  return <Page title="运行状态" subtitle="查看 Gateway、Worker、队列、迁移和后端状态；详细分析请使用 Jaeger、Prometheus 或 Grafana。">
    <ErrorState error={state.error} />
    {state.data && <>
      <div className="summary-grid">
        {[
          ['Gateway 状态', statusText(state.data.gateway_readiness)],
          ['Worker 状态', statusText(state.data.worker_readiness)],
          ['Worker 数量', countText(state.data.worker_count)],
          ['Worker 容量', workerCapacityText(state.data.worker_capacity)],
          ['Worker 利用率', workerUtilizationText(state.data.worker_utilization, state.data.worker_capacity)],
          ['任务积压', countText(state.data.queue_backlog)],
          ['重试积压', countText(state.data.retry_backlog)],
          ['回复积压', countText(state.data.reply_backlog)],
          ['卡住的迁移', countText(state.data.stuck_migrations)],
          ['迁移进度', migrationProgressText(state.data.active_migrations, state.data.migration_progress)],
          ['审计积压', countText(state.data.audit_backlog)],
        ].map(([label, value]) => <div className="summary-row" key={label}><span>{label}</span><strong>{value}</strong></div>)}
      </div>
      <Card title="后端状态" className="panel">
        <Table rowKey="name" columns={[{ colKey: 'name', title: '后端' }, { colKey: 'provider', title: 'Provider' }, { colKey: 'status', title: '状态', cell: (_: unknown, row: any) => <StatusTag status={row.status} /> }] as any} data={state.data.backends ?? []} bordered stripe />
      </Card>
      {(state.data.jaeger_url || state.data.prometheus_url || state.data.grafana_url) && <div className="link-strip"><span className="link-strip-label">可观测性</span>{state.data.jaeger_url && <a href={state.data.jaeger_url} target="_blank" rel="noreferrer">Jaeger</a>}{state.data.prometheus_url && <a href={state.data.prometheus_url} target="_blank" rel="noreferrer">Prometheus</a>}{state.data.grafana_url && <a href={state.data.grafana_url} target="_blank" rel="noreferrer">Grafana</a>}</div>}
    </>}
  </Page>
}
