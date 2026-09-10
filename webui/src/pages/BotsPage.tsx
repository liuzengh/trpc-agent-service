import { lazy, Suspense, useMemo, useState } from 'react'

import { updateApplication } from '../api'
import { payloadFromSnapshot } from '../botDraft'
import { ChannelBrandIcon } from '../components/ChannelBrand'
import { channelLabel } from '../components/channelMeta'
import { FeedbackBanner } from '../components/FeedbackBanner'
import { LoadingState } from '../components/LoadingState'
import { SearchField } from '../components/SearchField'
import {
  BotIcon,
  ChatIcon,
  SettingsIcon,
} from '../components/Icons'
import { LayersIcon, PlayIcon, PlusIcon, PowerIcon, RouteIcon } from '../components/PageIcons'
import { StatusIndicator } from '../components/StatusIndicator'
import { useAppContext } from '../context'
import { channelsOf, type Snapshot } from '../types'

const loadBotModal = () => import('./BotModal')
const BotModal = lazy(async () => ({ default: (await loadBotModal()).BotModal }))
const loadVersionHistory = () => import('./VersionHistoryDialog')
const VersionHistoryDialog = lazy(async () => ({ default: (await loadVersionHistory()).VersionHistoryDialog }))

export function BotsPage({ onOpenChat }: { onOpenChat: (app: Snapshot) => void }) {
  const { apps, appsLoading, appsError, tenants, tenantsLoading, tenant, refresh } = useAppContext()
  const [editing, setEditing] = useState<{ mode: 'create' } | { mode: 'edit'; app: Snapshot } | null>(null)
  const [busy, setBusy] = useState('')
  const [versioning, setVersioning] = useState<Snapshot | null>(null)
  const [query, setQuery] = useState('')
  const [status, setStatus] = useState('all')
  const [actionError, setActionError] = useState('')

  const tenantApps = useMemo(() => apps.filter((app) => app.Config.tenant_id === tenant), [apps, tenant])
  const activeCount = tenantApps.filter((app) => app.Config.status === 'active').length
  const bindings = tenantApps.reduce((total, app) => total + channelsOf(app.Config).length, 0)
  const visibleApps = useMemo(() => tenantApps.filter((app) =>
    (status === 'all' || app.Config.status === status) &&
    app.Config.app_code.toLocaleLowerCase().includes(query.trim().toLocaleLowerCase()),
  ), [query, status, tenantApps])

  const toggleStatus = async (app: Snapshot) => {
    setBusy(`${app.Config.tenant_id}/${app.Config.app_code}`)
    setActionError('')
    try {
      const next = app.Config.status === 'active' ? 'disabled' : 'active'
      await updateApplication(app.Config.tenant_id, app.Config.app_code, payloadFromSnapshot(app, next))
      await refresh()
    } catch (error) {
      setActionError((error as Error).message)
    } finally {
      setBusy('')
    }
  }

  const preloadEditor = () => {
    void loadBotModal()
  }

  if (appsLoading || tenantsLoading) {
    return <div className="page-stack applications-page"><LoadingState label="正在读取机器人…" /></div>
  }

  return (
    <div className="page-stack applications-page">
      {appsError && <FeedbackBanner tone="error">{appsError}</FeedbackBanner>}
      {actionError && <FeedbackBanner tone="error">{actionError}</FeedbackBanner>}

      <div className="application-overview">
        <dl className="application-stats">
          <div className="application-stat application-stat-total">
            <span className="application-stat-icon" aria-hidden="true"><BotIcon size={22} /></span>
            <dt>机器人总数</dt>
            <dd>{tenantApps.length}</dd>
          </div>
          <div className="application-stat application-stat-active">
            <span className="application-stat-icon" aria-hidden="true"><PlayIcon size={20} /></span>
            <dt>运行中</dt>
            <dd className="active-count">{activeCount}</dd>
          </div>
          <div className="application-stat application-stat-bindings">
            <span className="application-stat-icon" aria-hidden="true"><RouteIcon size={20} /></span>
            <dt>已绑定渠道</dt>
            <dd>{bindings}</dd>
          </div>
        </dl>
      </div>

      <div className="application-toolbar">
        <div className="application-filters">
          {[
            { id: 'all', label: '全部', count: tenantApps.length },
            { id: 'draft', label: '草稿', count: tenantApps.filter((app) => app.Config.status === 'draft').length },
            { id: 'active', label: '运行中', count: activeCount },
            { id: 'disabled', label: '已停用', count: tenantApps.filter((app) => app.Config.status === 'disabled').length },
          ].map((filter) => (
            <button
              type="button"
              key={filter.id}
              className="secondary"
              aria-pressed={status === filter.id}
              onClick={() => setStatus(filter.id)}
            >
              {filter.label}<span>{filter.count}</span>
            </button>
          ))}
        </div>
        <SearchField
          className="application-search"
          value={query}
          onValueChange={setQuery}
          ariaLabel="搜索机器人"
          placeholder="搜索机器人…"
          onClear={() => setQuery('')}
        />
        <button
          type="button"
          className="primary create-bot-button"
          onPointerEnter={preloadEditor}
          onFocus={preloadEditor}
          onClick={() => setEditing({ mode: 'create' })}
        >
          <PlusIcon size={16} /> 创建机器人
        </button>
      </div>

      <section className="application-table" aria-label="机器人列表">
        <table>
          <thead>
            <tr>
              <th>机器人</th>
              <th>模型</th>
              <th>渠道</th>
              <th>版本</th>
              <th>状态</th>
              <th className="actions-col">操作</th>
            </tr>
          </thead>
          <tbody>
            {visibleApps.length === 0 && (
              <tr>
                <td colSpan={6} className="table-empty">
                  {tenantApps.length === 0 ? '当前租户还没有机器人，点击「创建机器人」开始' : '没有符合条件的机器人，请调整搜索或筛选条件。'}
                </td>
              </tr>
            )}
            {visibleApps.map((app) => (
              <tr key={`${app.Config.tenant_id}/${app.Config.app_code}`}>
                <td>
                  <div className="app-cell">
                    <span className="avatar avatar-sm app-avatar">{app.Config.app_code.slice(0, 1).toUpperCase()}</span>
                    <div>
                      <button
                        type="button"
                        className="app-name application-name"
                        onPointerEnter={preloadEditor}
                        onFocus={preloadEditor}
                        onClick={() => setEditing({ mode: 'edit', app })}
                        title="点击查看与编辑机器人配置"
                      >
                        {app.Config.app_code}
                      </button>
                      {app.Config.instruction && <div className="app-instruction" title={app.Config.instruction}>{app.Config.instruction}</div>}
                    </div>
                  </div>
                </td>
                <td className="model-cell">{app.Config.model?.name || '未配置模型'}</td>
                <td>
                  <div className="binding-list">
                    {channelsOf(app.Config).length === 0 && <span className="binding-chip">仅网页</span>}
                    {channelsOf(app.Config).map((binding) => (
                      <span key={`${binding.type}/${binding.binding_id}`} className={`binding-chip channel-${binding.type}`} title={binding.binding_id}>
                        <ChannelBrandIcon channel={binding.type} size={12} />
                        {channelLabel(binding.type)}
                      </span>
                    ))}
                  </div>
                </td>
                <td className="time-cell">v{app.Config.config_version}</td>
                <td>
                  <StatusIndicator tone={app.Config.status === 'active' ? 'success' : app.Config.status === 'draft' ? 'info' : 'neutral'} appearance="pill">
                    {app.Config.status === 'active' ? '运行中' : app.Config.status === 'draft' ? '草稿' : '已停用'}
                  </StatusIndicator>
                </td>
                <td>
                  <div className="row-actions">
                    <button type="button" className="secondary small-btn" disabled={app.Config.status !== 'active'} onClick={() => onOpenChat(app)}>
                      <ChatIcon size={14} /> 对话
                    </button>
                    <button
                      type="button"
                      className="secondary small-btn"
                      onPointerEnter={preloadEditor}
                      onFocus={preloadEditor}
                      onClick={() => setEditing({ mode: 'edit', app })}
                    >
                      <SettingsIcon size={13} /> 配置
                    </button>
                    <button
                      type="button"
                      className="secondary small-btn"
                      onPointerEnter={() => { void loadVersionHistory() }}
                      onFocus={() => { void loadVersionHistory() }}
                      onClick={() => setVersioning(app)}
                    >
                      <LayersIcon size={13} /> 版本发布
                    </button>
                    <button
                      type="button"
                      className={`secondary small-btn status-action ${app.Config.status === 'active' ? 'is-stop' : 'is-start'}`}
                      disabled={busy === `${app.Config.tenant_id}/${app.Config.app_code}`}
                      onClick={() => void toggleStatus(app)}
                    >
                      <PowerIcon size={13} /> {app.Config.status === 'active' ? '停用' : app.Config.status === 'draft' ? '启用' : '重新启用'}
                    </button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      {versioning && (
        <Suspense fallback={null}>
          <VersionHistoryDialog
            app={versioning}
            onClose={() => setVersioning(null)}
            onRestored={async () => {
              setVersioning(null)
              await refresh()
            }}
          />
        </Suspense>
      )}

      {editing && (
        <Suspense fallback={null}>
          <BotModal
            mode={editing.mode}
            app={editing.mode === 'edit' ? editing.app : null}
            tenants={tenants}
            currentTenant={tenant}
            onClose={() => setEditing(null)}
            onSaved={async (result) => {
              const stableApp = editing.mode === 'edit' ? editing.app : null
              setEditing(null)
              await refresh()
              if (result.action === 'candidate' && stableApp) {
                setVersioning(stableApp)
              }
            }}
          />
        </Suspense>
      )}
    </div>
  )
}
