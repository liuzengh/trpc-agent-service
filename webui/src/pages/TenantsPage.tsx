import { useEffect, useState } from 'react'
import * as Dialog from '@radix-ui/react-dialog'
import { useQuery } from '@tanstack/react-query'

import {
  createTenant,
  getBackendProfiles,
  getSystem,
  getTenantBackendPolicy,
  getTenantModelPolicy,
  getUsers,
  replaceTenantBackendPolicy,
  replaceTenantModelPolicy,
  updateTenantStatus,
} from '../api'
import { useAppContext } from '../context'
import { DialogHeader } from '../components/DialogHeader'
import { FeedbackBanner } from '../components/FeedbackBanner'
import { LoadingState } from '../components/LoadingState'
import { PanelHeader } from '../components/PanelHeader'
import { RefreshButton } from '../components/RefreshButton'
import { SelectControl } from '../components/SelectControl'
import { TogglePill } from '../components/TogglePill'
import { CpuIcon, DatabaseIcon, ShieldIcon } from '../components/Icons'
import { PlusIcon } from '../components/PageIcons'
import { memberDisplayName } from '../components/MemberIdentity'
import { modelProviderID, modelProviderModels, providerTypeLabel } from '../components/modelCatalog'

function modelKey(providerID: string, modelName: string) {
  return `${providerID}\u0000${modelName}`
}

export function TenantsPage() {
  const { tenantSummaries, tenantsLoading, user, refresh } = useAppContext()
  const [creating, setCreating] = useState(false)
  const [tenantID, setTenantID] = useState('')
  const [displayName, setDisplayName] = useState('')
  const [initialAdmin, setInitialAdmin] = useState(user?.platform_user_id ?? '')
  const [busy, setBusy] = useState('')
  const [error, setError] = useState('')
  const [notice, setNotice] = useState('')
  const [policyTenant, setPolicyTenant] = useState('')
  const [selectedModels, setSelectedModels] = useState<Set<string>>(new Set())
  const [selectedBackends, setSelectedBackends] = useState<Set<string>>(new Set())

  const usersQuery = useQuery({
    queryKey: ['console', 'tenant-initial-admin-candidates'],
    queryFn: ({ signal }) => getUsers({ limit: 200 }, signal),
    enabled: creating,
  })
  const candidates = (usersQuery.data?.users ?? []).filter((candidate) => candidate.status === 'active')
  const systemQuery = useQuery({
    queryKey: ['console', 'tenant-model-policy-assets'],
    queryFn: ({ signal }) => getSystem(signal),
    enabled: Boolean(policyTenant),
    staleTime: 10_000,
  })
  const policyQuery = useQuery({
    queryKey: ['console', 'tenant-model-policy', policyTenant],
    queryFn: ({ signal }) => getTenantModelPolicy(policyTenant, signal),
    enabled: Boolean(policyTenant),
  })
  const backendAssetsQuery = useQuery({
    queryKey: ['console', 'tenant-backend-policy-assets'],
    queryFn: ({ signal }) => getBackendProfiles(signal),
    enabled: Boolean(policyTenant),
    staleTime: 10_000,
  })
  const backendPolicyQuery = useQuery({
    queryKey: ['console', 'tenant-backend-policy', policyTenant],
    queryFn: ({ signal }) => getTenantBackendPolicy(policyTenant, signal),
    enabled: Boolean(policyTenant),
  })
  const modelProviders = systemQuery.data?.info.model_providers ?? []
  const backendProfiles = backendAssetsQuery.data ?? []

  useEffect(() => {
    if (!creating) return
    if (candidates.some((candidate) => candidate.platform_user_id === initialAdmin)) return
    setInitialAdmin(candidates.find((candidate) => candidate.platform_user_id === user?.platform_user_id)?.platform_user_id ?? candidates[0]?.platform_user_id ?? '')
  }, [candidates, creating, initialAdmin, user?.platform_user_id])

  useEffect(() => {
    if (!policyQuery.data) return
    setSelectedModels(new Set(policyQuery.data.models.map((model) => modelKey(model.provider_id, model.name))))
  }, [policyQuery.data])

  useEffect(() => {
    if (!backendPolicyQuery.data) return
    setSelectedBackends(new Set(backendPolicyQuery.data.profiles.map((profile) => profile.profile_id)))
  }, [backendPolicyQuery.data])

  const submit = async () => {
    if (!tenantID.trim() || !displayName.trim() || !initialAdmin) return
    setBusy('create')
    setError('')
    setNotice('')
    try {
      await createTenant(tenantID.trim(), displayName.trim(), initialAdmin)
      await refresh()
      setTenantID('')
      setDisplayName('')
      setCreating(false)
      setNotice('租户已创建，并已设置首个租户管理员')
    } catch (caught) {
      setError((caught as Error).message)
    } finally {
      setBusy('')
    }
  }

  const setStatus = async (tenantIDValue: string, active: boolean) => {
    setBusy(tenantIDValue)
    setError('')
    setNotice('')
    try {
      await updateTenantStatus(tenantIDValue, active ? 'active' : 'suspended')
      await refresh()
      setNotice(active ? '租户已恢复' : '租户已停用')
    } catch (caught) {
      setError((caught as Error).message)
    } finally {
      setBusy('')
    }
  }

  const toggleModel = (providerID: string, modelName: string) => {
    const key = modelKey(providerID, modelName)
    setSelectedModels((current) => {
      const next = new Set(current)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }

  const saveModelPolicy = async () => {
    if (!policyTenant) return
    setBusy('model-policy')
    setError('')
    setNotice('')
    try {
      const models = modelProviders.flatMap((provider) => {
        const providerID = modelProviderID(provider)
        return modelProviderModels(provider)
          .filter((model) => selectedModels.has(modelKey(providerID, model.name)))
          .map((model) => ({ provider_id: providerID, name: model.name }))
      })
      await replaceTenantModelPolicy(policyTenant, models)
      await policyQuery.refetch()
      setNotice(`已更新 ${tenantSummaries.find((entry) => entry.tenant_id === policyTenant)?.display_name || policyTenant} 的模型授权`)
    } catch (caught) {
      setError((caught as Error).message)
    } finally {
      setBusy('')
    }
  }

  const toggleBackend = (profileID: string) => {
    setSelectedBackends((current) => {
      const next = new Set(current)
      if (next.has(profileID)) next.delete(profileID)
      else next.add(profileID)
      return next
    })
  }

  const saveBackendPolicy = async () => {
    if (!policyTenant) return
    setBusy('backend-policy')
    setError('')
    setNotice('')
    try {
      await replaceTenantBackendPolicy(policyTenant, [...selectedBackends])
      await backendPolicyQuery.refetch()
      setNotice(`已更新 ${tenantSummaries.find((entry) => entry.tenant_id === policyTenant)?.display_name || policyTenant} 的数据后端授权`)
    } catch (caught) {
      setError((caught as Error).message)
    } finally {
      setBusy('')
    }
  }

  if (tenantsLoading) return <div className="page-stack members-page"><LoadingState label="正在读取租户…" /></div>

  return (
    <div className="page-stack members-page">
      {!policyTenant && (error || usersQuery.error) && <FeedbackBanner tone="error">{error || (usersQuery.error as Error).message}</FeedbackBanner>}
      {!policyTenant && notice && <FeedbackBanner tone="success">{notice}</FeedbackBanner>}
      <section className="members-panel">
        <PanelHeader
          icon={<ShieldIcon size={17} />}
          title="租户"
          description={`${tenantSummaries.length} 个租户；租户是业务与数据隔离边界。`}
          actions={<div className="members-header-actions">
            <RefreshButton onClick={() => void refresh()} label="刷新租户" />
            <button type="button" className="secondary" onClick={() => setCreating(true)} disabled={busy !== ''}><PlusIcon size={14} /> 新建租户</button>
          </div>}
        />

        <div className="members-table-wrap">
          <table className="members-table">
            <thead><tr><th>租户</th><th>标识</th><th>状态</th><th className="actions-col">平台策略</th></tr></thead>
            <tbody>
              {tenantSummaries.length === 0 ? <tr><td colSpan={4} className="table-empty">暂无租户</td></tr> : tenantSummaries.map((entry) => (
                <tr key={entry.tenant_id}>
                  <td><strong>{entry.display_name || entry.tenant_id}</strong></td>
                  <td><span className="mono-value">{entry.tenant_id}</span></td>
                  <td><TogglePill active={entry.status === 'active'} activeLabel="正常" inactiveLabel="已停用" inactiveClassName="suspended" disabled={busy !== ''} onChange={(active) => void setStatus(entry.tenant_id, active)} /></td>
                  <td className="actions-col"><button type="button" className="secondary small-btn" disabled={busy !== ''} onClick={() => { setError(''); setNotice(''); setPolicyTenant(entry.tenant_id) }}><ShieldIcon size={14} /> 资源授权</button></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

      </section>

      <Dialog.Root open={Boolean(policyTenant)} onOpenChange={(open) => { if (!open && busy === '') setPolicyTenant('') }}>
        <Dialog.Portal>
          <Dialog.Overlay className="modal-backdrop" />
          <Dialog.Content className="modal bot-dialog tenant-policy-dialog">
            <DialogHeader
              className="bot-dialog-head"
              closeClassName="bot-dialog-close"
              title="资源授权"
              description={`配置 ${tenantSummaries.find((entry) => entry.tenant_id === policyTenant)?.display_name || policyTenant} 可使用的数据后端和模型资产。`}
              closeLabel="关闭资源授权"
            />
            <div className="bot-dialog-body tenant-policy-dialog-body">
              {(error || policyQuery.error || systemQuery.error || backendAssetsQuery.error || backendPolicyQuery.error) && (
                <FeedbackBanner tone="error">{error || ((policyQuery.error ?? systemQuery.error ?? backendAssetsQuery.error ?? backendPolicyQuery.error) as Error).message}</FeedbackBanner>
              )}
              {notice && <FeedbackBanner tone="success">{notice}</FeedbackBanner>}

              <section className="tenant-resource-section" aria-label="数据后端授权">
                <div className="tenant-resource-section-head">
                  <div><DatabaseIcon size={16} /><div><strong>数据后端</strong><span>{selectedBackends.size} 个已授权</span></div></div>
                  <button type="button" className="secondary small-btn" disabled={busy !== '' || backendAssetsQuery.isLoading || backendPolicyQuery.isLoading} onClick={() => void saveBackendPolicy()}>{busy === 'backend-policy' ? '保存中…' : '保存数据后端授权'}</button>
                </div>
                {(backendAssetsQuery.isLoading || backendPolicyQuery.isLoading) ? (
                  <LoadingState compact label="正在读取数据后端与授权…" />
                ) : backendProfiles.length === 0 ? (
                  <div className="table-empty">平台还没有可授权的数据后端</div>
                ) : (
                  <div className="tenant-backend-options">
                    {backendProfiles.map((profile) => (
                      <label key={profile.profile_id} className={`tenant-backend-option${profile.status === 'active' ? '' : ' is-disabled'}`}>
                        <input type="checkbox" disabled={profile.status !== 'active'} checked={selectedBackends.has(profile.profile_id)} onChange={() => toggleBackend(profile.profile_id)} />
                        <span className="tenant-backend-option-copy">
                          <strong>{profile.display_name}</strong>
                          <small>{profile.driver} · {profile.domains.map((domain) => ({ session: '会话', memory: '偏好', knowledge: '知识库', artifact: '文件' } as Record<string, string>)[domain] ?? domain).join(' / ')}</small>
                        </span>
                        <span className="tenant-backend-option-state">{profile.status === 'active' ? '可用' : '已停用'}</span>
                      </label>
                    ))}
                  </div>
                )}
              </section>

              <section className="tenant-resource-section" aria-label="模型资产授权">
                <div className="tenant-resource-section-head">
                  <div><CpuIcon size={16} /><div><strong>模型资产</strong><span>{selectedModels.size} 个已授权</span></div></div>
                  <button type="button" className="secondary small-btn" disabled={busy !== '' || policyQuery.isLoading || systemQuery.isLoading} onClick={() => void saveModelPolicy()}>{busy === 'model-policy' ? '保存中…' : '保存模型授权'}</button>
                </div>
                {(policyQuery.isLoading || systemQuery.isLoading) ? (
                  <LoadingState compact label="正在读取模型资产与授权…" />
                ) : modelProviders.length === 0 ? (
                  <div className="table-empty">平台还没有可授权的模型资产</div>
                ) : (
                  <div className="tenant-model-provider-list">
                    {modelProviders.map((provider) => {
                      const providerID = modelProviderID(provider)
                      const models = modelProviderModels(provider)
                      const selectedCount = models.filter((model) => selectedModels.has(modelKey(providerID, model.name))).length
                      return (
                        <div className="tenant-model-provider" key={providerID}>
                          <div className="tenant-model-provider-title">
                            <span className="provider-icon"><CpuIcon size={17} /></span>
                            <div><strong>{providerID}</strong><span>{providerTypeLabel(provider.type ?? '')} · {models.length} 个模型</span></div>
                            <span className="tenant-model-provider-count">{selectedCount} / {models.length}</span>
                          </div>
                          <div className="tenant-model-options">
                            {models.length === 0 ? <span className="users-muted">暂无模型</span> : models.map((model) => (
                              <label key={model.name} className={`tenant-model-option${selectedModels.has(modelKey(providerID, model.name)) ? ' is-selected' : ''}`}>
                                <input type="checkbox" checked={selectedModels.has(modelKey(providerID, model.name))} onChange={() => toggleModel(providerID, model.name)} />
                                <span className="tenant-model-option-name">{model.name}</span>
                                <span className="tenant-model-option-state">{selectedModels.has(modelKey(providerID, model.name)) ? '已授权' : '未授权'}</span>
                              </label>
                            ))}
                          </div>
                        </div>
                      )
                    })}
                  </div>
                )}
              </section>
            </div>
            <div className="bot-dialog-footer">
              <Dialog.Close asChild><button type="button" className="secondary" disabled={busy !== ''}>关闭</button></Dialog.Close>
            </div>
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>

      <Dialog.Root open={creating} onOpenChange={(open) => { if (!open && busy !== 'create') setCreating(false) }}>
        <Dialog.Portal>
          <Dialog.Overlay className="modal-backdrop" />
          <Dialog.Content className="modal bot-dialog form-dialog">
            <DialogHeader
              className="bot-dialog-head"
              closeClassName="bot-dialog-close"
              title="新建租户"
              description="创建租户并指定首个租户管理员。"
              closeLabel="关闭新建租户"
            />
            <div className="bot-dialog-body form-dialog-body">
              {(error || usersQuery.error) && <FeedbackBanner tone="error">{error || (usersQuery.error as Error).message}</FeedbackBanner>}
              <div className="form-dialog-grid">
                <label>租户标识<input placeholder="例如 support" value={tenantID} onChange={(event) => setTenantID(event.target.value)} /></label>
                <label>租户名称<input placeholder="例如客户支持团队" value={displayName} onChange={(event) => setDisplayName(event.target.value)} /></label>
                <label className="form-dialog-full">
                  首个租户管理员
                  <SelectControl
                    ariaLabel="首个租户管理员"
                    value={initialAdmin}
                    disabled={usersQuery.isLoading || candidates.length === 0}
                    placeholder={usersQuery.isLoading ? '正在读取用户…' : '选择首个管理员'}
                    options={candidates.map((candidate) => ({ value: candidate.platform_user_id, label: memberDisplayName(candidate) }))}
                    onValueChange={setInitialAdmin}
                  />
                </label>
              </div>
            </div>
            <div className="bot-dialog-footer">
              <Dialog.Close asChild><button type="button" className="secondary" disabled={busy === 'create'}>取消</button></Dialog.Close>
              <button type="button" className="primary" disabled={busy !== '' || !tenantID.trim() || !displayName.trim() || !initialAdmin} onClick={() => void submit()}>{busy === 'create' ? '正在创建…' : '创建租户'}</button>
            </div>
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>
    </div>
  )
}
