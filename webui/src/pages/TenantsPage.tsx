import { useEffect, useState } from 'react'
import * as Dialog from '@radix-ui/react-dialog'
import { useQuery } from '@tanstack/react-query'

import {
  createTenant,
  getBackendProfiles,
  getSystem,
  getTenantBackendPolicy,
  getTenantModelPolicy,
  getTenantToolPolicy,
  getUsers,
  replaceTenantBackendPolicy,
  replaceTenantModelPolicy,
  replaceTenantToolPolicy,
  updateTenantStatus,
} from '../api'
import { useAppContext } from '../context'
import { isTenantConfigurableTool } from '../toolPolicy'
import { DialogHeader } from '../components/DialogHeader'
import { FeedbackBanner } from '../components/FeedbackBanner'
import { LoadingState } from '../components/LoadingState'
import { PanelHeader } from '../components/PanelHeader'
import { RefreshButton } from '../components/RefreshButton'
import { SegmentedControl } from '../components/SegmentedControl'
import { SelectControl } from '../components/SelectControl'
import { TogglePill } from '../components/TogglePill'
import { CpuIcon, DatabaseIcon, SettingsIcon, ShieldIcon } from '../components/Icons'
import { PlusIcon } from '../components/PageIcons'
import { memberDisplayName } from '../components/MemberIdentity'
import { modelProviderID, modelProviderModels, providerTypeLabel } from '../components/modelCatalog'
import { toolDisplayDescription, toolDisplayName } from '../components/toolPresentation'

type PolicySection = 'backends' | 'models' | 'tools'

function modelKey(providerID: string, modelName: string) {
  return `${providerID}\u0000${modelName}`
}

function setsEqual(left: Set<string>, right: Set<string>) {
  if (left.size !== right.size) return false
  for (const value of left) if (!right.has(value)) return false
  return true
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
  const [policySection, setPolicySection] = useState<PolicySection>('backends')
  const [selectedModels, setSelectedModels] = useState<Set<string>>(new Set())
  const [selectedBackends, setSelectedBackends] = useState<Set<string>>(new Set())
  const [selectedTools, setSelectedTools] = useState<Set<string>>(new Set())

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
  const toolPolicyQuery = useQuery({
    queryKey: ['console', 'tenant-tool-policy', policyTenant],
    queryFn: ({ signal }) => getTenantToolPolicy(policyTenant, signal),
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
  const platformTools = (toolPolicyQuery.data?.catalog ?? []).filter((tool) => isTenantConfigurableTool(tool.name))
  const savedModels = new Set((policyQuery.data?.models ?? []).map((model) => modelKey(model.provider_id, model.name)))
  const savedBackends = new Set((backendPolicyQuery.data?.profiles ?? []).map((profile) => profile.profile_id))
  const savedTools = new Set((toolPolicyQuery.data?.tools ?? []).map((tool) => tool.name).filter(isTenantConfigurableTool))
  const backendPolicyChanged = Boolean(backendPolicyQuery.data) && !setsEqual(selectedBackends, savedBackends)
  const modelPolicyChanged = Boolean(policyQuery.data) && !setsEqual(selectedModels, savedModels)
  const toolPolicyChanged = Boolean(toolPolicyQuery.data) && !setsEqual(selectedTools, savedTools)
  const policyChanged = backendPolicyChanged || modelPolicyChanged || toolPolicyChanged
  const policyLoading = systemQuery.isLoading || policyQuery.isLoading || toolPolicyQuery.isLoading || backendAssetsQuery.isLoading || backendPolicyQuery.isLoading
  const policyError = policyQuery.error ?? toolPolicyQuery.error ?? systemQuery.error ?? backendAssetsQuery.error ?? backendPolicyQuery.error
  const totalModels = modelProviders.reduce((total, provider) => total + modelProviderModels(provider).length, 0)

  useEffect(() => {
    if (!creating) return
    if (candidates.some((candidate) => candidate.platform_user_id === initialAdmin)) return
    setInitialAdmin(candidates.find((candidate) => candidate.platform_user_id === user?.platform_user_id)?.platform_user_id ?? candidates[0]?.platform_user_id ?? '')
  }, [candidates, creating, initialAdmin, user?.platform_user_id])

  useEffect(() => {
    if (!policyTenant || !policyQuery.data) return
    setSelectedModels(new Set(policyQuery.data.models.map((model) => modelKey(model.provider_id, model.name))))
  }, [policyTenant, policyQuery.data])

  useEffect(() => {
    if (!policyTenant || !backendPolicyQuery.data) return
    setSelectedBackends(new Set(backendPolicyQuery.data.profiles.map((profile) => profile.profile_id)))
  }, [policyTenant, backendPolicyQuery.data])

  useEffect(() => {
    if (!policyTenant || !toolPolicyQuery.data) return
    const available = new Set(toolPolicyQuery.data.catalog.map((tool) => tool.name).filter(isTenantConfigurableTool))
    setSelectedTools(new Set(toolPolicyQuery.data.tools.map((tool) => tool.name).filter((name) => available.has(name))))
  }, [policyTenant, toolPolicyQuery.data])

  const closePolicy = () => {
    if (busy !== '') return
    if (policyQuery.data) {
      setSelectedModels(new Set(policyQuery.data.models.map((model) => modelKey(model.provider_id, model.name))))
    }
    if (backendPolicyQuery.data) {
      setSelectedBackends(new Set(backendPolicyQuery.data.profiles.map((profile) => profile.profile_id)))
    }
    if (toolPolicyQuery.data) {
      setSelectedTools(new Set(toolPolicyQuery.data.tools.map((tool) => tool.name).filter(isTenantConfigurableTool)))
    }
    setError('')
    setNotice('')
    setPolicySection('backends')
    setPolicyTenant('')
  }

  const openPolicy = (tenant: string) => {
    setError('')
    setNotice('')
    setPolicySection('backends')
    setSelectedModels(new Set())
    setSelectedBackends(new Set())
    setSelectedTools(new Set())
    setPolicyTenant(tenant)
  }

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

  const toggleBackend = (profileID: string) => {
    setSelectedBackends((current) => {
      const next = new Set(current)
      if (next.has(profileID)) next.delete(profileID)
      else next.add(profileID)
      return next
    })
  }

  const toggleTool = (name: string) => {
    setSelectedTools((current) => {
      const next = new Set(current)
      if (next.has(name)) next.delete(name)
      else next.add(name)
      return next
    })
  }

  const saveResourcePolicy = async () => {
    if (!policyTenant || !policyChanged || policyLoading || policyError) return
    setBusy('resource-policy')
    setError('')
    setNotice('')
    try {
      const updates: Promise<unknown>[] = []
      if (backendPolicyChanged) updates.push(replaceTenantBackendPolicy(policyTenant, [...selectedBackends]))
      if (modelPolicyChanged) {
        const models = modelProviders.flatMap((provider) => {
          const providerID = modelProviderID(provider)
          return modelProviderModels(provider)
            .filter((model) => selectedModels.has(modelKey(providerID, model.name)))
            .map((model) => ({ provider_id: providerID, name: model.name }))
        })
        updates.push(replaceTenantModelPolicy(policyTenant, models))
      }
      if (toolPolicyChanged) {
        const available = new Set(platformTools.map((tool) => tool.name))
        updates.push(replaceTenantToolPolicy(policyTenant, [...selectedTools].filter((name) => available.has(name)).map((name) => ({ name }))))
      }
      await Promise.all(updates)
      await Promise.all([policyQuery.refetch(), backendPolicyQuery.refetch(), toolPolicyQuery.refetch()])
      setNotice(`已保存 ${tenantSummaries.find((entry) => entry.tenant_id === policyTenant)?.display_name || policyTenant} 的资源授权`)
    } catch (caught) {
      await Promise.allSettled([policyQuery.refetch(), backendPolicyQuery.refetch(), toolPolicyQuery.refetch()])
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

        <div className="table-scroll members-table-wrap">
          <table className="ui-table members-table">
            <thead><tr><th>租户</th><th>标识</th><th>状态</th><th>平台策略</th></tr></thead>
            <tbody>
              {tenantSummaries.length === 0 ? <tr><td colSpan={4} className="table-empty">暂无租户</td></tr> : tenantSummaries.map((entry) => (
                <tr key={entry.tenant_id}>
                  <td><strong>{entry.display_name || entry.tenant_id}</strong></td>
                  <td><span className="mono-value">{entry.tenant_id}</span></td>
                  <td><TogglePill active={entry.status === 'active'} activeLabel="正常" inactiveLabel="已停用" inactiveClassName="suspended" disabled={busy !== ''} onChange={(active) => void setStatus(entry.tenant_id, active)} /></td>
                  <td><button type="button" className="table-action" disabled={busy !== ''} onClick={() => openPolicy(entry.tenant_id)}>资源授权</button></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

      </section>

      <Dialog.Root open={Boolean(policyTenant)} onOpenChange={(open) => { if (!open) closePolicy() }}>
        <Dialog.Portal>
          <Dialog.Overlay className="modal-backdrop" />
          <Dialog.Content className="modal bot-dialog tenant-policy-dialog">
            <DialogHeader
              className="bot-dialog-head"
              closeClassName="bot-dialog-close"
              title="资源授权"
              description={`为 ${tenantSummaries.find((entry) => entry.tenant_id === policyTenant)?.display_name || policyTenant} 分配平台资源；机器人只能使用这里已授权的资源。`}
              closeLabel="关闭资源授权"
            />
            <div className="bot-dialog-body tenant-policy-dialog-body">
              {(error || policyError) && (
                <FeedbackBanner tone="error">{error || (policyError as Error).message}</FeedbackBanner>
              )}
              {notice && <FeedbackBanner tone="success">{notice}</FeedbackBanner>}

              <SegmentedControl
                ariaLabel="资源授权分类"
                className="tenant-policy-tabs"
                value={policySection}
                onValueChange={setPolicySection}
                items={[
                  { value: 'backends', label: '数据后端' },
                  { value: 'models', label: '模型' },
                  { value: 'tools', label: '工具' },
                ]}
              />

              {policySection === 'backends' && <section className="tenant-resource-section" aria-label="数据后端授权">
                <div className="tenant-resource-section-head">
                  <div><DatabaseIcon size={16} /><div><strong>数据后端</strong><span>{selectedBackends.size} / {backendProfiles.length} 个已授权</span></div></div>
                </div>
                {(backendAssetsQuery.isLoading || backendPolicyQuery.isLoading) ? (
                  <LoadingState compact label="正在读取数据后端与授权…" />
                ) : backendProfiles.length === 0 ? (
                  <div className="table-empty">平台还没有可授权的数据后端</div>
                ) : (
                  <div className="tenant-backend-options">
                    {backendProfiles.map((profile) => {
                      const selected = selectedBackends.has(profile.profile_id)
                      const cannotGrant = profile.status !== 'active' && !selected
                      return (
                        <label key={profile.profile_id} className={`tenant-backend-option${cannotGrant ? ' is-disabled' : ''}`}>
                          <input type="checkbox" disabled={busy !== '' || cannotGrant} checked={selected} onChange={() => toggleBackend(profile.profile_id)} />
                          <span className="tenant-backend-option-copy">
                            <strong>{profile.display_name}</strong>
                            <small>{profile.driver} · {profile.domains.map((domain) => ({ session: '会话', memory: '偏好', knowledge: '知识库', artifact: '文件' } as Record<string, string>)[domain] ?? domain).join(' / ')}</small>
                          </span>
                          <span className="tenant-backend-option-state">{profile.status === 'active' ? (selected ? '已授权' : '未授权') : (selected ? '已停用，可移除' : '已停用')}</span>
                        </label>
                      )
                    })}
                  </div>
                )}
              </section>}

              {policySection === 'models' && <section className="tenant-resource-section" aria-label="模型授权">
                <div className="tenant-resource-section-head">
                  <div><CpuIcon size={16} /><div><strong>模型</strong><span>{selectedModels.size} / {totalModels} 个已授权</span></div></div>
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
                                <input type="checkbox" disabled={busy !== ''} checked={selectedModels.has(modelKey(providerID, model.name))} onChange={() => toggleModel(providerID, model.name)} />
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
              </section>}

              {policySection === 'tools' && <section className="tenant-resource-section" aria-label="工具授权">
                <div className="tenant-resource-section-head">
                  <div><SettingsIcon size={16} /><div><strong>工具</strong><span>{selectedTools.size} / {platformTools.length} 个已授权</span></div></div>
                </div>
                {toolPolicyQuery.isLoading ? (
                  <LoadingState compact label="正在读取平台工具与授权…" />
                ) : platformTools.length === 0 ? (
                  <div className="table-empty">平台还没有可授权的工具</div>
                ) : (
                  <div className="tenant-backend-options">
                    {platformTools.map((tool) => {
                      const displayName = toolDisplayName(tool)
                      return (
                        <label key={tool.name} className="tenant-backend-option" title={tool.name}>
                          <input type="checkbox" disabled={busy !== ''} aria-label={`授权 ${displayName}`} checked={selectedTools.has(tool.name)} onChange={() => toggleTool(tool.name)} />
                          <span className="tenant-backend-option-copy">
                            <strong>{displayName}</strong>
                            <small>{toolDisplayDescription(tool)}</small>
                          </span>
                          <span className="tenant-backend-option-state">{selectedTools.has(tool.name) ? '已授权' : '未授权'}</span>
                        </label>
                      )
                    })}
                  </div>
                )}
              </section>}
            </div>
            <div className="bot-dialog-footer">
              <Dialog.Close asChild><button type="button" className="secondary" disabled={busy !== ''}>{policyChanged ? '取消' : '关闭'}</button></Dialog.Close>
              <button type="button" className="primary" disabled={busy !== '' || policyLoading || Boolean(policyError) || !policyChanged} onClick={() => void saveResourcePolicy()}>{busy === 'resource-policy' ? '保存中…' : '保存授权'}</button>
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
