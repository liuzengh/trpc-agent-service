import { useState, type ReactNode } from 'react'
import { useQuery } from '@tanstack/react-query'
import { deleteModel, getSystem, syncModels } from '../api'
import type { ModelInfo, ModelProviderInfo } from '../types'
import { CheckCircleIcon, CpuIcon, ServerIcon, TrashIcon } from '../components/Icons'
import { CopyButton } from '../components/CopyButton'
import { StatusIndicator } from '../components/StatusIndicator'
import { LoadingState } from '../components/LoadingState'
import { modelProviderID, modelProviderModels, modelProviderType, providerTypeLabel } from '../components/modelCatalog'
import { ConfirmDialog } from '../components/ConfirmDialog'
import { FeedbackBanner } from '../components/FeedbackBanner'
import { RefreshButton } from '../components/RefreshButton'
import { SearchField } from '../components/SearchField'
import { Toast, type ToastTone } from '../components/Toast'

export function ModelsPage() {
  const [error, setError] = useState('')
  const [syncing, setSyncing] = useState(false)
  const [modelFilter, setModelFilter] = useState('')
  const [toast, setToast] = useState<{ message: string; tone: ToastTone } | null>(null)
  const [modelToDelete, setModelToDelete] = useState<{ providerID: string; model: ModelInfo } | null>(null)
  const [deleting, setDeleting] = useState(false)
  const statusQuery = useQuery({
    queryKey: ['console', 'system-status'],
    queryFn: ({ signal }) => getSystem(signal),
    staleTime: 10_000,
  })
  const status = statusQuery.data ?? null
  const loading = statusQuery.isLoading

  const handleSync = async () => {
    try {
      setSyncing(true)
      setError('')
      const result = await syncModels()
      const failed = result.model_providers.filter((provider) => provider.sync_error)
      await statusQuery.refetch()
      setToast(failed.length > 0
        ? { message: `${failed.length} 个模型服务同步失败，请检查连接或凭据`, tone: 'warning' }
        : { message: '模型目录已同步', tone: 'success' })
    } catch (err) {
      setToast({ message: `同步失败：${(err as Error).message}`, tone: 'error' })
    } finally {
      setSyncing(false)
    }
  }

  const handleDeleteModel = async () => {
    if (!modelToDelete) return
    try {
      setDeleting(true)
      await deleteModel(modelToDelete.providerID, modelToDelete.model.name)
      await statusQuery.refetch()
      setToast({ message: `已移除 ${modelToDelete.model.name}`, tone: 'success' })
      setModelToDelete(null)
    } catch (err) {
      setToast({ message: (err as Error).message, tone: 'error' })
    } finally {
      setDeleting(false)
    }
  }

  const providers: ModelProviderInfo[] = status?.info.model_providers ?? []
  const configuredCount = providers.filter((p) => p.configured).length
  const totalModelsCount = providers.reduce((acc, provider) => acc + modelProviderModels(provider).length, 0)

  if (loading) {
    return <div className="page-stack models-page"><LoadingState label="正在读取模型目录…" /></div>
  }

  return (
    <div className="page-stack models-page">
      {(error || statusQuery.error) && <FeedbackBanner tone="error">{error || (statusQuery.error as Error).message}</FeedbackBanner>}
      {toast && <Toast key={`${toast.tone}:${toast.message}`} message={toast.message} tone={toast.tone} onClose={() => setToast(null)} />}

      <dl className="model-summary" aria-label="模型资产汇总">
        <ModelSummaryCard icon={<ServerIcon size={23} />} label="模型服务" value={String(providers.length)} tone="info" />
        <ModelSummaryCard icon={<CheckCircleIcon size={23} />} label="已配置" value={`${configuredCount} / ${providers.length}`} tone="success" />
        <ModelSummaryCard icon={<CpuIcon size={23} />} label="可用模型" value={String(totalModelsCount)} tone="violet" />
      </dl>

      <section className="models-catalog-panel">
        <div className="models-catalog-head">
          <h2>模型服务</h2>
          <div className="models-catalog-actions">
            {totalModelsCount > 0 && (
              <SearchField
                className="models-search"
                value={modelFilter}
                onValueChange={setModelFilter}
                ariaLabel="搜索模型"
                placeholder="搜索模型…"
                onClear={() => setModelFilter('')}
              />
            )}
            <RefreshButton onClick={() => void handleSync()} disabled={loading} loading={syncing} label="同步模型" />
          </div>
        </div>

        {providers.length === 0 && !loading && (
          <div className="table-empty">尚未配置模型服务</div>
        )}

        <div className="model-providers-grid">
          {providers.map((p) => {
            const id = modelProviderID(p)
            const providerType = modelProviderType(p)
            const isConfigured = p.configured ?? false
            const baseURL = p.base_url ?? ''
            const credentialRefs = p.credential_refs ?? []
            const syncError = p.sync_error ?? ''
            const allModels = modelProviderModels(p)
            const models = modelFilter.trim()
              ? allModels.filter((m) => m.name.toLowerCase().includes(modelFilter.trim().toLowerCase()))
              : allModels

            return (
              <article key={id} className="provider-card">
                <div className="provider-head">
                  <div className="provider-name-row">
                    <span className="provider-icon"><CpuIcon size={19} /></span>
                    <div className="provider-title-copy">
                      <div className="provider-title-line">
                        <h3 className="provider-id">{id}</h3>
                        <StatusIndicator tone={isConfigured ? 'success' : 'danger'} appearance="pill">
                          {isConfigured ? '已配置' : '缺少凭据'}
                        </StatusIndicator>
                      </div>
                    </div>
                  </div>
                  <span className="provider-protocol">{providerTypeLabel(providerType)}</span>
                </div>

                <div className="provider-body">
                  <div className="provider-endpoint-block">
                    <span className="provider-field-label">端点</span>
                    <div className="provider-endpoint-value">
                      <span className="mono-soft provider-url" title={baseURL || '官方默认端点'}>
                        {baseURL || '官方默认端点'}
                      </span>
                      {baseURL && (
                        <CopyButton
                          value={baseURL}
                          label={`复制 ${id} 端点`}
                          copiedLabel={`已复制 ${id} 端点`}
                          className="provider-copy-button"
                          iconOnly
                        />
                      )}
                    </div>
                  </div>

                  <div className="provider-endpoint-block">
                    <span className="provider-field-label">凭据引用</span>
                    <div className="provider-credential-refs">
                      {credentialRefs.length === 0
                        ? <span className="muted-soft hint">未配置</span>
                        : credentialRefs.map((reference) => <code key={reference}>{reference}</code>)}
                    </div>
                  </div>

                  <div className="models-section">
                    <div className="models-section-head">
                      <span className="provider-field-label">模型 {allModels.length}</span>
                    </div>
                    <div className="model-tags-list model-tags-scroll">
                      {allModels.length === 0 ? (
                        <span className={`muted-soft hint ${syncError ? 'model-sync-error' : ''}`}>
                          {syncError || '暂无模型'}
                        </span>
                      ) : models.length === 0 ? (
                        <span className="muted-soft hint">无匹配项</span>
                      ) : (
                        models.map((model) => (
                          <div key={model.name} className="model-tag-item">
                            <span className="model-name">{model.name}</span>
                            {model.source === 'discovered' && (
                              <button
                                type="button"
                                className="model-delete-button"
                                aria-label={`删除模型 ${model.name}`}
                                title="删除模型"
                                onClick={() => setModelToDelete({ providerID: id, model })}
                              >
                                <TrashIcon size={14} />
                              </button>
                            )}
                          </div>
                        ))
                      )}
                    </div>
                  </div>
                </div>
              </article>
            )
          })}
        </div>
      </section>

      <ConfirmDialog
        open={Boolean(modelToDelete)}
        title="删除模型？"
        description={modelToDelete
          ? `将 ${modelToDelete.model.name} 从当前平台模型目录移除。正在被机器人使用的模型不能删除；重新同步模型后可以再次获取。`
          : ''}
        confirmLabel="删除模型"
        busy={deleting}
        onConfirm={() => void handleDeleteModel()}
        onOpenChange={(open) => { if (!open && !deleting) setModelToDelete(null) }}
      />
    </div>
  )
}

function ModelSummaryCard({
  icon,
  label,
  value,
  tone,
}: {
  icon: ReactNode
  label: string
  value: string
  tone: 'info' | 'success' | 'violet'
}) {
  return (
    <div className={`model-summary-card tone-${tone}`}>
      <span className="model-summary-icon" aria-hidden="true">{icon}</span>
      <div>
        <dt>{label}</dt>
        <dd>{value}</dd>
      </div>
      <span className="model-summary-ghost" aria-hidden="true">{icon}</span>
    </div>
  )
}
