import { useEffect, useMemo, useState } from 'react'
import * as Dialog from '@radix-ui/react-dialog'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { FormProvider, useForm, useWatch, type UseFormSetValue } from 'react-hook-form'

import { createApplication, getApplicationRollout, getChannelStatuses, getTenantCatalog, stageApplicationVersion, updateApplication } from '../api'
import { applicationPayloadFromDraft, botDraftFromSnapshot, type BotDraft } from '../botDraft'
import { BotChannelsSection } from '../components/bots/BotChannelsSection'
import { BotModelSection } from '../components/bots/BotModelSection'
import { BotToolsSection } from '../components/bots/BotToolsSection'
import { DialogHeader } from '../components/DialogHeader'
import { FeedbackBanner } from '../components/FeedbackBanner'
import { DatabaseIcon, FileTextIcon } from '../components/Icons'
import { BranchIcon } from '../components/PageIcons'
import { PanelHeader } from '../components/PanelHeader'
import { SelectControl } from '../components/SelectControl'
import { findManagedModel, firstManagedModel } from '../components/modelCatalog'
import type { BackendDomain, Snapshot, TenantBackendProfile } from '../types'

export type BotSaveResult = {
  action: 'candidate' | 'saved'
  snapshot: Snapshot
}

export function BotModal({
  mode,
  app,
  tenants,
  currentTenant,
  onClose,
  onSaved,
}: {
  mode: 'create' | 'edit'
  app: Snapshot | null
  tenants: string[]
  currentTenant: string
  onClose: () => void
  onSaved: (result: BotSaveResult) => void | Promise<void>
}) {
  const queryClient = useQueryClient()
  const form = useForm<BotDraft>({
    defaultValues: botDraftFromSnapshot(app, currentTenant),
    mode: 'onSubmit',
  })
  const { control, getValues, handleSubmit, register, setValue, formState: { isSubmitting } } = form
  const tenantID = useWatch({ control, name: 'tenant_id' })
  const appCode = useWatch({ control, name: 'app_code' })
  const instruction = useWatch({ control, name: 'instruction' })
  const providerID = useWatch({ control, name: 'provider_id' })
  const modelName = useWatch({ control, name: 'model_name' })
  const sessionProfileID = useWatch({ control, name: 'session_profile_id' })
  const memoryProfileID = useWatch({ control, name: 'memory_profile_id' })
  const knowledgeProfileID = useWatch({ control, name: 'knowledge_profile_id' })
  const artifactProfileID = useWatch({ control, name: 'artifact_profile_id' })
  const [newTenant, setNewTenant] = useState(false)
  const [error, setError] = useState('')

  const catalogQuery = useQuery({
    queryKey: ['console', 'tenant-catalog', currentTenant],
    queryFn: ({ signal }) => getTenantCatalog(currentTenant, signal),
    enabled: Boolean(currentTenant),
  })
  const channelStatusQuery = useQuery({
    queryKey: ['console', 'channel-statuses', app?.Config.tenant_id ?? '', app?.Config.app_code ?? ''],
    queryFn: ({ signal }) => {
      if (!app) throw new Error('缺少机器人配置')
      return getChannelStatuses(app.Config.tenant_id, app.Config.app_code, signal)
    },
    enabled: Boolean(app),
    retry: false,
  })
  const rolloutQuery = useQuery({
    queryKey: ['console', 'application-rollout', app?.Config.tenant_id ?? '', app?.Config.app_code ?? ''],
    queryFn: () => {
      if (!app) throw new Error('缺少机器人配置')
      return getApplicationRollout(app.Config.tenant_id, app.Config.app_code)
    },
    enabled: mode === 'edit' && Boolean(app),
    retry: false,
  })
  const providers = catalogQuery.data?.model_providers ?? []
  const backendProfiles = catalogQuery.data?.backend_profiles ?? []
  const toolCatalog = catalogQuery.data?.tools ?? []
  const credentialRefs = catalogQuery.data?.channel_credential_refs ?? []
  const toolCredentialRefs = catalogQuery.data?.tool_credential_refs ?? []
  const channelStatuses = channelStatusQuery.data ?? []
  const queryError = catalogQuery.error ?? channelStatusQuery.error ?? rolloutQuery.error
  const visibleError = error || (queryError instanceof Error ? queryError.message : '')
  const rolloutActive = Boolean(rolloutQuery.data)

  useEffect(() => {
    if (!catalogQuery.data) return
    const currentProviderID = getValues('provider_id')
    const currentModelName = getValues('model_name')
    if (findManagedModel(providers, currentProviderID, currentModelName)) return
    if (mode === 'edit' && currentProviderID && currentModelName) return
    const first = firstManagedModel(providers)
    if (!first) return
    setValue('provider_id', first.providerID)
    setValue('model_name', first.modelName)
  }, [catalogQuery.data, getValues, mode, providers, setValue])

  useEffect(() => {
    if (!catalogQuery.data || mode !== 'create') return
    const defaults: Array<[BackendDomain, keyof Pick<BotDraft, 'session_profile_id' | 'memory_profile_id' | 'knowledge_profile_id' | 'artifact_profile_id'>]> = [
      ['session', 'session_profile_id'],
      ['memory', 'memory_profile_id'],
      ['knowledge', 'knowledge_profile_id'],
      ['artifact', 'artifact_profile_id'],
    ]
    for (const [domain, field] of defaults) {
      if (getValues(field)) continue
      const profile = backendProfiles.find((entry) => entry.available && entry.domains.includes(domain))
      if (profile) setValue(field, profile.profile_id)
    }
  }, [backendProfiles, catalogQuery.data, getValues, mode, setValue])

  const selectedModel = useMemo(() => findManagedModel(providers, providerID, modelName), [modelName, providerID, providers])
  const selectedCapabilities = selectedModel?.model.capabilities

  const submit = handleSubmit(async (draft, event) => {
    const submitter = (event?.nativeEvent as SubmitEvent | undefined)?.submitter as HTMLButtonElement | null | undefined
    const stageCandidate = mode === 'edit' && submitter?.value === 'stage'
    setError('')
    try {
      if (!selectedModel) throw new Error('请选择平台模型目录中的可用模型')
      const payload = applicationPayloadFromDraft({ draft, mode, app, capabilities: selectedCapabilities })
      let saved: Snapshot
      if (mode === 'create') saved = await createApplication(payload)
      else if (app && stageCandidate) saved = await stageApplicationVersion(app.Config.tenant_id, app.Config.app_code, payload)
      else if (app) saved = await updateApplication(app.Config.tenant_id, app.Config.app_code, payload)
      else throw new Error('缺少机器人配置')

      if (app && stageCandidate) {
        const candidateKey = ['console', 'application-candidate', app.Config.tenant_id, app.Config.app_code] as const
        const versionsKey = ['console', 'application-versions', app.Config.tenant_id, app.Config.app_code] as const
        queryClient.setQueryData(candidateKey, saved)
        queryClient.setQueryData<Snapshot[]>(versionsKey, (current) => current
          ? [saved, ...current.filter((version) => version.Config.config_version !== saved.Config.config_version)]
          : [saved])
      }
      await onSaved({ action: stageCandidate ? 'candidate' : 'saved', snapshot: saved })
    } catch (caught) {
      setError((caught as Error).message)
    }
  })

  return (
    <Dialog.Root open onOpenChange={(open) => { if (!open) onClose() }}>
      <Dialog.Portal>
        <Dialog.Overlay className="modal-backdrop" />
        <Dialog.Content className="modal bot-dialog" asChild>
          <form onSubmit={submit}>
            <FormProvider {...form}>
              <DialogHeader
                className="bot-dialog-head"
                closeClassName="bot-dialog-close"
                closeLabel="关闭机器人配置"
                title={mode === 'create' ? '创建机器人' : `编辑机器人 · ${app?.Config.app_code}`}
                description={mode === 'edit'
                  ? rolloutActive
                    ? '当前候选版本正在灰度。需要先在“版本发布”中停止或完成本轮发布，才能保存新的配置版本。'
                    : '可以直接发布为稳定版本；保存候选版本后会进入“版本发布”，再选择灰度范围和比例。'
                  : '完成基础配置后即可开始网页对话，之后仍可继续调整。'}
              />

              <div className="bot-dialog-body">
                <section className="bot-form-section">
                  <PanelHeader level={3} icon={<FileTextIcon size={18} />} title="基本信息" description="定义机器人的基本信息、业务场景和回答规则。" />
                  <div className="bot-section-content">
                    <div className="bot-two-column">
                      <div className="bot-field">
                        <label htmlFor="bot-tenant">
                          所属租户
                          {!newTenant ? (
                            <SelectControl id="bot-tenant" value={tenantID} disabled={mode === 'edit'} onValueChange={(value) => setValue('tenant_id', value, { shouldDirty: true })} options={tenants.map((entry) => ({ value: entry, label: entry }))} />
                          ) : (
                            <input id="bot-tenant" disabled={mode === 'edit'} placeholder="输入新租户标识" {...register('tenant_id')} />
                          )}
                        </label>
                        {mode === 'create' && <label className="checkbox bot-inline-option"><input type="checkbox" checked={newTenant} onChange={(event) => setNewTenant(event.target.checked)} />使用新租户</label>}
                      </div>
                      <label>机器人标识<input disabled={mode === 'edit'} placeholder="例如 support" {...register('app_code')} /></label>
                    </div>
                    <label className="bot-instruction-field">业务指令<textarea rows={4} maxLength={2000} placeholder="描述机器人的角色、业务范围和回答规则。" {...register('instruction')} /></label>
                    <div className="bot-field-meta"><span>用于定义机器人的业务场景、角色和回答规则。</span><span>{instruction.length}/2000</span></div>
                  </div>
                </section>

                <BotModelSection providers={providers} />

                <section className="bot-form-section compact">
                  <PanelHeader
                    level={3}
                    icon={<DatabaseIcon size={18} />}
                    title="数据后端"
                    description="只选择当前租户已获授权的平台数据后端；连接地址和凭据由系统管理员维护。"
                    actions={backendProfiles.length > 0 ? <span className="bot-section-count">已授权 {backendProfiles.filter((profile) => profile.available).length} 个后端</span> : undefined}
                  />
                  {backendProfiles.length === 0 ? (
                    <div className="bot-section-content"><div className="bot-storage-empty">当前租户还没有可用的数据后端授权，请先联系系统管理员配置。</div></div>
                  ) : (
                    <div className="bot-section-content">
                      <div className="bot-storage-grid">
                        <BackendProfileSelect label="会话" domain="session" field="session_profile_id" profiles={backendProfiles} value={sessionProfileID} setValue={setValue} />
                        <BackendProfileSelect label="长期偏好" domain="memory" field="memory_profile_id" profiles={backendProfiles} value={memoryProfileID} setValue={setValue} />
                        <BackendProfileSelect label="知识库" domain="knowledge" field="knowledge_profile_id" profiles={backendProfiles} value={knowledgeProfileID} setValue={setValue} />
                        <BackendProfileSelect label="文件" domain="artifact" field="artifact_profile_id" profiles={backendProfiles} value={artifactProfileID} setValue={setValue} />
                      </div>
                      <p className="bot-storage-hint">
                        每个下拉框只显示“支持该数据域 + 当前租户已授权”的数据后端。需要 Redis、Qdrant、S3/COS 或 Mem0 时，请先在系统管理的“数据后端”创建，再到租户资源授权中分配。
                      </p>
                    </div>
                  )}
                </section>

                <BotToolsSection toolCatalog={toolCatalog} toolCredentialRefs={toolCredentialRefs} />

                <section className="bot-form-section compact">
                  <PanelHeader level={3} icon={<BranchIcon size={18} />} title="运行配额" description="跨执行节点共享并发限制和每小时 Token 预算；未知用量会保留预留，不按 0 处理。" />
                  <div className="bot-section-content bot-three-column">
                    <label>最大并发运行<input type="number" min={0} {...register('max_concurrent_runs', { valueAsNumber: true })} /><small>0 表示不限制。</small></label>
                    <label>每小时 Token 预算<input type="number" min={0} placeholder="0 表示不限制" {...register('token_budget_per_hour')} /></label>
                    <label>单次 Token 预留<input type="number" min={0} placeholder="启用 Token 预算时必填" {...register('token_reservation')} /><small>模型服务未返回用量时保留该额度。</small></label>
                  </div>
                </section>

                <BotChannelsSection mode={mode} credentialRefs={credentialRefs} channelStatuses={channelStatuses} />
                {visibleError && <FeedbackBanner tone="error">{visibleError}</FeedbackBanner>}
              </div>

              <div className="bot-dialog-footer">
                <Dialog.Close asChild><button type="button" className="secondary">取消</button></Dialog.Close>
                {mode === 'edit' && <button type="submit" name="release_action" value="stage" className="secondary" disabled={isSubmitting || rolloutActive || !tenantID.trim() || !appCode.trim()}>{isSubmitting ? '保存中…' : '保存并配置灰度'}</button>}
                <button type="submit" className="primary" disabled={isSubmitting || rolloutActive || !tenantID.trim() || !appCode.trim()}>{isSubmitting ? '保存中…' : mode === 'create' ? '创建机器人' : '直接发布'}</button>
              </div>
            </FormProvider>
          </form>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  )
}

function BackendProfileSelect({
  label,
  domain,
  field,
  profiles,
  value,
  setValue,
}: {
  label: string
  domain: BackendDomain
  field: 'session_profile_id' | 'memory_profile_id' | 'knowledge_profile_id' | 'artifact_profile_id'
  profiles: TenantBackendProfile[]
  value: string
  setValue: UseFormSetValue<BotDraft>
}) {
  const options = profiles
    .filter((profile) => profile.available && profile.domains.includes(domain))
    .map((profile) => ({ value: profile.profile_id, label: `${profile.display_name} · ${profile.driver}` }))
  if (value && !options.some((option) => option.value === value)) {
    options.unshift({ value, label: `${value} · 当前不可用` })
  }
  return (
    <label className="bot-backend-field">
      <span className="bot-backend-field-label"><span>{label}</span><small>{options.length} 个可用</small></span>
      <SelectControl
        value={value}
        placeholder={options.length > 0 ? `选择${label}后端` : `没有可用的${label}后端`}
        disabled={options.length === 0}
        options={options}
        onValueChange={(next) => setValue(field, next, { shouldDirty: true })}
      />
    </label>
  )
}
