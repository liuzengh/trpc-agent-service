import { useFieldArray, useFormContext, useWatch } from 'react-hook-form'

import type { BotDraft } from '../../botDraft'
import type { ChannelBindingStatus } from '../../types'
import { ChannelBrandIcon } from '../ChannelBrand'
import { XIcon } from '../Icons'
import { PlusIcon, RouteIcon } from '../PageIcons'
import { PanelHeader } from '../PanelHeader'
import { SelectControl } from '../SelectControl'
import { StatusIndicator } from '../StatusIndicator'
import { CHANNEL_OPTIONS, channelBindingPlaceholder, channelCredentialLabel, channelLabel } from '../channelMeta'

export function BotChannelsSection({
  mode,
  credentialRefs,
  channelStatuses,
}: {
  mode: 'create' | 'edit'
  credentialRefs: string[]
  channelStatuses: ChannelBindingStatus[]
}) {
  const { control, register, setValue } = useFormContext<BotDraft>()
  const channels = useWatch({ control, name: 'channels' }) ?? []
  const channelFields = useFieldArray({ control, name: 'channels' })
  const credentialValues = new Set(credentialRefs)
  for (const binding of channels) if (binding.credential_ref.trim()) credentialValues.add(binding.credential_ref.trim())
  const credentialOptions = [...credentialValues].map((value) => ({ value, label: channelCredentialLabel(value) }))
  const statusByBinding = new Map(channelStatuses.map((entry) => [`${entry.channel}/${entry.binding_id}`, entry]))

  return (
    <section className="bot-form-section compact">
      <PanelHeader
        level={3}
        icon={<RouteIcon size={18} />}
        title="外部渠道"
        description="需要接入企业微信、Telegram 或飞书时再添加；仅网页对话无需配置。"
        actions={
          <>
            {channels.length > 0 && <span>已配置 {channels.length} 个</span>}
            <button
              type="button"
              className="secondary small-btn"
              onClick={() => channelFields.append({
                type: 'telegram', binding_id: '', credential_ref: credentialRefs[0] ?? '', access_policy: 'member_only', allowlist: '',
              })}
            >
              <PlusIcon size={13} /> 添加渠道
            </button>
          </>
        }
      />
      <div className="bot-section-content channels-body">
        {channels.length === 0 && <p className="bot-empty-inline">当前仅使用网页对话。</p>}
        {channelFields.fields.map((field, index) => {
          const binding = channels[index] ?? field
          const runtime = mode === 'edit' ? statusByBinding.get(`${binding.type}/${binding.binding_id}`) : undefined
          const state = runtime?.state ?? 'offline'
          const labels: Record<string, string> = { ready: '已就绪', connecting: '连接中', connected: '已连接', error: '异常', offline: '离线' }
          const tone = state === 'connected' || state === 'ready' ? 'success' : state === 'error' ? 'danger' : state === 'connecting' ? 'info' : 'neutral'
          return (
            <article key={field.id} className="bot-channel-card">
              <header className="bot-channel-card-head">
                <span className={`bot-channel-card-brand channel-${binding.type}`}><ChannelBrandIcon channel={binding.type} size={19} /></span>
                <div className="bot-channel-card-title">
                  <strong>{channelLabel(binding.type)}</strong>
                  <small>{binding.binding_id.trim() || `渠道 ${index + 1} · 尚未填写绑定标识`}</small>
                </div>
                {mode === 'edit' && <span className="bot-channel-health" title={runtime?.last_error || ''}><StatusIndicator tone={tone}>{labels[state] ?? state}</StatusIndicator></span>}
                <button type="button" className="icon-btn" aria-label={`删除渠道 ${index + 1}`} onClick={() => channelFields.remove(index)}><XIcon size={15} /></button>
              </header>
              <div className="bot-channel-fields">
                <label>
                  渠道类型
                  <SelectControl
                    ariaLabel={`渠道 ${index + 1}`}
                    value={binding.type}
                    shellClassName={`channel-select channel-${binding.type}`}
                    leading={<ChannelBrandIcon channel={binding.type} size={18} />}
                    onValueChange={(value) => setValue(`channels.${index}.type`, value, { shouldDirty: true })}
                    options={CHANNEL_OPTIONS.map((channel) => ({ value: channel.value, label: channel.label, leading: <ChannelBrandIcon channel={channel.value} size={17} /> }))}
                  />
                </label>
                <label>
                  机器人账号 / Binding ID
                  <input
                    placeholder={channelBindingPlaceholder(binding.type)}
                    aria-label={`${channelLabel(binding.type)}机器人账号标识`}
                    {...register(`channels.${index}.binding_id`)}
                  />
                </label>
                <label>
                  平台凭据
                  <SelectControl
                    value={binding.credential_ref}
                    ariaLabel={`${channelLabel(binding.type)}平台凭据`}
                    placeholder={credentialOptions.length > 0 ? '选择平台凭据' : '暂无平台凭据'}
                    disabled={credentialOptions.length === 0}
                    options={credentialOptions}
                    valueLabel={binding.credential_ref ? channelCredentialLabel(binding.credential_ref) : undefined}
                    onValueChange={(value) => setValue(`channels.${index}.credential_ref`, value, { shouldDirty: true })}
                  />
                </label>
                <label>
                  访问范围
                  <SelectControl
                    value={binding.access_policy}
                    ariaLabel={`${channelLabel(binding.type)}访问范围`}
                    options={[
                      { value: 'member_only', label: '仅租户成员' },
                      { value: 'allowlist', label: '成员或白名单' },
                      { value: 'public', label: '公开访问' },
                    ]}
                    onValueChange={(value) => setValue(`channels.${index}.access_policy`, value as BotDraft['channels'][number]['access_policy'], { shouldDirty: true })}
                  />
                </label>
                {binding.access_policy === 'allowlist' && (
                  <label className="bot-channel-allowlist">
                    外部用户白名单
                    <input
                      placeholder="外部用户 ID，多个用逗号分隔"
                      aria-label={`${channelLabel(binding.type)}白名单`}
                      {...register(`channels.${index}.allowlist`)}
                    />
                  </label>
                )}
              </div>
            </article>
          )
        })}
        {channels.length > 0 && credentialOptions.length === 0 && <p className="bot-channel-hint">当前没有可用的平台渠道凭据，请先由平台管理员配置凭据后再发布。</p>}
      </div>
    </section>
  )
}
