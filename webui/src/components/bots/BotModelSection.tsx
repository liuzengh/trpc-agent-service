import { useMemo } from 'react'
import { useFieldArray, useFormContext, useWatch } from 'react-hook-form'

import type { BotDraft } from '../../botDraft'
import type { ModelProviderInfo } from '../../types'
import { CpuIcon, XIcon } from '../Icons'
import { PanelHeader } from '../PanelHeader'
import { SelectControl } from '../SelectControl'
import {
  findManagedModel,
  hasReasoningControls,
  modelProviderID,
  modelProviderModels,
  modelProviderType,
  modelSelectionKey,
  parseModelSelection,
  providerTypeLabel,
  reasoningEffortLabel,
} from '../modelCatalog'

export function BotModelSection({ providers }: { providers: ModelProviderInfo[] }) {
  const { control, register, setValue } = useFormContext<BotDraft>()
  const [
    providerID,
    modelName,
    customTemperature,
    temperature,
    maxTokens,
    topP,
    thinkingEnabled,
    reasoningEffort,
    thinkingTokens,
    failoverCandidates = [],
  ] = useWatch({
    control,
    name: [
      'provider_id',
      'model_name',
      'custom_temperature',
      'temperature',
      'max_tokens',
      'top_p',
      'thinking_enabled',
      'reasoning_effort',
      'thinking_tokens',
      'failover_candidates',
    ],
  })
  const failover = useFieldArray({ control, name: 'failover_candidates' })
  const selectedModel = useMemo(() => findManagedModel(providers, providerID, modelName), [modelName, providerID, providers])
  const selectedCapabilities = selectedModel?.model.capabilities
  const reasoningControlsAvailable = hasReasoningControls(selectedCapabilities)
  const reasoningEffortDisabled = Boolean(selectedCapabilities?.thinking_toggle && !thinkingEnabled)
  const modelGroups = useMemo(() => providers
    .map((provider) => {
      const id = modelProviderID(provider)
      const providerType = providerTypeLabel(modelProviderType(provider))
      return {
        id,
        label: (
          <span className="model-select-group">
            <strong>{id}</strong>
            <span>{providerType}</span>
          </span>
        ),
        options: modelProviderModels(provider).map((model) => ({
          value: modelSelectionKey(id, model.name),
          label: (
            <span className="model-select-option">
              <span>{model.name}</span>
              {hasReasoningControls(model.capabilities) && <small>支持推理</small>}
            </span>
          ),
        })),
      }
    })
    .filter((group) => group.options.length > 0), [providers])
  const fallbackGroups = useMemo(() => modelGroups
    .map((group) => ({
      ...group,
      options: group.options.filter((option) => {
        const parsed = parseModelSelection(option.value)
        if (parsed.providerID === providerID && parsed.modelName === modelName) return false
        return !failoverCandidates.some((candidate) => candidate.provider_id === parsed.providerID && candidate.name === parsed.modelName)
      }),
    }))
    .filter((group) => group.options.length > 0), [failoverCandidates, modelGroups, modelName, providerID])

  const selectModel = (value: string) => {
    const parsed = parseModelSelection(value)
    const match = findManagedModel(providers, parsed.providerID, parsed.modelName)
    if (!match) return
    const capabilities = match.model.capabilities
    setValue('provider_id', parsed.providerID, { shouldDirty: true })
    setValue('model_name', parsed.modelName, { shouldDirty: true })
    if (!capabilities?.reasoning_efforts?.some((effort) => effort === reasoningEffort)) {
      setValue('reasoning_effort', '', { shouldDirty: true })
    }
    if (!capabilities?.thinking_toggle) setValue('thinking_enabled', false, { shouldDirty: true })
    if (!capabilities?.thinking_budget) setValue('thinking_tokens', '', { shouldDirty: true })
  }

  return (
    <section className="bot-form-section">
      <PanelHeader level={3} icon={<CpuIcon size={18} />} title="模型" description="选择机器人使用的模型，并配置相关参数。" />
      <div className="bot-section-content">
        {modelGroups.length > 0 ? (
          <label htmlFor="bot-model-select">
            平台模型
            <SelectControl
              id="bot-model-select"
              value={modelSelectionKey(providerID, modelName)}
              onValueChange={selectModel}
              groups={modelGroups}
              valueLabel={modelName ? (
                <span className="model-select-value">
                  <strong>{modelName}</strong>
                  <span>{providerID}{selectedModel ? ` · ${providerTypeLabel(modelProviderType(selectedModel.provider))}` : ''}</span>
                </span>
              ) : undefined}
              placeholder="选择平台模型"
            />
          </label>
        ) : (
          <div className="bot-model-empty">当前平台还没有可用模型，请先在「模型资产」中同步模型。</div>
        )}
        {providerID && modelName && !selectedModel && <div className="bot-model-warning" role="alert">当前版本使用的模型已不在平台目录，请重新选择后再发布。</div>}

        <details className="bot-model-advanced" open>
          <summary>
            <span className="bot-advanced-title">高级模型设置</span>
            <span className="bot-advanced-state">
              {customTemperature || maxTokens.trim() || topP.trim() || thinkingEnabled || reasoningEffort || thinkingTokens.trim()
                ? '已配置调优参数'
                : '使用模型默认'}
            </span>
          </summary>
          <div className="bot-advanced-body">
            <div className="bot-advanced-row">
              <label className="checkbox bot-setting-checkbox">
                <input type="checkbox" {...register('custom_temperature')} />
                <span><strong>自定义采样温度</strong><small>未启用时使用模型默认温度。</small></span>
              </label>
              {customTemperature && (
                <div className="slider-control">
                  <input type="range" min="0" max="2" step="0.1" {...register('temperature', { valueAsNumber: true })} />
                  <span className="slider-val">{Number(temperature).toFixed(1)}</span>
                </div>
              )}
            </div>

            <div className="bot-two-column">
              <label>最大输出长度<input type="number" min={1} placeholder="留空为模型默认" {...register('max_tokens')} /></label>
              <label>采样阈值<input type="number" min={0} max={1} step={0.05} placeholder="例如 0.95（留空为默认）" {...register('top_p')} /></label>
            </div>

            {selectedModel && (
              <div className="bot-reasoning-config">
                <div className="bot-reasoning-heading">
                  <strong>推理设置</strong>
                  <small>{reasoningControlsAvailable ? '仅显示当前模型实际支持的推理参数。' : '当前模型没有声明可调推理参数，将使用模型默认行为。'}</small>
                </div>
                {selectedCapabilities?.thinking_toggle && (
                  <label className="checkbox bot-setting-checkbox">
                    <input
                      type="checkbox"
                      {...register('thinking_enabled', {
                        onChange: (event) => {
                          if (!event.target.checked) setValue('reasoning_effort', '', { shouldDirty: true })
                        },
                      })}
                    />
                    <span><strong>启用推理模式</strong><small>显式开启该模型提供的思考模式；关闭时使用服务默认行为。</small></span>
                  </label>
                )}
                {selectedCapabilities?.reasoning_efforts && selectedCapabilities.reasoning_efforts.length > 0 && (
                  <label htmlFor="bot-reasoning-effort">
                    推理强度
                    <SelectControl
                      id="bot-reasoning-effort"
                      value={reasoningEffortDisabled ? '__default__' : (reasoningEffort || '__default__')}
                      disabled={reasoningEffortDisabled}
                      onValueChange={(value) => setValue('reasoning_effort', value === '__default__' ? '' : value, { shouldDirty: true })}
                      options={[
                        { value: '__default__', label: '跟随模型默认' },
                        ...selectedCapabilities.reasoning_efforts.map((effort) => ({ value: effort, label: reasoningEffortLabel(effort) })),
                      ]}
                    />
                    {reasoningEffortDisabled && <small className="field-help">请先启用推理模式，再选择推理强度。</small>}
                  </label>
                )}
                {selectedCapabilities?.thinking_budget && (
                  <label>
                    推理 Token 上限
                    <input type="number" min={1} placeholder="留空为模型默认" {...register('thinking_tokens')} />
                    <small className="field-help">限制模型内部推理最多可使用的 Token 数，仅当前模型支持时才会生效。</small>
                  </label>
                )}
              </div>
            )}
          </div>
        </details>

        <div className="bot-failover-block">
          <div className="bot-subsection-head">
            <div><strong>备用模型</strong><p>主模型超时、限流或服务故障时，按顺序尝试备用模型。</p></div>
            <div className="fallback-add-select">
              <SelectControl
                ariaLabel="添加备用模型"
                value=""
                placeholder={fallbackGroups.length > 0 ? '添加备用模型' : '没有更多可选模型'}
                disabled={fallbackGroups.length === 0}
                groups={fallbackGroups}
                onValueChange={(value) => {
                  const parsed = parseModelSelection(value)
                  if (parsed.providerID && parsed.modelName) failover.append({ provider_id: parsed.providerID, name: parsed.modelName })
                }}
              />
            </div>
          </div>
          {failover.fields.length > 0 && (
            <div className="failover-editor">
              {failover.fields.map((field, index) => {
                const candidate = failoverCandidates[index] ?? field
                return (
                  <div key={field.id} className="bot-failover-row">
                    <span className="bot-failover-index">备选 #{index + 1}</span>
                    <SelectControl
                      ariaLabel={`备用模型 ${index + 1}`}
                      value={modelSelectionKey(candidate.provider_id, candidate.name)}
                      groups={modelGroups}
                      valueLabel={<span className="model-select-value"><strong>{candidate.name}</strong><span>{candidate.provider_id}</span></span>}
                      onValueChange={(value) => {
                        const parsed = parseModelSelection(value)
                        failover.update(index, { provider_id: parsed.providerID, name: parsed.modelName })
                      }}
                    />
                    <button type="button" className="icon-btn" aria-label={`删除备用模型 ${index + 1}`} onClick={() => failover.remove(index)}>
                      <XIcon size={14} />
                    </button>
                  </div>
                )
              })}
            </div>
          )}
        </div>
      </div>
    </section>
  )
}
