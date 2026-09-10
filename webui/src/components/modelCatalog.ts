import type { ModelCapabilities, ModelInfo, ModelProviderInfo } from '../types'

export function modelProviderID(provider: ModelProviderInfo): string {
  return provider.id ?? ''
}

export function modelProviderType(provider: ModelProviderInfo): string {
  return provider.type ?? ''
}

export function modelProviderModels(provider: ModelProviderInfo): ModelInfo[] {
  return provider.models ?? []
}

export function providerTypeLabel(type: string): string {
  const normalized = type.toLowerCase()
  if (normalized === 'hunyuan') return '腾讯混元'
  if (normalized === 'huggingface') return 'HuggingFace'
  if (normalized.includes('openai')) return 'OpenAI 协议'
  return type || '未标注协议'
}

export function modelSelectionKey(providerID: string, modelName: string): string {
  return `${providerID}:::${modelName}`
}

export function parseModelSelection(value: string): { providerID: string; modelName: string } {
  const [providerID = '', modelName = ''] = value.split(':::')
  return { providerID, modelName }
}

export function findManagedModel(
  providers: ModelProviderInfo[],
  providerID: string,
  modelName: string,
): { provider: ModelProviderInfo; model: ModelInfo } | null {
  for (const provider of providers) {
    if (modelProviderID(provider) !== providerID) continue
    const model = modelProviderModels(provider).find((entry) => entry.name === modelName)
    if (model) return { provider, model }
  }
  return null
}

export function firstManagedModel(providers: ModelProviderInfo[]): { providerID: string; modelName: string } | null {
  for (const provider of providers) {
    const first = modelProviderModels(provider)[0]
    if (first) return { providerID: modelProviderID(provider), modelName: first.name }
  }
  return null
}

export function hasReasoningControls(capabilities?: ModelCapabilities): boolean {
  return Boolean(
    capabilities?.reasoning_efforts?.length ||
    capabilities?.thinking_toggle ||
    capabilities?.thinking_budget,
  )
}

export function reasoningEffortLabel(effort: string): string {
  return ({
    low: '低',
    medium: '中',
    high: '高',
    max: '最大',
    xhigh: '超高',
  } as Record<string, string>)[effort] ?? effort
}
