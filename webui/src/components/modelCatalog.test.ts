import { describe, expect, it } from 'vitest'
import {
  findManagedModel,
  firstManagedModel,
  hasReasoningControls,
  modelProviderModels,
  providerTypeLabel,
  reasoningEffortLabel,
} from './modelCatalog'
import type { ModelProviderInfo } from '../types'

const providers: ModelProviderInfo[] = [
  {
    id: 'primary',
    type: 'openai',
    models: [
      { name: 'plain' },
      { name: 'reasoning', capabilities: { reasoning_efforts: ['low', 'high'], thinking_budget: true } },
    ],
  },
]

describe('model catalog', () => {
  it('keeps model identity and capabilities together', () => {
    expect(modelProviderModels(providers[0]).map((model) => model.name)).toEqual(['plain', 'reasoning'])
    expect(findManagedModel(providers, 'primary', 'reasoning')?.model.capabilities?.reasoning_efforts).toEqual(['low', 'high'])
    expect(findManagedModel(providers, 'primary', 'missing')).toBeNull()
    expect(firstManagedModel(providers)).toEqual({ providerID: 'primary', modelName: 'plain' })
  })

  it('only exposes reasoning controls declared by the model', () => {
    expect(hasReasoningControls(findManagedModel(providers, 'primary', 'plain')?.model.capabilities)).toBe(false)
    expect(hasReasoningControls(findManagedModel(providers, 'primary', 'reasoning')?.model.capabilities)).toBe(true)
    expect(reasoningEffortLabel('low')).toBe('低')
    expect(reasoningEffortLabel('high')).toBe('高')
    expect(providerTypeLabel('huggingface')).toBe('HuggingFace')
  })
})
