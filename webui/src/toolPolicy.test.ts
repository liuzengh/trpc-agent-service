import { describe, expect, it } from 'vitest'

import { isTenantConfigurableTool, PRESENT_CARD_TOOL_NAME } from './toolPolicy'

describe('tool policy', () => {
  it('keeps result-card rendering as a platform capability instead of a tenant-configurable tool', () => {
    expect(isTenantConfigurableTool(PRESENT_CARD_TOOL_NAME)).toBe(false)
    expect(isTenantConfigurableTool('duckduckgo_search')).toBe(true)
  })
})
