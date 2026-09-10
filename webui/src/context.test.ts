import { describe, expect, it } from 'vitest'

import { resolveActiveAppKey } from './context'
import type { Snapshot } from './types'

function app(tenant: string, code: string, status: 'draft' | 'active' | 'disabled' = 'active'): Snapshot {
  return {
    Config: {
      tenant_id: tenant,
      app_code: code,
      status,
      config_version: 1,
      channels: [],
      instruction: '',
      model: { provider_id: '', name: '' },
      tools: {},
      storage: {
        session: { profile_id: '' },
        memory: { profile_id: '' },
        knowledge: { profile_id: '' },
        artifact: { profile_id: '' },
      },
      governance: { max_tool_calls: 0, budget_units: 0 },
      audit: { retention_days: 0 },
    },
    Checksum: '',
    PublishedAt: '',
  }
}

describe('resolveActiveAppKey', () => {
  it('keeps the selected active application when it still exists', () => {
    expect(resolveActiveAppKey([app('tenant-a', 'one'), app('tenant-a', 'two')], 'tenant-a', 'tenant-a/two'))
      .toBe('tenant-a/two')
  })

  it('falls back in the same render when the selected application disappeared', () => {
    expect(resolveActiveAppKey([app('tenant-a', 'one')], 'tenant-a', 'tenant-a/deleted'))
      .toBe('tenant-a/one')
  })

  it('never carries an application selection across tenants', () => {
    expect(resolveActiveAppKey([app('tenant-a', 'one'), app('tenant-b', 'two')], 'tenant-b', 'tenant-a/one'))
      .toBe('tenant-b/two')
  })

  it('does not select a draft or disabled application for app-scoped pages', () => {
    expect(resolveActiveAppKey([app('tenant-a', 'draft', 'draft'), app('tenant-a', 'off', 'disabled')], 'tenant-a', 'tenant-a/off'))
      .toBe('')
  })
})
