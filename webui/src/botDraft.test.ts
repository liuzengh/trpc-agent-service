import { describe, expect, it } from 'vitest'
import { applicationPayloadFromDraft, botDraftFromSnapshot, EMPTY_BOT_DRAFT } from './botDraft'
import type { Snapshot } from './types'

const storageProfiles = {
  session_profile_id: 'platform-postgres',
  memory_profile_id: 'platform-postgres',
  knowledge_profile_id: 'platform-pgvector',
  artifact_profile_id: 'platform-postgres',
}

describe('bot draft', () => {
  it('creates a clean draft for a new bot', () => {
    const draft = botDraftFromSnapshot(null)
    expect(draft).not.toHaveProperty('tenant_id')
    expect(draft).toMatchObject({
      status: 'draft',
      session_profile_id: '',
      knowledge_profile_id: '',
      max_tool_calls: 8,
    })
  })

  it('preserves public channel access for legacy bindings without a policy', () => {
    const app = {
      Config: {
        tenant_id: 'tenant-a',
        app_code: 'support',
        status: 'active',
        model: {},
        tools: {},
        storage: {},
        governance: { max_tool_calls: 8, budget_units: 100 },
        audit: { retention_days: 90 },
        channels: [{ type: 'feishu', binding_id: 'support-feishu' }],
      },
    } as unknown as Snapshot

    expect(botDraftFromSnapshot(app).channels[0]?.access_policy).toBe('public')
  })

  it('takes tenant identity from the surrounding tenant context', () => {
    const draft = {
      ...EMPTY_BOT_DRAFT,
      app_code: 'support',
      provider_id: 'provider-a',
      model_name: 'model-a',
      ...storageProfiles,
    }
    const payload = applicationPayloadFromDraft({ draft, tenantID: 'tenant-a', mode: 'create', app: null })
    expect(payload.tenant_id).toBe('tenant-a')
  })

  it('rejects MCP confirmation tools that are not allowed', () => {
    const draft = {
      ...EMPTY_BOT_DRAFT,
      app_code: 'support',
      provider_id: 'provider-a',
      model_name: 'model-a',
      ...storageProfiles,
      tools_mcp: [{
        name: 'crm', description: '', transport: 'streamable' as const, url: 'https://example.com/mcp', credential_ref: '',
        allowed_tools: 'find_customer', confirmation_tools: 'create_ticket',
      }],
    }
    expect(() => applicationPayloadFromDraft({ draft, tenantID: 'tenant-a', mode: 'create', app: null }))
      .toThrow('审批工具 create_ticket 必须先加入允许工具')
  })

  it('gates model-specific reasoning settings by capabilities', () => {
    const draft = {
      ...EMPTY_BOT_DRAFT,
      app_code: 'support',
      provider_id: 'provider-a',
      model_name: 'model-a',
      ...storageProfiles,
      reasoning_effort: 'high',
      thinking_enabled: true,
      thinking_tokens: '2048',
    }
    const withoutCapabilities = applicationPayloadFromDraft({ draft, tenantID: 'tenant-a', mode: 'create', app: null })
    expect(withoutCapabilities.model.generation).toBeUndefined()

    const withCapabilities = applicationPayloadFromDraft({
      draft,
      tenantID: 'tenant-a',
      mode: 'create',
      app: null,
      capabilities: { reasoning_efforts: ['high'], thinking_toggle: true, thinking_budget: true },
    })
    expect(withCapabilities.model.generation).toEqual({
      reasoning_effort: 'high',
      thinking_enabled: true,
      thinking_tokens: 2048,
    })

    const reasoningDisabled = applicationPayloadFromDraft({
      draft: { ...draft, thinking_enabled: false },
      tenantID: 'tenant-a',
      mode: 'create',
      app: null,
      capabilities: { reasoning_efforts: ['high'], thinking_toggle: true },
    })
    expect(reasoningDisabled.model.generation).toBeUndefined()
  })

  it('keeps allowed roles only for tools that remain allowed', () => {
    const app = {
      Config: {
        tools: {
          allowed_roles: { search: ['member'], delete: ['admin'] },
        },
      },
    } as unknown as Snapshot
    const draft = {
      ...EMPTY_BOT_DRAFT,
      app_code: 'support',
      provider_id: 'provider-a',
      model_name: 'model-a',
      ...storageProfiles,
      tools_allowed: ['search'],
    }
    const payload = applicationPayloadFromDraft({ draft, tenantID: 'tenant-a', mode: 'edit', app })
    expect(payload.tools.allowed_roles).toEqual({ search: ['member'] })
  })

  it('drops legacy explicit result-card configuration because it is always available', () => {
    const app = {
      Config: {
        tenant_id: 'tenant-a', app_code: 'support', status: 'active', model: {}, storage: {},
        tools: { allowed: ['platform_present_card', 'duckduckgo_search'], require_confirmation: ['platform_present_card'] },
        governance: { max_tool_calls: 8, budget_units: 100 }, audit: { retention_days: 90 }, channels: [],
      },
    } as unknown as Snapshot
    const draft = botDraftFromSnapshot(app)
    expect(draft.tools_allowed).toEqual(['duckduckgo_search'])
    expect(draft.tools_require_confirmation).toEqual([])

    const payload = applicationPayloadFromDraft({
      draft: {
        ...draft,
        provider_id: 'provider-a', model_name: 'model-a', ...storageProfiles,
        tools_allowed: ['platform_present_card', 'duckduckgo_search'],
        tools_require_confirmation: ['platform_present_card'],
      },
      tenantID: 'tenant-a', mode: 'edit', app,
    })
    expect(payload.tools.allowed).toEqual(['duckduckgo_search'])
    expect(payload.tools.require_confirmation ?? []).toEqual([])
  })
})
