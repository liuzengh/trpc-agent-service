import { describe, expect, it } from 'vitest'

import type { MemberSummary } from '../api'
import { memberDisplayName, memberProviderText } from './MemberIdentity'

function member(overrides: Partial<MemberSummary> = {}): MemberSummary {
  return {
    platform_user_id: '617727f4-a118-4e86-942a-96e3392c4ca5',
    display_name: '',
    email: '',
    role: 'member',
    status: 'active',
    is_system_admin: false,
    last_login_at: '',
    providers: [],
    conversation_content_audit: false,
    ...overrides,
  }
}

describe('member identity copy', () => {
  it('does not present an opaque platform id as a human name', () => {
    expect(memberDisplayName(member({ display_name: '617727f4-a118-4e86-942a-96e3392c4ca5' }))).toBe('未命名用户')
  })

  it('prefers a real display name and explains missing login identity', () => {
    const target = member({ display_name: 'Mock Admin' })
    expect(memberDisplayName(target)).toBe('Mock Admin')
    expect(memberProviderText(target)).toBe('未关联')
  })

  it('renders configured login providers as readable names', () => {
    expect(memberProviderText(member({ providers: ['飞书', '企业微信'] }))).toBe('飞书 · 企业微信')
  })
})
