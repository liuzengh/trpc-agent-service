import { describe, expect, it } from 'vitest'

import { toolDisplayDescription, toolDisplayName } from './toolPresentation'

describe('toolPresentation', () => {
  it('uses Chinese names for platform built-ins', () => {
    expect(toolDisplayName({ name: 'duckduckgo_search', description: 'Search the web' })).toBe('网络搜索')
    expect(toolDisplayDescription({ name: 'duckduckgo_search', description: 'Search the web' })).toBe('搜索公开网页并返回结果摘要。')
    expect(toolDisplayName({ name: 'platform_present_card', description: 'Present a card' })).toBe('结果卡片')
  })

  it('uses a concise Chinese description as the friendly name for custom platform tools', () => {
    expect(toolDisplayName({ name: 'query_order', description: '查询订单' })).toBe('查询订单')
    expect(toolDisplayDescription({ name: 'query_order', description: '查询订单' })).toBe('平台提供的可授权工具。')
  })

  it('keeps the stable tool name when no safe friendly label exists', () => {
    expect(toolDisplayName({ name: 'context7_query_docs', description: 'Query package documentation and examples.' })).toBe('context7_query_docs')
  })
})
