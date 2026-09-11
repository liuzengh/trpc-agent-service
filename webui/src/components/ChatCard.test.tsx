import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it } from 'vitest'
import { ChatCard } from './ChatCard'

describe('ChatCard', () => {
  it('renders markdown and safe HTTPS actions', () => {
    const html = renderToStaticMarkup(<ChatCard body={<p><strong>已找到</strong>订单。</p>} card={{
      title: '订单信息',
      body: '已找到订单。',
      actions: [{ label: '查看订单', url: 'https://support.example.test/orders/42', style: 'primary' }],
    }} />)
    expect(html).toContain('订单信息')
    expect(html).toContain('<strong>已找到</strong>')
    expect(html).toContain('href="https://support.example.test/orders/42"')
    expect(html).toContain('target="_blank"')
  })

  it('does not expose callback-only or unsafe actions as links', () => {
    const html = renderToStaticMarkup(<ChatCard card={{
      body: '请选择',
      actions: [
        { label: '审批', action_id: 'approval:approve:token' },
        { label: '不安全', url: 'javascript:alert(1)' },
      ],
    }} />)
    expect(html).not.toContain('approval:approve:token')
    expect(html).not.toContain('javascript:')
    expect(html).not.toContain('<a ')
  })
})
