import { describe, expect, it, vi } from 'vitest'
import { renderToStaticMarkup } from 'react-dom/server'
import { marked } from 'marked'
import { Markdown } from './Markdown'

describe('Markdown component', () => {
  it('renders plain text and paragraphs', () => {
    const html = renderToStaticMarkup(<Markdown content="Hello world" />)
    expect(html).toContain('<p>Hello world</p>')
    expect(html).toContain('markdown-content')
  })

  it('renders headings and bold text', () => {
    const html = renderToStaticMarkup(<Markdown content="# Title&#10;&#10;**bold text**" />)
    expect(html).toContain('<h1>Title</h1>')
    expect(html).toContain('<strong>bold text</strong>')
  })

  it('renders lists', () => {
    const html = renderToStaticMarkup(<Markdown content="- Item 1&#10;- Item 2" />)
    expect(html).toContain('<ul>')
    expect(html).toContain('<li>Item 1</li>')
    expect(html).toContain('<li>Item 2</li>')
  })

  it('renders code blocks and inline code', () => {
    const html = renderToStaticMarkup(<Markdown content="`const x = 1`&#10;&#10;```ts&#10;console.log('hello')&#10;```" />)
    expect(html).toContain('<code>const x = 1</code>')
    expect(html).toContain('<pre><code class="language-ts">')
  })

  it('renders links with target=_blank and rel=noopener noreferrer', () => {
    const html = renderToStaticMarkup(<Markdown content="[Google](https://google.com)" />)
    expect(html).toContain('href="https://google.com"')
    expect(html).toContain('target="_blank"')
    expect(html).toContain('rel="noopener noreferrer"')
  })

  it('handles empty content gracefully', () => {
    const html = renderToStaticMarkup(<Markdown content="" />)
    expect(html).toBe('<div class="markdown-content"></div>')
  })

  it('escapes raw HTML from untrusted markdown', () => {
    const html = renderToStaticMarkup(<Markdown content={'<img src=x onerror="alert(1)">'} />)
    expect(html).not.toContain('<img')
    expect(html).not.toContain('onerror=')
  })

  it('drops unsafe javascript links', () => {
    const html = renderToStaticMarkup(<Markdown content="[click](javascript:alert(1))" />)
    expect(html).not.toContain('javascript:')
    expect(html).not.toContain('<a ')
    expect(html).toContain('click')
  })

  it('escapes untrusted content when markdown parsing fails', () => {
    const parse = vi.spyOn(marked, 'parse').mockImplementationOnce(() => {
      throw new Error('parse failed')
    })
    const html = renderToStaticMarkup(<Markdown content={'<img src=x onerror="alert(1)">'} />)
    parse.mockRestore()

    expect(html).not.toContain('<img')
    expect(html).not.toContain('onerror="alert(1)"')
    expect(html).toContain('&lt;img')
  })
})
