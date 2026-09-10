import { Component, type ErrorInfo, type ReactNode } from 'react'

interface PageErrorBoundaryProps {
  resetKey: string
  children: ReactNode
}

interface PageErrorBoundaryState {
  error: Error | null
}

export class PageErrorBoundary extends Component<PageErrorBoundaryProps, PageErrorBoundaryState> {
  state: PageErrorBoundaryState = { error: null }

  static getDerivedStateFromError(error: Error): PageErrorBoundaryState {
    return { error }
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('page render failed', error, info.componentStack)
  }

  componentDidUpdate(previous: PageErrorBoundaryProps) {
    if (previous.resetKey !== this.props.resetKey && this.state.error) {
      this.setState({ error: null })
    }
  }

  render() {
    if (!this.state.error) return this.props.children

    const dynamicImportFailure = /dynamically imported module|Outdated Optimize Dep/i.test(this.state.error.message)
    return (
      <section className="page-load-error" role="alert">
        <strong>{dynamicImportFailure ? '页面资源已更新' : '当前页面加载失败'}</strong>
        <p>{dynamicImportFailure ? '开发服务刚完成依赖更新，刷新后即可继续。' : '其他页面仍可继续使用，也可以刷新后重试。'}</p>
        <button type="button" className="secondary" onClick={() => window.location.reload()}>
          刷新页面
        </button>
      </section>
    )
  }
}
