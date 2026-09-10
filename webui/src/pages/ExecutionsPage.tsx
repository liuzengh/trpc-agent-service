import { useMemo, useState, type ReactNode } from 'react'
import { useAppContext } from '../context'
import { ExecutionRun } from '../components/ExecutionRun'
import {
  ActivityIcon,
  AlertIcon,
  CheckCircleIcon,
  FileTextIcon,
} from '../components/Icons'
import { PanelHeader } from '../components/PanelHeader'
import { RefreshButton } from '../components/RefreshButton'
import { SearchField } from '../components/SearchField'
import { FeedbackBanner } from '../components/FeedbackBanner'
import { SegmentedControl } from '../components/SegmentedControl'
import { channelLabel } from '../components/channelMeta'
import {
  durationLabel,
  executionKey,
  formatExecutionTime,
  listRunStatus,
  runStatusLabel,
} from '../execution'
import { useExecutionFeed } from '../useExecutionFeed'

type ListFilter = 'all' | 'running' | 'completed' | 'failed'

export function ExecutionsPage() {
  const { tenant, appsError, activeAppKey } = useAppContext()
  const appCode = activeAppKey.startsWith(`${tenant}/`) ? activeAppKey.slice(tenant.length + 1) : ''
  const feed = useExecutionFeed(tenant, appCode)
  const [filter, setFilter] = useState<ListFilter>('all')
  const [query, setQuery] = useState('')

  const visibleClaims = useMemo(() => {
    const normalizedQuery = query.trim().toLowerCase()
    return feed.claims.filter((claim) => {
      const matchesStatus = filter === 'all' || listRunStatus(claim) === filter
      if (!matchesStatus) return false
      if (!normalizedQuery) return true
      return [claim.message_id, claim.trace_id, claim.channel, runStatusLabel(listRunStatus(claim))]
        .some((value) => value.toLowerCase().includes(normalizedQuery))
    })
  }, [feed.claims, filter, query])
  const processing = feed.claims.filter((claim) => listRunStatus(claim) === 'running').length
  const failed = feed.claims.filter((claim) => listRunStatus(claim) === 'failed').length
  const completed = feed.claims.length - processing - failed

  return (
    <div className="page-stack executions-page">
      {appsError && <FeedbackBanner tone="error">{appsError}</FeedbackBanner>}
      {feed.error && <FeedbackBanner tone="error">{feed.error}</FeedbackBanner>}

      <dl className="execution-summary" aria-label="执行记录汇总">
        <ExecutionSummaryCard icon={<FileTextIcon size={22} />} label="当前记录" value={feed.claims.length} tone="info" />
        <ExecutionSummaryCard icon={<ActivityIcon size={22} />} label="处理中" value={processing} tone="warning" />
        <ExecutionSummaryCard icon={<CheckCircleIcon size={22} />} label="已完成" value={completed} tone="success" />
        <ExecutionSummaryCard icon={<AlertIcon size={22} />} label="失败" value={failed} tone="danger" />
      </dl>

      <div className="execution-workspace">
        <section className="data-table-panel execution-list" aria-label="执行记录列表">
          <div className="execution-list-head">
            <PanelHeader
              level={2}
              icon={<FileTextIcon size={18} />}
              title="执行记录"
              actions={
                <div className="execution-list-actions">
                  <SearchField className="execution-search" value={query} onValueChange={setQuery} ariaLabel="搜索执行记录" placeholder="搜索消息 ID 或关键词…" onClear={() => setQuery('')} />
                  <RefreshButton onClick={() => feed.refresh()} loading={feed.loadingList} label="刷新执行记录" />
                </div>
              }
            />
          </div>
          <div className="execution-list-toolbar">
            <SegmentedControl
              ariaLabel="按状态筛选"
              value={filter}
              className="execution-filter"
              items={[
                { value: 'all', label: '全部' },
                { value: 'running', label: '处理中' },
                { value: 'completed', label: '已完成' },
                { value: 'failed', label: '失败' },
              ]}
              onValueChange={setFilter}
            />
            {feed.loadingList && !feed.claims.length && <span className="toolbar-note">正在读取…</span>}
          </div>
          <table>
            <thead>
              <tr>
                <th>渠道</th>
                <th className="execution-status-column">状态</th>
                <th>更新时间</th>
                <th className="execution-duration-column">耗时</th>
              </tr>
            </thead>
            <tbody>
              {visibleClaims.length === 0 && (
                <tr>
                  <td colSpan={4} className="table-empty">
                    {feed.claims.length === 0 ? '暂无执行记录。' : '没有符合筛选的记录。'}
                  </td>
                </tr>
              )}
              {visibleClaims.map((claim) => {
                const key = executionKey(claim)
                const status = listRunStatus(claim)
                return (
                  <tr
                    key={key}
                    className={feed.selectedKey === key ? 'is-selected' : ''}
                    onClick={() => feed.setSelectedKey(key)}
                  >
                    <td>{channelLabel(claim.channel)}</td>
                    <td className="execution-status-column">
                      <span className={`state-text ${status === 'running' ? 'is-running' : status === 'failed' ? 'is-failed' : 'is-ok'}`}>
                        {runStatusLabel(status)}
                      </span>
                    </td>
                    <td className="time-cell">{formatExecutionTime(claim.updated_at)}</td>
                    <td className="num-cell execution-duration-column">{durationLabel(claim.started_at ?? '', claim.ended_at ?? '')}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
          <div className="execution-list-footer">
            <span>{query.trim() || filter !== 'all' ? `显示 ${visibleClaims.length} / 共 ${feed.claims.length} 条` : `共 ${feed.claims.length} 条记录`}</span>
          </div>
        </section>

        <aside className="detail-inspector execution-inspector">
          <PanelHeader
            level={2}
            icon={<FileTextIcon size={18} />}
            title="执行详情"
            className="execution-inspector-heading"
          />
          {!feed.selectedClaim && <div className="inspector-empty">选择一条记录查看这次运行。</div>}
          {feed.selectedClaim && feed.loadingDetail && !feed.detail && (
            <div className="thinking inspector-loading">加载执行详情…</div>
          )}
          {feed.detail && (
            <ExecutionRun
              detail={feed.detail}
              channel={feed.selectedClaim ? channelLabel(feed.selectedClaim.channel) : undefined}
            />
          )}
        </aside>
      </div>
    </div>
  )
}

function ExecutionSummaryCard({
  icon,
  label,
  value,
  tone,
}: {
  icon: ReactNode
  label: string
  value: number
  tone: 'info' | 'warning' | 'success' | 'danger'
}) {
  return (
    <div className={`execution-summary-card tone-${tone}`}>
      <span className="execution-summary-icon" aria-hidden="true">{icon}</span>
      <div>
        <dt>{label}</dt>
        <dd>{value}</dd>
      </div>
      <span className="execution-summary-decoration" aria-hidden="true" />
    </div>
  )
}
