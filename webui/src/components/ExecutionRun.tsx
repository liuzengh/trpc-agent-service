import type { AgentExecutionTraceUsage, ExecutionTrace } from '../types'
import { ActivityIcon, DatabaseIcon } from './Icons'
import { PanelHeader } from './PanelHeader'
import { StatusIndicator } from './StatusIndicator'
import {
  deliveryLabel,
  durationLabel,
  executionProcessCount,
  executionTimelineItems,
  executionTraceUsage,
  formatCount,
  formatPercent,
  formatTokenCompact,
  runStatus,
  runStatusLabel,
  tokenParts,
  type ExecutionTimelineItem,
} from '../execution'

export function ExecutionRun({ detail, channel }: { detail: ExecutionTrace; channel?: string }) {
  const status = runStatus(detail)
  const trace = detail.agent_trace
  const timeline = executionTimelineItems(detail)
  const processCount = executionProcessCount(detail)
  const traceUsage = executionTraceUsage(trace)
  const usage = tokenParts(traceUsage)

  return (
    <div className="execution-run">
      <section className="execution-overview" aria-label="执行概览">
        <div className="execution-overview-status">
          <StatusIndicator
            tone={status === 'completed' ? 'success' : status === 'failed' ? 'danger' : 'info'}
            appearance="text"
          >
            {runStatusLabel(status)}
          </StatusIndicator>
          {channel && <span>{channel}</span>}
        </div>
        <dl className="execution-overview-facts">
          {trace && <div><dt>耗时</dt><dd>{durationLabel(trace.started_at, trace.ended_at)}</dd></div>}
          <div><dt>步骤</dt><dd>{processCount} 步</dd></div>
          <div><dt>发送</dt><dd>{deliveryLabel(detail)}</dd></div>
        </dl>
      </section>

      {processCount > 0 && (
        <section className="execution-detail-card execution-steps">
          <PanelHeader level={3} icon={<ActivityIcon size={17} />} title="运行过程" />
          <div className="execution-timeline">
            {timeline.map((item) => <TimelineStep key={item.id} item={item} />)}
          </div>
        </section>
      )}

      {usage && traceUsage && (
        <section className="execution-detail-card execution-usage">
          <PanelHeader level={3} icon={<DatabaseIcon size={17} />} title="用量" />
          <UsageMeter usage={traceUsage} />
        </section>
      )}

      <section className="execution-detail-card execution-technical">
        <details>
          <summary>技术信息</summary>
          <dl className="execution-technical-facts">
            <div><dt>消息 ID</dt><dd><code className="mono-plain">{detail.event_id}</code></dd></div>
            {detail.attempts > 0 && <div><dt>重试</dt><dd>{detail.attempts} 次</dd></div>}
          </dl>
        </details>
      </section>
    </div>
  )
}

function TimelineStep({ item }: { item: ExecutionTimelineItem }) {
  const compact = formatTokenCompact(item.usage)
  const duration = item.ended_at
    ? durationLabel(item.started_at ?? '', item.ended_at)
    : item.status === 'running' ? '进行中' : '—'
  return (
    <article className={`execution-step is-${item.kind} ${item.status === 'failed' ? 'is-failed' : ''} ${item.status === 'running' ? 'is-running' : ''}`}>
      <span className="execution-step-dot" aria-hidden="true" />
      <div className="execution-step-body">
        <div className="execution-step-title">
          <strong>{item.title}</strong>
          <span>{duration}</span>
        </div>
        {(item.subject || item.detail || compact) && (
          <div className="execution-step-detail">
            {item.subject && <span>{item.subject}</span>}
            {item.subject && item.detail && <span aria-hidden="true">·</span>}
            {item.detail && <span>{item.detail}</span>}
            {(item.subject || item.detail) && compact && <span aria-hidden="true">·</span>}
            {compact && <span>{compact}</span>}
          </div>
        )}
      </div>
    </article>
  )
}

function UsageMeter({ usage }: { usage: AgentExecutionTraceUsage }) {
  const parts = tokenParts(usage)
  if (!parts) return null
  return (
    <dl className="usage-meter-facts">
      <div><dt>输入</dt><dd>{formatCount(parts.prompt)}</dd></div>
      <div><dt>输出</dt><dd>{formatCount(parts.completion)}</dd></div>
      <div><dt>合计</dt><dd>{formatCount(parts.total)}</dd></div>
      {parts.cacheRate !== null && <div><dt>缓存率</dt><dd>{formatPercent(parts.cacheRate)}</dd></div>}
      {parts.cacheCreation > 0 && <div><dt>写入缓存</dt><dd>{formatCount(parts.cacheCreation)}</dd></div>}
    </dl>
  )
}
