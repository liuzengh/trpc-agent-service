import type { AgentExecutionTraceUsage, ExecutionTrace } from '../types'
import { ActivityIcon, DatabaseIcon } from './Icons'
import { GridIcon } from './PageIcons'
import { PanelHeader } from './PanelHeader'
import { StatusIndicator } from './StatusIndicator'
import {
  deliveryLabel,
  durationLabel,
  executionProcessCount,
  executionTraceUsage,
  formatCount,
  formatPercent,
  formatTokenCompact,
  invocationDepth,
  nodeTypeLabel,
  runStatus,
  runStatusLabel,
  stepSubject,
  tokenParts,
  toolExecutionDurationLabel,
  toolExecutionStatusLabel,
  visibleExecutionTraceSteps,
} from '../execution'

export function ExecutionRun({ detail, channel }: { detail: ExecutionTrace; channel?: string }) {
  const status = runStatus(detail)
  const trace = detail.agent_trace
  const steps = visibleExecutionTraceSteps(detail)
  const toolExecutions = [...(detail.tool_executions ?? [])].sort((left, right) => Date.parse(left.started_at) - Date.parse(right.started_at))
  const processCount = executionProcessCount(detail)
  const traceUsage = executionTraceUsage(trace)
  const usage = tokenParts(traceUsage)

  return (
    <div className="execution-run">
      <section className="execution-detail-card execution-basic-card">
        <PanelHeader level={3} icon={<GridIcon size={17} />} title="基本信息" />
        <dl className="execution-primary-facts">
          {channel && (
            <div>
              <dt>渠道</dt>
              <dd>{channel}</dd>
            </div>
          )}
          <div className="execution-primary-status">
            <dt>状态</dt>
            <dd>
              <StatusIndicator
                tone={status === 'completed' ? 'success' : status === 'failed' ? 'danger' : 'info'}
                appearance="text"
              >
                {runStatusLabel(status)}
              </StatusIndicator>
            </dd>
          </div>
        </dl>
        <dl className="execution-facts">
          <div className="is-wide execution-message-fact"><dt>消息</dt><dd><code className="mono-plain">{detail.event_id}</code></dd></div>
          <div><dt>发送</dt><dd>{deliveryLabel(detail)}</dd></div>
          {trace && <div><dt>耗时</dt><dd>{durationLabel(trace.started_at, trace.ended_at)}</dd></div>}
          {processCount > 0 && <div><dt>步骤</dt><dd>{processCount} 步</dd></div>}
          {detail.attempts > 0 && <div><dt>重试</dt><dd>{detail.attempts} 次</dd></div>}
        </dl>
      </section>

      {usage && traceUsage && (
        <section className="execution-detail-card execution-usage">
          <PanelHeader level={3} icon={<DatabaseIcon size={17} />} title="Token 统计" />
          <UsageMeter usage={traceUsage} />
        </section>
      )}

      {processCount > 0 && (
        <section className="execution-detail-card execution-steps">
          <PanelHeader level={3} icon={<ActivityIcon size={17} />} title="运行过程" />
          <div className="execution-timeline">
            {steps.map((step) => {
              const depth = invocationDepth(step, steps)
              const subject = stepSubject(step)
              const compact = formatTokenCompact(step.usage)
              return (
                <article className={`execution-step ${step.failed ? 'is-failed' : ''} is-${step.node_type || 'step'}`} key={step.step_id} style={{ paddingLeft: depth * 12 }}>
                  <span className="execution-step-dot" aria-hidden="true" />
                  <div className="execution-step-body">
                    <div className="execution-step-title">
                      <strong>{step.node_type === 'agent' && steps.length === 1 ? '模型与编排' : nodeTypeLabel(step.node_type)}{step.failed ? '失败' : ''}</strong>
                      <span>{durationLabel(step.started_at, step.ended_at)}</span>
                    </div>
                    {subject && <div className="execution-step-subject">{subject}</div>}
                    {compact && <div className="execution-step-meta">{compact}</div>}
                  </div>
                </article>
              )
            })}
            {toolExecutions.map((tool) => {
              const failed = tool.status === 'failed' || tool.status === 'outcome_unknown'
              return (
                <article className={`execution-step is-tool ${failed ? 'is-failed' : ''}`} key={`${tool.tool_call_id}:${tool.started_at}`}>
                  <span className="execution-step-dot" aria-hidden="true" />
                  <div className="execution-step-body">
                    <div className="execution-step-title">
                      <strong>工具调用</strong>
                      <span>{toolExecutionDurationLabel(tool)}</span>
                    </div>
                    <div className="execution-step-subject">{tool.tool_name} · {toolExecutionStatusLabel(tool.status)}</div>
                    {tool.error_type && <div className="execution-step-meta">错误类型：{tool.error_type}</div>}
                  </div>
                </article>
              )
            })}
          </div>
        </section>
      )}


    </div>
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
