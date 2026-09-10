import { outboxReplyText, type AgentExecutionTrace, type AgentExecutionTraceStep, type AgentExecutionTraceUsage, type Claim, type ExecutionTrace, type ToolExecution } from './types'

export type TokenParts = {
  prompt: number
  completion: number
  total: number
  cached: number
  cacheCreation: number
  promptFresh: number
  cacheRate: number | null
}

export function tokenParts(usage?: AgentExecutionTraceUsage | null): TokenParts | null {
  if (!usage) return null
  const prompt = usage.prompt_tokens ?? 0
  const reportedCompletion = usage.completion_tokens ?? 0
  const reasoning = usage.reasoning_tokens || 0
  const completion = reportedCompletion > 0 ? reportedCompletion : reasoning
  const cached = Math.min(prompt, usage.cache_read_tokens || usage.cached_tokens || 0)
  const cacheCreation = usage.cache_creation_tokens || 0
  const total = usage.total_tokens || prompt + completion
  if (prompt === 0 && completion === 0 && total === 0 && cached === 0 && cacheCreation === 0) {
    return null
  }
  return {
    prompt,
    completion,
    total,
    cached,
    cacheCreation,
    promptFresh: Math.max(0, prompt - cached),
    cacheRate: prompt > 0 ? cached / prompt : null,
  }
}

export function executionTraceUsage(trace?: AgentExecutionTrace | null): AgentExecutionTraceUsage | undefined {
  if (!trace) return undefined
  if (trace.usage) return trace.usage

  let found = false
  const usage: AgentExecutionTraceUsage = {
    prompt_tokens: 0,
    completion_tokens: 0,
    total_tokens: 0,
    cached_tokens: 0,
    cache_creation_tokens: 0,
    cache_read_tokens: 0,
    reasoning_tokens: 0,
  }
  for (const step of trace.steps ?? []) {
    if (!step.usage) continue
    found = true
    usage.prompt_tokens += step.usage.prompt_tokens ?? 0
    usage.completion_tokens += step.usage.completion_tokens ?? 0
    usage.total_tokens += step.usage.total_tokens ?? 0
    usage.cached_tokens = (usage.cached_tokens ?? 0) + (step.usage.cached_tokens ?? 0)
    usage.cache_creation_tokens = (usage.cache_creation_tokens ?? 0) + (step.usage.cache_creation_tokens ?? 0)
    usage.cache_read_tokens = (usage.cache_read_tokens ?? 0) + (step.usage.cache_read_tokens ?? 0)
    usage.reasoning_tokens = (usage.reasoning_tokens ?? 0) + (step.usage.reasoning_tokens ?? 0)
  }
  return found ? usage : undefined
}

export function enrichClaimFromExecution(claim: Claim, detail?: ExecutionTrace | null): Claim {
  const trace = detail?.agent_trace
  if (!trace) return claim
  const failed = trace.status === 'failed' || trace.steps.some((step) => Boolean(step.failed))
  return {
    ...claim,
    started_at: trace.started_at || claim.started_at,
    ended_at: trace.ended_at || claim.ended_at,
    failed,
  }
}

export function mergeClaimsWithExecutionDetails(
  claims: Claim[],
  details: Record<string, ExecutionTrace>,
): Claim[] {
  return claims.map((claim) => enrichClaimFromExecution(claim, details[executionKey(claim)]))
}

export function formatCount(value: number): string {
  return value.toLocaleString('zh-CN')
}

export function formatPercent(rate: number): string {
  const percent = Math.max(0, Math.min(100, rate * 100))
  const rounded = Math.round(percent * 10) / 10
  return Number.isInteger(rounded) ? `${rounded}%` : `${rounded.toFixed(1)}%`
}

export function formatCacheRate(usage?: AgentExecutionTraceUsage | null): string {
  const parts = tokenParts(usage)
  if (!parts || parts.cacheRate === null) return '—'
  return formatPercent(parts.cacheRate)
}

export function formatTokenPair(usage?: AgentExecutionTraceUsage | null): string {
  const parts = tokenParts(usage)
  if (!parts) return '—'
  if (parts.prompt === 0 && parts.completion === 0) return `${formatCount(parts.total)} Token`
  return `入 ${formatCount(parts.prompt)} / 出 ${formatCount(parts.completion)}`
}

export function formatTokenCompact(usage?: AgentExecutionTraceUsage | null): string {
  const parts = tokenParts(usage)
  if (!parts) return ''
  const bits = [`入 ${formatCount(parts.prompt)}`, `出 ${formatCount(parts.completion)}`]
  if (parts.cacheRate !== null) bits.push(`缓存 ${formatPercent(parts.cacheRate)}`)
  if (parts.cacheCreation > 0) bits.push(`写入 ${formatCount(parts.cacheCreation)}`)
  return bits.join(' · ')
}

export function stepSubject(step: AgentExecutionTraceStep): string {
  const node = (step.node_id ?? '').trim()
  if (node) {
    const namedTool = node.match(/#tool[:/#](.+)$/i)
    if (namedTool?.[1]) return namedTool[1]
    if (!/#(?:model|tool)$/i.test(node)) return node
  }
  return (step.agent_name ?? '').trim()
}

export type RunStatus = 'running' | 'completed' | 'failed' | 'incomplete'

export function orderExecutionSteps(steps: AgentExecutionTraceStep[]): AgentExecutionTraceStep[] {
  const byID = new Map(steps.map((step) => [step.step_id, step]))
  const ordered: AgentExecutionTraceStep[] = []
  const seen = new Set<string>()
  const visit = (step: AgentExecutionTraceStep) => {
    if (seen.has(step.step_id)) return
    seen.add(step.step_id)
    for (const predecessorID of step.predecessor_step_ids ?? []) {
      const predecessor = byID.get(predecessorID)
      if (predecessor) visit(predecessor)
    }
    ordered.push(step)
  }
  const chronological = [...steps].sort((left, right) => Date.parse(left.started_at) - Date.parse(right.started_at))
  for (const step of chronological) visit(step)
  return ordered
}

export function visibleExecutionTraceSteps(detail: ExecutionTrace): AgentExecutionTraceStep[] {
  const trace = detail.agent_trace
  if (!trace) return []
  const tools = detail.tool_executions ?? []
  const steps = orderExecutionSteps(trace.steps ?? []).filter((step) => tools.length === 0 || step.node_type !== 'tool')
  if (tools.length > 0 && steps.length === 1 && steps[0].node_type === 'agent') return []
  return steps
}

export function executionProcessCount(detail: ExecutionTrace): number {
  return visibleExecutionTraceSteps(detail).length + (detail.tool_executions?.length ?? 0)
}

export function invocationDepth(step: AgentExecutionTraceStep, steps: AgentExecutionTraceStep[]): number {
  const byInvocation = new Map<string, AgentExecutionTraceStep>()
  for (const candidate of steps) {
    if (candidate.invocation_id && !byInvocation.has(candidate.invocation_id)) {
      byInvocation.set(candidate.invocation_id, candidate)
    }
  }
  let depth = 0
  let parentID = step.parent_invocation_id
  while (parentID && depth < 8) {
    const parent = byInvocation.get(parentID)
    if (!parent) break
    depth += 1
    parentID = parent.parent_invocation_id
  }
  return depth
}

export function durationLabel(start: string, end: string): string {
  const startMs = Date.parse(start)
  const endMs = Date.parse(end)
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs < startMs) return '—'
  const duration = endMs - startMs
  return duration < 1000 ? `${duration} ms` : `${(duration / 1000).toFixed(duration < 10000 ? 2 : 1)} s`
}

export function toolExecutionDurationLabel(execution: ToolExecution): string {
  if (!execution.completed_at) return execution.status === 'running' ? '执行中' : '—'
  return durationLabel(execution.started_at, execution.completed_at)
}

export function toolExecutionStatusLabel(status: ToolExecution['status']): string {
  return ({
    running: '执行中',
    completed: '已完成',
    failed: '失败',
    outcome_unknown: '结果未知',
  })[status]
}

export function nodeTypeLabel(type?: string): string {
  return ({ llm: '模型调用', tool: '工具调用', agent: '机器人处理', function: '函数处理' } as Record<string, string>)[type ?? ''] ?? '处理步骤'
}

export function runStatus(detail: ExecutionTrace): RunStatus {
  const status = detail.status || detail.agent_trace?.status
  if (status === 'failed') return 'failed'
  if (status === 'incomplete') return 'incomplete'
  if (detail.claim?.status === 'processing') return 'running'
  if (status === 'completed' || detail.claim?.status === 'completed') return 'completed'
  return 'running'
}

export function listRunStatus(claim: Claim): RunStatus {
  if (claim.status !== 'completed') return 'running'
  if (claim.failed) return 'failed'
  return 'completed'
}

export function runStatusLabel(status: RunStatus): string {
  return ({ running: '处理中', completed: '已完成', failed: '失败', incomplete: '未完成' })[status]
}

export function runSettled(detail: ExecutionTrace): boolean {
  const status = runStatus(detail)
  return status === 'completed' || status === 'failed' || (status === 'incomplete' && Boolean(detail.agent_trace))
}

export function replyFromExecution(detail: ExecutionTrace): string {
  for (const entry of detail.outbox ?? []) {
    const text = outboxReplyText(entry.Payload)
    if (text) return text
  }
  return ''
}

export function deliveryLabel(detail: ExecutionTrace): string {
  const outbox = detail.outbox ?? []
  if (outbox.some((entry) => Boolean(entry.DeliveredAt))) return '已送达'
  if (outbox.length > 0) return '待发送'
  if (detail.claim?.status === 'completed') return '无回复'
  return '等待中'
}

export function sameJSON(left: unknown, right: unknown): boolean {
  return JSON.stringify(left) === JSON.stringify(right)
}

export function executionKey(claim: { channel: string; message_id: string }): string {
  return `${claim.channel}\u0000${claim.message_id}`
}

export function claimFingerprint(claim: { status: string; trace_id: string; updated_at: string } | undefined): string {
  if (!claim) return ''
  return `${claim.status}\u0000${claim.trace_id}\u0000${claim.updated_at}`
}

export function nextSelectedKey(claims: { channel: string; message_id: string }[], current: string): string {
  if (current && claims.some((claim) => executionKey(claim) === current)) return current
  return claims[0] ? executionKey(claims[0]) : ''
}

export function formatExecutionTime(value?: string | null): string {
  if (!value) return '—'
  return value.slice(0, 19).replace('T', ' ')
}

export function formatExecutionText(detail: ExecutionTrace): string {
  const status = runStatusLabel(runStatus(detail))
  const trace = detail.agent_trace
  const lines = [`运行 ${status}`]
  if (detail.trace_id) lines[0] += `  关联 ${detail.trace_id}`
  if (trace) {
    lines[0] += `  ${durationLabel(trace.started_at, trace.ended_at)}`
    const usage = formatTokenCompact(trace.usage)
    if (usage) lines.push(`  ${usage}`)
    for (const step of visibleExecutionTraceSteps(detail)) {
      const mark = step.failed ? '×' : '·'
      const extra = formatTokenCompact(step.usage)
      const subject = stepSubject(step)
      lines.push(`  ${mark} ${nodeTypeLabel(step.node_type)}${subject ? `  ${subject}` : ''}  ${durationLabel(step.started_at, step.ended_at)}${extra ? `  ${extra}` : ''}`)
    }
  }
  for (const tool of detail.tool_executions ?? []) {
    const mark = tool.status === 'completed' ? '·' : tool.status === 'running' ? '…' : '×'
    lines.push(`  ${mark} 工具调用  ${tool.tool_name}  ${toolExecutionDurationLabel(tool)}  ${toolExecutionStatusLabel(tool.status)}`)
  }
  lines.push(`  发送 ${deliveryLabel(detail)}`)
  if (detail.attempts > 0) lines.push(`  重试 ${detail.attempts} 次`)
  return lines.join('\n')
}
