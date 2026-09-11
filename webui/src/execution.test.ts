import { describe, expect, it } from 'vitest'
import { claimFingerprint, enrichClaimFromExecution, executionKey, executionProcessCount, executionTimelineItems, executionTraceUsage, executionViewSettled, formatCacheRate, formatExecutionText, formatExecutionTime, formatTokenCompact, formatTokenPair, listRunStatus, mergeClaimsWithExecutionDetails, nextSelectedKey, orderExecutionSteps, replyFromExecution, runSettled, runStatus, sameJSON, stepSubject, tokenParts, visibleExecutionTraceSteps } from './execution'
import { outboxReplyText, type ExecutionTrace } from './types'

const detail: ExecutionTrace = {
  event_id: 'msg-1',
  trace_id: 'trace-abc',
  status: 'completed',
  attempts: 1,
  claim: { channel: 'web', binding_id: 'web', message_id: 'msg-1', status: 'completed', trace_id: 'trace-abc', updated_at: '2026-09-09T01:00:02Z' },
  outbox: [{ ID: 'out-1', TenantID: 'example', AggregateKey: 'k', Type: 'channel_reply', Payload: '{}', CreatedAt: '2026-09-09T01:00:02Z', DeliveredAt: '2026-09-09T01:00:03Z' }],
  agent_trace: {
    status: 'completed',
    started_at: '2026-09-09T01:00:00Z',
    ended_at: '2026-09-09T01:00:02Z',
    usage: { prompt_tokens: 120, completion_tokens: 42, total_tokens: 162, cached_tokens: 80, reasoning_tokens: 10 },
    steps: [
      {
        step_id: 'tool-1',
        invocation_id: 'inv-1',
        parent_invocation_id: 'inv-root',
        agent_name: 'assistant',
        node_id: 'assistant#tool:query_order',
        node_type: 'tool',
        started_at: '2026-09-09T01:00:01Z',
        ended_at: '2026-09-09T01:00:01.200Z',
        predecessor_step_ids: ['llm-1'],
      },
      {
        step_id: 'llm-1',
        invocation_id: 'inv-root',
        agent_name: 'assistant',
        node_id: 'assistant#model',
        node_type: 'llm',
        started_at: '2026-09-09T01:00:00Z',
        ended_at: '2026-09-09T01:00:01Z',
        usage: { prompt_tokens: 120, completion_tokens: 42, total_tokens: 162, cached_tokens: 80, reasoning_tokens: 10 },
      },
    ],
  },
}

describe('execution view', () => {
  it('orders steps by predecessor before clock time', () => {
    const ordered = orderExecutionSteps(detail.agent_trace!.steps)
    expect(ordered.map((step) => step.step_id)).toEqual(['llm-1', 'tool-1'])
  })

  it('formats one readable run for operators and agents', () => {
    expect(runStatus(detail)).toBe('completed')
    expect(runSettled(detail)).toBe(true)
    expect(formatExecutionText(detail)).toContain('运行 已完成')
    expect(formatExecutionText(detail)).toContain('模型调用')
    expect(formatExecutionText(detail)).toContain('工具调用')
    expect(formatExecutionText(detail)).toContain('query_order')
    expect(formatExecutionText(detail)).not.toContain('assistant#model')
    expect(formatExecutionText(detail)).toContain('入 120')
    expect(formatExecutionText(detail)).toContain('出 42')
    expect(formatExecutionText(detail)).toContain('缓存 66.7%')
    expect(formatExecutionText(detail)).not.toContain('推理')
    expect(formatExecutionText(detail)).toContain('消息发送  已送达')
    expect(formatExecutionText(detail)).toContain('关联 trace-abc')
  })

  it('keeps refreshing a completed run until its outbox delivery settles', () => {
    expect(executionViewSettled(detail)).toBe(true)
    expect(executionViewSettled({
      ...detail,
      outbox: detail.outbox?.map((entry) => ({ ...entry, DeliveredAt: null })),
    })).toBe(false)
    expect(executionViewSettled({ ...detail, outbox: [] })).toBe(true)
  })

  it('uses durable tool executions when the framework trace is only an aggregate agent step', () => {
    const aggregateOnly: ExecutionTrace = {
      ...detail,
      agent_trace: {
        ...detail.agent_trace!,
        steps: [{
          step_id: 'agent-1',
          invocation_id: 'inv-root',
          agent_name: 'assistant',
          node_id: 'assistant',
          node_type: 'agent',
          started_at: '2026-09-09T01:00:00Z',
          ended_at: '2026-09-09T01:00:04Z',
        }],
      },
      tool_executions: [
        {
          request_id: 'msg-1', tool_call_id: 'call-1', tool_name: 'duckduckgo_search', status: 'completed', trace_id: 'trace-abc',
          started_at: '2026-09-09T01:00:01Z', completed_at: '2026-09-09T01:00:02Z',
        },
        {
          request_id: 'msg-1', tool_call_id: 'call-2', tool_name: 'context7_query-docs', status: 'completed', trace_id: 'trace-abc',
          started_at: '2026-09-09T01:00:02Z', completed_at: '2026-09-09T01:00:03.500Z',
        },
      ],
    }
    expect(visibleExecutionTraceSteps(aggregateOnly)).toEqual([])
    expect(executionProcessCount(aggregateOnly)).toBe(3)
    expect(formatExecutionText(aggregateOnly)).toContain('duckduckgo_search')
    expect(formatExecutionText(aggregateOnly)).toContain('context7_query-docs')
    expect(formatExecutionText(aggregateOnly)).not.toContain('机器人处理')
    expect(formatExecutionText(aggregateOnly)).toContain('消息发送')
  })

  it('builds a platform timeline from approval, tool, and delivery evidence', () => {
    const aggregateOnly: ExecutionTrace = {
      ...detail,
      agent_trace: {
        ...detail.agent_trace!,
        started_at: '2026-09-09T01:00:00Z',
        ended_at: '2026-09-09T01:00:12Z',
        steps: [{
          step_id: 'agent-1', invocation_id: 'inv-root', agent_name: 'assistant', node_id: 'assistant', node_type: 'agent',
          started_at: '2026-09-09T01:00:00Z', ended_at: '2026-09-09T01:00:12Z',
        }],
      },
      audit_events: [
        {
          ID: 'approval-1', TenantID: 'example', TraceID: 'trace-abc', RequestID: 'msg-1', Channel: 'wecom', ToolName: 'request_refund',
          Action: 'approval.requested', Result: 'pending', Decision: 'pending', Detail: '', CreatedAt: '2026-09-09T01:00:02Z',
        },
        {
          ID: 'approval-2', TenantID: 'example', TraceID: 'trace-abc', RequestID: 'msg-1', Channel: 'wecom', ToolName: 'request_refund',
          Action: 'approval.approved', Result: 'approved', Decision: 'approved', LatencyMS: 5000, Detail: '', CreatedAt: '2026-09-09T01:00:07Z',
        },
      ],
      tool_executions: [{
        request_id: 'msg-1', tool_call_id: 'call-1', tool_name: 'request_refund', status: 'completed', trace_id: 'trace-abc',
        started_at: '2026-09-09T01:00:07Z', completed_at: '2026-09-09T01:00:08Z',
      }],
      outbox: [{
        ID: 'out-1', TenantID: 'example', AggregateKey: 'k', Type: 'channel_reply.wecom', Payload: '{}',
        CreatedAt: '2026-09-09T01:00:12Z', DeliveredAt: '2026-09-09T01:00:12.300Z',
      }],
    }
    const timeline = executionTimelineItems(aggregateOnly)
    expect(timeline.map((item) => [item.kind, item.title, item.subject])).toEqual([
      ['approval', '审批通过', 'request_refund'],
      ['tool', '工具调用', 'request_refund'],
      ['delivery', '消息发送', '已送达'],
    ])
    expect(timeline[0].started_at).toBe('2026-09-09T01:00:02Z')
    expect(timeline[0].ended_at).toBe('2026-09-09T01:00:07Z')
    expect(formatExecutionText(aggregateOnly)).toContain('审批通过  request_refund')
  })

  it('keeps a legacy aggregate LLMAgent step honest when no per-call model evidence exists', () => {
    const direct: ExecutionTrace = {
      ...detail,
      outbox: [],
      tool_executions: [],
      audit_events: [],
      agent_trace: {
        ...detail.agent_trace!,
        steps: [{
          step_id: 'agent-1', invocation_id: 'inv-root', agent_name: 'assistant', node_id: 'assistant', node_type: 'agent',
          started_at: '2026-09-09T01:00:00Z', ended_at: '2026-09-09T01:00:02Z', usage: detail.agent_trace!.usage,
        }],
      },
    }
    expect(executionTimelineItems(direct).map((item) => item.title)).toEqual(['机器人运行'])
  })

  it('shows each persisted model invocation around a real tool call', () => {
    const traced: ExecutionTrace = {
      ...detail,
      agent_trace: {
        ...detail.agent_trace!,
        steps: [{
          step_id: 'agent-1', invocation_id: 'inv-root', agent_name: 'assistant', node_id: 'assistant', node_type: 'agent',
          started_at: '2026-09-09T01:00:00Z', ended_at: '2026-09-09T01:00:05Z',
        }],
      },
      audit_events: [
        { ID: 'm1-start', TenantID: 'example', TraceID: 'trace-abc', RequestID: 'msg-1', AgentName: 'assistant', Action: 'model.requested', Result: 'running', Detail: '', CreatedAt: '2026-09-09T01:00:00Z' },
        { ID: 'm1-end', TenantID: 'example', TraceID: 'trace-abc', RequestID: 'msg-1', AgentName: 'assistant', Action: 'model.completed', Result: 'completed', Detail: '', CreatedAt: '2026-09-09T01:00:01Z' },
        { ID: 'm2-start', TenantID: 'example', TraceID: 'trace-abc', RequestID: 'msg-1', AgentName: 'assistant', Action: 'model.requested', Result: 'running', Detail: '', CreatedAt: '2026-09-09T01:00:03Z' },
        { ID: 'm2-end', TenantID: 'example', TraceID: 'trace-abc', RequestID: 'msg-1', AgentName: 'assistant', Action: 'model.completed', Result: 'completed', Detail: '', CreatedAt: '2026-09-09T01:00:04Z' },
      ],
      tool_executions: [{
        request_id: 'msg-1', tool_call_id: 'call-1', tool_name: 'query_order', status: 'completed', trace_id: 'trace-abc',
        started_at: '2026-09-09T01:00:02Z', completed_at: '2026-09-09T01:00:02.500Z',
      }],
      outbox: [{
        ID: 'out-1', TenantID: 'example', AggregateKey: 'k', Type: 'channel_reply.web', Payload: '{}',
        CreatedAt: '2026-09-09T01:00:05Z', DeliveredAt: '2026-09-09T01:00:05.100Z',
      }],
    }
    expect(executionTimelineItems(traced).map((item) => [item.kind, item.title, item.subject])).toEqual([
      ['model', '模型调用', 'assistant'],
      ['tool', '工具调用', 'query_order'],
      ['model', '模型调用', 'assistant'],
      ['delivery', '消息发送', '已送达'],
    ])
  })

  it('shows an expired approval instead of the aggregate agent wrapper', () => {
    const expired: ExecutionTrace = {
      ...detail,
      agent_trace: {
        ...detail.agent_trace!,
        started_at: '2026-09-09T01:00:00Z',
        ended_at: '2026-09-09T01:10:08Z',
        steps: [{
          step_id: 'agent-1', invocation_id: 'inv-root', agent_name: 'assistant', node_id: 'assistant', node_type: 'agent',
          started_at: '2026-09-09T01:00:00Z', ended_at: '2026-09-09T01:10:08Z',
        }],
      },
      tool_executions: [],
      audit_events: [
        {
          ID: 'approval-start', TenantID: 'example', TraceID: 'trace-abc', RequestID: 'msg-1', ToolName: 'request_refund',
          Action: 'approval.requested', Result: 'pending', Decision: 'pending', Detail: '', CreatedAt: '2026-09-09T01:00:02Z',
        },
        {
          ID: 'approval-expired', TenantID: 'example', TraceID: 'trace-abc', RequestID: 'msg-1', ToolName: 'request_refund',
          Action: 'approval.expired', Result: 'failed', Decision: 'failed', ErrorType: 'approval_expired', LatencyMS: 600000,
          Detail: '', CreatedAt: '2026-09-09T01:10:02Z',
        },
      ],
    }
    const timeline = executionTimelineItems(expired)
    expect(timeline[0]).toMatchObject({ kind: 'approval', title: '审批过期', subject: 'request_refund', status: 'failed' })
    expect(timeline.some((item) => item.title === '机器人处理' || item.title === '模型调用')).toBe(false)
  })

  it('splits usage into input, output, and cache hit rate', () => {
    expect(tokenParts(detail.agent_trace!.usage)).toEqual({
      prompt: 120,
      completion: 42,
      total: 162,
      cached: 80,
      cacheCreation: 0,
      promptFresh: 40,
      cacheRate: 80 / 120,
    })
    expect(formatTokenPair(detail.agent_trace!.usage)).toBe('入 120 / 出 42')
    expect(formatTokenCompact(detail.agent_trace!.usage)).toBe('入 120 · 出 42 · 缓存 66.7%')
    expect(formatCacheRate(detail.agent_trace!.usage)).toBe('66.7%')
    expect(formatCacheRate({ prompt_tokens: 10, completion_tokens: 2, total_tokens: 12 })).toBe('0%')
    expect(tokenParts({ prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 })).toBeNull()
    expect(tokenParts({ prompt_tokens: 10, completion_tokens: 2, total_tokens: 12, cache_read_tokens: 8 })?.cacheRate).toBe(0.8)
    expect(tokenParts({ prompt_tokens: 0, completion_tokens: 0, total_tokens: 10, reasoning_tokens: 10 })?.completion).toBe(10)
  })

  it('uses one execution projection for list and detail metrics', () => {
    const sparseClaim = {
      ...detail.claim!,
      started_at: undefined,
      ended_at: undefined,
      failed: undefined,
    }
    const enriched = enrichClaimFromExecution(sparseClaim, detail)
    expect(enriched.started_at).toBe(detail.agent_trace!.started_at)
    expect(enriched.ended_at).toBe(detail.agent_trace!.ended_at)
    expect(enriched.failed).toBe(false)

    const merged = mergeClaimsWithExecutionDetails(
      [sparseClaim],
      { [executionKey(sparseClaim)]: detail },
    )
    expect(merged[0]).toEqual(enriched)
  })

  it('aggregates step usage when a trace has no root usage', () => {
    const trace = {
      ...detail.agent_trace!,
      usage: undefined,
      steps: [
        { ...detail.agent_trace!.steps[0], usage: { prompt_tokens: 4, completion_tokens: 1, total_tokens: 5, cached_tokens: 2 } },
        { ...detail.agent_trace!.steps[1], usage: { prompt_tokens: 6, completion_tokens: 2, total_tokens: 8, cache_read_tokens: 3 } },
      ],
    }
    expect(executionTraceUsage(trace)).toEqual({
      prompt_tokens: 10,
      completion_tokens: 3,
      total_tokens: 13,
      cached_tokens: 2,
      cache_creation_tokens: 0,
      cache_read_tokens: 3,
      reasoning_tokens: 0,
    })
  })

  it('names steps without framework graph jargon', () => {
    expect(stepSubject(detail.agent_trace!.steps[1])).toBe('assistant')
    expect(stepSubject(detail.agent_trace!.steps[0])).toBe('query_order')
    expect(stepSubject({ step_id: 's', started_at: '', ended_at: '', node_id: 'search_web', node_type: 'tool' })).toBe('search_web')
  })

  it('treats a completed claim with a failed trace as failed in the list', () => {
    expect(listRunStatus(detail.claim!)).toBe('completed')
    expect(listRunStatus({ ...detail.claim!, failed: true })).toBe('failed')
    expect(listRunStatus({ ...detail.claim!, status: 'processing', failed: true })).toBe('running')
    expect(listRunStatus({ ...detail.claim!, status: 'failed' })).toBe('failed')
    expect(runStatus({ ...detail, status: undefined, agent_trace: undefined, claim: { ...detail.claim!, status: 'failed' } })).toBe('failed')
  })

  it('applies list updates only when claim content changes', () => {
    const first = detail.claim!
    expect(sameJSON([first], [first])).toBe(true)
    expect(sameJSON([first], [{ ...first, status: 'processing' }])).toBe(false)
    expect(executionKey(first)).toBe('web\u0000msg-1')
    expect(nextSelectedKey([first], 'missing')).toBe(executionKey(first))
    expect(nextSelectedKey([first], executionKey(first))).toBe(executionKey(first))
    expect(claimFingerprint(first)).not.toBe(claimFingerprint({ ...first, updated_at: '2026-09-09T02:00:00Z' }))
  })

  it('formats execution timestamps without crashing on missing values', () => {
    expect(formatExecutionTime('2026-09-09T09:30:00Z')).toBe('2026-09-09 09:30:00')
    expect(formatExecutionTime(undefined)).toBe('—')
    expect(formatExecutionTime(null)).toBe('—')
  })

  it('reads reply text only from the base64 outbox contract', () => {
    expect(outboxReplyText(btoa(JSON.stringify({ text: 'hello-reply' })))).toBe('hello-reply')
    expect(outboxReplyText(JSON.stringify({ text: 'plain-reply' }))).toBe('')
    expect(replyFromExecution(detail)).toBe('')
    expect(runSettled({ event_id: 'msg-1', attempts: 0 })).toBe(false)
  })
})
