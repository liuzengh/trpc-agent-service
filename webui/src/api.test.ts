import { afterEach, describe, expect, it, vi } from 'vitest'

import {
  createBackendProfile,
  createApplication,
  getArtifact,
  getApps,
  getClaims,
  getSystem,
  getTenantCatalog,
  getUsers,
  postChat,
  setUnauthorizedHandler,
  type ApplicationPayload,
} from './api'

function jsonResponse(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

function sseResponse(chunks: string[]): Response {
  const encoder = new TextEncoder()
  return new Response(
    new ReadableStream<Uint8Array>({
      start(controller) {
        for (const chunk of chunks) controller.enqueue(encoder.encode(chunk))
        controller.close()
      },
    }),
    { status: 200 },
  )
}

afterEach(() => {
  vi.unstubAllGlobals()
  setUnauthorizedHandler(null)
})

describe('console API client', () => {
  it('creates backend profiles without a client-controlled status', async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({
      profile_id: 'session-redis-a', display_name: '会话 Redis A', driver: 'redis', status: 'active',
    }, 201))
    vi.stubGlobal('fetch', fetchMock)

    await createBackendProfile({
      profile_id: 'session-redis-a', display_name: '会话 Redis A', driver: 'redis', connection_ref: 'env:SESSION_REDIS_URL',
    })

    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit]
    expect(url).toBe('/api/v1/backend-profiles')
    expect(init.method).toBe('POST')
    expect(JSON.parse(String(init.body))).toEqual({
      profile_id: 'session-redis-a', display_name: '会话 Redis A', driver: 'redis', connection_ref: 'env:SESSION_REDIS_URL',
    })
  })

  it('normalizes nullable member arrays from historical user rows', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse({
      users: [{
        platform_user_id: 'user-1',
        display_name: 'Ming',
        email: '',
        role: '',
        status: 'active',
        is_system_admin: false,
        last_login_at: '',
        providers: null,
        conversation_content_audit: null,
      }],
      next_cursor: null,
    })))

    await expect(getUsers()).resolves.toEqual({
      users: [expect.objectContaining({ providers: [], conversation_content_audit: false })],
      next_cursor: '',
    })
  })

  it('maps application responses without changing the endpoint contract', async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ applications: [{ tenant_id: 'tenant-a' }] }))
    vi.stubGlobal('fetch', fetchMock)

    await expect(getApps()).resolves.toEqual([{ tenant_id: 'tenant-a' }])
    expect(fetchMock).toHaveBeenCalledWith(
      '/api/v1/apps',
      expect.objectContaining({ credentials: 'same-origin' }),
    )
  })

  it('sends CSRF protection on application writes', async () => {
    vi.stubGlobal('document', { cookie: 'csrf_token=csrf-value' })
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ application: { tenant_id: 'tenant-a' } }))
    vi.stubGlobal('fetch', fetchMock)
    const payload = {
      tenant_id: 'tenant-a',
      app_code: 'support',
      status: 'active',
      instruction: 'help',
      model: {},
      tools: {},
      storage: {},
      governance: {},
      audit: {},
      channels: [],
    } as unknown as ApplicationPayload

    await createApplication(payload)

    const [, init] = fetchMock.mock.calls[0] as [string, RequestInit]
    expect(new Headers(init.headers).get('X-CSRF-Token')).toBe('csrf-value')
    expect(init.method).toBe('POST')
  })

  it('notifies the application on an unauthorized response', async () => {
    const onUnauthorized = vi.fn()
    setUnauthorizedHandler(onUnauthorized)
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse({ error: 'expired' }, 401)))

    await expect(getApps()).rejects.toMatchObject({ status: 401, message: 'expired' })
    expect(onUnauthorized).toHaveBeenCalledTimes(1)
  })

  it('accepts only the current snake_case execution-list contract', async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(jsonResponse({
        claims: [{
          channel: 'web',
          binding_id: 'web',
          message_id: 'msg-1',
          status: 'completed',
          trace_id: 'trace-1',
          updated_at: '2026-09-09T09:30:00Z',
          started_at: '2026-09-09T09:29:58Z',
          ended_at: '2026-09-09T09:30:00Z',
        }],
      }))
      .mockResolvedValueOnce(jsonResponse({
        claims: [{
          Channel: 'web',
          BindingID: 'web',
          MessageID: 'msg-1',
          Status: 'completed',
          TraceID: 'trace-1',
          UpdatedAt: '2026-09-09T09:30:00Z',
        }],
      }))
    vi.stubGlobal('fetch', fetchMock)

    await expect(getClaims('tenant-a', 'support')).resolves.toHaveLength(1)
    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      '/api/v1/claims?tenant=tenant-a&app=support',
      expect.anything(),
    )
    await expect(getClaims('tenant-a')).rejects.toThrow(/API 契约不一致.*旧版 PascalCase/)
  })

  it('accepts only the current snake_case system-status contract', async () => {
    const current = {
      info: {
        version: 'v1',
        listen_address: ':8080',
        kafka_topic: 'agent.inbound.v1',
        kafka_brokers: 'kafka:9092',
        redis_address: 'redis:6379',
        model_providers: [],
      },
      status: { postgres: 'ok', redis: 'ok', kafka: 'ok' },
      nodes: [],
    }
    const legacy = {
      info: {
        Version: 'v1',
        ListenAddress: ':8080',
        KafkaTopic: 'agent.inbound.v1',
        KafkaBrokers: 'kafka:9092',
        RedisAddress: 'redis:6379',
        ModelProviders: [],
      },
      status: { postgres: 'ok' },
    }
    vi.stubGlobal('fetch', vi.fn()
      .mockResolvedValueOnce(jsonResponse(current))
      .mockResolvedValueOnce(jsonResponse(legacy)))

    await expect(getSystem()).resolves.toEqual(current)
    await expect(getSystem()).rejects.toThrow(/API 契约不一致.*旧版 PascalCase/)
  })

  it('loads the tenant-selectable tool catalog', async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({
      model_providers: [],
      channel_credential_refs: [],
      tools: [
        { name: 'query_order', description: '查询订单' },
        { name: 'refund_order', description: '发起退款' },
      ],
    }))
    vi.stubGlobal('fetch', fetchMock)

    await expect(getTenantCatalog('tenant-a')).resolves.toEqual({
      model_providers: [],
      channel_credential_refs: [],
      tools: [
        { name: 'query_order', description: '查询订单' },
        { name: 'refund_order', description: '发起退款' },
      ],
    })
    expect(fetchMock).toHaveBeenCalledWith('/api/v1/catalog?tenant=tenant-a', expect.anything())
  })

  it('reconnects chat SSE with Last-Event-ID and suppresses replayed delta events', async () => {
    vi.stubGlobal('window', globalThis)
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse({ stream_url: '/api/v1/chat/events/event-1', event_id: 'event-1', session_key: 'session-1' }, 202))
      .mockResolvedValueOnce(sseResponse(['id: 1\ndata: {"type":"delta","content":"A"}\n\n']))
      .mockResolvedValueOnce(
        sseResponse([
          'id: 1\ndata: {"type":"delta","content":"A"}\n\n',
          'id: 2\ndata: {"type":"done","reply":"answer"}\n\n',
        ]),
      )
    vi.stubGlobal('fetch', fetchMock)
    const deltas: string[] = []

    await expect(postChat({
      tenant_id: 'tenant-a',
      app_code: 'support',
      conversation_id: 'chat-1',
      text: 'hello',
      request_id: 'request-1',
    }, (delta) => deltas.push(delta))).resolves.toEqual({
      reply: 'answer',
      eventId: 'event-1',
      sessionKey: 'session-1',
    })

    const [, streamInit] = fetchMock.mock.calls[2] as [string, RequestInit]
    expect(new Headers(streamInit.headers).get('Last-Event-ID')).toBe('1')
    expect(deltas).toEqual(['A'])
  })

  it('ignores malformed SSE events and continues to the next event', async () => {
    vi.stubGlobal('window', globalThis)
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse({ stream_url: '/api/v1/chat/events/event-malformed', event_id: 'event-malformed', session_key: 'session-malformed' }, 202))
      .mockResolvedValueOnce(sseResponse([
        'id: bad\ndata: {not-json}\n\n',
        'id: 2\ndata: {"type":"done","reply":"still works"}\n\n',
      ]))
    vi.stubGlobal('fetch', fetchMock)

    await expect(postChat({
      tenant_id: 'tenant-a',
      app_code: 'support',
      conversation_id: 'chat-1',
      text: 'hello',
      request_id: 'request-malformed',
    }, () => {})).resolves.toEqual({
      reply: 'still works',
      eventId: 'event-malformed',
      sessionKey: 'session-malformed',
    })
  })

  it('sends chat files as multipart without overriding the browser boundary', async () => {
    vi.stubGlobal('window', globalThis)
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse({ stream_url: '/api/v1/chat/events/event-file', event_id: 'event-file', session_key: 'session-file' }, 202))
      .mockResolvedValueOnce(sseResponse(['id: 1\ndata: {"type":"done","reply":"file received"}\n\n']))
    vi.stubGlobal('fetch', fetchMock)

    const file = new File(['notes'], 'notes.txt', { type: 'text/plain' })
    await postChat({
      tenant_id: 'tenant-a', app_code: 'support', conversation_id: 'chat-1',
      text: 'read this', request_id: 'request-file',
    }, () => {}, undefined, [file])

    const [, init] = fetchMock.mock.calls[0] as [string, RequestInit]
    const headers = new Headers(init.headers)
    expect(headers.has('Content-Type')).toBe(false)
    expect(init.body).toBeInstanceOf(FormData)
    const form = init.body as FormData
    expect(form.get('text')).toBe('read this')
    expect((form.get('files') as File).name).toBe('notes.txt')
  })

  it('lists and deletes knowledge documents with tenant query params', async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({
      documents: [{ document_id: 'doc-1', name: 'Doc 1', total_chunks: 3, status: 'ready' }],
    }))
    vi.stubGlobal('fetch', fetchMock)

    const { listKnowledgeDocuments, deleteKnowledgeDocument } = await import('./api')
    const docs = await listKnowledgeDocuments('tenant-a', 'support')
    expect(docs).toHaveLength(1)
    expect(docs[0].document_id).toBe('doc-1')
    expect(fetchMock).toHaveBeenCalledWith(
      '/api/v1/knowledge/documents?tenant=tenant-a&app=support',
      expect.anything(),
    )

    fetchMock.mockResolvedValueOnce(jsonResponse({}))
    await deleteKnowledgeDocument('tenant-a', 'support', 'doc-1')
    expect(fetchMock).toHaveBeenCalledWith(
      '/api/v1/knowledge?tenant=tenant-a&app=support&document_id=doc-1',
      expect.objectContaining({ method: 'DELETE' }),
    )

  })

  it('uploads knowledge to the selected tenant and bot', async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ job_id: 'job-1', status: 'queued' }))
    vi.stubGlobal('fetch', fetchMock)

    const { uploadKnowledgeDocument } = await import('./api')
    const file = new File(['policy body'], 'policy.md', { type: 'text/markdown' })
    await uploadKnowledgeDocument({
      tenant_id: 'tenant-a', app_code: 'trailforge', document_id: 'policy', file,
      chunk_size: 800, overlap: 100,
    })

    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit]
    expect(url).toBe('/api/v1/knowledge')
    expect(init.method).toBe('POST')
    expect(init.body).toBeInstanceOf(FormData)
    const form = init.body as FormData
    expect(form.get('tenant_id')).toBe('tenant-a')
    expect(form.get('app_code')).toBe('trailforge')
    expect(form.get('document_id')).toBe('policy')
    expect((form.get('file') as File).name).toBe('policy.md')
  })

  it('lists sessions and reads the durable transcript', async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(jsonResponse({
        sessions: [{ SessionKey: 'acme/support/session/session-1', Preview: '订单到哪了' }],
      }))
      .mockResolvedValueOnce(jsonResponse({
        messages: [{ id: 'u1', role: 'user', content: '订单到哪了', time: '2026-09-09T09:00:00Z' }],
      }))
    vi.stubGlobal('fetch', fetchMock)

    const { listSessions, getSessionMessages } = await import('./api')
    await expect(listSessions('acme', { app: 'support', channel: 'web', status: 'active' }, 'mine')).resolves.toEqual([
      { SessionKey: 'acme/support/session/session-1', Preview: '订单到哪了' },
    ])
    expect(fetchMock).toHaveBeenCalledWith(
      '/api/v1/sessions/mine?tenant=acme&app=support&channel=web&status=active',
      expect.anything(),
    )
    await expect(getSessionMessages('acme', 'acme/support/session/session-1')).resolves.toEqual({
      messages: [{ id: 'u1', role: 'user', content: '订单到哪了', time: '2026-09-09T09:00:00Z' }],
      next_cursor: '',
    })
    expect(fetchMock).toHaveBeenCalledWith(
      '/api/v1/sessions/messages?tenant=acme&session_key=acme%2Fsupport%2Fsession%2Fsession-1',
      expect.anything(),
    )
  })

  it('reads artifact content as binary instead of base64 JSON', async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response('report body', {
      status: 200,
      headers: {
        'Content-Type': 'text/plain; charset=utf-8',
        'X-Artifact-Version': '3',
      },
    }))
    vi.stubGlobal('fetch', fetchMock)

    const artifact = await getArtifact('acme', 'support', 'session-1', 'report.txt')
    expect(artifact.filename).toBe('report.txt')
    expect(artifact.version).toBe(3)
    expect(artifact.mime_type).toBe('text/plain; charset=utf-8')
    expect(artifact.text).toBe('report body')
    expect(artifact.data).toBeInstanceOf(Blob)
    expect(fetchMock).toHaveBeenCalledWith(
      '/api/v1/artifacts?tenant=acme&app=support&session=session-1&filename=report.txt',
      expect.anything(),
    )
  })

  it('reads shared channel runtime status', async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({
      channels: [{ channel: 'wecom', binding_id: 'support', state: 'connected' }],
    }))
    vi.stubGlobal('fetch', fetchMock)
    const { getChannelStatuses } = await import('./api')
    await expect(getChannelStatuses('acme', 'support')).resolves.toEqual([
      { channel: 'wecom', binding_id: 'support', state: 'connected' },
    ])
    expect(fetchMock).toHaveBeenCalledWith('/api/v1/channels/status?tenant=acme&app=support', expect.anything())
  })

  it('queries memories with kind, time range and ordering parameters', async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({
      memories: [{
        id: 'mem-1',
        memory: { kind: 'episode', memory: 'Visited Shanghai', event_time: '2026-09-01T00:00:00Z', location: 'Shanghai' },
        updated_at: '2026-09-01T12:00:00Z',
      }],
    }))
    vi.stubGlobal('fetch', fetchMock)

    const { getMemories } = await import('./api')
    const memories = await getMemories({
      tenant: 'tenant-a',
      app: 'support',
      query: 'Shanghai',
      kind: 'episode',
      timeAfter: '2026-09-01',
      timeBefore: '2026-09-02',
      order: 'event_time',
    })
    expect(memories).toHaveLength(1)
    expect(memories[0].id).toBe('mem-1')
    expect(fetchMock).toHaveBeenCalledWith(
      '/api/v1/memory?tenant=tenant-a&app=support&query=Shanghai&kind=episode&time_after=2026-09-01&time_before=2026-09-02&order=event_time',
      expect.anything(),
    )
  })
})
