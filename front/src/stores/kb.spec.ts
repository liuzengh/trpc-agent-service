import { beforeEach, describe, expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { useKBStore } from './kb'
import * as api from '../api/kb'

vi.mock('../api/kb', () => ({
  listKBs: vi.fn(),
  createKB: vi.fn(),
  deleteKB: vi.fn(),
  addDocument: vi.fn(),
  listDocuments: vi.fn(),
  searchKB: vi.fn(),
}))

describe('useKBStore', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    vi.clearAllMocks()
  })

  it('fetch populates knowledge bases from the api', async () => {
    vi.mocked(api.listKBs).mockResolvedValue([
      {
        id: 'kb-1',
        tenant_id: 't1',
        name: 'docs',
        embedding_endpoint_id: 'e-1',
        collection_name: 't1_kb_1',
        dimension: 64,
      },
    ])
    const store = useKBStore()
    await store.fetch()
    expect(store.kbs).toHaveLength(1)
    expect(store.kbs[0]?.name).toBe('docs')
  })

  it('create appends the new kb', async () => {
    vi.mocked(api.createKB).mockResolvedValue({
      id: 'kb-2',
      tenant_id: 't1',
      name: 'faq',
      embedding_endpoint_id: 'e-1',
      collection_name: 't1_kb_2',
    })
    const store = useKBStore()
    await store.create({ tenant_id: 't1', name: 'faq', embedding_endpoint_id: 'e-1' })
    expect(store.kbs).toHaveLength(1)
    expect(api.createKB).toHaveBeenCalled()
  })

  it('remove deletes via the api', async () => {
    vi.mocked(api.deleteKB).mockResolvedValue()
    const store = useKBStore()
    await store.remove('kb-1')
    expect(api.deleteKB).toHaveBeenCalledWith('kb-1')
  })

  it('delegates documents and search to the api', async () => {
    vi.mocked(api.addDocument).mockResolvedValue({
      id: 'd-1',
      kb_id: 'kb-1',
      title: 'x',
      chunk_count: 1,
      status: 'ready',
    })
    vi.mocked(api.listDocuments).mockResolvedValue([])
    vi.mocked(api.searchKB).mockResolvedValue([])
    const store = useKBStore()
    await store.addDocument('kb-1', { title: 'x', text: 'y' })
    expect(api.addDocument).toHaveBeenCalledWith('kb-1', { title: 'x', text: 'y' })
    await store.listDocuments('kb-1')
    expect(api.listDocuments).toHaveBeenCalledWith('kb-1')
    await store.search('kb-1', 'q')
    expect(api.searchKB).toHaveBeenCalledWith('kb-1', 'q')
  })
})
