import api from './index'

export interface KnowledgeBase {
  id: string
  tenant_id: string
  name: string
  embedding_endpoint_id: string
  collection_name: string
  dimension?: number
}

export interface KBInput {
  id?: string
  tenant_id: string
  name: string
  embedding_endpoint_id: string
}

export interface Document {
  id: string
  kb_id: string
  title?: string
  source_uri?: string // http(s) URL, or "inline" for text docs
  text?: string // inline content; not persisted
  chunk_count: number
  status: string
  error?: string
}

export interface SearchHit {
  document_id: string
  source_name: string
  content: string
  score: number
}

export async function listKBs(tenantId = ''): Promise<KnowledgeBase[]> {
  const { data } = await api.get<KnowledgeBase[]>('/kbs', { params: { tenant_id: tenantId } })
  return data
}

export async function getKB(id: string): Promise<KnowledgeBase> {
  const { data } = await api.get<KnowledgeBase>(`/kbs/${id}`)
  return data
}

export async function createKB(kb: KBInput): Promise<KnowledgeBase> {
  const { data } = await api.post<KnowledgeBase>('/kbs', kb)
  return data
}

export async function deleteKB(id: string): Promise<void> {
  await api.delete(`/kbs/${id}`)
}

export async function addDocument(kbId: string, doc: Partial<Document>): Promise<Document> {
  const { data } = await api.post<Document>(`/kbs/${kbId}/documents`, doc)
  return data
}

export async function listDocuments(kbId: string): Promise<Document[]> {
  const { data } = await api.get<Document[]>(`/kbs/${kbId}/documents`)
  return data
}

export async function searchKB(kbId: string, query: string, limit = 5): Promise<SearchHit[]> {
  const { data } = await api.post<SearchHit[]>(`/kbs/${kbId}/search`, { query, limit })
  return data
}
