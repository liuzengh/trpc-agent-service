import { defineStore } from 'pinia'
import * as api from '../api/kb'
import type { Document, KnowledgeBase, SearchHit } from '../api/kb'

export const useKBStore = defineStore('kb', {
  state: () => ({
    kbs: [] as KnowledgeBase[],
    loading: false,
    error: '',
  }),
  actions: {
    async fetch(tenantId = '') {
      this.loading = true
      this.error = ''
      try {
        this.kbs = await api.listKBs(tenantId)
      } catch (e) {
        this.error = String(e)
      } finally {
        this.loading = false
      }
    },
    async create(kb: api.KBInput) {
      const created = await api.createKB(kb)
      this.kbs.push(created)
    },
    async remove(id: string) {
      await api.deleteKB(id)
      this.kbs = this.kbs.filter((x) => x.id !== id)
    },
    async addDocument(kbId: string, doc: Partial<Document>) {
      return api.addDocument(kbId, doc)
    },
    async listDocuments(kbId: string): Promise<Document[]> {
      return api.listDocuments(kbId)
    },
    async search(kbId: string, query: string): Promise<SearchHit[]> {
      return api.searchKB(kbId, query)
    },
  },
})
