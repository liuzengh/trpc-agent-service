import { defineStore } from 'pinia'
import * as api from '../api/endpoint'
import type { Endpoint } from '../api/endpoint'

export const useEndpointStore = defineStore('endpoint', {
  state: () => ({
    endpoints: [] as Endpoint[],
    loading: false,
    error: '',
  }),
  actions: {
    async fetch(tenantId = '') {
      this.loading = true
      this.error = ''
      try {
        this.endpoints = await api.listEndpoints(tenantId)
      } catch (e) {
        this.error = String(e)
      } finally {
        this.loading = false
      }
    },
    async create(e: Endpoint) {
      const created = await api.createEndpoint(e)
      this.endpoints.push(created)
    },
    async update(e: Endpoint) {
      await api.updateEndpoint(e)
      const i = this.endpoints.findIndex((x) => x.id === e.id)
      if (i >= 0) this.endpoints[i] = { ...e }
    },
    async remove(id: string) {
      await api.deleteEndpoint(id)
      this.endpoints = this.endpoints.filter((x) => x.id !== id)
    },
  },
})
