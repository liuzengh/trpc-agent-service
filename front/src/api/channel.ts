import axios from 'axios'

export interface ChannelBinding {
  binding_id: string
  tenant_id: string
  agent_id: string
  channel: 'wecom' | 'feishu'
  account_id: string
  credential_ref?: string
  created_at: string
}

export interface ChannelInput {
  tenant_id: string
  agent_id: string
  channel: 'wecom' | 'feishu'
  account_id: string
  credential_ref?: string
}

const baseURL = import.meta.env.VITE_API_BASE ?? 'http://localhost:8080'
const client = axios.create({ baseURL })

export async function listChannels(tenantId = '', channel = ''): Promise<ChannelBinding[]> {
  const { data } = await client.get<ChannelBinding[]>('/channels', {
    params: { tenant_id: tenantId, channel },
  })
  return data
}

export async function createChannel(input: ChannelInput): Promise<void> {
  await client.post('/channels', input)
}

export async function deleteChannel(bindingId: string): Promise<void> {
  await client.delete(`/channels/${bindingId}`)
}
