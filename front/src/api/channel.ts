import api from './index'

export interface ChannelBinding {
  binding_id: string
  tenant_id: string
  agent_id: string
  channel: 'wecom' | 'feishu'
  account_id: string
  credential_ref?: string
  verification_token_ref?: string
  created_at: string
}

export interface ChannelInput {
  tenant_id: string
  agent_id: string
  channel: 'wecom' | 'feishu'
  account_id: string
  credential_ref?: string
  verification_token_ref?: string
}

export async function listChannels(tenantId = '', channel = ''): Promise<ChannelBinding[]> {
  const { data } = await api.get<ChannelBinding[]>('/channels', {
    params: { tenant_id: tenantId, channel },
  })
  return data
}

export async function createChannel(input: ChannelInput): Promise<void> {
  await api.post('/channels', input)
}

export async function deleteChannel(bindingId: string): Promise<void> {
  await api.delete(`/channels/${bindingId}`)
}
