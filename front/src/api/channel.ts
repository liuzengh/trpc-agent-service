import api from './index'
import type { AssetVisibility } from './asset'

export interface ChannelBinding {
  binding_id: string
  tenant_id: string
  agent_id: string
  channel: 'wecom' | 'feishu'
  account_id: string
  credential_ref?: string
  verification_token_ref?: string
  /** 作者（成员 id）；作者可解绑并决定是否共享 */
  created_by?: string
  /** private=仅作者与租户管理员可见；shared=租户内共享只读 */
  visibility?: AssetVisibility
  created_at: string
}

export interface ChannelInput {
  tenant_id: string
  agent_id: string
  channel: 'wecom' | 'feishu'
  account_id: string
  credential_ref?: string
  verification_token_ref?: string
  visibility?: AssetVisibility
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
