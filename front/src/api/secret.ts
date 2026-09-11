import api from './index'

// putSecret stores a plaintext credential in the secret store under key. The
// store encrypts it at rest; reads never return the plaintext, so callers
// keep their own reference key.
export async function putSecret(key: string, value: string): Promise<void> {
  await api.post('/secrets', { key, value })
}

/** Metadata of one stored credential. The value is never returned. */
export interface Secret {
  key: string
  updated_at: string
}

/** Lists credential metadata (keys + timestamps only, never plaintext). */
export async function listSecrets(): Promise<Secret[]> {
  const { data } = await api.get<Secret[]>('/secrets')
  return data
}

/** Deletes a stored credential by key. */
export async function deleteSecret(key: string): Promise<void> {
  await api.delete(`/secrets/${encodeURIComponent(key)}`)
}
