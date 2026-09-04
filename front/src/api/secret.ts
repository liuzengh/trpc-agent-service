import axios from 'axios'

const baseURL = import.meta.env.VITE_API_BASE ?? 'http://localhost:8080'
const client = axios.create({ baseURL })

// putSecret stores a plaintext credential in the secret store under key. The
// store encrypts it at rest; reads never return the plaintext, so callers
// keep their own reference key.
export async function putSecret(key: string, value: string): Promise<void> {
  await client.post('/secrets', { key, value })
}

/** Metadata of one stored credential. The value is never returned. */
export interface Secret {
  key: string
  updated_at: string
}

/** Lists credential metadata (keys + timestamps only, never plaintext). */
export async function listSecrets(): Promise<Secret[]> {
  const { data } = await client.get<Secret[]>('/secrets')
  return data
}

/** Deletes a stored credential by key. */
export async function deleteSecret(key: string): Promise<void> {
  await client.delete(`/secrets/${encodeURIComponent(key)}`)
}
