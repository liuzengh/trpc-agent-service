import axios from 'axios'

const baseURL = import.meta.env.VITE_API_BASE ?? 'http://localhost:8080'
const client = axios.create({ baseURL })

// putSecret stores a plaintext credential in the secret store under key. The
// store encrypts it at rest; reads never return the plaintext, so callers
// keep their own reference key.
export async function putSecret(key: string, value: string): Promise<void> {
  await client.post('/secrets', { key, value })
}
