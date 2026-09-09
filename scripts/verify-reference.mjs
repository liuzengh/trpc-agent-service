import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';

// Run against a disposable local demo process; publication is restored on exit.
const base = new URL(process.argv[2] ?? 'http://127.0.0.1:8080');
assert(base.protocol === 'http:' && ['127.0.0.1', 'localhost', '[::1]'].includes(base.hostname),
  'reference verification requires a local HTTP demo');
assert(!base.username && !base.password && base.pathname === '/' && !base.search && !base.hash);
const chatKey = process.env.TRPC_SERVICE_API_KEY ?? 'local-development-key-not-a-secret';
const adminKey = process.env.TRPC_SERVICE_ADMIN_API_KEY ??
  (await readFile(new URL('../data/admin-api-key', import.meta.url), 'utf8')).trim();
const adminPath = '/admin/v1/tenants/demo/apps/echo';
const nonce = randomUUID();
const revisionID = `verify-${nonce}`;
const payload = { model: 'deterministic-echo', messages: [{ role: 'user', content: 'reference check' }] };
let checks = 0;

async function request(path, status, { key, body, headers = {} } = {}) {
  const response = await fetch(new URL(path, base), {
    method: body === undefined ? 'GET' : 'POST',
    redirect: 'error',
    signal: AbortSignal.timeout(10000),
    headers: {
      ...(key ? { Authorization: `Bearer ${key}` } : {}),
      ...(body === undefined ? {} : { 'Content-Type': 'application/json' }),
      ...headers,
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  assert.equal(response.status, status, `${path}: unexpected HTTP status`);
  const text = await response.text();
  return { headers: response.headers, text, json: () => JSON.parse(text) };
}

function passed(name, details = {}) {
  checks++;
  console.log(JSON.stringify({ check: name, result: 'PASS', ...details }));
}

function chat(headers = {}, body = payload, status = 200) {
  return request('/v1/chat/completions', status, {
    key: chatKey, body, headers: { 'X-Agent-App-ID': 'echo', ...headers },
  });
}

function publish(id) {
  return request(`${adminPath}/revisions/${id}/publish`, 200, { key: adminKey, body: {} });
}

await request('/healthz', 200);
passed('health');
for (const path of ['/v1/chat/completions', adminPath]) {
  const response = await request(path, 401, path.startsWith('/v1') ? { body: payload } : {});
  assert.equal(response.json().error.code, 'unauthenticated');
}
await request(adminPath, 401, { key: chatKey });
passed('authentication-and-credential-separation');

const app = (await request(adminPath, 200, { key: adminKey })).json();
const original = app.routing_policy.default_revision_id;
assert(original, 'demo App must have a published default Revision');
const first = await chat({ 'X-Request-ID': 'untrusted-client-id' });
assert.equal(first.json().choices[0].message.content, 'echo: reference check');
const oldSession = first.headers.get('x-session-id');
const requestID = first.headers.get('x-request-id');
assert(oldSession && requestID && requestID !== 'untrusted-client-id');
assert.equal(first.headers.get('x-agent-revision-id'), original);
passed('http-echo-and-server-identifiers', { session_id: oldSession, request_id: requestID, revision_id: original });

const stream = await chat({ 'X-Session-ID': oldSession }, { ...payload, stream: true, user: 'untrusted-user' });
assert.match(stream.headers.get('content-type'), /text\/event-stream/);
const frames = stream.text.split(/\r?\n/).filter(line => line.startsWith('data: ')).map(line => line.slice(6));
assert.equal(frames.at(-1), '[DONE]');
const content = frames.slice(0, -1).map(frame => JSON.parse(frame).choices?.[0]?.delta?.content ?? '').join('');
assert.equal(content, 'echo: reference check');
assert.equal(stream.headers.get('x-session-id'), oldSession);
assert.equal(stream.headers.get('x-agent-revision-id'), original);
passed('sse-and-session-continuation', { session_id: oldSession, revision_id: original });

await chat({ 'X-Tenant-ID': 'another-tenant' }, payload, 403);
passed('tenant-assertion-refused');

let restore = false;
try {
  await request(`${adminPath}/revisions`, 201, {
    key: adminKey,
    body: {
      id: revisionID, revision_no: Date.now(),
      config: { agent_name: 'reference-echo', instruction: 'Echo the input.', model: { provider: 'deterministic', name: 'reference-echo' } },
    },
  });
  restore = true;
  await publish(revisionID);
  const pinned = await chat({ 'X-Session-ID': oldSession });
  assert.equal(pinned.headers.get('x-agent-revision-id'), original);
  const fresh = await chat();
  const newSession = fresh.headers.get('x-session-id');
  assert(newSession && newSession !== oldSession);
  assert.equal(fresh.headers.get('x-agent-revision-id'), revisionID);
  passed('publication-keeps-existing-session-pin', { old_revision: original, new_revision: revisionID });

  const conflict = await chat({ 'X-Session-ID': oldSession, 'X-Agent-Revision-ID': revisionID }, payload, 409);
  assert.equal(conflict.json().error.code, 'pin_conflict');
  passed('revision-pin-conflict');

  await publish(original);
  restore = false;
  const afterRollback = await chat();
  const stillPinned = await chat({ 'X-Session-ID': newSession });
  assert.equal(afterRollback.headers.get('x-agent-revision-id'), original);
  assert.equal(stillPinned.headers.get('x-agent-revision-id'), revisionID);
  passed('rollback-keeps-existing-session-pin');
} finally {
  if (restore) await publish(original);
}

console.log(JSON.stringify({ result: 'PASS', checks, base_url: base.origin,
  boundary: 'Local deterministic HTTP/SSE only; no real model, IM, database or cross-worker claim.' }));
