import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseEnv } from 'node:util';

const root = fileURLToPath(new URL('../', import.meta.url));
const result = {};

async function request(path, options) {
  const response = await fetch(`https://open.feishu.cn${path}`, {
    ...options,
    redirect: 'error',
    signal: AbortSignal.timeout(15000),
  });
  return { ok: response.ok, status: response.status, body: await response.json() };
}

function status(response) {
  return {
    http: response.status,
    code: Number.isInteger(response.body?.code) ? response.body.code : null,
  };
}

function nonempty(value) {
  return typeof value === 'string' && value.trim().length > 0;
}

try {
  const env = parseEnv(readFileSync(resolve(root, process.argv[2] ?? 'data/feishu.env'), 'utf8'));
  if (!nonempty(env.TRPC_SERVICE_FEISHU_APP_ID) || !nonempty(env.TRPC_SERVICE_FEISHU_APP_SECRET)) {
    throw new Error('missing configuration');
  }
  const token = await request('/open-apis/auth/v3/tenant_access_token/internal', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      app_id: env.TRPC_SERVICE_FEISHU_APP_ID,
      app_secret: env.TRPC_SERVICE_FEISHU_APP_SECRET,
    }),
  });
  result.credentials = {
    ...status(token),
    valid: token.ok && token.body?.code === 0 && nonempty(token.body?.tenant_access_token),
  };
  if (result.credentials.valid) {
    const headers = { Authorization: `Bearer ${token.body.tenant_access_token}` };
    const bot = await request('/open-apis/bot/v3/info', { headers });
    result.bot = {
      ...status(bot),
      identified: bot.ok && bot.body?.code === 0 && nonempty(bot.body?.bot?.open_id),
    };
    const tenant = await request('/open-apis/tenant/v2/tenant/query', { headers });
    result.tenant = {
      ...status(tenant),
      identified: tenant.ok && tenant.body?.code === 0 && nonempty(tenant.body?.data?.tenant?.tenant_key),
    };
    // Print only a known scope, never provider messages, account identifiers or tokens.
    if (Array.isArray(tenant.body?.error?.permission_violations) &&
        tenant.body.error.permission_violations.some(item => item?.subject === 'tenant:tenant:readonly')) {
      result.tenant.missingScope = 'tenant:tenant:readonly';
    }
  }
  console.log(JSON.stringify(result));
  if (!result.credentials.valid || !result.bot?.identified || !result.tenant?.identified) process.exitCode = 1;
} catch {
  if (Object.keys(result).length) console.log(JSON.stringify(result));
  console.error('Feishu preflight incomplete; check the local file, network and app permissions.');
  process.exitCode = 1;
}
