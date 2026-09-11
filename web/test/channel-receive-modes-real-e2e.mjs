import strictAssert from "node:assert/strict";
import { randomUUID } from "node:crypto";

let assertions = 0;
const assert = Object.fromEntries(
  ["ok", "equal", "deepEqual"].map((name) => [name, (...args) => {
    assertions += 1;
    return strictAssert[name](...args);
  }]),
);

// Run only against an explicitly selected, isolated Control/Next test environment.
// This script never enables an account, invokes preflight, or contacts Telegram.
const required = (name) => { const value = process.env[name]; assert.ok(value, `${name} is required`); return value; };
const base = required("WEB_BASE_URL").replace(/\/$/, "");
const username = required("CONTROL_E2E_USERNAME");
const password = required("CONTROL_E2E_PASSWORD");
const tenant = required("CONTROL_E2E_TENANT_ID");
let cookie = "";
let requests = 0;
async function request(path, { method = "GET", body, key, contract, expected = 200, anonymous = false } = {}) {
  const headers = { accept: "application/json" };
  if (cookie && !anonymous) headers.cookie = cookie;
  if (body !== undefined) headers["content-type"] = "application/json";
  if (key) headers["Idempotency-Key"] = key;
  if (contract) headers["X-Channel-Create-Contract"] = contract;
  const response = await fetch(`${base}/api/control${path}`, { method, headers, body: body === undefined ? undefined : JSON.stringify(body), redirect: "manual", signal: AbortSignal.timeout(10_000) });
  assert.equal(response.status, expected, `${method} ${path}: unexpected HTTP status`);
  assert.equal(response.headers.get("cache-control"), "no-store");
  const cookies = response.headers.getSetCookie();
  if (cookies.length && !anonymous) cookie = cookies.map((v) => v.split(";", 1)[0]).join("; ");
  requests += 1;
  const value = response.status === 204 ? null : await response.json();
  return { value, contract: response.headers.get("X-Channel-Result-Contract") };
}

await request("/v1/auth/login", { method: "POST", body: { username, password } });
const me = await request("/v1/me");
assert.equal(me.value.password_change_required, false, "Use an unrestricted test Session");
const root = `/v1/tenants/${encodeURIComponent(tenant)}/channel-accounts`;
await request(root, { anonymous: true, expected: 401 });
const suffix = Date.now().toString();
const create = {
  provider: "telegram", provider_account_id: `8${suffix}`, name: `Receive modes Web E2E ${suffix}`,
  config: { receive_mode: "long_polling" },
  credentials: { "telegram.bot_token": { action: "replace", value: "synthetic-local-test-token" } },
};
const createKey = randomUUID();
const created = await request(root, { method: "POST", body: create, key: createKey, expected: 201 });
assert.equal(created.contract, "receive-modes-v1");
const accountID = created.value.account.account_id;
const detail = `${root}/${encodeURIComponent(accountID)}`;
let account = (await request(detail)).value.account;
assert.equal(account.enabled, false);
assert.equal(account.config.receive_mode, "long_polling");
assert.equal(typeof account.config.webhook_path, "string");
const initialToken = account.credentials.find((c) => c.purpose === "telegram.bot_token");
const initialSecret = account.credentials.find((c) => c.purpose === "telegram.webhook_secret");
assert.deepEqual(initialSecret, { purpose: "telegram.webhook_secret", credential_version: 1, configured: false });
assert.equal((await request(root, { method: "POST", body: create, key: createKey, expected: 201 })).value.account.account_id, accountID);

const before = structuredClone(account);
const modeBody = { expected_account_revision: account.account_revision, config: { receive_mode: "webhook" } };
const modeKey = randomUUID();
const modeResult = await request(detail, { method: "PATCH", body: modeBody, key: modeKey });
assert.equal(modeResult.contract, "receive-modes-v1");
account = (await request(detail)).value.account;
assert.equal(account.config.receive_mode, "webhook");
assert.equal(account.enabled, false);
assert.equal(account.account_revision, before.account_revision + 1);
assert.equal(account.connection_revision, before.connection_revision + 1);
assert.deepEqual(account.credentials, before.credentials);
assert.equal(account.config.webhook_path, before.config.webhook_path);
await request(detail, { method: "PATCH", body: modeBody, key: modeKey });
await request(detail, { method: "PATCH", body: { ...modeBody, config: { receive_mode: "long_polling" } }, key: randomUUID(), expected: 409 });

const connectionRevision = account.connection_revision;
await request(detail, { method: "PATCH", body: { expected_account_revision: account.account_revision, name: `${create.name} renamed` }, key: randomUUID() });
account = (await request(detail)).value.account;
assert.equal(account.connection_revision, connectionRevision);
await request(detail, { method: "PATCH", body: { expected_account_revision: account.account_revision, config: { receive_mode: "long_polling" } }, key: randomUUID() });
account = (await request(detail)).value.account;
await request(`${detail}/credentials/telegram.webhook_secret/update`, { method: "POST", body: { expected_account_revision: account.account_revision, expected_credential_version: 1, action: "replace", value: "synthetic_optional_secret" }, key: randomUUID() });
account = (await request(detail)).value.account;
assert.equal(account.enabled, false);
assert.equal(account.config.receive_mode, "long_polling");
assert.deepEqual(account.credentials.find((c) => c.purpose === "telegram.bot_token"), initialToken);
assert.deepEqual(account.credentials.find((c) => c.purpose === "telegram.webhook_secret"), { purpose: "telegram.webhook_secret", credential_version: 2, configured: true });

// Simulate a persisted old Web marker whose original command never reached Control.
const legacy = { provider: "telegram", provider_account_id: `9${suffix}`, name: `Legacy Web E2E ${suffix}`, credentials: { ...create.credentials, "telegram.webhook_secret": { action: "replace", value: "synthetic_legacy_secret" } } };
const legacyKey = randomUUID();
const restored = await request(root, { method: "POST", body: legacy, key: legacyKey, contract: "webhook-v1", expected: 201 });
assert.equal(restored.value.account.config.receive_mode, "webhook");
assert.equal(restored.value.account.enabled, false);
const repeated = await request(root, { method: "POST", body: legacy, key: legacyKey, contract: "webhook-v1", expected: 201 });
assert.equal(repeated.value.account.account_id, restored.value.account.account_id);
await request(root, { method: "POST", body: { ...legacy, config: { receive_mode: "long_polling" } }, key: legacyKey, expected: 409 });
assert.equal((await request(`${root}/${encodeURIComponent(restored.value.account.account_id)}`)).value.account.enabled, false);
assert.equal((await request(detail)).value.account.enabled, false);
console.log(JSON.stringify({ result: "PASS", assertions, requests, account_id: accountID, legacy_account_id: restored.value.account.account_id, web_account_url: `${base}/tenants/${encodeURIComponent(tenant)}/channels/${encodeURIComponent(accountID)}`, accounts_enabled: false, telegram_requests: 0 }));
