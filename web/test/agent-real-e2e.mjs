import assert from "node:assert/strict";

const baseURL = (process.env.WEB_BASE_URL ?? "http://127.0.0.1:13000").replace(/\/$/, "");
const username = required("CONTROL_E2E_USERNAME");
const temporaryPassword = required("CONTROL_E2E_PASSWORD");
const replacementPassword = required("CONTROL_E2E_NEW_PASSWORD");

let cookie = "";

function required(name) {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function rememberCookie(headers) {
  const values = typeof headers.getSetCookie === "function"
    ? headers.getSetCookie()
    : [headers.get("set-cookie")].filter(Boolean);
  for (const value of values) {
    const first = value.split(";", 1)[0];
    if (first) cookie = first;
  }
}

async function request(path, { method = "GET", body, expected = [200] } = {}) {
  const headers = { accept: "application/json" };
  if (cookie) headers.cookie = cookie;
  if (body !== undefined) headers["content-type"] = "application/json";
  const response = await fetch(`${baseURL}${path}`, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
    redirect: "manual",
  });
  rememberCookie(response.headers);
  const text = await response.text();
  const payload = text ? JSON.parse(text) : undefined;
  assert.ok(
    expected.includes(response.status),
    `${method} ${path}: expected ${expected.join("/")}, got ${response.status}: ${text}`,
  );
  return { status: response.status, payload };
}

let login = await request("/api/control/v1/auth/login", {
  method: "POST",
  body: { username, password: temporaryPassword },
  expected: [200, 401],
});
if (login.status === 401) {
  login = await request("/api/control/v1/auth/login", {
    method: "POST",
    body: { username, password: replacementPassword },
  });
}
if (login.payload.password_change_required) {
  await request("/api/control/v1/me/change-password", {
    method: "POST",
    body: { current_password: temporaryPassword, new_password: replacementPassword },
    expected: [204],
  });
}

const me = await request("/api/control/v1/me");
assert.equal(me.payload.password_change_required, false);

const suffix = `${Date.now()}-${Math.random().toString(16).slice(2, 8)}`;
const tenant = await request("/api/control/v1/admin/tenants", {
  method: "POST",
  body: {
    slug: `web-e2e-${suffix}`,
    name: `Web E2E ${suffix}`,
    owner_user_id: me.payload.user.id,
  },
  expected: [201],
});
const tenantID = tenant.payload.id;

const encodedTenant = encodeURIComponent(tenantID);
const created = await request(`/api/control/v1/tenants/${encodedTenant}/agents`, {
  method: "POST",
  body: { name: `Canvas Agent ${suffix}`, description: "real web proxy e2e" },
  expected: [201],
});
const agentID = created.payload.agent.id;
const encodedAgent = encodeURIComponent(agentID);
assert.equal(created.payload.draft.revision, 1);
assert.deepEqual(created.payload.draft.spec, {});

const agentBase = `/api/control/v1/tenants/${encodedTenant}/agents/${encodedAgent}`;
const fetchedAgent = await request(agentBase);
assert.equal(fetchedAgent.payload.id, agentID);
assert.equal(fetchedAgent.payload.latest_version_number, null);

const spec = {
  schema_version: "v1",
  root: "main",
  requirements: {
    models: { primary: { capabilities: ["chat", "tool_call"] } },
    tools: { search: { capability: "web.search" } },
    knowledge: {},
  },
  nodes: {
    main: { kind: "sequence", children: ["research", "review_loop"] },
    research: {
      kind: "llm",
      name: "Research",
      instruction: "Research the requested topic.",
      model_slot: "primary",
      tool_slots: ["search"],
      knowledge_slots: [],
    },
    review_loop: { kind: "loop", body: "review", max_iterations: 2 },
    review: {
      kind: "llm",
      name: "Review",
      instruction: "Review and improve the result.",
      model_slot: "primary",
      tool_slots: [],
      knowledge_slots: [],
    },
  },
};

const saved = await request(`${agentBase}/draft`, {
  method: "PUT",
  body: { expected_revision: 1, spec },
});
assert.equal(saved.payload.revision, 2);

const fetchedDraft = await request(`${agentBase}/draft`);
assert.equal(fetchedDraft.payload.revision, 2);
assert.deepEqual(fetchedDraft.payload.spec, spec);

const validated = await request(`${agentBase}/draft/validate`, {
  method: "POST",
  body: { expected_revision: 2 },
});
assert.equal(validated.payload.valid, true);
assert.deepEqual(validated.payload.diagnostics, []);

const published = await request(`${agentBase}/versions`, {
  method: "POST",
  body: { expected_revision: 2 },
  expected: [201],
});
assert.equal(published.payload.version.version_number, 1);
assert.match(published.payload.version.spec_digest, /^sha256:[0-9a-f]{64}$/);

const idempotent = await request(`${agentBase}/versions`, {
  method: "POST",
  body: { expected_revision: 2 },
  expected: [200],
});
assert.equal(idempotent.payload.version.id, published.payload.version.id);

const versions = await request(`${agentBase}/versions?offset=0&limit=20`);
assert.equal(versions.payload.total, 1);
assert.equal(versions.payload.versions[0].version_number, 1);

const version = await request(`${agentBase}/versions/1`);
assert.equal(version.payload.id, published.payload.version.id);
assert.deepEqual(version.payload.spec, published.payload.version.spec);

const updated = await request(agentBase, {
  method: "PATCH",
  body: { description: "updated through real web proxy" },
});
assert.equal(updated.payload.description, "updated through real web proxy");

const agents = await request(`/api/control/v1/tenants/${encodedTenant}/agents?offset=0&limit=20`);
assert.equal(agents.payload.total, 1);
assert.equal(agents.payload.agents[0].latest_version_number, 1);

const conflict = await request(`${agentBase}/draft`, {
  method: "PUT",
  body: { expected_revision: 1, spec },
  expected: [409],
});
assert.equal(conflict.payload.error.code, "AGENT_DRAFT_REVISION_CONFLICT");

console.log(JSON.stringify({
  result: "REAL_BACKEND_E2E_OK",
  tenant_id: tenantID,
  agent_id: agentID,
  saved_revision: saved.payload.revision,
  validation_valid: validated.payload.valid,
  version_number: published.payload.version.version_number,
  idempotent_status: idempotent.status,
  conflict_code: conflict.payload.error.code,
}, null, 2));
