/** Real P0a acceptance. Creates only dedicated Agents in an existing test tenant.
 * Never changes passwords or existing Agents; never mocks Control responses.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
const required = (key) => { assert.ok(process.env[key], `${key} is required`); return process.env[key]; };
const origin = new URL(required("WEB_BASE_URL"));
assert.ok(["http:", "https:"].includes(origin.protocol));
const tenant = required("CONTROL_E2E_TENANT_ID");
let cookie = "";
async function request(path, method = "GET", body, statuses = [200]) {
  const response = await fetch(new URL(`/api/control/v1${path}`, origin), {
    method, redirect: "manual", signal: AbortSignal.timeout(15000),
    headers: { "Content-Type": "application/json", Origin: origin.origin, ...(cookie ? { Cookie: cookie } : {}) },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
  for (const value of response.headers.getSetCookie()) cookie = value.split(";")[0];
  assert.ok(statuses.includes(response.status), `${method} ${path}: HTTP ${response.status}`);
  return response.status === 204 ? null : response.json();
}
function canonical(value) {
  const result = structuredClone(value);
  for (const node of Object.values(result.nodes)) if (node.memory) node.memory.tools.sort();
  return result;
}
try {
  const login = await request("/auth/login", "POST", { username: required("CONTROL_E2E_USERNAME"), password: required("CONTROL_E2E_PASSWORD") });
  assert.equal(login.password_change_required, false, "Use an initialized dedicated acceptance identity");
  const me = await request("/me"); assert.ok(me.user.id);
  for (const name of ["runtime-data-capabilities", "runtime-data-disabled"]) {
    const spec = JSON.parse(readFileSync(new URL(`../../api/schemas/agentspec/v1/examples/valid/${name}.json`, import.meta.url), "utf8"));
    const base = `/tenants/${encodeURIComponent(tenant)}/agents`;
    const created = await request(base, "POST", { name: `P0a ${name} ${Date.now()}`, description: "Isolated real runtime-data authoring acceptance" }, [201]);
    console.log(JSON.stringify({ created_agent_id: created.agent.id, tenant_id: tenant, purpose: "P0a dedicated acceptance; retain for audit, dispose only with isolated test environment" }));
    const target = `${base}/${encodeURIComponent(created.agent.id)}`;
    const saved = await request(`${target}/draft`, "PUT", { expected_revision: created.draft.revision, spec });
    await request(`${target}/draft`, "PUT", { expected_revision: created.draft.revision, spec }, [409]);
    const read = await request(`${target}/draft`);
    assert.equal(read.revision, saved.revision); assert.deepEqual(canonical(read.spec), canonical(spec));
    const report = await request(`${target}/draft/validate`, "POST", { expected_revision: read.revision });
    assert.equal(report.valid, true);
    const publication = await request(`${target}/versions`, "POST", { expected_revision: read.revision }, [201]);
    const version = await request(`${target}/versions/${publication.version.version_number}`);
    assert.deepEqual(canonical(version.spec), canonical(spec));
    const again = await request(`${target}/versions`, "POST", { expected_revision: read.revision });
    assert.equal(again.version.version_number, version.version_number);
    console.log(JSON.stringify({ fixture: name, agent_id: created.agent.id, draft_revision: read.revision, version_number: version.version_number, result: "PASS" }));
  }
  console.log("P0A_REAL_AGENT_AUTHORING=PASS; DEPLOYMENT=NOT_TESTED_P0A_GATE");
} finally {
  if (cookie) await request("/auth/logout", "POST", undefined, [204]);
}
