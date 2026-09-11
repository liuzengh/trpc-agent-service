import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { managedProfileIssues } from "./managed-profile";
import { runtimeProfileApi } from "./runtime-profile-api";
import type { ProfileConfig } from "./runtime-profile-api";
function projection(): ProfileConfig {
  const fixture = JSON.parse(readFileSync(resolve(process.cwd(), "../api/schemas/runtimeprofile/v1/examples/valid/managed-platform-resources.json"), "utf8"));
  // Test-only projection: canonical credential IDs are never authoring fields.
  delete fixture.knowledge.docs.embedding.api_key_credential_id;
  return { models: fixture.models, tools: fixture.tools, knowledge: fixture.knowledge, storage: fixture.storage };
}
describe("managed public config projection", () => {
  it("accepts projected Control fixture with all four managed roles", () => expect(managedProfileIssues(projection())).toEqual([]));
  it.each([0, 1.5, Number.MAX_SAFE_INTEGER + 1])("rejects invalid revision %s", (backend_revision) => {
    const config = projection(); config.storage.memory.backend_revision = backend_revision;
    expect(managedProfileIssues(config).map((item) => item.pointer)).toContain("/storage/memory/backend_revision");
  });
  it("does not accept role mismatch or physical storage destination", () => {
    const config = projection(); config.storage.memory.kind = "managed_session"; config.storage.memory.destination = { host: "private" };
    expect(managedProfileIssues(config).map((item) => item.pointer)).toEqual(expect.arrayContaining(["/storage/memory/kind", "/storage/memory/destination"]));
  });
  it("requires embedding and rejects Qdrant target fields in managed knowledge", () => {
    const config = projection(); config.knowledge.docs.host = "private"; delete config.knowledge.docs.embedding;
    expect(managedProfileIssues(config).map((item) => item.pointer)).toEqual(expect.arrayContaining(["/knowledge/docs/host", "/knowledge/docs/embedding"]));
  });
  it("does not mutate config or rewrite legacy resources", () => {
    const config = projection();config.storage.legacy = { kind: "postgres_state", destination: { host: "db" } };
    const before = JSON.stringify(config);managedProfileIssues(config);expect(JSON.stringify(config)).toBe(before);
  });
});

afterEach(() => vi.restoreAllMocks());
it("writes the public managed projection and embedding action separately with CAS/idempotency", async () => {
  const config = projection();
  const fetcher = vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(Response.json({ profile_id: "profile", draft_revision: 3, updated_at: "now" })).mockResolvedValueOnce(Response.json({ config, draft_revision: 3, credential_states: { knowledge: { docs: { embedding_api_key: { configured: true, status: "active", credential_revision: 1 } } } } }));
  await runtimeProfileApi.saveDraft("tenant", "profile", { expected_draft_revision: 2, credential_protocol_version: "v1", config, credentials: { knowledge: { docs: { embedding_api_key: { action: "replace", value: "test-only-embedding-key" } } } } }, "managed-save");
  const [, init] = fetcher.mock.calls[0];
  const body = JSON.parse(String(init?.body));
  expect(body.expected_draft_revision).toBe(2);
  expect(new Headers(init?.headers).get("Idempotency-Key")).toBe("managed-save");
  expect(body.config).toEqual(config);
  expect(JSON.stringify(body.config)).not.toMatch(/credential_id|password|destination|collection|test-only/);
  expect(body.credentials).toEqual({ knowledge: { docs: { embedding_api_key: { action: "replace", value: "test-only-embedding-key" } } } });
  const read = await runtimeProfileApi.getDraft("tenant", "profile");
  expect(read.config).toEqual(config);expect(read.credential_states?.knowledge?.docs?.embedding_api_key.configured).toBe(true);
});

it("sends PG Memory raw password as dsn_password without changing public binding", async () => {
  const config = projection();
  const fetcher = vi.spyOn(globalThis, "fetch").mockResolvedValue(Response.json({ profile_id: "p", draft_revision: 3 }));
  await runtimeProfileApi.saveDraft("t", "p", { expected_draft_revision: 2, credential_protocol_version: "v1", config, credentials: { storage: { memory: { dsn_password: { action: "replace", value: "test-raw-password" } } } } }, "memory-save");
  const body = JSON.parse(String(fetcher.mock.calls[0][1]?.body));
  expect(body.config.storage.memory).toEqual(config.storage.memory);
  expect(JSON.stringify(body.config)).not.toMatch(/test-raw-password|dsn_credential_id|credential_audience_digest/);
  expect(body.credentials.storage.memory.dsn_password).toEqual({ action: "replace", value: "test-raw-password" });
});
