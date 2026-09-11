import { afterEach, describe, expect, it, vi } from "vitest";

import {
  RuntimeProfileApiError,
  runtimeProfileApi,
  type CredentialUpdate,
  type ProfileConfig,
  type ProfileWrite,
} from "./runtime-profile-api";

const config: ProfileConfig = {
  models: { primary: { kind: "openai_compatible", model: "chat-model", base_url: "https://models.example.test/v1", capabilities: ["chat", "tool_call"] } },
  tools: { search: { kind: "mcp_streamable_http", server_url: "https://tools.example.test/mcp", toolset_name: "web", tool_name: "search", auth: { kind: "bearer" }, capability: "web.search" } },
  knowledge: { docs: { kind: "qdrant_openai", host: "qdrant.example.test", port: 6334, tls: true, collection: "docs", embedding: { model: "embedding-model", base_url: "https://models.example.test/v1", dimensions: 1536 } } },
  storage: { session: { kind: "postgres_state" } },
};
const write: ProfileWrite = {
  expected_draft_revision: 3,
  credential_protocol_version: "v1",
  config,
  credentials: { models: { primary: { api_key: { action: "replace", value: "test-only-key" } } } },
};
const update: CredentialUpdate = {
  target: { profile_revision_number: 7, category: "models", resource_name: "primary", purpose_field: "api_key", association_token: "a".repeat(64) },
  action: "replace",
  expected_credential_revision: 2,
  value: "test-only-replacement",
};
function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}

afterEach(() => vi.restoreAllMocks());

describe("Runtime Profile browser API", () => {
  it("maps all 11 public operations with encoded path segments, JSON inputs and idempotency keys", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch").mockImplementation(async () => jsonResponse({ marker: "response" }));
    const t = "tenant/a";
    const p = "profile b";
    await runtimeProfileApi.listProfiles(t, { offset: 20, limit: 10 });
    await runtimeProfileApi.createProfile(t, { name: "Research", description: "Research resources" });
    await runtimeProfileApi.getProfile(t, p);
    await runtimeProfileApi.updateProfile(t, p, { description: "Changed" });
    await runtimeProfileApi.getDraft(t, p);
    await runtimeProfileApi.saveDraft(t, p, write, "save-key-1");
    await runtimeProfileApi.validateDraft(t, p, { expected_revision: 4 });
    await runtimeProfileApi.publishRevision(t, p, { expected_revision: 4 });
    await runtimeProfileApi.listRevisions(t, p, { offset: 0, limit: 25 });
    await runtimeProfileApi.getRevision(t, p, 7);
    await runtimeProfileApi.updateCredential(t, p, update, "credential-key-1");

    const base = "/api/control/v1/tenants/tenant%2Fa/runtime-profiles";
    const profile = `${base}/profile%20b`;
    const read = { credentials: "include", cache: "no-store" };
    const json = (method: string, body: unknown, key?: string) => ({
      ...read, method,
      headers: { "Content-Type": "application/json", ...(key ? { "Idempotency-Key": key } : {}) },
      body: JSON.stringify(body),
    });
    expect(fetchMock.mock.calls).toEqual([
      [`${base}?offset=20&limit=10`, read],
      [base, json("POST", { name: "Research", description: "Research resources" })],
      [profile, read],
      [profile, json("PATCH", { description: "Changed" })],
      [`${profile}/draft`, read],
      [`${profile}/draft`, json("PUT", write, "save-key-1")],
      [`${profile}/draft/validate`, json("POST", { expected_revision: 4 })],
      [`${profile}/revisions`, json("POST", { expected_revision: 4 })],
      [`${profile}/revisions?offset=0&limit=25`, read],
      [`${profile}/revisions/7`, read],
      [`${profile}/credentials/update`, json("POST", update, "credential-key-1")],
    ]);
  });

  it("returns the save receipt without pretending it is a Draft or fetching/retrying implicitly", async () => {
    const receipt = { profile_id: "p", draft_revision: 4, updated_at: "2026-09-05T01:00:00Z" };
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(jsonResponse(receipt));
    expect(await runtimeProfileApi.saveDraft("t", "p", write, "same-logical-save")).toEqual(receipt);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("preserves credential actions and supports clearing a published credential without a value", async () => {
    const receipt = { credential_revision: 3, status: "cleared" };
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(jsonResponse(receipt));
    const clear: CredentialUpdate = { target: update.target, action: "clear", expected_credential_revision: 2 };
    expect(await runtimeProfileApi.updateCredential("t", "p", clear, "clear-once")).toEqual(receipt);
    expect(JSON.parse(String(fetchMock.mock.calls[0][1]?.body))).toEqual(clear);
  });

  it("leaves read projections and immutable publish responses unchanged", async () => {
    const draft = { tenant_id: "t", profile_id: "p", draft_revision: 4, schema_version: "v1", credential_protocol_version: "v1", config, credential_states: { models: { primary: { api_key: { configured: true, status: "active", credential_revision: 2, association_token: "a".repeat(64) } } } } };
    const revision = { id: "rev-7", tenant_id: "t", profile_id: "p", revision_number: 7, source_draft_revision: 4, schema_version: "v1", credential_protocol_version: "v1", config, spec_digest: "sha256:abc", published_by: "u", published_at: "2026-09-05T01:00:00Z" };
    vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(jsonResponse(draft))
      .mockResolvedValueOnce(jsonResponse({ revision }, 201))
      .mockResolvedValueOnce(jsonResponse({ ...revision, credential_states: draft.credential_states }));
    expect(await runtimeProfileApi.getDraft("t", "p")).toEqual(draft);
    expect(await runtimeProfileApi.publishRevision("t", "p", { expected_revision: 4 })).toEqual({ revision });
    expect(await runtimeProfileApi.getRevision("t", "p", 7)).toEqual({ ...revision, credential_states: draft.credential_states });
  });

  it("exposes typed resource diagnostics from failed publication", async () => {
    const validation = { valid: false, schema_version: "v1", draft_revision: 3, diagnostics: [{ code: "MODEL_REQUIRED", severity: "error", pointer: "/models/primary/model", resource_kind: "model", resource_key: "primary", message: "Model is required" }] };
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(jsonResponse({ error: { code: "RUNTIME_PROFILE_SPEC_INVALID", message: "RuntimeProfileSpec validation failed" }, validation }, 422));
    await expect(runtimeProfileApi.publishRevision("t", "p", { expected_revision: 3 })).rejects.toMatchObject({ name: "RuntimeProfileApiError", status: 422, code: "RUNTIME_PROFILE_SPEC_INVALID", validation });
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it.each([
    [401, "UNAUTHENTICATED"], [403, "TENANT_FORBIDDEN"], [409, "RUNTIME_PROFILE_DRAFT_REVISION_CONFLICT"],
    [409, "CREDENTIAL_ASSOCIATION_CONFLICT"], [409, "CREDENTIAL_REVISION_CONFLICT"], [409, "IDEMPOTENCY_CONFLICT"],
  ])("propagates HTTP %s %s without automatic retry", async (status, code) => {
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(jsonResponse({ error: { code, message: "Request conflict" } }, status));
    let error: unknown;
    try { await runtimeProfileApi.saveDraft("t", "p", write, "retry-key"); } catch (caught) { error = caught; }
    expect(error).toBeInstanceOf(RuntimeProfileApiError);
    expect(error).toMatchObject({ status, code, message: "Request conflict" });
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("uses a useful error for a non-JSON upstream response", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("gateway error", { status: 502 }));
    await expect(runtimeProfileApi.getProfile("t", "p")).rejects.toMatchObject({ status: 502, code: "HTTP_ERROR", message: "Control API returned HTTP 502" });
  });
});
