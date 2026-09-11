import { afterEach, describe, expect, it, vi } from "vitest";
import { deploymentApi, deploymentRead, DeploymentApiError, DEPLOYMENT_TIMEOUT_MS } from "./deployment-api";
import { input, invalidReport, validReport } from "../test/deployment-fixtures";
afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers(); });
describe("Deployment public API", () => {
  it("maps all eight routes, exact bodies and scoped idempotency headers", async () => {
    const fetch = vi.spyOn(globalThis, "fetch").mockImplementation(async () => Response.json({}));
    const t = "tenant/a"; const d = "dep b";
    await deploymentApi.create(t, { name: "n", description: "d" }, "create-key");
    await deploymentApi.list(t, 20, 10); await deploymentApi.get(t, d);
    await deploymentApi.update(t, d, { expected_metadata_revision: 2, name: "changed" });
    await deploymentApi.validate(t, d, input);
    await deploymentApi.publish(t, d, { expected_latest_revision_number: null, input }, "publish-key");
    await deploymentApi.listRevisions(t, d, 20, 10); await deploymentApi.getRevision(t, d, 5);
    const base = "/api/control/v1/tenants/tenant%2Fa/deployments";
    expect(fetch.mock.calls.map(([url]) => url)).toEqual([base, `${base}?offset=20&limit=10`, `${base}/dep%20b`, `${base}/dep%20b`, `${base}/dep%20b/validate`, `${base}/dep%20b/revisions`, `${base}/dep%20b/revisions?offset=20&limit=10`, `${base}/dep%20b/revisions/5`]);
    for (const [, init] of fetch.mock.calls) expect(init).toMatchObject({ credentials: "include", cache: "no-store", signal: expect.any(AbortSignal) });
    expect(fetch.mock.calls[0][1]).toMatchObject({ method: "POST", headers: { "Idempotency-Key": "create-key" } });
    expect(JSON.parse(String(fetch.mock.calls[4][1]?.body))).toEqual(input);
    expect(new Headers(fetch.mock.calls[4][1]?.headers).has("Idempotency-Key")).toBe(false);
    expect(JSON.parse(String(fetch.mock.calls[5][1]?.body))).toEqual({ expected_latest_revision_number: null, input });
    expect(new Headers(fetch.mock.calls[5][1]?.headers).get("Idempotency-Key")).toBe("publish-key");
  });
  it.each([200, 201])("accepts publication status %s without generating a key", async (status) => {
    const fetch = vi.spyOn(globalThis, "fetch").mockImplementation(async () => Response.json({ revision: { revision_number: 7 }, validation: validReport }, { status }));
    const result = await deploymentApi.publish("t", "d", { expected_latest_revision_number: 6, input }, "stable");
    expect(result.revision.revision_number).toBe(7); expect(fetch).toHaveBeenCalledTimes(1);
  });
  it("does not confuse HTTP 200 invalid with successful validation", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(Response.json(invalidReport));
    expect((await deploymentApi.validate("t", "d", input)).valid).toBe(false);
  });
  it.each([[422, "DEPLOYMENT_REVISION_INVALID"], [409, "DEPLOYMENT_LATEST_REVISION_CONFLICT"], [503, "DEPENDENCY_UNAVAILABLE"], [403, "TENANT_FORBIDDEN"]])("retains status %s, code and validation diagnostics", async (status, code) => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(Response.json({ error: { code, message: "diagnostic" }, validation: invalidReport }, { status: Number(status) }));
    await expect(deploymentApi.publish("t", "d", { expected_latest_revision_number: null, input }, "same")).rejects.toMatchObject({ status, code, validation: invalidReport });
  });
  it("bounds body consumption and reports an uncertain timed-out request", async () => {
    vi.useFakeTimers();
    vi.spyOn(globalThis, "fetch").mockImplementation(async (_url, init) => ({ ok: true, status: 201, json: () => new Promise((_resolve, reject) => init?.signal?.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")))) }) as Response);
    const promise = expect(deploymentApi.publish("t", "d", { expected_latest_revision_number: null, input }, "same")).rejects.toMatchObject({ status: 0, code: "REQUEST_TIMEOUT" });
    await vi.advanceTimersByTimeAsync(DEPLOYMENT_TIMEOUT_MS); await promise;
  });
  it("retains HTTP identity for non-JSON server failures", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("bad gateway", { status: 502 }));
    await expect(deploymentApi.get("t", "d")).rejects.toMatchObject({ status: 502, code: "INVALID_RESPONSE" });
  });
  it("bounds existing source client reads without retrying them", async () => {
    vi.useFakeTimers(); const assertion = expect(deploymentRead(new Promise(() => {}))).rejects.toBeInstanceOf(DeploymentApiError);
    await vi.advanceTimersByTimeAsync(DEPLOYMENT_TIMEOUT_MS); await assertion;
  });
});
