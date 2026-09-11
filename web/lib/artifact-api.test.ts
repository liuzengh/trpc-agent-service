// @vitest-environment node
import { afterEach, expect, it, vi } from "vitest";
import { ARTIFACT_MAX_BYTES, artifactApi, validArtifactName } from "./artifact-api";

afterEach(() => vi.restoreAllMocks());
const scope = { tenantId: "tenant/a", deploymentId: "d?&", revisionNumber: 2, runId: "run_test" };
const receipt = { name: "report #1.txt", version: 0, ref: "artifact://opaque", mime_type: "text/plain", size_bytes: 3, sha256: "a".repeat(64) };
it("uploads raw bytes to the fixed Control route with cookies and validates the owner receipt", async () => {
  const fetcher = vi.spyOn(globalThis, "fetch").mockResolvedValue(Response.json(receipt, { status: 201 }));
  const file = new Blob(["abc"], { type: "text/plain" });
  expect(await artifactApi.upload(scope, receipt.name, file)).toEqual(receipt);
  expect(fetcher.mock.calls[0][0]).toBe("/api/control/v1/tenants/tenant%2Fa/deployments/d%3F%26/revisions/2/artifacts/report%20%231.txt?run_id=run_test");
  const options = fetcher.mock.calls[0][1]!;
  expect(options).toMatchObject({ method: "PUT", body: file, credentials: "include", cache: "no-store", redirect: "error" });
  expect(Object.fromEntries(new Headers(options.headers))).toEqual({ "content-type": "text/plain" });
  expect(fetcher).toHaveBeenCalledTimes(1);
});
it("reads original bytes using explicit version zero instead of an implicit latest", async () => {
  const fetcher = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(new Uint8Array([0, 255, 1]), { headers: { "content-type": "application/octet-stream" } }));
  const result = await artifactApi.read(scope, "bytes.bin", 0);
  expect([...new Uint8Array(result.bytes)]).toEqual([0, 255, 1]);
  expect(result.mimeType).toBe("application/octet-stream");
  expect(fetcher.mock.calls[0][0]).toContain("/bytes.bin?run_id=run_test&version=0");
  expect(fetcher.mock.calls[0][1]?.body).toBeUndefined();
});
it.each(["", " ", ".", "..", "dir/file", "dir\\file", "bad\u0000name", "bad\nname"])("rejects non-basename %j before a request", async (name) => {
  const fetcher = vi.spyOn(globalThis, "fetch");
  expect(validArtifactName(name)).toBe(false);
  await expect(artifactApi.upload(scope, name, new Blob(["abc"]))).rejects.toThrow();
  expect(fetcher).not.toHaveBeenCalled();
});
it("rejects explicit transport overflow and invalid versions without contacting Control", async () => {
  const fetcher = vi.spyOn(globalThis, "fetch");
  await expect(artifactApi.upload(scope, "large.bin", new Blob([new Uint8Array(ARTIFACT_MAX_BYTES + 1)]))).rejects.toThrow(/16 MiB/);
  await expect(artifactApi.read(scope, "ok.txt", -1)).rejects.toThrow();
  expect(fetcher).not.toHaveBeenCalled();
});
it("does not automatically retry or claim acceptance after a lost upload response", async () => {
  const fetcher = vi.spyOn(globalThis, "fetch").mockRejectedValue(new TypeError("credential-like-private-diagnostic"));
  await expect(artifactApi.upload(scope, "ok.txt", new Blob(["abc"]))).rejects.toThrow(/结果尚未确认/);
  expect(fetcher).toHaveBeenCalledTimes(1);
});
it.each([401, 403, 404, 409, 413, 503])("reports HTTP %s without reflecting upstream body secrets", async (status) => {
  vi.spyOn(globalThis, "fetch").mockResolvedValue(Response.json({ error: { message: "private-secret" } }, { status }));
  await expect(artifactApi.read(scope, "ok.txt", 0)).rejects.toThrow(new RegExp(`HTTP ${status}`));
});
it.each([500, 502, 503, 504])("keeps an upload HTTP %s outcome uncertain rather than inviting blind replay", async (status) => {
  const fetcher = vi.spyOn(globalThis, "fetch").mockResolvedValue(Response.json({ error: { message: "private-secret" } }, { status }));
  await expect(artifactApi.upload(scope, "ok.txt", new Blob(["abc"]))).rejects.toThrow(/上传结果尚未确认.*版本可能已生成/);
  expect(fetcher).toHaveBeenCalledTimes(1);
});
it.each([{ ...receipt, name: "other.txt" }, { ...receipt, version: -1 }, { ...receipt, size_bytes: 4 }, { ...receipt, secret: "private" }])("rejects mismatched or non-public upload receipt", async (value) => {
  vi.spyOn(globalThis, "fetch").mockResolvedValue(Response.json(value, { status: 201 }));
  await expect(artifactApi.upload(scope, receipt.name, new Blob(["abc"], { type: "text/plain" }))).rejects.toThrow(/结果尚未确认/);
});
