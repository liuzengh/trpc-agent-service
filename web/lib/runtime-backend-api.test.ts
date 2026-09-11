import { afterEach, describe, expect, it, vi } from "vitest";
import { decodeBackendDirectory, listRuntimeBackends, selectableBackend, backendDirectoryMessage, BackendDirectoryError } from "./runtime-backend-api";
const backend = { id: "pg", revision: 1, label: "Postgres", kind: "postgresql", roles: ["session", "memory"], available: true };
afterEach(() => vi.restoreAllMocks());
describe("real backend directory contract client (unit transport tests)", () => {
  it("uses only the authenticated tenant BFF route without role query or pagination", async () => {
    const fetcher = vi.spyOn(globalThis, "fetch").mockResolvedValue(Response.json({ items: [backend] }));
    expect(await listRuntimeBackends("t/a")).toEqual([backend]);
    expect(fetcher).toHaveBeenCalledWith("/api/control/v1/tenants/t%2Fa/runtime-backends", expect.objectContaining({ credentials: "include", cache: "no-store" }));
  });
  it.each([0, -1, 1.5, 9007199254740992])("rejects unsafe revision %s", (revision) => {
    expect(() => decodeBackendDirectory({ items: [{ ...backend, revision }] })).toThrow("INVALID_BACKEND_DIRECTORY");
  });
  it.each([{ ...backend, target: "private" }, { ...backend, password: "private" }, { ...backend, roles: ["artifact"] }, { ...backend, roles: ["memory", "memory"] }])("rejects malformed or private directory fields", (item) => {
    expect(() => decodeBackendDirectory({ items: [item] })).toThrow();
  });
  it("rejects duplicate IDs but permits legitimate prototype-like identifiers", () => {
    expect(() => decodeBackendDirectory({ items: [backend, backend] })).toThrow();
    expect(decodeBackendDirectory({ items: [{ ...backend, id: "constructor" }] })[0].id).toBe("constructor");
  });
  it("filters by explicit roles and availability, not backend kind alone", () => {
    const [item] = decodeBackendDirectory({ items: [{ ...backend, roles: ["session"] }] });
    expect(selectableBackend(item, "memory")).toBe(false);
    expect(selectableBackend({ ...item, available: false }, "session")).toBe(false);
  });
  it.each([401, 403, 404, 503])("does not return fake empty success on HTTP %s", async (status) => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("private diagnostics", { status }));
    await expect(listRuntimeBackends("tenant")).rejects.toBeInstanceOf(BackendDirectoryError);
  });
  it("does not claim all 404s mean missing wiring", () => {
    const message = backendDirectoryMessage(new BackendDirectoryError("BACKEND_DIRECTORY_NOT_FOUND", 404));
    expect(message).toContain("暂不可用（HTTP 404）"); expect(message).not.toContain("未接线");
  });
});
