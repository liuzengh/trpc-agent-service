// @vitest-environment node
import { afterEach, describe, expect, it, vi } from "vitest";
import { NextRequest } from "next/server";
import { GET, PATCH, POST, PUT } from "./route";
import { ARTIFACT_MAX_BYTES } from "../../../../lib/artifact-api";

afterEach(() => vi.restoreAllMocks());

const params = { params: Promise.resolve({ path: ["v1", "tenants", "tenant/a", "runtime-profiles", "profile b", "draft"] }) };

describe("Control API same-origin proxy", () => {
  it("forwards the idempotency key, cookie, exact body and encoded route, but no arbitrary headers", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(Response.json({ draft_revision: 4 }));
    const body = JSON.stringify({ expected_draft_revision: 3, credentials: {} });
    const request = new NextRequest("http://console.test/api/control/v1/tenants/t/runtime-profiles/p/draft?limit=20", {
      method: "PUT", body,
      headers: { accept: "application/json", "content-type": "application/json", cookie: "session=opaque", "Idempotency-Key": "persisted-attempt-key", authorization: "not-forwarded" },
    });
    const response = await PUT(request, params);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe(`${(process.env.CONTROL_API_BASE ?? "http://127.0.0.1:8080").replace(/\/$/, "")}/v1/tenants/tenant%2Fa/runtime-profiles/profile%20b/draft?limit=20`);
    expect(init).toMatchObject({ method: "PUT", cache: "no-store", redirect: "manual" });
    expect(Object.fromEntries(new Headers(init?.headers))).toEqual({ accept: "application/json", "content-type": "application/json", cookie: "session=opaque", "idempotency-key": "persisted-attempt-key" });
    expect(new TextDecoder().decode(init?.body as ArrayBuffer)).toBe(body);
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(await response.json()).toEqual({ draft_revision: 4 });
  });

  it("preserves multiple Set-Cookie headers and Retry-After on an upstream error while disabling browser caching", async () => {
    const upstream = new Response("rate limited", { status: 429, headers: { "content-type": "text/plain", "retry-after": "10", "cache-control": "public, max-age=300" } });
    upstream.headers.append("set-cookie", "session=new; HttpOnly; Path=/");
    upstream.headers.append("set-cookie", "refresh=new; HttpOnly; Path=/");
    vi.spyOn(globalThis, "fetch").mockResolvedValue(upstream);
    const response = await POST(new NextRequest("http://console.test/api/control/v1/auth/login", { method: "POST", body: "{}" }), { params: Promise.resolve({ path: ["v1", "auth", "login"] }) });
    expect(response.status).toBe(429);
    expect(response.headers.getSetCookie()).toEqual(["session=new; HttpOnly; Path=/", "refresh=new; HttpOnly; Path=/"]);
    expect(response.headers.get("retry-after")).toBe("10");
    expect(response.headers.get("content-type")).toBe("text/plain");
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(await response.text()).toBe("rate limited");
  });

  it("sends GET without a body and marks its response no-store", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(Response.json({ config: {} }));
    const response = await GET(new NextRequest("http://console.test/api/control/v1/runtime-profiles"), params);
    expect(fetchMock.mock.calls[0][1]?.body).toBeUndefined();
    expect(response.headers.get("cache-control")).toBe("no-store");
  });

  it("preserves the preflight 202 receipt location and retry interval", async () => {
    const path = ["v1", "tenants", "t", "channel-accounts", "cha_test", "preflights"];
    const location = "/v1/tenants/t/channel-accounts/cha_test/preflights/cpf_test";
    const receipt = { preflight_id: "cpf_test", tenant_id: "t", account_id: "cha_test", requested_at: "2026-09-06T12:00:00Z", job_deadline_at: "2026-09-06T12:02:00Z", status_url: location };
    vi.spyOn(globalThis, "fetch").mockResolvedValue(Response.json(receipt, { status: 202, headers: { Location: location, "Retry-After": "2" } }));
    const response = await POST(new NextRequest(`http://console.test/api/control/${path.join("/")}`, { method: "POST", body: JSON.stringify({ expected_account_revision: 1, expected_connection_revision: 1, expected_bot_token_version: 1 }), headers: { "Idempotency-Key": "original-preflight" } }), { params: Promise.resolve({ path }) });
    expect(response.status).toBe(202);
    expect(response.headers.get("location")).toBe(location);
    expect(response.headers.get("retry-after")).toBe("2");
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(await response.json()).toEqual(receipt);
  });

  it("preserves the fallback Set-Cookie behavior when getSetCookie is unavailable", async () => {
    const upstream = Response.json({}, { headers: { "set-cookie": "session=legacy; HttpOnly; Path=/" } });
    Object.defineProperty(upstream.headers, "getSetCookie", { value: undefined });
    vi.spyOn(globalThis, "fetch").mockResolvedValue(upstream);
    const response = await GET(new NextRequest("http://console.test/api/control/v1/me"), params);
    expect(response.headers.get("set-cookie")).toBe("session=legacy; HttpOnly; Path=/");
  });

  it("returns a non-cacheable 502 on a network failure", async () => {
    vi.spyOn(globalThis, "fetch").mockRejectedValue(new TypeError("connection refused"));
    const response = await GET(new NextRequest("http://console.test/api/control/v1/me"), params);
    expect(response.status).toBe(502);
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(await response.json()).toEqual({ error: { code: "CONTROL_API_UNAVAILABLE", message: "Control API is unavailable" } });
  });
  it.each([200, 201, 409, 422, 503])("preserves Deployment publication status %s, diagnostics and idempotency", async (status) => {
    const body = JSON.stringify({ expected_latest_revision_number: null, input: { schema_version: "v1", agent: { agent_id: "a", version_number: 3 }, profile: { profile_id: "p", revision_number: 2 } } });
    const payload = status < 300 ? { revision: { revision_number: 1 } } : { error: { code: "DEPLOYMENT_REVISION_INVALID" }, validation: { valid: false, diagnostics: [{ code: "DEPLOYMENT_RESOURCE_MISSING" }] } };
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(Response.json(payload, { status }));
    const response = await POST(new NextRequest("http://console.test/api/control/v1/tenants/t/deployments/d/revisions", { method: "POST", body, headers: { "content-type": "application/json", "Idempotency-Key": "same-publication" } }), { params: Promise.resolve({ path: ["v1", "tenants", "t", "deployments", "d", "revisions"] }) });
    expect(response.status).toBe(status); expect(await response.json()).toEqual(payload);
    expect(new Headers(fetchMock.mock.calls[0][1]?.headers).get("idempotency-key")).toBe("same-publication");
    expect(new TextDecoder().decode(fetchMock.mock.calls[0][1]?.body as ArrayBuffer)).toBe(body);
  });

  it.each([
    { method: "POST" as const, path: ["channel-accounts"], status: 201, body: { provider: "telegram", provider_account_id: "12345", name: "test account", credentials: { "telegram.bot_token": { action: "replace", value: "test-only-token" }, "telegram.webhook_secret": { action: "replace", value: "test_secret" } } } },
    { method: "PATCH" as const, path: ["channel-accounts", "account:a"], status: 200, body: { expected_account_revision: 2, name: "renamed", description: "metadata only" } },
    { method: "POST" as const, path: ["channel-accounts", "account:a", "credentials", "telegram.bot_token", "update"], status: 200, body: { expected_account_revision: 2, expected_credential_version: 1, action: "replace", value: "test-only-replacement" } },
    { method: "POST" as const, path: ["channel-accounts", "account:a", "enabled"], status: 200, body: { expected_account_revision: 2, enabled: false } },
    { method: "POST" as const, path: ["channel-bindings"], status: 201, body: { account_id: "account:a", target: { deployment_id: "deployment:a", revision_number: 3 } } },
    { method: "POST" as const, path: ["channel-bindings", "binding:a", "target"], status: 200, body: { expected_binding_revision: 4, target: { deployment_id: "deployment:a", revision_number: 3 } } },
    { method: "POST" as const, path: ["channel-bindings", "binding:a", "enabled"], status: 200, body: { expected_binding_revision: 4, enabled: true } },
  ])("forwards Channel $method $path without changing CAS, cookie, idempotency or body", async (write) => {
    const payload = { account: { id: "account:a" }, binding: null, distribution: { state: "NOT_EMITTED", event_id: null } };
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(Response.json(payload, { status: write.status }));
    const path = ["v1", "tenants", "tenant:a", ...write.path];
    const body = JSON.stringify(write.body);
    const request = new NextRequest(`http://console.test/api/control/${path.map(encodeURIComponent).join("/")}`, {
      method: write.method, body,
      headers: { "content-type": "application/json", accept: "application/json", cookie: "session=opaque", "Idempotency-Key": "channel-original-attempt", authorization: "not-forwarded", "x-gateway-id": "not-forwarded" },
    });
    const response = await ({ POST, PATCH }[write.method])(request, { params: Promise.resolve({ path }) });
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe(`${(process.env.CONTROL_API_BASE ?? "http://127.0.0.1:8080").replace(/\/$/, "")}/${path.map(encodeURIComponent).join("/")}`);
    expect(init).toMatchObject({ method: write.method, cache: "no-store", redirect: "manual" });
    expect(Object.fromEntries(new Headers(init?.headers))).toEqual({ accept: "application/json", "content-type": "application/json", cookie: "session=opaque", "idempotency-key": "channel-original-attempt" });
    expect(new TextDecoder().decode(init?.body as ArrayBuffer)).toBe(body);
    expect(response.status).toBe(write.status);
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(await response.json()).toEqual(payload);
  });

});

it("bounds raw Artifact uploads before forwarding instead of buffering arbitrary bytes", async () => {
  const fetcher = vi.spyOn(globalThis, "fetch").mockResolvedValue(Response.json({ version: 0 }, { status: 201 }));
  const path = ["v1", "tenants", "t", "deployments", "d", "revisions", "2", "artifacts", "large.bin"];
  const response = await PUT(new NextRequest(`http://console.test/api/control/${path.join("/")}?run_id=run_1`, { method: "PUT", body: new Uint8Array(ARTIFACT_MAX_BYTES + 1), headers: { "content-type": "application/octet-stream" } }), { params: Promise.resolve({ path }) });
  expect(response.status).toBe(413);
  expect(response.headers.get("cache-control")).toBe("no-store");
  expect(fetcher).not.toHaveBeenCalled();
});

it("forwards exact Artifact bytes and cookie but no client identity headers", async () => {
  const raw = new Uint8Array([0, 255, 1]);
  const fetcher = vi.spyOn(globalThis, "fetch").mockResolvedValue(Response.json({ name: "report.txt", version: 0 }, { status: 201 }));
  const path = ["v1", "tenants", "t", "deployments", "d", "revisions", "2", "artifacts", "report #1.txt"];
  const response = await PUT(new NextRequest("http://console.test/api/control/v1/artifact?run_id=run_1", { method: "PUT", body: raw, headers: { "content-type": "text/plain", cookie: "session=opaque", "x-user-id": "forged", "x-tenant-id": "forged", authorization: "forged" } }), { params: Promise.resolve({ path }) });
  expect(response.status).toBe(201);
  expect(fetcher.mock.calls[0][0]).toContain("/artifacts/report%20%231.txt?run_id=run_1");
  expect([...new Uint8Array(fetcher.mock.calls[0][1]?.body as ArrayBuffer)]).toEqual([...raw]);
  expect(Object.fromEntries(new Headers(fetcher.mock.calls[0][1]?.headers))).toEqual({ "content-type": "text/plain", cookie: "session=opaque" });
});

it.each(["text/html", "image/svg+xml"])("forces Artifact %s into attachment delivery, never same-origin active content", async (mime) => {
  const raw = "<svg onload=alert(document.domain)>";
  vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(raw, { headers: { "content-type": mime, "content-disposition": "inline" } }));
  const path = ["v1", "tenants", "t", "deployments", "d", "revisions", "2", "artifacts", "active.svg"];
  const response = await GET(new NextRequest(`http://console.test/api/control/${path.join("/")}?run_id=run_1&version=0`), { params: Promise.resolve({ path }) });
  expect(response.headers.get("content-disposition")).toBe("attachment");
  expect(response.headers.get("x-content-type-options")).toBe("nosniff");
  expect(response.headers.get("content-type")).toBe(mime);
  expect(await response.text()).toBe(raw);
});

it("propagates Artifact request cancellation to the upstream without widening forwarded headers", async () => {
  const fetcher = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("bytes"));
  const controller = new AbortController();
  const path = ["v1", "tenants", "t", "deployments", "d", "revisions", "2", "artifacts", "file.txt"];
  const response = await GET(new NextRequest(`http://console.test/api/control/${path.join("/")}`, { signal: controller.signal }), { params: Promise.resolve({ path }) });
  const upstreamSignal = fetcher.mock.calls[0][1]?.signal;
  expect(upstreamSignal).toBeDefined();expect(upstreamSignal?.aborted).toBe(false);
  controller.abort();expect(upstreamSignal?.aborted).toBe(true);
  await response.body?.cancel();
});

it("forwards the legacy create interpreter only on the Account POST and exposes the result contract", async () => {
  const fetch = vi.spyOn(globalThis, "fetch").mockResolvedValue(Response.json({ account: {} }, { status: 201, headers: { "X-Channel-Result-Contract": "webhook-v1" } }));
  const path = ["v1", "tenants", "t", "channel-accounts"];
  const response = await POST(new NextRequest(`http://console.test/api/control/${path.join("/")}`, { method: "POST", body: "{}", headers: { "X-Channel-Create-Contract": "webhook-v1", "X-Other-Override": "ignored", "Idempotency-Key": "original" } }), { params: Promise.resolve({ path }) });
  const headers = new Headers(fetch.mock.calls[0][1]?.headers);
  expect(headers.get("X-Channel-Create-Contract")).toBe("webhook-v1");
  expect(headers.get("X-Other-Override")).toBeNull();
  expect(response.headers.get("X-Channel-Result-Contract")).toBe("webhook-v1");
  expect(response.headers.get("cache-control")).toBe("no-store");
  await PATCH(new NextRequest(`http://console.test/api/control/${path.join("/")}/a`, { method: "PATCH", body: "{}", headers: { "X-Channel-Create-Contract": "webhook-v1" } }), { params: Promise.resolve({ path: [...path, "a"] }) });
  expect(new Headers(fetch.mock.calls[1][1]?.headers).get("X-Channel-Create-Contract")).toBeNull();
});

describe("Knowledge text import proxy",()=>{
 const path=["v1","tenants","t","deployments","d","revisions","2","knowledge","docs","import"];
 it("rejects streamed JSON over 2MiB before forwarding",async()=>{
  const f=vi.spyOn(globalThis,"fetch").mockResolvedValue(Response.json({documents:1}));const bytes=new Uint8Array(2*1024*1024+1);const request=new NextRequest("http://console.test/api/control/"+path.join("/"),{method:"POST",body:bytes,headers:{"content-type":"application/json"}});const r=await POST(request,{params:Promise.resolve({path})});expect(r.status).toBe(413);expect(f).not.toHaveBeenCalled();
 });
 it("forwards only original owner cookie and exact JSON, with client abort to upstream",async()=>{
  const cancel=new AbortController();const f=vi.spyOn(globalThis,"fetch").mockResolvedValue(Response.json({documents:1}));const body=JSON.stringify({name:"note.txt",text:"hello"});const r=await POST(new NextRequest("http://console.test/api/control/"+path.join("/"),{method:"POST",body,signal:cancel.signal,headers:{cookie:"session=opaque","content-type":"application/json","x-tenant-id":"spoof","x-user-id":"spoof"}}),{params:Promise.resolve({path})});expect(await r.json()).toEqual({documents:1});expect(new TextDecoder().decode(f.mock.calls[0][1]?.body as ArrayBuffer)).toBe(body);expect(new Headers(f.mock.calls[0][1]?.headers).has("x-tenant-id")).toBe(false);const signal=f.mock.calls[0][1]?.signal;expect(signal).toBeDefined();cancel.abort();expect(signal?.aborted).toBe(true);
 });
});
