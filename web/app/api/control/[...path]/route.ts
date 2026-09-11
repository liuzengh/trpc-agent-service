import type { NextRequest } from "next/server";
import { KNOWLEDGE_MAX_REQUEST_BYTES } from "../../../../lib/knowledge-api";
import { ARTIFACT_MAX_BYTES } from "../../../../lib/artifact-api";

export const dynamic = "force-dynamic";

const upstream = (process.env.CONTROL_API_BASE ?? "http://127.0.0.1:8080").replace(/\/$/, "");

async function boundedBody(request: NextRequest, limit: number): Promise<ArrayBuffer | null> {
  if (Number(request.headers.get("content-length")) > limit) { await request.body?.cancel(); return null; }
  const reader = request.body?.getReader();
  if (!reader) return new ArrayBuffer(0);
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > limit) { await reader.cancel(); return null; }
      chunks.push(value);
    }
  } finally { reader.releaseLock(); }
  const bytes = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
  return bytes.buffer;
}

async function proxy(request: NextRequest, context: { params: Promise<{ path: string[] }> }) {
  const { path } = await context.params;
  const artifactRoute = path.length === 9 && path[0] === "v1" && path[1] === "tenants" && path[3] === "deployments" && path[5] === "revisions" && path[7] === "artifacts";
  const knowledgeImport = request.method === "POST" && path.length === 10 && path[0] === "v1" && path[1] === "tenants" && path[3] === "deployments" && path[5] === "revisions" && path[7] === "knowledge" && path[9] === "import";
  const target = `${upstream}/${path.map(encodeURIComponent).join("/")}${request.nextUrl.search}`;
  const headers = new Headers();
  for (const name of ["accept", "content-type", "cookie", "idempotency-key"]) {
    const value = request.headers.get(name);
    if (value) headers.set(name, value);
  }
  // Only the owner-defined legacy create interpreter is forwarded; it changes no authorization.
  if (request.method === "POST" && path.length === 4 && path[0] === "v1" && path[1] === "tenants" && path[3] === "channel-accounts") {
    const contract = request.headers.get("x-channel-create-contract");
    if (contract) headers.set("x-channel-create-contract", contract);
  }
  let body: ArrayBuffer | undefined;
  if (!["GET", "HEAD"].includes(request.method)) {
    const artifactUpload = request.method === "PUT" && artifactRoute;
    if (artifactUpload || knowledgeImport) {
      try {
        const bytes = await boundedBody(request, knowledgeImport ? KNOWLEDGE_MAX_REQUEST_BYTES : ARTIFACT_MAX_BYTES);
        if (bytes === null) return Response.json({ error: { code: knowledgeImport ? "KNOWLEDGE_TOO_LARGE" : "ARTIFACT_TOO_LARGE", message: knowledgeImport ? "Knowledge JSON HTTP limit is 2 MiB" : "Artifact HTTP transport limit is 16 MiB" } }, { status: 413, headers: { "cache-control": "no-store" } });
        body = bytes;
      } catch {
        return Response.json({ error: { code: knowledgeImport ? "KNOWLEDGE_BODY_UNAVAILABLE" : "ARTIFACT_BODY_UNAVAILABLE", message: "Request body could not be read" } }, { status: 400, headers: { "cache-control": "no-store" } });
      }
    } else body = await request.arrayBuffer();
  }
  try {
    const response = await fetch(target, { method: request.method, headers, body, cache: "no-store", redirect: "manual", ...((artifactRoute || knowledgeImport) ? { signal: AbortSignal.any([request.signal, AbortSignal.timeout(30_000)]) } : {}) });
    const outgoing = new Headers({ "cache-control": "no-store" });
    const contentType = response.headers.get("content-type");
    if (contentType) outgoing.set("content-type", contentType);
    // Artifact bytes may be HTML/SVG. Direct BFF navigation must not execute them as Console origin.
    if (artifactRoute && request.method === "GET") {
      outgoing.set("content-disposition", "attachment");
      outgoing.set("x-content-type-options", "nosniff");
    }
    const responseHeaders = response.headers as Headers & { getSetCookie?: () => string[] };
    const cookies = responseHeaders.getSetCookie?.() ?? [];
    if (cookies.length) cookies.forEach((cookie) => outgoing.append("set-cookie", cookie));
    else if (response.headers.get("set-cookie")) outgoing.append("set-cookie", response.headers.get("set-cookie")!);
    const resultContract = response.headers.get("x-channel-result-contract");
    if (resultContract) outgoing.set("x-channel-result-contract", resultContract);
    const retryAfter = response.headers.get("retry-after");
    if (retryAfter) outgoing.set("retry-after", retryAfter);
    // An accepted diagnostic job exposes its stable Control resource, not a browser redirect.
    const location = response.headers.get("location");
    if (response.status === 202 && location) outgoing.set("location", location);
    return new Response(response.body, { status: response.status, headers: outgoing });
  } catch {
    return Response.json({ error: { code: "CONTROL_API_UNAVAILABLE", message: "Control API is unavailable" } }, { status: 502, headers: { "cache-control": "no-store" } });
  }
}

export const GET = proxy;
export const POST = proxy;
export const PUT = proxy;
export const PATCH = proxy;
export const DELETE = proxy;
