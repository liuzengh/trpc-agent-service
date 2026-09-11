import { afterEach, describe, expect, it, vi } from "vitest";
import { GET } from "./route";
afterEach(() => vi.unstubAllEnvs());
describe("runtime-configured help destination", () => {
  it.each(["https://docs.example.org/project/docs/", "http://127.0.0.1:13642/docs/"])("redirects to the exact configured URL %s", (url) => {
    vi.stubEnv("DOCS_SITE_URL", url); const response = GET();
    expect(response.status).toBe(307); expect(response.headers.get("location")).toBe(url);
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(response.headers.get("referrer-policy")).toBe("no-referrer");
  });
  it.each(["", "javascript:alert(1)", "//evil.example", "/docs", "https://user:pass@example.org", "https://docs.example/?token=secret", "https://docs.example/#fragment"])("shows actionable configuration state for %s", async (url) => {
    vi.stubEnv("DOCS_SITE_URL", url); const response = GET();
    expect(response.status).toBe(503); expect(response.headers.get("location")).toBeNull();
    const body = await response.text(); expect(body).toContain("DOCS_SITE_URL");
    if(url) expect(body).not.toContain(url);
  });
  it("reads config at request time instead of freezing it at build time", () => {
    vi.stubEnv("DOCS_SITE_URL", "https://a.example/docs/"); expect(GET().headers.get("location")).toContain("a.example");
    vi.stubEnv("DOCS_SITE_URL", "https://b.example/docs/"); expect(GET().headers.get("location")).toContain("b.example");
  });
});
