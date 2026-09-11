import fs from "node:fs";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { renderToStaticMarkup } from "react-dom/server";
import { JSDOM } from "jsdom";
import { describe, expect, it } from "vitest";
import Home from "../app/page";
import Docs from "../app/docs/page";
import { docTopics } from "../components/site/doc-topics";
import { normalizeBasePath, siteUrl, basePath } from "../lib/urls.mjs";
const parse = (html: string) => new JSDOM(html).window.document;
const guide = parse(fs.readFileSync("public/docs/guide.html", "utf8"));
describe("independent static documentation site", () => {
  it("keeps the homepage design but guides visitors to deploy their own service", () => {
    const doc = parse(renderToStaticMarkup(<Home />));
    expect(doc.querySelector("h1")?.textContent).toBe("让你的 Agent，真正开始工作。");
    expect(doc.body.textContent).toContain("流程示意 · 非运行状态");
    expect(doc.body.textContent).not.toContain("进入控制台");
    expect(doc.querySelector(`a[href="${siteUrl("/docs/guide.html#chapter-13")}"]`)).not.toBeNull();
  });
  it("renders all six documentation topics", () => {
    const doc = parse(renderToStaticMarkup(<Docs />));
    for(const {slug,title} of docTopics) {
      expect(doc.body.textContent).toContain(title);
      expect(doc.querySelector(`a[href="${siteUrl(`/docs/reference/${slug}.html`)}"]`)).not.toBeNull();
    }
  });
  it("keeps source, generated online pages and offline guide synchronized", () => {
    expect(execFileSync(process.execPath, ["scripts/build-docs.mjs", "--check"], {encoding:"utf8"})).toContain("DOCS_CHECK=PASS pages=7");
  });
  it("retains fourteen chapters, seven embedded figures and homepage colors", () => {
    expect(guide.querySelectorAll(".content>h2").length).toBe(14);
    expect(guide.querySelectorAll('figure img[src^="data:image/"]').length).toBe(7);
    expect(guide.body.dataset.guideTheme).toBe("ivory-forest");
    for(const color of ["#153533","#087f68","#fbfcf9","#dce6e1"]) {
      expect(fs.readFileSync("scripts/guide.css","utf8")).toContain(color);
      expect(fs.readFileSync("components/site/site.module.css","utf8")).toContain(color);
    }
    expect(guide.querySelector('a[aria-current="page"]')?.getAttribute("href")).toBe(siteUrl("/docs/guide.html"));
  });
  it("does not put login, tenant state or console links into the public site", () => {
    const docs = [guide, parse(renderToStaticMarkup(<Home />)), parse(renderToStaticMarkup(<Docs />)), ...docTopics.map(({slug}) => parse(fs.readFileSync(`public/docs/reference/${slug}.html`,"utf8")))];
    for(const doc of docs) for(const a of doc.querySelectorAll("a[href]")) {
      const href=a.getAttribute("href")!;
      expect(href).not.toMatch(/^(?:.*\/)?(?:console|login|tenants|admin)(?:[/?#]|$)/);
      if(href.startsWith("/") && !href.startsWith("//")) expect(href.startsWith(`${basePath}/`)).toBe(true);
    }
    expect(fs.existsSync("app/api")).toBe(false);
  });
  it("documents the direct login and runtime help configuration instead of the old homepage detour", () => {
    expect(guide.body.textContent).not.toContain("在项目主页点击");
    expect(guide.body.textContent).toContain("DOCS_SITE_URL");
    expect(guide.body.textContent).toContain("帮助文档");
    expect(guide.body.textContent).toContain("未登录时会直接进入登录页");
  });
  it("keeps the offline guide self-contained and independent of the deployment base path", () => {
    const offline=parse(fs.readFileSync("../docs/user-guide/v1/部署与使用指南.html","utf8"));
    expect(offline.querySelector(".offline-header")).not.toBeNull();
    expect(offline.querySelectorAll('figure img[src^="data:image/"]').length).toBe(7);
    expect(offline.querySelectorAll('link[rel="stylesheet"],script[src]').length).toBe(0);
  });
  it.each(["bad", "//host", "/../repo", "/repo?x=1"])("rejects ambiguous Pages base path %s", (value) => { expect(()=>normalizeBasePath(value)).toThrow(); });
  it("preserves fragments, external and relative URLs", () => {
    for(const value of ["#chapter-1","https://example.org","agent.html"]) expect(siteUrl(value)).toBe(value);
    expect(normalizeBasePath("/")).toBe(""); expect(normalizeBasePath("/project/")).toBe("/project");
  });
});
