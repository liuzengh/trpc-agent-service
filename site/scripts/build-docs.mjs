import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
import { Marked } from "marked";
import { prefixLinks, siteUrl } from "../lib/urls.mjs";
import { renderUserGuide } from "./render-user-guide.mjs";

const check = process.argv.includes("--check");
function writeArtifact(file, content) {
  if (check) {
    if (!fs.existsSync(file) || fs.readFileSync(file, "utf8") !== content) throw new Error(`Generated documentation is stale: ${file}. Run npm run docs:sync.`);
  } else fs.writeFileSync(file, content);
}
const root = fileURLToPath(new URL("../../", import.meta.url));
const output = path.join(root, "site/public/docs");
const source = path.join(root, "docs/site");
const files = ["concepts", "agent", "runtime-profile", "deployment", "channels", "administration"];
const escape = (value) => value.replace(/[&<>"']/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[char]));
const titles = Object.fromEntries(files.map((slug) => [slug, fs.readFileSync(path.join(source, `${slug}.md`), "utf8").match(/^# (.+)$/m)[1]]));
const css = fs.readFileSync(new URL("./reference.css", import.meta.url), "utf8");
if (!check) fs.mkdirSync(path.join(output, "reference"), { recursive: true });
const manifest = { format: 1, pages: [] };
for (const slug of files) {
  const markdown = fs.readFileSync(path.join(source, `${slug}.md`), "utf8");
  let index = 0;
  const toc = [];
  const parser = new Marked({ gfm: true, renderer: {
    html: ({ text }) => escape(text),
    heading({ tokens, depth }) {
      const text = this.parser.parseInline(tokens);
      if (depth === 1) return "";
      const id = `section-${++index}`;
      toc.push(`<a href="#${id}">${text}</a>`);
      return `<h${depth} id="${id}">${text}</h${depth}>`;
    },
  } });
  let body = parser.parse(markdown).replace(/<table>/g, '<div class="table-scroll"><table>').replace(/<\/table>/g, "</table></div>");
  const nav = files.map((item) => `<a href="${item}.html"${item === slug ? ' aria-current="page"' : ""}>${escape(titles[item])}</a>`).join("");
  const html = `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="description" content="Agent tRPC V1 使用参考：${escape(titles[slug])}"><title>${escape(titles[slug])} · Agent tRPC 文档</title><style>${css}</style></head><body><a class="skip" href="#main-content">跳到正文</a><header><a class="brand" href="/">⬡ Agent <strong>tRPC</strong></a><nav aria-label="站点导航"><a href="/docs/guide.html">教程</a><a href="/docs">文档中心</a><a class="console" href="/docs/guide.html#chapter-13">开始部署 ↗</a></nav></header><div class="layout"><aside><a class="back" href="/docs">← 返回文档中心</a><span class="label">REFERENCE / V1</span><nav aria-label="参考文档">${nav}</nav><span class="label">本文目录</span><nav aria-label="本文目录">${toc.join("")}</nav></aside><main id="main-content"><div class="eyebrow">AGENT TRPC / USER REFERENCE</div><h1>${escape(titles[slug])}</h1><p class="meta">V1 · 使用者参考 · 2026.09.07</p><details><summary>文档与本页目录</summary><nav>${nav}${toc.join("")}</nav></details><article>${body}</article><footer><a href="/docs">← 返回文档中心</a><a href="/docs/guide.html">阅读完整图文教程 →</a></footer></main></div></body></html>`;
  writeArtifact(path.join(output, "reference", `${slug}.html`), prefixLinks(html));
  manifest.pages.push({ url: siteUrl(`/docs/reference/${slug}.html`), source: `docs/site/${slug}.md`, sha256: createHash("sha256").update(markdown).digest("hex") });
}
writeArtifact(path.join(output, "guide.html"), prefixLinks(renderUserGuide({ siteNavigation: true })));
manifest.pages.push({ url: siteUrl("/docs/guide.html"), source: "docs/user-guide/v1/README.md", sha256: createHash("sha256").update(fs.readFileSync(path.join(root, "docs/user-guide/v1/README.md"))).digest("hex") });
writeArtifact(path.join(output, "manifest.json"), JSON.stringify(manifest, null, 2) + "\n");

writeArtifact(path.join(root, "docs/user-guide/v1/部署与使用指南.html"), renderUserGuide());

console.log(`${check ? "DOCS_CHECK" : "DOCS_SYNC"}=PASS pages=${manifest.pages.length}`);
