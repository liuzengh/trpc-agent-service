import fs from 'node:fs';
import path from 'node:path';
import { marked } from 'marked';
import { fileURLToPath } from 'node:url';

const dir = fileURLToPath(new URL('../../docs/user-guide/v1/', import.meta.url));
const markdown = fs.readFileSync(path.join(dir, 'README.md'), 'utf8');
const css = fs.readFileSync(new URL('./guide.css', import.meta.url), 'utf8');
let html = marked.parse(markdown, { gfm: true, breaks: false });
const headings = [];
html = html.replace(/<h1>.*?<\/h1>/, '');
html = html.replace(/<h2>(.*?)<\/h2>/g, (_, title) => {
  const id = `chapter-${headings.length + 1}`;
  headings.push({ id, title });
  return `<h2 id="${id}">${title}</h2>`;
});
html = html.replace(/<table>/g, '<div class="table-scroll" tabindex="0" role="region" aria-label="可横向滚动的数据表"><table>').replace(/<\/table>/g, '</table></div>');
html = html.replace(/<p><img src="([^"]+)" alt="([^"]*)"><\/p>/g, (_, src, alt) => {
  const file = path.resolve(dir, src);
  const mime = path.extname(file) === '.svg' ? 'image/svg+xml' : 'image/jpeg';
  const data = fs.readFileSync(file).toString('base64');
  return `<figure><button type="button" class="zoom" aria-label="放大插图：${alt}" onclick="openFigure(this)"><img src="data:${mime};base64,${data}" alt="${alt}" loading="eager"/></button><figcaption><span>${alt}</span><span class="figure-action">↗ 点击放大</span></figcaption></figure>`;
});
html = html.replace(/<a href="https:\/\//g, '<a target="_blank" rel="noopener" href="https://');
const toc = headings.map((heading, i) => `<a href="#${heading.id}" data-chapter="${heading.id}"><span class="chapter-number">${String(i + 1).padStart(2, '0')}</span><span>${heading.title.replace(/^\d+\.\s*/, '')}</span></a>`).join('\n');
const logo = '<span class="brand-icon"><svg width="23" height="23" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="m21 16-9 5-9-5V6l9-5 9 5z"/></svg></span>Agent <strong>tRPC</strong><span class="version">V1</span>';

export function renderUserGuide({ siteNavigation = false } = {}) {
  const navigation = siteNavigation
    ? `<header class="site-header"><a class="brand" href="/" aria-label="Agent tRPC 主页">${logo}</a><nav class="site-nav" aria-label="主导航"><a href="/">概览</a><a href="/docs/guide.html" aria-current="page">使用教程</a><a href="/docs">文档</a></nav><a class="nav-cta" href="/docs/guide.html#chapter-13">开始部署 <span aria-hidden="true">→</span></a></header>`
    : `<header class="site-header offline-header"><div class="brand">${logo}</div><span class="offline-label">部署与使用指南 · 离线阅读版</span></header>`;
  return `<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="description" content="面向使用者的 Agent tRPC V1 图文指南：26 个页面入口，Agent、运行配置、部署、Telegram 与企业微信接入完整流程。"><meta name="theme-color" content="#fbfcf9"><title>Agent tRPC V1 · 部署与使用指南</title><style>${css}</style></head><body data-guide-theme="ivory-forest"><a class="skip-link" href="#main-content">跳到主要内容</a><div class="site-header-wrap">${navigation}</div><div class="reader-layout"><aside class="sidebar" aria-label="指南目录"><div class="sidebar-title"><span class="book-symbol" aria-hidden="true">▤</span><div><strong>部署与使用指南</strong><small>YOUR FIRST AGENT / V1</small></div></div><div class="nav-label">阅读目录 <span>14 章</span></div><nav class="toc" aria-label="章节导航">${toc}</nav><div class="sidebar-footer"><span>26 个页面入口 · 7 张图解</span><button type="button" onclick="window.print()"><span aria-hidden="true">↗</span> 打印 / 存为 PDF</button></div></aside><main id="main-content"><div class="breadcrumb">${siteNavigation ? '<a href="/docs">文档中心</a>' : '<span>Agent tRPC</span>'}<span aria-hidden="true">/</span><span>部署与使用指南</span></div><header class="cover"><div class="eyebrow">THE FIELD GUIDE / V1</div><h1>部署自己的 <span>Agent</span></h1><p class="cover-subtitle">从第一次配置，到机器人里的第一段对话。</p><p class="cover-description">知道该点哪里、该填什么，也知道每个“成功”究竟代表哪一步。<br />一份面向使用者的完整操作指南。</p><div class="chips"><span>图文教程</span><span>Telegram 长轮询</span><span>企微长连接</span><span>升级与回退</span></div><div class="guide-meta"><span>V1 用户操作版</span><span>更新于 2026.09.07</span></div></header><div class="reading-path"><a href="#chapter-4"><span class="path-label">已有平台账号</span><strong>开始创建我的 Agent <span aria-hidden="true">↗</span></strong></a><a class="secondary" href="#chapter-13"><span class="path-label">准备自行安装</span><strong>先搭好自己的平台 <span aria-hidden="true">↗</span></strong></a></div><details class="mobile-toc"><summary>展开阅读目录 <span>14 章</span></summary><nav class="toc" aria-label="移动端章节导航">${toc}</nav><button type="button" onclick="window.print()">打印 / 存为 PDF</button></details><article class="content">${html}<footer class="article-footer"><div><strong>从一个想法，到一次真实的对话。</strong><span>Agent tRPC V1 · 部署与使用指南</span></div>${siteNavigation ? '<a href="/docs">返回文档中心 →</a>' : '<span>全部插图已内嵌，可离线阅读。</span>'}</footer></article></main></div><dialog id="figure-dialog" aria-label="插图放大阅读"><div class="zoom-toolbar"><span id="zoom-caption"></span><button type="button" onclick="document.getElementById('figure-dialog').close()">关闭插图</button></div><div class="zoom-scroll"><img id="zoom-image" alt=""/></div></dialog><script>
function openFigure(button){const source=button.querySelector('img');const target=document.getElementById('zoom-image');target.src=source.src;target.alt=source.alt;document.getElementById('zoom-caption').textContent=source.alt+' · 可横向滚动查看';document.getElementById('figure-dialog').showModal();}
const chapterLinks=[...document.querySelectorAll('[data-chapter]')];
function selectChapter(id){chapterLinks.forEach(link=>{if(link.dataset.chapter===id)link.setAttribute('aria-current','location');else link.removeAttribute('aria-current');});}
function selectHash(){selectChapter(location.hash.slice(1));}
selectHash();window.addEventListener('hashchange',selectHash);
chapterLinks.forEach(link=>link.addEventListener('click',()=>{selectChapter(link.dataset.chapter);const drawer=link.closest('details');if(drawer)drawer.open=false;}));
if('IntersectionObserver' in window){const visible=new Set();const chapters=[...document.querySelectorAll('.content>h2')];const observer=new IntersectionObserver(entries=>{entries.forEach(entry=>entry.isIntersecting?visible.add(entry.target):visible.delete(entry.target));const first=chapters.find(chapter=>visible.has(chapter));if(first)selectChapter(first.id);},{rootMargin:'-18% 0px -60% 0px',threshold:0});chapters.forEach(chapter=>observer.observe(chapter));}
</script></body></html>`;
}
