import fs from 'node:fs';
import path from 'node:path';
import { JSDOM } from 'jsdom';
import { normalizeBasePath } from '../lib/urls.mjs';
const root = path.resolve('out');
const manifest = JSON.parse(fs.readFileSync(path.join(root, 'docs/manifest.json'), 'utf8'));
const guideUrl = manifest.pages.find(page => page.source === 'docs/user-guide/v1/README.md')?.url;
if (!guideUrl?.endsWith('/docs/guide.html')) throw new Error('Missing guide deployment path');
const basePath = normalizeBasePath(guideUrl.slice(0, -'/docs/guide.html'.length));
const htmlFiles = [];
function walk(dir) {
  for (const e of fs.readdirSync(dir, {withFileTypes:true})) {
    const p=path.join(dir,e.name);
    if(e.isDirectory()) walk(p); else if(p.endsWith('.html')) htmlFiles.push(p);
  }
}
walk(root);
let checked=0;
for(const file of htmlFiles) {
  const pathname=path.relative(root,file).replaceAll(path.sep,'/');
  const url=new URL(`${basePath}/${pathname.replace(/index\.html$/, '')}`, 'https://docs.test');
  const doc=new JSDOM(fs.readFileSync(file,'utf8')).window.document;
  for(const el of doc.querySelectorAll('a[href],script[src],link[href],img[src]')) {
    const value=el.getAttribute('href') ?? el.getAttribute('src');
    if(!value || /^(data:|mailto:)/.test(value)) continue;
    const target=new URL(value,url);
    if(target.origin !== url.origin) continue;
    if(!target.pathname.startsWith(`${basePath}/`)) throw new Error(`Base path escape: ${pathname} -> ${value}`);
    let p=path.join(root, decodeURIComponent(target.pathname.slice(basePath.length)));
    if(fs.existsSync(p) && fs.statSync(p).isDirectory()) p=path.join(p,'index.html');
    if(!fs.existsSync(p)) throw new Error(`Missing target: ${pathname} -> ${value}`);
    if(target.hash && p.endsWith('.html')) {
      const other=p===file?doc:new JSDOM(fs.readFileSync(p,'utf8')).window.document;
      if(!other.getElementById(decodeURIComponent(target.hash.slice(1)))) throw new Error(`Missing anchor: ${pathname} -> ${value}`);
    }
    checked++;
  }
}
if(fs.existsSync(path.join(root,'api')) || fs.existsSync(path.join(root,'console'))) throw new Error('Console routes leaked into site');
console.log(`STATIC_OUTPUT=PASS html=${htmlFiles.length} links_assets=${checked} base=${basePath || '/'}`);
