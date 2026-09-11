// Extracts design-system CSS blocks from the static design mockups into the
// React project. Run from im-console/: node scripts/extract-css.mjs
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const pages = join(root, '..', 'im-console-redesign.design', 'pages');

function block(file, startMarker, endMarker) {
  const html = readFileSync(join(pages, file), 'utf8');
  const start = html.indexOf(startMarker);
  const end = html.indexOf(endMarker, start);
  if (start < 0 || end < 0) throw new Error(`block not found in ${file}: ${startMarker}`);
  return html.slice(start + startMarker.length, end).trim();
}

// The four mockups share identical theme-vars / component-vars / icon styles.
const shared = [
  block('console.html', '<style id="theme-vars">', '</style>'),
  block('console.html', '<style id="component-vars">', '</style>'),
  '.no-scrollbar::-webkit-scrollbar ' + block('console.html', '.no-scrollbar::-webkit-scrollbar', '</style>'),
].join('\n\n').replace(/;\s*};\s*}/g, '; } }');

// Extract the LAST <style>…</style> block of each page (the page-specific
// styles that follow the body markup in every mockup).
function lastStyleBlock(file) {
  const html = readFileSync(join(pages, file), 'utf8');
  const start = html.lastIndexOf('    <style>');
  const end = html.lastIndexOf('</style>');
  if (start < 0 || end < 0) throw new Error(`page style block not found in ${file}`);
  return html.slice(start + '    <style>'.length, end).trim();
}

const pageStyles = {
  'login.css': lastStyleBlock('login.html'),
  'console.css': lastStyleBlock('console.html'),
  'detail.css': lastStyleBlock('message-detail.html'),
  'monitor.css': lastStyleBlock('monitor.html'),
};

mkdirSync(join(root, 'src', 'styles'), { recursive: true });
writeFileSync(join(root, 'src', 'styles', 'design.css'), shared + '\n');
for (const [name, css] of Object.entries(pageStyles)) {
  writeFileSync(join(root, 'src', 'styles', name), css + '\n');
}
console.log('extracted: design.css +', Object.keys(pageStyles).join(', '));
