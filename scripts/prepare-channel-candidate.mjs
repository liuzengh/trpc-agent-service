#!/usr/bin/env node
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { lstat, mkdir, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const manifestPath = join(root, 'scripts/channel-selection.json');
const sha256 = (bytes) => createHash('sha256').update(bytes).digest('hex');

async function main() {
  if (process.argv.length > 3) throw new Error('usage: node scripts/prepare-channel-candidate.mjs [new-directory]');
  const manifest = JSON.parse(await readFile(manifestPath, 'utf8'));
  if (manifest.version !== 1 || !/^[a-f0-9]{40}$/.test(manifest.base)) {
    throw new Error('invalid selection manifest');
  }

  // Validate every source before creating output; never silently pick up new experiments.
  const files = [];
  const seen = new Set();
  for (const item of manifest.files) {
    const path = item.path;
    if (typeof path !== 'string' || !path.startsWith('trpcservice/channels/') ||
        !path.endsWith('.go') || path.includes('..') || isAbsolute(path) || seen.has(path)) {
      throw new Error('invalid or duplicate selection path');
    }
    seen.add(path);
    const source = join(root, path);
    if (!(await lstat(source)).isFile()) throw new Error(`source is not a regular file: ${path}`);
    const bytes = await readFile(source);
    if (sha256(bytes) !== item.sha256) throw new Error(`source changed since verification: ${path}`);
    files.push({ path, bytes });
  }
  const archive = execFileSync('git', ['archive', '--format=tar', manifest.base], {
    cwd: root, maxBuffer: 32 * 1024 * 1024,
  });
  let output;
  if (process.argv[2]) {
    output = resolve(process.argv[2]);
    const location = relative(root, output);
    if (!location || (!location.startsWith(`..${sep}`) && location !== '..' && !isAbsolute(location))) {
      throw new Error('candidate directory must be outside the source repository');
    }
    await mkdir(output); // Existing directories are rejected, including symlinks.
  } else {
    output = await mkdtemp(join(tmpdir(), 'trpc-channel-candidate-'));
  }
  execFileSync('tar', ['-xf', '-', '-C', output], { input: archive, maxBuffer: 1024 * 1024 });
  for (const { path, bytes } of files) {
    const target = join(output, path);
    await mkdir(dirname(target), { recursive: true });
    await writeFile(target, bytes);
  }
  await writeFile(join(output, 'CHANNEL_SELECTION.json'), JSON.stringify(manifest, null, 2) + '\n');
  console.log(JSON.stringify({
    directory: output, base: manifest.base, selectedFiles: files.length,
    selectedLines: manifest.files.reduce((total, item) => total + item.lines, 0),
    scope: manifest.purpose,
  }, null, 2));
}

main().catch((error) => {
  console.error(`prepare-channel-candidate: ${error.message}`);
  process.exitCode = 1;
});
