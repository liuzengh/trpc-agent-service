# Agent tRPC public site

Independent static homepage, tutorial and reference docs, preserving the ivory/forest design. No Control API, login, tenant state or Node runtime is required to serve the output. `web/` remains the separate management service.

## Local development

```sh
npm ci
npm run dev -- --hostname 127.0.0.1 --port 13642
```

Run these commands from this directory. Markdown lives in `../docs/site` and `../docs/user-guide/v1`. After editing content run `npm run docs:sync`; `npm run build` and `npm test` do this automatically. `docs:check` verifies exact generated content without writing. The renderer has no file-write side effects when imported.

## Build for a static host

```sh
# Domain root
npm run build
# GitHub project Pages (replace with the actual repository path)
SITE_BASE_PATH=/trpc-agent-service-dev npm run build
npm run verify
```

Upload only `out/`. Homepage, docs navigation, image and framework assets honor the build-time base path. No API route, rewrite, cookie or runtime environment is needed on the host. `out/` and generated `public/docs/` are ignored; the source Markdown and offline HTML are versioned. The offline HTML remains independent of the host/path.

## GitHub Pages

`../.github/workflows/docs-pages.yml` checks pull requests, then builds/deploys on main changes or manual dispatch. It uses the repository name as the project base path and publishes only `site/out`. Configure the repository Pages source as GitHub Actions when ready. For a root custom domain set the workflow base path to an empty string. Hosting visibility and remote activation are separate from local code changes.

## Connect the console Help link

Set the management Web runtime environment `DOCS_SITE_URL` to the complete docs URL, e.g. `https://YOUR_DOMAIN/PROJECT/docs/`, and restart Web. For local previews use `http://127.0.0.1:13642/docs/`. The link opens a separate tab and does not transmit current-route identifiers. A missing setting renders setup instructions instead of assuming a public URL already exists.

## Source map

- `app/`: static homepage and documentation hub.
- `components/site/`: shared public layout/cards and base-aware links.
- `scripts/`: Markdown HTML generation, offline export, styles and output validation.
- `lib/urls.mjs`: shared project-path normalization for JSX and generated HTML.
- `test/`: source-sync, navigation, theme, offline and isolation regression tests.

The public build uses a fixed allowlist of the six user-reference topics and the V1 guide. It does not publish other repository documentation or acceptance artifacts.
