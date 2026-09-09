async page => {
  // Browser-only protocol fixtures. A separate tab intercepts every /api/v1/
  // request, including POST; this script never changes the lab database.
  // Playwright CLI's run-code sandbox does not expose the Node URL global.
  const parseHTTPURL = value => {
    const parts = String(value).match(/^(https?:\/\/[^/?#]+)([^?#]*)(\?[^#]*)?/);
    if (!parts) throw new Error('Open the HTTP(S) console page before running this script');
    const search = parts[3] || '';
    const decode = part => decodeURIComponent(part.replace(/\+/g, ' '));
    const searchParams = new Map(search.slice(1).split('&').filter(Boolean).map(pair => {
      const split = pair.indexOf('=');
      return split < 0 ? [decode(pair), ''] : [decode(pair.slice(0, split)), decode(pair.slice(split + 1))];
    }));
    return { origin: parts[1], pathname: parts[2] || '/', search, searchParams };
  };
  const consoleURL = parseHTTPURL(page.url()).origin + '/console/';
  const probe = await page.context().newPage();
  const failures = [], posts = [], reads = [];
  let mode = 'good', held = [], notifyHeld = null;
  const tenantID = 'deployment-browser-tenant';
  const appID = 'deployment-browser-app';
  const tenant = { id: tenantID, name: '部署边界验收', status: 'active', configVersion: 1, agents: [], models: [], channels: [], storage: {} };
  const item = (id, appId, status, extra = {}) => ({ id, tenantId: tenantID, appId, status, createdAt: '2026-09-09T00:00:00Z', ...extra });
  const version = (id, number) => item(id, appID, 'published', { versionNumber: number });
  const deployments = () => [
    item(mode === 'changed' ? 'stable-new-deployment' : 'stable-deployment', appID, 'active', { kind: 'stable', versionId: 'version-stable', trafficBps: 10000 }),
    item('canary-deployment', appID, 'active', { kind: 'canary', versionId: 'version-canary', trafficBps: 1500 })
  ];
  const ensure = (condition, message) => { if (!condition) throw new Error(message); };
  const ready = async () => {
    await probe.locator('input[name="stableVersionId"]').waitFor();
    ensure(await probe.locator('input[name="stableVersionId"]').inputValue() === 'version-stable', 'Lost active stable version outside history page');
    ensure(await probe.locator('input[name="canaryVersionId"]').inputValue() === 'version-canary', 'Lost active canary version outside history page');
    ensure(await probe.locator('input[name="canaryPercent"]').inputValue() === '15', 'Lost active canary traffic');
    ensure(await probe.locator('#published-versions option[value="version-newest"]').count() === 1, 'Published version from subsequent page is unavailable');
  };
  const open = async selectedMode => { mode = selectedMode; await probe.getByRole('button', { name: '设置部署', exact: true }).click(); };
  const close = async () => { await probe.locator('#dialog-close').click(); await probe.locator('#action-dialog').waitFor({ state: 'hidden' }); };
  probe.on('pageerror', error => failures.push(String(error)));
  try {
    await probe.route('**/api/v1/**', async route => {
      const request = route.request(), url = parseHTTPURL(request.url());
      const reply = (body, status = 200) => route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) });
      if (request.method() === 'POST') {
        ensure(url.pathname === '/api/v1/deployments', 'Unexpected mutation URL');
        posts.push(request.postDataJSON()); await reply({}, 201); return;
      }
      ensure(request.method() === 'GET', 'Unexpected API method');
      reads.push(url.pathname + url.search);
      if (url.pathname === '/api/v1/operations/me') return reply({ id: 'browser-fixture-admin', role: 'platform_admin', permissions: ['tenant.read', 'agent.deploy'] });
      if (url.pathname === '/api/v1/tenants') return reply([tenant]);
      if (url.pathname === '/api/v1/tenants/' + tenantID) return reply(tenant);
      if (url.pathname === '/api/v1/operations/overview') return reply({ counts: {}, inboxStates: [], executionStates: [], outboxStates: [] });
      if (url.pathname === '/api/v1/operations/apps') return reply({ items: [item(appID, appID, 'active', { name: 'support' })] });
      if (url.pathname === '/api/v1/operations/versions') {
        if (!url.searchParams.has('status')) return reply({ items: [] });
        ensure(url.searchParams.get('status') === 'published' && url.searchParams.get('limit') === '100', 'Published list lacks explicit status or page size');
        return reply(url.searchParams.has('cursor') ? { items: [version('version-newest', 3), version('version-canary', 2), version('version-stable', 1)] } : { items: [item('other-version', 'other-app', 'published', { versionNumber: 1 })], nextCursor: 'versions-page-2' });
      }
      if (url.pathname === '/api/v1/operations/deployments') {
        if (!url.searchParams.has('status')) return reply({ items: [item('old-deployment', appID, 'completed', { kind: 'stable', versionId: 'retired-version', trafficBps: 10000 })] });
        ensure(url.searchParams.get('status') === 'active' && url.searchParams.get('limit') === '100', 'Active list lacks explicit status or page size');
        if (mode === 'unauthorized') return reply({ error: 'unauthorized' }, 401);
        if (mode === 'repeat') return reply({ items: [item('repeat-' + reads.length, 'other-app', 'active', { kind: 'stable', versionId: 'other-version', trafficBps: 10000 })], nextCursor: 'repeated-cursor' });
        if (url.searchParams.has('cursor')) {
          if (mode === 'fail-second') return reply({ error: 'unavailable' }, 503);
          if (mode === 'hold') { await new Promise(resolve => { held.push(resolve); notifyHeld?.(); notifyHeld = null; }); try { return await reply({ items: deployments() }); } catch { return; } }
          if (mode === 'foreign') return reply({ items: [{ ...deployments()[0], tenantId: 'another-tenant' }] });
          return reply({ items: deployments() });
        }
        return reply({ items: [item('other-deployment', 'other-app', 'active', { kind: 'stable', versionId: 'other-version', trafficBps: 10000 })], nextCursor: 'deployments-page-2' });
      }
      throw new Error('Unexpected fixture API: ' + url.pathname);
    });
    await probe.goto(consoleURL);
    await probe.locator('#access-token').fill('browser-only-fixture-token');
    await probe.locator('#login-submit').click();
    await probe.getByRole('button', { name: '应用与发布', exact: true }).click();
    await probe.getByRole('heading', { name: 'Agent 应用', exact: true }).waitFor();

    await open('good'); await ready();
    await probe.locator('input[name="canaryPercent"]').fill('100');
    await probe.locator('#dialog-submit').click();
    ensure(await probe.locator('#dialog-title').textContent() === '设置应用部署' && posts.length === 0, '100 percent crossed browser validation');
    await probe.locator('input[name="canaryPercent"]').fill('99.99');
    await probe.locator('#dialog-submit').click();
    ensure((await probe.locator('.traffic-review').innerText()).includes('0.01%'), 'Stable traffic displays a floating point artifact');
    await probe.locator('#dialog-submit').click();
    await probe.locator('#action-dialog').waitFor({ state: 'hidden' });
    ensure(posts.length === 1 && posts[0].canaryBps === 9999 && posts[0].stableVersionId === 'version-stable', '99.99 percent did not preserve BPS or active stable identity');

    for (const failingMode of ['fail-second', 'repeat', 'foreign']) {
      await open(failingMode);
      await probe.locator('[data-deploy-retry]').waitFor();
      ensure(await probe.locator('input[name="stableVersionId"]').count() === 0, 'Partial or invalid list populated defaults: ' + failingMode);
      ensure(await probe.locator('#dialog-submit').isHidden(), 'Incomplete list permits submit: ' + failingMode);
      mode = 'good'; await probe.locator('[data-deploy-retry]').click(); await ready(); await close();
    }

    await open('good'); await ready();
    await probe.locator('#dialog-submit').click();
    mode = 'changed'; await probe.locator('#dialog-submit').click();
    await probe.locator('#dialog-error').waitFor();
    ensure((await probe.locator('#dialog-error').innerText()).includes('当前部署已发生变化') && posts.length === 1, 'Changed deployment was overwritten');
    await close();

    const holdStarted = new Promise(resolve => { notifyHeld = resolve; });
    await open('hold'); await holdStarted;
    await close(); mode = 'good'; held.splice(0).forEach(resolve => resolve());
    await open('good'); await ready(); await close();

    await open('unauthorized');
    await probe.locator('#login-view').waitFor();
    ensure(await probe.locator('#action-dialog').isHidden(), 'Unauthorized read left tenant dialog open');
    ensure(await probe.locator('#page-content').textContent() === '', 'Unauthorized read retained tenant page content');
    ensure(!failures.length, failures.join('\n'));
    return { passed: true, scenarios: ['complete-active-pagination', 'complete-published-pagination', 'canary-boundaries', 'partial-read-retry', 'repeated-cursor', 'foreign-tenant-response', 'changed-config-preflight', 'close-during-read', 'unauthorized-cleanup'], mockedPosts: posts.length, apiReads: reads.length };
  } finally {
    held.splice(0).forEach(resolve => resolve());
    await probe.close();
  }
}
