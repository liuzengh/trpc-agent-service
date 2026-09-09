async page => {
  // A real browser loads the served console assets. Every API request in the
  // separate test tab is intercepted; these are browser protocol regressions,
  // not evidence that a real Admin/database/provider accepted the operation.
  const origin = String(page.url()).match(/^https?:\/\/[^/?#]+/)?.[0];
  if (!origin) throw new Error('Open the HTTP(S) console before running this script');
  const probe = await page.context().newPage();
  const errors = [], posts = [], scenarios = [];
  const ensure = (condition, message) => { if (!condition) throw new Error(message); };
  const stamp = '2026-09-09T00:00:00Z';
  const tenantID = 'form-contract-tenant';
  const secretRef = 'env://TRPC_SECRET_BROWSER_MODEL';
  const oneTimeKey = 'browser-fixture-model-key-never-use';
  const agents = [
    { name: 'implicit-limit' },
    { name: 'zero-limit', maxLLMCalls: 0 },
    { name: 'null-limit', maxLLMCalls: null },
    { name: 'explicit-limit', maxLLMCalls: 4 }
  ].map(agent => ({ ...agent, type: 'llm', defaultModel: 'gpt-4o-mini', systemPrompt: 'Handle the request.', tools: ['memory_add'] }));
  const tenant = {
    id: tenantID, name: '表单协议验收', status: 'active', configVersion: 1, agents,
    models: [{ provider: 'openai', modelName: 'gpt-4o-mini', apiKeyRef: secretRef, maxTokens: 1024 }],
    channels: [], storage: { sessionBackend: 'redis', sessionProfile: 'local-redis', memoryBackend: 'postgres', memoryProfile: 'local-postgres' },
    toolPolicy: { mode: 'whitelist', allowed: ['memory_add'] }
  };
  const tenants = [tenant];
  const apps = agents.map(agent => ({ id: 'app-' + agent.name, tenantId: tenantID, name: agent.name, status: 'active', createdAt: stamp }));
  let submittedTenants = 0;
  const openTenant = async () => {
    await probe.locator('#create-tenant').click();
    await probe.locator('input[name="tenantName"]').fill('表单创建验收');
  };
  const closeDialog = async () => {
    await probe.locator('#dialog-cancel').click();
    await probe.locator('#action-dialog').waitFor({ state: 'hidden' });
  };
  const submit = () => probe.locator('#dialog-submit').click();
  const reviewTenant = async () => {
    await submit();
    await probe.locator('select[name="sessionBackend"]').waitFor();
    await submit();
    await probe.locator('textarea[name="advancedTenant"]').waitFor({ state: 'attached' });
    ensure(!(await probe.locator('#dialog-body').innerHTML()).includes(oneTimeKey), 'Model key appeared in review markup');
    ensure(!(await probe.locator('textarea[name="advancedTenant"]').inputValue()).includes(oneTimeKey), 'Model key appeared in advanced JSON');
  };
  const editAdvanced = async edit => {
    await probe.locator('#dialog-body details summary').click();
    const input = probe.locator('textarea[name="advancedTenant"]');
    const body = JSON.parse(await input.inputValue());
    edit(body);
    await input.fill(JSON.stringify(body));
  };
  const expectRejected = async (before, message) => {
    await submit();
    await probe.locator('#dialog-error').waitFor();
    ensure(posts.length === before, message);
    ensure((await probe.locator('#dialog-error').innerText()).length > 0, 'Validation failed without an actionable error');
  };
  const assertCleared = async () => {
    ensure(!(await probe.locator('body').innerHTML()).includes(oneTimeKey), 'Completed/cancelled submission retained key in markup');
    const stored = await probe.evaluate(() => JSON.stringify({ local: { ...localStorage }, session: { ...sessionStorage } }));
    ensure(!stored.includes(oneTimeKey) && !stored.includes('browser-only-form-fixture-token'), 'Console persisted credentials in browser storage');
    await openTenant();
    ensure(await probe.locator('input[name="modelApiKey"]').inputValue() === '', 'Reopened form retained model key');
    await expectRejected(posts.length, 'Reopened form reused a previous key');
    await closeDialog();
  };
  probe.on('pageerror', error => errors.push(String(error)));
  try {
    await probe.route('**/api/v1/**', async route => {
      const request = route.request();
      const path = String(request.url()).replace(/^https?:\/\/[^/]+/, '').split('?')[0];
      const reply = (body, status = 200) => route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) });
      if (request.method() === 'POST') {
        const body = request.postDataJSON();
        posts.push({ path, body });
        if (path === '/api/v1/tenants') {
          submittedTenants += 1;
          const created = { ...body.config, id: 'form-created-' + submittedTenants, name: body.name, status: 'active', configVersion: 1, createdAt: stamp };
          // Mirror the Admin read contract: the write response is redacted.
          created.models = created.models.map(model => ({ ...model, apiKey: model.apiKey ? '***REDACTED***' : '' }));
          tenants.push(created);
          return reply(created, 201);
        }
        if (path === '/api/v1/agent-versions') return reply({ id: 'form-version-' + posts.length, status: 'draft' }, 201);
        throw new Error('Unexpected mutation URL: ' + path);
      }
      ensure(request.method() === 'GET', 'Unexpected API method');
      if (path === '/api/v1/operations/me') return reply({ id: 'browser-fixture-admin', role: 'platform_admin', permissions: ['tenant.read', 'tenant.create', 'agent.write'] });
      if (path === '/api/v1/tenants') return reply(tenants);
      if (path.startsWith('/api/v1/tenants/')) {
        const found = tenants.find(item => item.id === path.slice('/api/v1/tenants/'.length));
        ensure(found, 'Unknown tenant read'); return reply(found);
      }
      if (path === '/api/v1/operations/overview') return reply({ counts: {}, inboxStates: [], executionStates: [], outboxStates: [] });
      if (path === '/api/v1/operations/apps') return reply({ items: request.url().includes('tenantId=' + tenantID) ? apps : [] });
      if (path === '/api/v1/operations/versions' || path === '/api/v1/operations/deployments') return reply({ items: [] });
      throw new Error('Unexpected fixture API: ' + path);
    });
    await probe.goto(origin + '/console/');
    await probe.locator('#access-token').fill('browser-only-form-fixture-token');
    await probe.locator('#login-submit').click();
    await probe.locator('#workspace').waitFor();

    await openTenant();
    await expectRejected(0, 'Missing credentials reached the API');
    ensure(await probe.locator('input[name="tenantName"]').isVisible(), 'Missing credentials advanced beyond the first step');
    scenarios.push('missing-credentials-blocked');
    await probe.locator('input[name="modelApiKey"]').fill('   ');
    await expectRejected(0, 'Whitespace-only key reached the API');
    scenarios.push('whitespace-credentials-blocked');
    await probe.locator('input[name="modelApiKey"]').fill(oneTimeKey);
    await probe.locator('input[name="secretRef"]').fill(secretRef);
    await expectRejected(0, 'Two credential sources reached the API');
    scenarios.push('mutually-exclusive-credentials');
    await closeDialog();
    await assertCleared();
    scenarios.push('cancel-clears-credentials');

    await openTenant();
    await probe.locator('input[name="secretRef"]').fill(secretRef);
    await reviewTenant();
    await editAdvanced(body => { delete body.config.models[0].apiKeyRef; });
    await expectRejected(0, 'Advanced JSON removed the only credential source and reached the API');
    await closeDialog();
    scenarios.push('advanced-json-missing-credentials');

    await openTenant();
    await probe.locator('input[name="modelApiKey"]').fill(oneTimeKey);
    await reviewTenant();
    await editAdvanced(body => { body.config.models.push({ provider: 'openai', modelName: 'gpt-4o', maxTokens: 1024 }); });
    await expectRejected(0, 'An additional credential-free model reached the API');
    const advanced = probe.locator('textarea[name="advancedTenant"]');
    const repaired = JSON.parse(await advanced.inputValue());
    repaired.config.models.pop();
    await advanced.fill(JSON.stringify(repaired));
    await submit();
    await probe.locator('#action-dialog').waitFor({ state: 'hidden' });
    ensure(posts.length === 1 && posts[0].path === '/api/v1/tenants', 'Corrected key-based tenant did not submit exactly once');
    ensure(posts[0].body.config.models[0].apiKey === oneTimeKey && !posts[0].body.config.models[0].apiKeyRef, 'One-time key was not submitted with exactly one source');
    scenarios.push('all-models-validated-and-key-retry');
    await assertCleared();
    scenarios.push('successful-key-submit-clears-credentials');

    await openTenant();
    await probe.locator('input[name="secretRef"]').fill(secretRef);
    await reviewTenant();
    await submit();
    await probe.locator('#action-dialog').waitFor({ state: 'hidden' });
    ensure(posts.length === 2 && posts[1].body.config.models[0].apiKeyRef === secretRef && !posts[1].body.config.models[0].apiKey, 'SecretRef-only tenant submission changed its credential source');
    await assertCleared();
    scenarios.push('secretref-submit-and-cleanup');

    await probe.locator('#tenant-select').selectOption(tenantID);
    await probe.getByRole('button', { name: '应用与发布', exact: true }).click();
    await probe.getByRole('heading', { name: 'Agent 应用', exact: true }).waitFor();
    for (const [name, expected] of [['implicit-limit', 8], ['zero-limit', 8], ['null-limit', 8], ['explicit-limit', 4]]) {
      await probe.locator('[data-action="version"][data-app="app-' + name + '"]').click();
      ensure(await probe.locator('input[name="maxLLMCalls"]').inputValue() === String(expected), 'Wrong effective model call limit for ' + name);
      await submit();
      await probe.locator('#action-dialog').waitFor({ state: 'hidden' });
      const sent = posts[posts.length - 1];
      ensure(sent.path === '/api/v1/agent-versions' && sent.body.tenantId === tenantID && sent.body.appName === name, 'Version bound to the wrong tenant or app');
      ensure(sent.body.snapshot.agent.maxLLMCalls === expected && JSON.stringify(sent.body.snapshot.agent.tools) === '["memory_add"]', 'Version lost effective call limit or tool capability for ' + name);
      ensure(sent.body.snapshot.model.apiKeyRef === secretRef && !sent.body.snapshot.model.apiKey, 'Version model credential binding changed');
      scenarios.push('version-limit-' + name);
    }
    await probe.locator('#logout').click();
    ensure(await probe.locator('#page-content').textContent() === '', 'Logout retained tenant content');
    ensure(!(await probe.locator('body').innerHTML()).includes(oneTimeKey), 'Logout retained the model key');
    scenarios.push('logout-clears-tenant-and-credentials');
    ensure(!errors.length, errors.join('\n'));
    return { passed: true, scenarios, mockedPosts: posts.length, realAPIMutations: 0 };
  } finally {
    await probe.close();
  }
}
