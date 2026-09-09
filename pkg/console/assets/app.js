(() => {
  'use strict';

  // The credential deliberately lives only in this closure. It is never part
  // of a URL, browser storage, generated markup, or a diagnostic message.
  let accessToken = '';
  let modelCredential = '';
  let generation = 0;
  let readController = null;
  let toastTimer = null;
  const requests = new Set();
  const state = { me: null, tenants: [], tenantId: '', page: 'overview', requestTab: 'inbox', requestId: '', status: '', pages: {}, data: {}, dialog: null, busy: false };
  const $ = id => document.getElementById(id);
  const esc = value => String(value ?? '').replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));
  const text = value => value === null || value === undefined || value === '' ? '—' : String(value);
  const list = value => Array.isArray(value) ? value : [];
  const selectedTenant = () => state.tenants.find(item => item.id === state.tenantId);
  const allowed = permission => list(state.me?.permissions).includes(permission);
  const roles = { platform_admin: '平台管理员', tenant_admin: '租户管理员', release_manager: '发布管理员', auditor: '审计员' };
  const pageMeta = {
    overview: ['运行概览', '当前租户的应用、执行与投递状态。'],
    apps: ['应用与发布', '管理应用、版本发布与部署流量。'],
    backends: ['后端组合', '各数据域的后端类型与 Profile 绑定。'],
    requests: ['请求追踪', '查询接入、执行与投递记录。'],
    audit: ['操作审计', '查询控制面变更对象、操作者与操作时间。'],
    migrations: ['数据迁移', '迁移阶段与暂停状态。迁移执行由运维流程推进。']
  };
  const statusNames = {
    active: '运行中', suspended: '已暂停', deleted: '已删除', draft: '草稿',
    published: '已发布', retired: '已退役', RECEIVED: '已接入',
    PROCESSING: '处理中', RUNNING: '执行中', SUCCEEDED: '已成功',
    COMPLETED: '已完成', FAILED: '失败', DEAD_LETTERED: '死信',
    RETRY_WAIT: '等待重试', WAITING_APPROVAL: '等待审批',
    WAITING_RECONCILIATION: '待核对', REPLY_PENDING: '等待投递',
    DELIVERING: '投递中', DISPATCH_STARTED: '已开始外部投递', REPLIED: '已投递',
    ABANDONED: '已放弃', PREPARE: '准备', SNAPSHOT_COPY: '全量复制',
    DUAL_WRITE: '双写', CATCH_UP: '增量追平', VALIDATE: '校验',
    READ_SHADOW: '影子读', CUTOVER: '切换', ROLLBACK_WINDOW: '回滚观察期',
    COMPLETE: '已完成', ROLLED_BACK: '已回滚'
  };
  function badge(status) {
    const key = String(status ?? '');
    const upper = key.toUpperCase();
    const tone = /UNKNOWN|RECONCIL|RETRY|APPROVAL|DISPATCH_STARTED|ROLLBACK_WINDOW/.test(upper) ? 'warning' : /FAILED|DEAD|CANCEL|ABANDON|EXPIRED/.test(upper) ? 'bad' : /^(ACTIVE|SUCCEEDED|COMPLETED|REPLIED|PUBLISHED|COMPLETE)$/.test(upper) ? 'good' : /PROCESSING|RUNNING|DELIVERING|DUAL_WRITE|CATCH_UP|CUTOVER/.test(upper) ? 'progress' : 'neutral';
    return `<span class="badge ${tone}" title="${esc(key)}">${esc(statusNames[key] || statusNames[upper] || text(key))}</span>`;
  }
  function formatTime(value) {
    if (!value) return '—';
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? '—' : new Intl.DateTimeFormat('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false }).format(date);
  }
  function fullTime(value) { const date = new Date(value); return value && !Number.isNaN(date.getTime()) ? date.toLocaleString('zh-CN', { hour12: false }) : '—'; }
  function icon(name) {
    const paths = {
      session: '<rect x="3" y="4" width="18" height="14" rx="3"/><path d="m7 18-2 3v-3M7 9h10M7 13h6"/>',
      memory: '<ellipse cx="12" cy="5" rx="8" ry="3"/><path d="M4 5v14c0 4 16 4 16 0V5M4 12c0 4 16 4 16 0"/>',
      knowledge: '<circle cx="7" cy="6" r="3"/><circle cx="17" cy="17" r="3"/><circle cx="5" cy="19" r="2"/><path d="m9 8 6 7M7 9 5 17m2 2 7-2"/>',
      artifact: '<path d="m12 3 9 5-9 5-9-5 9-5ZM3 8v9l9 5 9-5V8M12 13v9"/>',
      copy: '<rect x="8" y="8" width="12" height="13" rx="2"/><path d="M15 5V3H3v13h2"/>',
      empty: '<path d="M5 4h14v16H5zM9 9h6M9 13h4"/>'
    };
    return `<svg class="ui-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${paths[name] || paths.empty}</svg>`;
  }
  function idCell(value, short = false) {
    if (value === null || value === undefined || value === '') return '<span class="muted">未提供</span>';
    const id = String(value);
    return `<button type="button" class="id-button" data-copy="${esc(id)}" aria-label="复制 ID ${esc(id)}" title="${esc(id)}">${esc(short && id.length > 24 ? id.slice(0, 12) + '…' + id.slice(-6) : id)}${icon('copy')}</button>`;
  }
  function empty(title, description, action = '') { return `<div class="empty-state"><span class="empty-symbol" aria-hidden="true">${icon('empty')}</span><h3>${esc(title)}</h3><p>${esc(description)}</p>${action}</div>`; }
  const loading = () => '<div class="loading-state" role="status"><span class="spinner" aria-hidden="true"></span>正在读取服务端记录…</div>';
  function table(headers, rows, title = '暂无记录', description = '当前租户尚未产生这类记录。') {
    if (!rows.length) return empty(title, description);
    return `<div class="table-wrap"><table><thead><tr>${headers.map(label => `<th scope="col">${esc(label)}</th>`).join('')}</tr></thead><tbody>${rows.map(row => `<tr>${row.map(cell => `<td>${cell}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;
  }
  function panel(title, body, actions = '', subtitle = '') { return `<section class="panel"><header class="panel-header"><div><h2>${esc(title)}</h2>${subtitle ? `<p>${esc(subtitle)}</p>` : ''}</div>${actions ? `<div class="panel-actions">${actions}</div>` : ''}</header>${body}</section>`; }
  function note(value, warning = false) { return `<div class="notice${warning ? ' warning' : ''}">${esc(value)}</div>`; }
  function actionButton(action, label, permission, attributes = '') { return allowed(permission) ? `<button class="btn small" type="button" data-action="${action}" ${attributes}>${esc(label)}</button>` : ''; }
  function pageState(resource) { return state.pages[resource] || (state.pages[resource] = { cursors: [''], index: 0 }); }
  function pager(resource, response) {
    const page = pageState(resource);
    return `<div class="pagination"><span>第 ${page.index + 1} 页 · 本页 ${list(response?.items).length} 条</span><div><button class="btn small" data-page-back="${resource}" type="button" ${page.index === 0 ? 'disabled' : ''}>上一页</button><button class="btn small" data-page-next="${resource}" data-cursor="${esc(response?.nextCursor || '')}" type="button" ${response?.nextCursor ? '' : 'disabled'}>下一页</button></div></div>`;
  }
  function endpoint(resource, extra = {}) {
    const params = new URLSearchParams({ tenantId: state.tenantId, ...extra });
    return `/api/v1/operations/${resource}?${params}`;
  }
  function listEndpoint(resource) {
    const page = pageState(resource);
    const extra = { limit: '50' };
    if (page.cursors[page.index]) extra.cursor = page.cursors[page.index];
    if (state.page === 'requests' && state.status) extra.status = state.status;
    return endpoint(resource, extra);
  }
  class APIError extends Error {
    constructor(status) { super('Request failed'); this.status = status; }
  }
  function errorMessage(error, write = false) {
    if (error instanceof UIError) return error.message;
    switch (error.status) {
      case 400: case 422: return write ? '配置未通过准入校验。请核对应用名称、模型与凭据绑定、Profile 授权和版本内容。' : '查询参数未被接受，请清除筛选后重试。';
      case 401: return '访问令牌无效或已过期，请重新登录。';
      case 403: return '当前身份没有此租户或操作的权限，请联系管理员调整授权。';
      case 404: return '记录不存在或接口尚未部署，请刷新列表后重试。';
      case 409: return '记录已存在或状态发生冲突。请刷新并核对当前版本后重试。';
      case 413: return '配置内容过大，请精简后再提交。';
      case 429: return '请求过于频繁，请稍后重试。';
      case 500: case 502: case 503: case 504: return '服务暂时不可用，请检查后端健康状态后重试。';
      default: return write ? '未能确认操作结果。请先刷新并核对服务端记录，避免重复提交。' : '无法读取数据，请检查连接后重试。';
    }
  }
  class UIError extends Error {}
  async function api(path, options = {}) {
    if (!path.startsWith('/api/v1/')) throw new UIError('请求路径无效。');
    const requestToken = accessToken;
    const controller = new AbortController();
    const parentSignal = options.signal;
    const abort = () => controller.abort();
    if (parentSignal?.aborted) controller.abort();
    parentSignal?.addEventListener('abort', abort, { once: true });
    requests.add(controller);
    let timedOut = false;
    const timer = setTimeout(() => { timedOut = true; controller.abort(); }, options.method === 'POST' ? 30000 : 15000);
    try {
      const response = await fetch(path, { method: options.method || 'GET', headers: { Authorization: `Bearer ${requestToken}`, Accept: 'application/json', ...(options.body ? { 'Content-Type': 'application/json' } : {}) }, body: options.body ? JSON.stringify(options.body) : undefined, signal: controller.signal, cache: 'no-store', credentials: 'omit', redirect: 'error' });
      if (!response.ok) {
        // Never render raw server errors: database/provider diagnostics can
        // contain credentials even when this particular API normally redacts.
        if (response.status === 401 && accessToken === requestToken && state.me) logout('访问令牌无效或已过期，请重新登录。');
        throw new APIError(response.status);
      }
      if (response.status === 204) return null;
      return await response.json();
    } catch (error) {
      if (timedOut) throw new APIError(0);
      throw error;
    } finally {
      clearTimeout(timer);
      requests.delete(controller);
      parentSignal?.removeEventListener('abort', abort);
    }
  }
  function abortReads() { generation += 1; readController?.abort(); readController = new AbortController(); return generation; }
  function toast(message) { clearTimeout(toastTimer); $('toast').textContent = message; $('toast').hidden = false; toastTimer = setTimeout(() => { $('toast').hidden = true; }, 4200); }
  function cleanTenant(value) {
    // Keep only fields the UI needs. In particular, discard any inline model
    // or channel credentials even if an older server accidentally returns one.
    return { id: String(value.id), name: String(value.name || value.id), status: value.status, configVersion: value.configVersion, createdAt: value.createdAt, updatedAt: value.updatedAt, storage: { sessionBackend: value.storage?.sessionBackend, sessionProfile: value.storage?.sessionProfile, memoryBackend: value.storage?.memoryBackend, memoryProfile: value.storage?.memoryProfile, knowledgeBackend: value.storage?.knowledgeBackend, knowledgeProfile: value.storage?.knowledgeProfile, artifactBackend: value.storage?.artifactBackend, artifactProfile: value.storage?.artifactProfile }, agents: list(value.agents).map(a => ({ name: a.name, type: a.type, systemPrompt: a.systemPrompt, defaultModel: a.defaultModel, maxLLMCalls: a.maxLLMCalls, tools: list(a.tools), runtime: a.runtime })), models: list(value.models).map(m => ({ provider: m.provider, modelName: m.modelName, apiKeyRef: m.apiKeyRef || '', maxTokens: m.maxTokens, temperature: m.temperature })), channels: list(value.channels).map(c => ({ type: c.type, accountId: c.accountId, agentApp: c.agentApp })), toolPolicy: { mode: value.toolPolicy?.mode, allowed: list(value.toolPolicy?.allowed) } };
  }
  function updateShell() {
    $('tenant-select').innerHTML = state.tenants.length ? state.tenants.map(item => `<option value="${esc(item.id)}" ${item.id === state.tenantId ? 'selected' : ''}>${esc(item.name)}</option>`).join('') : '<option value="">暂无授权租户</option>';
    $('scope-name').textContent = selectedTenant()?.name || '尚未选择租户';
    $('principal-role').textContent = roles[state.me?.role] || '已授权身份';
    $('principal-id').textContent = state.me?.id || '';
    $('principal-id').title = state.me?.id || '';
    $('create-tenant').hidden = !allowed('tenant.create');
    $('tenant-select').disabled = !state.tenants.length || state.busy;
  }
  function logout(message = '') {
    abortReads(); requests.forEach(controller => controller.abort()); requests.clear(); accessToken = ''; modelCredential = '';
    state.me = null; state.tenants = []; state.tenantId = ''; state.data = {}; state.pages = {}; state.requestId = ''; state.requestTab = 'inbox'; state.page = 'overview'; state.status = ''; state.busy = false; state.dialog = null;
    setBusy(false);
    $('sidebar').classList.remove('open'); $('menu-toggle').setAttribute('aria-expanded', 'false');
    $('action-dialog').close(); $('dialog-body').replaceChildren(); $('action-form').reset(); $('page-content').replaceChildren(); $('tenant-select').replaceChildren(); $('scope-name').textContent = ''; $('principal-id').textContent = ''; $('principal-id').removeAttribute('title'); $('principal-role').textContent = ''; $('updated-at').textContent = '';
    $('login-form').reset(); $('login-submit').disabled = false; $('login-submit').textContent = '进入工作区'; $('workspace').hidden = true; $('login-view').hidden = false; $('toast').hidden = true;
    $('login-error').textContent = message; $('login-error').hidden = !message; $('access-token').focus();
  }
  $('login-form').addEventListener('submit', async event => {
    event.preventDefault();
    const token = $('access-token').value.trim().replace(/^Bearer\s+/i, '');
    $('access-token').value = '';
    if (!token || /[\r\n]/.test(token)) return;
    const current = abortReads(); accessToken = token;
    $('login-submit').disabled = true; $('login-submit').textContent = '正在验证身份…'; $('login-error').hidden = true;
    try {
      const me = await api('/api/v1/operations/me', { signal: readController.signal });
      const tenants = await api('/api/v1/tenants', { signal: readController.signal });
      if (current !== generation) return;
      if (!me?.id || !Array.isArray(tenants)) throw new UIError('服务响应不完整，请检查操作台 API 是否已部署。');
      state.me = me; state.tenants = tenants.map(cleanTenant); state.tenantId = state.tenants[0]?.id || '';
      $('login-view').hidden = true; $('workspace').hidden = false; updateShell(); await loadPage(); $('main').focus();
    } catch (error) {
      if (current === generation && error.name !== 'AbortError') { accessToken = ''; $('login-error').textContent = errorMessage(error); $('login-error').hidden = false; }
    } finally { if (current <= generation) { $('login-submit').disabled = false; $('login-submit').textContent = '进入工作区'; } }
  });
  $('logout').addEventListener('click', () => logout());
  $('tenant-select').addEventListener('change', event => { state.tenantId = event.target.value; state.pages = {}; state.status = ''; state.requestId = ''; state.data = {}; closeDialog(); updateShell(); loadPage(); });
  $('navigation').addEventListener('click', event => { const button = event.target.closest('[data-page]'); if (button && !state.busy) navigate(button.dataset.page); });
  $('refresh').addEventListener('click', () => { if (!state.busy) loadPage(); });
  $('menu-toggle').addEventListener('click', () => { const open = $('sidebar').classList.toggle('open'); $('menu-toggle').setAttribute('aria-expanded', String(open)); });
  document.addEventListener('keydown', event => { if (event.key === 'Escape') { $('sidebar').classList.remove('open'); $('menu-toggle').setAttribute('aria-expanded', 'false'); } });
  function navigate(page) { state.page = page; state.requestId = ''; state.pages = {}; state.status = ''; $('sidebar').classList.remove('open'); $('menu-toggle').setAttribute('aria-expanded', 'false'); closeDialog(); loadPage(); $('main').focus(); }
  async function loadPage() {
    if (!accessToken || !state.me) return;
    const current = abortReads(); const [title, description] = pageMeta[state.page];
    $('page-title').textContent = title; $('breadcrumb-title').textContent = title; $('page-description').textContent = description;
    document.querySelectorAll('[data-page]').forEach(button => { const selected = button.dataset.page === state.page; button.classList.toggle('active', selected); if (selected) button.setAttribute('aria-current', 'page'); else button.removeAttribute('aria-current'); });
    $('updated-at').textContent = ''; $('page-content').setAttribute('aria-busy', 'true'); $('page-content').innerHTML = loading();
    if (!state.tenantId) { $('page-content').innerHTML = empty('从第一个租户开始', '创建租户后，即可管理应用发布、数据后端和执行记录。', allowed('tenant.create') ? '<button class="btn primary" type="button" data-action="tenant">＋ 创建租户</button>' : ''); $('page-content').setAttribute('aria-busy', 'false'); return; }
    try {
      // Refresh tenant configuration with each page request; a changed backend
      // selection must not be presented as current from a prior login snapshot.
      const tenantResult = await api(`/api/v1/tenants/${encodeURIComponent(state.tenantId)}`, { signal: readController.signal });
      if (current !== generation) return;
      const refreshed = cleanTenant(tenantResult); state.tenants = state.tenants.map(t => t.id === refreshed.id ? refreshed : t); updateShell();
      const fetchList = resource => api(listEndpoint(resource), { signal: readController.signal });
      let html = '';
      if (state.page === 'overview') { state.data.overview = await api(endpoint('overview'), { signal: readController.signal }); html = renderOverview(state.data.overview); }
      else if (state.page === 'backends') html = renderBackends();
      else if (state.page === 'apps') {
        const responses = await Promise.all(['apps', 'versions', 'deployments'].map(fetchList));
        if (current !== generation) return;
        ['apps', 'versions', 'deployments'].forEach((key, index) => { state.data[key] = responses[index]; }); html = renderApps();
      } else if (state.page === 'requests' && state.requestId) {
        const detail = await api(endpoint(`requests/${encodeURIComponent(state.requestId)}`), { signal: readController.signal }); html = renderRequestDetail(detail);
      } else {
        const resource = state.page === 'requests' ? state.requestTab : state.page;
        const response = await fetchList(resource);
        if (current !== generation) return;
        state.data[resource] = response; html = state.page === 'requests' ? renderRequests(response) : state.page === 'audit' ? renderAudit(response) : renderMigrations(response);
      }
      if (current !== generation) return;
      $('page-content').innerHTML = html; $('updated-at').textContent = `更新于 ${formatTime(new Date())}`;
    } catch (error) {
      if (current !== generation || error.name === 'AbortError') return;
      $('page-content').innerHTML = `<div class="panel error-state"><span class="empty-symbol" aria-hidden="true">!</span><h3>数据未能加载</h3><p>${esc(errorMessage(error))}</p><button class="btn" type="button" data-action="retry">重新读取</button></div>`;
    } finally { if (current === generation) $('page-content').setAttribute('aria-busy', 'false'); }
  }
  function tenantRibbon() {
    const tenant = selectedTenant();
    const storage = tenant.storage;
    return `<section class="tenant-ribbon" aria-label="租户工作区"><div><h2>${esc(tenant.name)}</h2><p>${esc(tenant.models.map(model => model.provider + ' / ' + model.modelName).join(' · ') || '尚未配置模型')}</p></div><dl class="ribbon-context"><div><dt>Agent 配置</dt><dd>${esc(tenant.agents.length)}</dd></div><div><dt>配置版本</dt><dd>${esc(tenant.configVersion ?? '—')}</dd></div><div><dt>Session</dt><dd>${esc(storage.sessionBackend || '未配置')}</dd></div><div><dt>Memory</dt><dd>${esc(storage.memoryBackend || '未配置')}</dd></div></dl></section>`;
  }
  function storageMap(domains, storage) {
    const tenant = selectedTenant();
    return `<section class="panel storage-map" aria-label="租户数据域路由"><header class="panel-header"><div><h2>数据后端绑定</h2><p>${esc(tenant.name)} · 配置版本 ${esc(tenant.configVersion ?? '—')}</p></div><span class="storage-scope">${idCell(tenant.id, true)}</span></header><div class="storage-columns" aria-hidden="true"><span>数据域</span><span>存储引擎 / Profile</span><span>数据职责与作用域</span><span>绑定状态</span></div><ol>${domains.map(([key, name, chinese, description]) => { const configured = Boolean(storage[key + 'Backend']); return `<li class="storage-row"><div class="storage-domain"><span class="domain-icon">${icon(key)}</span><div><h3>${name}</h3><span>${chinese}</span></div></div><div class="storage-binding"><strong class="${configured ? '' : 'unconfigured'}">${esc(storage[key + 'Backend'] || '未配置')}</strong><code>${esc(storage[key + 'Profile'] || '未指定 Profile')}</code></div><p class="storage-description">${description}</p><span class="binding-state${configured ? ' configured' : ''}">${configured ? '已绑定' : '未绑定'}</span></li>`; }).join('')}</ol><footer class="storage-note">绑定信息来自已保存的租户配置。后端运行状态请结合监控指标与请求记录查看。</footer></section>`;
  }
  function renderOverview(data) {
    const counts = data?.counts || {};
    const metrics = [['apps', 'Agent 应用', '已创建应用'], ['versions', '应用版本', '草稿与发布版本'], ['deployments', '部署记录', '全部部署历史'], ['inbox', '接入请求', 'Inbox 记录'], ['executions', '执行记录', '包含重试执行'], ['outbox', '投递记录', 'Outbox 记录']];
    const cards = `<div class="stats-grid">${metrics.map(([key, name, foot]) => `<article class="stat-card"><span class="stat-label">${name}</span><strong class="stat-number">${esc(counts[key] ?? '—')}</strong><span class="stat-foot">${foot}</span></article>`).join('')}</div>`;
    const states = [['接入', 'inboxStates', 'Inbox'], ['执行', 'executionStates', 'Execution'], ['投递', 'outboxStates', 'Outbox']].map(([name, key, label]) => `<section class="state-column"><h3>${name}<span>${label}</span></h3>${list(data?.[key]).length ? `<ul class="states-list">${data[key].map(item => `<li>${badge(item.status)}<strong>${esc(item.count)}</strong></li>`).join('')}</ul>` : '<p class="state-empty">暂无记录</p>'}</section>`).join('');
    const tenant = selectedTenant();
    return tenantRibbon() + cards + panel('请求状态', `<div class="state-columns">${states}</div>`, '', '按持久化记录汇总') + panel('租户配置', `<div class="panel-body"><dl class="definition-list"><dt>租户名称</dt><dd>${esc(tenant.name)} ${badge(tenant.status)}</dd><dt>租户 ID</dt><dd>${idCell(tenant.id)}</dd><dt>配置版本</dt><dd>${esc(tenant.configVersion ?? '—')}</dd><dt>已配置通道</dt><dd>${tenant.channels.length ? tenant.channels.map(channel => `<span class="badge">${esc(channel.type)}</span>`).join(' ') : '尚未配置 IM 通道'}</dd><dt>最后配置变更</dt><dd>${esc(fullTime(tenant.updatedAt))}</dd></dl></div>`, '<button class="btn small" data-action="backends" type="button">查看后端组合</button>');
  }
  function renderBackends() {
    const storage = selectedTenant()?.storage || {};
    const domains = [['session', 'Session', '会话', '保存会话事件与状态。每个租户独立选择后端和授权 Profile。'], ['memory', 'Memory', '长期记忆', '保存跨会话记忆。它与 Session 的后端选择相互独立。'], ['knowledge', 'Knowledge', '知识检索', '使用所选向量检索 Profile；检索仍受租户与应用作用域约束。'], ['artifact', 'Artifact', '产物存储', '使用所选对象存储 Profile；版本和生命周期元数据由平台管理。']];
    return storageMap(domains, storage);
  }
  function appName(id) { return list(state.data.apps?.items).find(item => item.id === id)?.name || id; }
  function renderApps() {
    const apps = state.data.apps || {}; const versions = state.data.versions || {}; const deployments = state.data.deployments || {};
    const appRows = list(apps.items).map(item => [`<span class="cell-primary">${esc(item.name)}</span><span class="cell-secondary">${idCell(item.id, true)}</span>`, badge(item.status), esc(formatTime(item.createdAt)), `<div class="panel-actions">${actionButton('version', '新建版本', 'agent.write', `data-app="${esc(item.id)}"`)}${actionButton('deploy', '设置部署', 'agent.deploy', `data-app="${esc(item.id)}"`)}</div>`]);
    const versionRows = list(versions.items).map(item => [`<span class="cell-primary">v${esc(item.versionNumber)}</span><span class="cell-secondary">${idCell(item.id, true)}</span>`, esc(appName(item.appId)), badge(item.status), esc(formatTime(item.publishedAt || item.createdAt)), item.status === 'draft' ? actionButton('publish', '发布版本', 'agent.publish', `data-id="${esc(item.id)}"`) : '<span class="section-caption">版本已固化</span>']);
    const deploymentRows = list(deployments.items).map(item => [esc(appName(item.appId)), `<span class="badge ${item.kind === 'canary' ? 'warning' : 'progress'}">${item.kind === 'canary' ? '灰度' : item.kind === 'stable' ? '稳定' : esc(item.kind)}</span>`, idCell(item.versionId, true), item.kind === 'stable' ? '默认承接剩余流量' : `${esc(Number.isFinite(Number(item.trafficBps)) ? (Number(item.trafficBps) / 100).toFixed(2).replace(/\.00$/, '') : '—')}%`, badge(item.status), esc(formatTime(item.updatedAt))]);
    return note('保存版本只创建草稿；发布使版本可用于部署；设置部署后才改变后续请求的版本选择。已有固定版本的请求遵循服务端重试规则。') + panel('Agent 应用', table(['应用 / ID', '状态', '创建时间', '操作'], appRows, '还没有 Agent 应用', '创建一个应用，然后为它保存并发布第一个版本。') + pager('apps', apps), actionButton('app', '＋ 创建应用', 'agent.write')) + panel('版本记录', table(['版本 / ID', '所属应用', '状态', '发布时间 / 创建时间', '操作'], versionRows, '暂无应用版本', '在上方应用行点击“新建版本”，从租户配置生成可审阅草稿。') + pager('versions', versions)) + panel('部署记录', table(['应用', '路线', '版本 ID', '路由策略', '状态', '更新时间'], deploymentRows, '尚无部署记录', '发布版本后，在对应应用上设置稳定或灰度部署。') + pager('deployments', deployments));
  }
  function renderRequests(response) {
    const resource = state.requestTab;
    const options = {
      inbox: ['RECEIVED', 'PROCESSING', 'RETRY_WAIT', 'WAITING_APPROVAL', 'WAITING_RECONCILIATION', 'COMPLETED', 'DEAD_LETTERED'],
      executions: ['RUNNING', 'SUCCEEDED', 'FAILED', 'ABANDONED'],
      outbox: ['REPLY_PENDING', 'DELIVERING', 'DISPATCH_STARTED', 'RETRY_WAIT', 'WAITING_RECONCILIATION', 'REPLIED', 'DEAD_LETTERED']
    };
    const tabs = `<div class="tabs" role="tablist" aria-label="请求记录类型">${[['inbox', '接入请求'], ['executions', '执行记录'], ['outbox', '投递记录']].map(([key, name]) => `<button type="button" role="tab" aria-selected="${key === resource}" class="${key === resource ? 'active' : ''}" data-request-tab="${key}">${name}</button>`).join('')}</div>`;
    const filters = `<div class="filters"><label><span class="visually-hidden">状态筛选</span><select id="status-filter"><option value="">全部状态</option>${options[resource].map(status => `<option value="${status}" ${state.status === status ? 'selected' : ''}>${statusNames[status] || status}</option>`).join('')}</select></label></div>`;
    let headers, rows;
    if (resource === 'inbox') { headers = ['请求 ID', '应用 / 通道', '状态', '尝试次数', '接入时间', '']; rows = list(response.items).map(item => [idCell(item.id), `<span class="cell-primary">${esc(item.appName)}</span><span class="cell-secondary">${esc(item.channelType)}</span>`, badge(item.status), `${esc(item.attemptCount ?? 0)} / ${esc(item.maxAttempts ?? '—')}`, esc(formatTime(item.createdAt)), `<button class="text-button" type="button" data-request="${esc(item.id)}">查看链路 →</button>`]); }
    else if (resource === 'executions') { headers = ['执行 ID', '应用 ID / 版本', '状态', '执行尝试', '开始时间', '重试判定']; rows = list(response.items).map(item => [idCell(item.id), `${idCell(item.appId, true)}<span class="cell-secondary">${idCell(item.versionId, true)}</span>`, badge(item.status), esc(item.attemptNumber ?? '—'), esc(formatTime(item.startedAt)), item.retrySafe === true ? badge('允许安全重试') : badge('需保守处理')]); }
    else { headers = ['投递 ID / 请求', '通道', '状态', '尝试次数', '更新时间', '']; rows = list(response.items).map(item => [idCell(item.id) + `<span class="cell-secondary">Inbox ${esc(item.inboxId)}</span>`, esc(item.channelType), badge(item.status), `${esc(item.attemptCount ?? 0)} / ${esc(item.maxAttempts ?? '—')}`, esc(formatTime(item.updatedAt)), `<button class="text-button" type="button" data-request="${esc(item.inboxId)}">查看链路 →</button>`]); }
    return note('列表是读取时的状态快照。投递“待核对 / 结果未知”不等于失败，也不表示可以安全重发。') + `<section class="panel">${tabs}<header class="panel-header"><div><h2>${resource === 'inbox' ? '持久化接入请求' : resource === 'executions' ? 'Agent 执行记录' : '对外投递记录'}</h2><p>每页最多 50 条，按服务端游标翻页</p></div>${filters}</header>${table(headers, rows, state.status ? '没有匹配的记录' : '暂无请求记录', state.status ? '尝试切换状态筛选，或刷新当前列表。' : '接入真实请求后，这里会显示服务端已持久化的记录。')}${pager(resource, response)}</section>`;
  }
  function renderRequestDetail(data) {
    const inbox = data.inbox || {}; const executions = list(data.executions); const outbox = list(data.outbox);
    const uncertain = outbox.some(row => /UNKNOWN|RECONCIL|DISPATCH_STARTED/.test(row.status || ''));
    const stage = (title, status, count) => `<div class="trail-step"><small>${title}</small><strong>${badge(status)}</strong><p>${esc(count)}</p></div>`;
    const summary = `<div class="trail">${stage('01 / 接入', inbox.status, `Inbox ${text(inbox.id)}`)}${stage('02 / 执行', executions[0]?.status || '尚无执行记录', `${executions.length} 条已关联执行`)}${stage('03 / 投递', outbox[0]?.status || '尚无投递记录', `${outbox.length} 条已关联投递`)}</div>`;
    const metadata = `<div class="panel-body detail-key-grid"><div><small>请求 ID</small>${idCell(inbox.id)}</div><div><small>应用 / 通道</small>${esc(inbox.appName)} · ${esc(inbox.channelType)}</div><div><small>Trace ID</small>${idCell(inbox.traceId)}</div><div><small>接入时间</small>${esc(fullTime(inbox.createdAt))}</div><div><small>尝试次数 / 最大次数</small>${esc(inbox.attemptCount ?? '—')} / ${esc(inbox.maxAttempts ?? '—')}</div><div><small>Inbox Lease Version</small><span class="mono">${esc(inbox.leaseVersion ?? '未提供')}</span></div></div>`;
    const executionRows = executions.map(row => [idCell(row.id), badge(row.status), esc(row.attemptNumber ?? '—'), idCell(row.versionId, true), esc(formatTime(row.startedAt)), esc(formatTime(row.completedAt))]);
    const outboxRows = outbox.map(row => [idCell(row.id), badge(row.status), `${esc(row.attemptCount ?? '—')} / ${esc(row.maxAttempts ?? '—')}`, `<span class="mono">${esc(row.leaseVersion ?? '未提供')}</span>`, idCell(row.traceId, true), esc(formatTime(row.deliveredAt))]);
    return '<div class="toolbar"><button class="btn" data-action="requests-back" type="button">← 返回请求列表</button><span class="section-caption">按持久化关联键查询</span></div>' + (uncertain ? note('此请求存在结果未知的外部投递。请依据平台回执和审计证据人工核对；本操作台不会自动重发。', true) : '') + (data.truncated ? note('关联记录超过服务端返回上限。以下链路不是完整历史，请结合分页列表继续核查。', true) : '') + summary + panel('请求信息', metadata) + panel('执行记录', table(['执行 ID', '状态', 'Attempt', '固定版本', '开始', '完成'], executionRows)) + panel('投递记录', table(['投递 ID', '状态', '尝试次数', 'Lease Version', 'Trace ID', '投递时间'], outboxRows)) + panel('精确关联的控制面审计', auditTable(list(data.audit)));
  }
  function auditTable(items) { return table(['操作', '操作者', '资源类型', '资源 ID', '审计 ID', '时间'], items.map(item => [`<span class="cell-primary">${esc(item.action)}</span>`, esc(text(item.actor)), esc(item.resourceType), idCell(item.resourceId, true), idCell(item.id), esc(formatTime(item.createdAt))]), '暂无关联操作', '这里仅显示控制面操作元数据，不包含业务正文或凭据。'); }
  function renderAudit(response) { return note('当前视图展示控制面变更审计。每条记录对应实际操作对象，不把同期记录推断成请求关联。') + panel('控制面审计', auditTable(list(response.items)) + pager('audit', response)); }
  function renderMigrations(response) { return note('迁移阶段与暂停状态来自服务端记录。本页用于观察；创建、推进、切换与回滚请使用当前部署的迁移操作流程。') + panel('迁移任务', table(['迁移 ID', '数据域', '当前阶段', '运行状态', '创建时间', '更新时间'], list(response.items).map(item => [idCell(item.id, true), `<span class="cell-primary">${esc(item.domain)}</span>`, badge(item.phase || item.status), badge(item.paused ? '已暂停' : '未暂停'), esc(formatTime(item.createdAt)), esc(formatTime(item.updatedAt))]), '暂无迁移任务', '租户出现数据迁移记录后，可在这里查看实际进度。') + pager('migrations', response)); }

  // All operations below use explicit user submission. No tenant/version
  // changes are sent while typing, navigating, or refreshing a page.
  const field = (name, label, value = '', attributes = '', help = '') => `<label class="form-group">${esc(label)}<input name="${name}" value="${esc(value)}" ${attributes}>${help ? `<span class="field-help">${esc(help)}</span>` : ''}</label>`;
  const selectField = (name, label, options, value, attributes = '') => `<label class="form-group">${esc(label)}<select name="${name}" ${attributes}>${options.map(([id, title]) => `<option value="${esc(id)}" ${String(id) === String(value) ? 'selected' : ''}>${esc(title)}</option>`).join('')}</select></label>`;
  const formValue = name => $('action-form').elements.namedItem(name)?.value?.trim() || '';
  function openDialog(type, context = {}) {
    if (state.busy) return;
    modelCredential = '';
    state.dialog = { type, tenantId: state.tenantId, step: 0, context, form: {} };
    $('dialog-error').hidden = true; renderDialog(); $('action-dialog').showModal();
    if (type === 'deploy') loadDeploymentOptions(state.dialog);
  }
  function closeDialog() { if (state.busy) return; state.dialog?.loadController?.abort(); modelCredential = ''; $('action-dialog').close(); $('dialog-body').replaceChildren(); $('dialog-error').textContent = ''; $('dialog-error').hidden = true; state.dialog = null; }
  $('dialog-close').addEventListener('click', closeDialog); $('dialog-cancel').addEventListener('click', closeDialog);
  $('action-dialog').addEventListener('cancel', event => { if (state.busy) event.preventDefault(); else closeDialog(); });
  $('create-tenant').addEventListener('click', () => openDialog('tenant'));
  function dialogButton(label) { $('dialog-submit').textContent = label; $('dialog-submit').disabled = state.busy; }
  function renderDialog() {
    const dialog = state.dialog; if (!dialog) return;
    $('dialog-back').hidden = true; $('dialog-cancel').textContent = '取消'; $('dialog-submit').hidden = false; $('dialog-error').hidden = true;
    const tenant = selectedTenant();
    if (dialog.type === 'tenant') return renderTenantWizard();
    if (dialog.type === 'app') {
      $('dialog-title').textContent = '创建 Agent 应用'; $('dialog-eyebrow').textContent = tenant.name;
      $('dialog-body').innerHTML = field('name', '应用名称', '', 'required maxlength="128" pattern="[A-Za-z0-9][A-Za-z0-9._\\-]*" placeholder="例如 support-agent"', '使用英文字母、数字、点、下划线或短横线。') + field('description', '用途说明（可选）', '', 'maxlength="2000" placeholder="例如售后咨询与知识问答"'); dialogButton('创建应用');
    } else if (dialog.type === 'version') {
      const app = dialog.context.app; const agent = tenant.agents.find(item => item.name === app.name) || tenant.agents[0] || {};
      const model = tenant.models.find(item => item.modelName === agent.defaultModel) || tenant.models[0];
      // Keep the inherited limit aligned with AgentConfig.EffectiveMaxLLMCalls.
      const maxLLMCalls = agent.maxLLMCalls == null || agent.maxLLMCalls === 0 ? 8 : agent.maxLLMCalls;
      if (!model) { $('dialog-body').innerHTML = note('租户尚未配置模型，请先完善租户配置。', true); $('dialog-submit').hidden = true; return; }
      $('dialog-title').textContent = '保存版本草稿'; $('dialog-eyebrow').textContent = `${tenant.name} / ${app.name}`;
      $('dialog-body').innerHTML = note(`新版本属于应用 ${app.name}，保存后仍需单独发布与部署。`) + `<div class="form-grid">${selectField('modelName', '已配置模型', tenant.models.map(m => [m.modelName, `${m.provider} / ${m.modelName}`]), model.modelName)}${field('maxTokens', '每次输出 token 上限', model.maxTokens || 1024, 'type="number" min="1" max="128000" required')}${field('maxLLMCalls', '最大模型调用次数', maxLLMCalls, 'type="number" min="1" max="32" required')}<label class="form-group full">系统提示词<textarea name="systemPrompt" maxlength="64000" placeholder="说明应用的职责、回答范围与风格">${esc(agent.systemPrompt || '')}</textarea></label></div><details><summary>高级版本 JSON（可选）</summary><p class="field-help">填写完整 snapshot 将替代上方字段。不得包含 API key 或其他凭据值；模型引用必须与租户绑定一致。</p><textarea name="advancedVersion" aria-label="高级版本 JSON" spellcheck="false" placeholder='{"agent": {...}, "model": {...}}'></textarea></details>`;
      dialog.context.agent = agent; dialogButton('保存草稿');
    } else if (dialog.type === 'publish') {
      const version = dialog.context.version; $('dialog-title').textContent = '确认发布版本'; $('dialog-eyebrow').textContent = tenant.name;
      $('dialog-body').innerHTML = `<p class="confirm-title">发布 ${esc(appName(version.appId))} · v${esc(version.versionNumber)}</p><div class="review-box"><dl class="definition-list"><dt>版本 ID</dt><dd class="mono">${esc(version.id)}</dd><dt>当前状态</dt><dd>${badge(version.status)}</dd><dt>影响</dt><dd>版本将成为可部署的不可变发布版本。当前流量配置保持不变。</dd></dl></div>`; dialogButton('确认发布此版本');
    } else if (dialog.type === 'deploy') renderDeployDialog();
    else if (dialog.type === 'copy') { $('dialog-title').textContent = '复制记录 ID'; $('dialog-eyebrow').textContent = ''; $('dialog-body').innerHTML = `<label>记录 ID<input class="copy-source" readonly value="${esc(dialog.context.id)}"></label><p class="field-help">浏览器未授予剪贴板权限，请选中文本复制。</p>`; $('dialog-submit').hidden = true; $('dialog-cancel').textContent = '关闭'; }
  }
  function renderTenantWizard() {
    const dialog = state.dialog; const values = dialog.form;
    $('dialog-title').textContent = '创建租户'; $('dialog-eyebrow').textContent = ''; $('dialog-back').hidden = dialog.step === 0;
    let html = `<div class="steps" aria-label="创建步骤">${['01 基本配置', '02 数据后端', '03 确认创建'].map((label, index) => `<span class="${index === dialog.step ? 'current' : ''}" ${index === dialog.step ? 'aria-current="step"' : ''}>${label}</span>`).join('')}</div>`;
    if (dialog.step === 0) {
      modelCredential = '';
      html += `<div class="form-grid">${field('tenantName', '租户名称', values.tenantName || '', 'required maxlength="120" placeholder="例如 客服业务部"')}${field('appName', '初始 Agent 名称', values.appName || 'support', 'required maxlength="128" pattern="[A-Za-z0-9][A-Za-z0-9._\\-]*"')}${field('modelName', '模型名称', values.modelName || 'gpt-4o-mini', 'required maxlength="128"', '模型需要包含在部署方的准入目录中。')}${field('modelApiKey', '模型 API key', '', 'type="password" maxlength="4096" autocomplete="new-password" spellcheck="false" placeholder="仅用于本次创建请求"', '与 SecretRef 必须且只能填写一项。进入下一步后不回显，提交或取消即清除；返回此步骤需重新输入。')}${field('secretRef', '模型 SecretRef', values.secretRef || '', 'maxlength="256" pattern="env://TRPC_SECRET_[A-Za-z0-9_]+" placeholder="env://TRPC_SECRET_OPENAI_API_KEY" autocomplete="off" spellcheck="false"', '填写运维引用名称。发布前需由运维绑定新租户授权。')}</div>` + note('创建租户必须提供模型凭据：填写 API key 或 SecretRef，二者不能同时使用。服务端继续校验凭据引用与租户授权。');
      dialogButton('下一步：数据后端');
    } else if (dialog.step === 1) {
      html += `<div class="form-grid">${selectField('sessionBackend', 'Session 后端', [['redis', 'Redis'], ['postgres', 'PostgreSQL']], values.sessionBackend || 'redis', 'data-backend-domain="session"')}${field('sessionProfile', 'Session Profile', values.sessionProfile || 'local-redis', 'required maxlength="64" pattern="[a-z][a-z0-9._\\-]*" list="profile-suggestions"')}${selectField('memoryBackend', 'Memory 后端', [['postgres', 'PostgreSQL'], ['redis', 'Redis']], values.memoryBackend || 'postgres', 'data-backend-domain="memory"')}${field('memoryProfile', 'Memory Profile', values.memoryProfile || 'local-postgres', 'required maxlength="64" pattern="[a-z][a-z0-9._\\-]*" list="profile-suggestions"')}</div><datalist id="profile-suggestions"><option value="local-postgres"></option><option value="local-redis"></option></datalist><p class="field-help">默认 Profile 来自本地部署模板，可修改为运维已注册的名称。保存时服务端校验存在性、后端类型与租户授权。</p>`;
      dialogButton('下一步：确认配置');
    } else {
      const config = tenantPayload();
      html += `<div class="review-box"><dl class="definition-list"><dt>租户</dt><dd>${esc(values.tenantName)}</dd><dt>初始 Agent</dt><dd>${esc(values.appName)}</dd><dt>模型</dt><dd>openai / ${esc(values.modelName)}</dd><dt>模型凭据</dt><dd>${modelCredential ? '已输入一次性提交密钥，不显示内容' : values.secretRef ? '使用运维 SecretRef，需完成授权' : '密钥已清除，请返回基本配置重新输入'}</dd><dt>Session</dt><dd>${esc(values.sessionBackend)} / ${esc(values.sessionProfile)}</dd><dt>Memory</dt><dd>${esc(values.memoryBackend)} / ${esc(values.memoryProfile)}</dd></dl></div><details><summary>高级租户 JSON</summary><p class="field-help">可编辑创建配置。这里禁止内联密钥；上一步输入的密钥仅在提交时添加到对应模型，不进入此 JSON。每个模型都必须有凭据来源；新增模型请配置 SecretRef。</p><textarea name="advancedTenant" aria-label="高级租户 JSON" spellcheck="false">${esc(values.advancedTenant || JSON.stringify(config, null, 2))}</textarea></details>`;
      dialogButton('确认创建租户');
    }
    $('dialog-body').innerHTML = html;
  }
  function tenantPayload() {
    const form = state.dialog.form;
    return { name: form.tenantName, config: { agents: [{ name: form.appName, type: 'llm', defaultModel: form.modelName, maxLLMCalls: 1, tools: [] }], models: [{ provider: 'openai', modelName: form.modelName, ...(form.secretRef ? { apiKeyRef: form.secretRef } : {}), maxTokens: 1024 }], toolPolicy: { mode: 'whitelist', allowed: [] }, channels: [], storage: { sessionBackend: form.sessionBackend, sessionProfile: form.sessionProfile, memoryBackend: form.memoryBackend, memoryProfile: form.memoryProfile }, governance: { auditLevel: 'detailed' } } };
  }
  function saveTenantStep() {
    const values = state.dialog.form;
    const fields = state.dialog.step === 0 ? ['tenantName', 'appName', 'modelName', 'secretRef'] : state.dialog.step === 1 ? ['sessionBackend', 'sessionProfile', 'memoryBackend', 'memoryProfile'] : ['advancedTenant'];
    fields.forEach(name => { values[name] = formValue(name); });
    if (state.dialog.step === 0) {
      const input = $('action-form').elements.namedItem('modelApiKey');
      const credential = input.value.trim();
      if (credential && values.secretRef) throw new UIError('模型 API key 与 SecretRef 只能填写其中一项。');
      if (!credential && !values.secretRef) throw new UIError('请填写模型 API key 或 SecretRef 后继续。');
      modelCredential = credential; input.value = '';
    }
    if (state.dialog.step < 2) delete values.advancedTenant;
  }
  async function deploymentRows(resource, status, dialog, signal) {
    const rows = [], cursors = new Set(), ids = new Set();
    let cursor = '';
    // The history table's page is never a source of deployment defaults.
    // A bounded, complete read either succeeds or leaves the form unavailable.
    for (let page = 0; page < 100; page += 1) {
      const params = new URLSearchParams({ tenantId: dialog.tenantId, status, limit: '100' });
      if (cursor) params.set('cursor', cursor);
      const response = await api(`/api/v1/operations/${resource}?${params}`, { signal });
      if (!Array.isArray(response?.items) || response.items.length > 100 || (response.nextCursor !== undefined && typeof response.nextCursor !== 'string')) throw new UIError('部署配置响应不完整，请重新读取。');
      for (const item of response.items) {
        if (!item || typeof item.id !== 'string' || !item.id || ids.has(item.id) || item.tenantId !== dialog.tenantId || item.status !== status) throw new UIError('部署配置在读取期间发生变化，请重新读取。');
        ids.add(item.id); rows.push(item);
      }
      cursor = response.nextCursor || '';
      if (!cursor) return rows.filter(item => item.appId === dialog.context.app.id);
      if (!response.items.length || cursor.length > 2048 || cursors.has(cursor)) throw new UIError('部署配置分页异常，请重新读取。');
      cursors.add(cursor);
    }
    throw new UIError('部署配置记录超过本次读取上限，请通过运维接口核对后操作。');
  }
  function deploymentFingerprint(rows) {
    return JSON.stringify(rows.map(item => [item.id, item.versionId, item.kind, item.trafficBps]).sort((a, b) => a[0].localeCompare(b[0])));
  }
  async function loadDeploymentOptions(dialog) {
    if (state.dialog !== dialog || !accessToken) return;
    dialog.loadController?.abort();
    const controller = new AbortController(); dialog.loadController = controller;
    const current = generation, token = accessToken;
    const valid = () => state.dialog === dialog && dialog.loadController === controller && dialog.tenantId === state.tenantId && current === generation && token === accessToken;
    let timedOut = false;
    const timer = setTimeout(() => { timedOut = true; controller.abort(); }, 30000);
    dialog.context.options = null; dialog.context.loadError = ''; renderDialog();
    try {
      const [currentDeployments, versions] = await Promise.all([
        deploymentRows('deployments', 'active', dialog, controller.signal),
        deploymentRows('versions', 'published', dialog, controller.signal)
      ]);
      if (!valid()) return;
      const stable = currentDeployments.filter(item => item.kind === 'stable'), canary = currentDeployments.filter(item => item.kind === 'canary');
      const versionIDs = new Set(versions.map(item => item.id));
      if (stable.length > 1 || canary.length > 1 || currentDeployments.length !== stable.length + canary.length || (canary.length && !stable.length) || currentDeployments.some(item => !versionIDs.has(item.versionId)) || (stable.length && stable[0].trafficBps !== 10000) || (canary.length && (!Number.isInteger(canary[0].trafficBps) || canary[0].trafficBps < 1 || canary[0].trafficBps > 9999 || canary[0].versionId === stable[0].versionId))) throw new UIError('当前部署与已发布版本不一致，请重新读取或联系运维核对。');
      dialog.context.options = { versions, stable: stable[0], canary: canary[0], fingerprint: deploymentFingerprint(currentDeployments) };
      renderDialog();
    } catch (error) {
      if (!valid() || (error.name === 'AbortError' && !timedOut)) return;
      dialog.context.loadError = timedOut ? '读取部署配置超时，请重新读取。' : errorMessage(error);
      renderDialog();
    } finally { clearTimeout(timer); controller.abort(); }
  }
  async function verifyDeploymentUnchanged(dialog) {
    const controller = new AbortController(); dialog.loadController = controller;
    let timedOut = false;
    const timer = setTimeout(() => { timedOut = true; controller.abort(); }, 30000);
    try {
      const current = await deploymentRows('deployments', 'active', dialog, controller.signal);
      if (deploymentFingerprint(current) !== dialog.context.options.fingerprint) throw new UIError('当前部署已发生变化。请关闭此窗口，重新读取配置后再审阅部署变更。');
    } catch (error) {
      if (timedOut) throw new UIError('未能及时核对当前部署，本次尚未提交，请稍后重试。');
      throw error;
    } finally { clearTimeout(timer); controller.abort(); }
  }
  function renderDeployDialog() {
    const dialog = state.dialog; const app = dialog.context.app;
    $('dialog-title').textContent = dialog.step ? '确认部署流量' : '设置应用部署'; $('dialog-eyebrow').textContent = `${selectedTenant().name} / ${app.name}`;
    if (!dialog.context.options) {
      $('dialog-submit').hidden = true;
      $('dialog-body').innerHTML = dialog.context.loadError ? note(dialog.context.loadError, true) + '<button class="btn" type="button" data-deploy-retry>重新读取部署配置</button>' : loading();
      return;
    }
    if (dialog.step) {
      const values = dialog.form;
      $('dialog-body').innerHTML = `<p class="confirm-title">应用 ${esc(app.name)}</p><div class="review-box"><dl class="definition-list"><dt>稳定版本 ID</dt><dd class="mono">${esc(values.stableVersionId)}</dd><dt>灰度版本 ID</dt><dd class="mono">${esc(values.canaryVersionId || '不启用')}</dd></dl></div><div class="traffic-review"><div><small>稳定版本流量</small><strong>${esc((10000 - Math.round(Number(values.canaryPercent) * 100)) / 100)}%</strong></div><div><small>灰度版本流量</small><strong>${esc(values.canaryPercent)}%</strong></div></div>` + note('确认后将更新该应用后续请求的版本选择。灰度按服务端路由规则分配；已绑定版本的请求保持其执行契约。', true); $('dialog-back').hidden = false; dialogButton('确认应用此部署'); return;
    }
    const { versions, stable, canary } = dialog.context.options;
    if (!versions.length) { $('dialog-body').innerHTML = note('此应用尚无已发布版本。请先发布版本，再设置部署。'); $('dialog-submit').hidden = true; return; }
    const publishedOptions = versions.map(version => [version.id, `v${version.versionNumber} · ${version.id}`]);
    $('dialog-body').innerHTML = field('stableVersionId', '稳定版本 ID', dialog.form.stableVersionId ?? stable?.versionId ?? versions[0]?.id ?? '', 'required list="published-versions" maxlength="128" placeholder="选择或粘贴已发布版本 ID"') + field('canaryVersionId', '灰度版本 ID（可选）', dialog.form.canaryVersionId ?? canary?.versionId ?? '', 'list="published-versions" maxlength="128" placeholder="留空表示只使用稳定版本"') + field('canaryPercent', '灰度流量比例（%）', dialog.form.canaryPercent ?? (Number(canary?.trafficBps || 0) / 100), 'type="number" required min="0" max="99.99" step="0.01"') + `<datalist id="published-versions">${publishedOptions.map(([id, label]) => `<option value="${esc(id)}">${esc(label)}</option>`).join('')}</datalist><p class="field-help">已读取此应用的当前部署与全部已发布版本。灰度上限为 99.99%；全量切换请选择目标稳定版本并清空灰度配置。</p>`; dialogButton('审阅部署变更');
  }
  $('action-form').addEventListener('click', event => { if (event.target.closest('[data-deploy-retry]') && state.dialog?.type === 'deploy' && !state.busy) loadDeploymentOptions(state.dialog); });
  $('dialog-back').addEventListener('click', () => { if (!state.dialog || state.busy) return; if (state.dialog.type === 'tenant') saveTenantStep(); state.dialog.step = Math.max(0, state.dialog.step - 1); renderDialog(); });
  $('action-form').addEventListener('change', event => {
    const domain = event.target.dataset.backendDomain;
    if (domain) { const input = $('action-form').elements.namedItem(domain + 'Profile'); if (['local-postgres', 'local-redis'].includes(input.value)) input.value = `local-${event.target.value}`; }
    if (state.dialog?.type === 'version' && event.target.name === 'modelName') { const model = selectedTenant().models.find(m => m.modelName === event.target.value); if (model) $('action-form').elements.namedItem('maxTokens').value = model.maxTokens || 1024; }
  });
  function parseSafeJSON(value) {
    let parsed;
    try { parsed = JSON.parse(value); } catch { throw new UIError('JSON 格式不正确，请检查引号、逗号与括号。'); }
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) throw new UIError('JSON 顶层必须是对象。');
    rejectInlineSecrets(parsed); return parsed;
  }
  function rejectInlineSecrets(value, depth = 0) {
    if (depth > 18) throw new UIError('JSON 嵌套过深，请简化配置。');
    if (!value || typeof value !== 'object') return;
    for (const [key, child] of Object.entries(value)) {
      if (/^(api[_-]?key|token|secret|password|passwd|authorization|access[_-]?key|secret[_-]?key|encodingAESKey|corp_secret|dsn|url)$/i.test(key) && child !== '' && child !== null) throw new UIError('高级 JSON 不允许内联凭据或连接字符串，请使用运维注册的 SecretRef 和 Profile。');
      if (key.toLowerCase().endsWith('ref') && typeof child === 'string' && child && !/^env:\/\/TRPC_SECRET_[A-Za-z0-9_]+$/.test(child)) throw new UIError('凭据引用必须使用 env://TRPC_SECRET_ 开头的运维名称。');
      rejectInlineSecrets(child, depth + 1);
    }
  }
  function versionPayload() {
    const app = state.dialog.context.app;
    let snapshot;
    if (formValue('advancedVersion')) snapshot = parseSafeJSON(formValue('advancedVersion'));
    else {
      const configured = selectedTenant().models.find(model => model.modelName === formValue('modelName'));
      if (!configured) throw new UIError('请从租户已配置模型中选择。');
      const agent = state.dialog.context.agent;
      snapshot = { agent: { name: app.name, type: agent.type || 'llm', defaultModel: configured.modelName, maxLLMCalls: Number(formValue('maxLLMCalls')), systemPrompt: formValue('systemPrompt'), tools: list(agent.tools), ...(agent.runtime ? { runtime: agent.runtime } : {}) }, model: { provider: configured.provider, modelName: configured.modelName, ...(configured.apiKeyRef ? { apiKeyRef: configured.apiKeyRef } : {}), maxTokens: Number(formValue('maxTokens')), ...(configured.temperature ? { temperature: configured.temperature } : {}) } };
    }
    rejectInlineSecrets(snapshot);
    if (snapshot.agent?.name !== app.name) throw new UIError('版本中的 agent.name 必须与当前应用名称一致。');
    if (!snapshot.model || snapshot.agent?.defaultModel !== snapshot.model.modelName) throw new UIError('Agent 的默认模型必须与版本模型一致。');
    return { tenantId: state.dialog.tenantId, appName: app.name, snapshot };
  }
  function setBusy(value) {
    state.busy = value; $('action-form').setAttribute('aria-busy', String(value));
    $('action-form').querySelectorAll('button,input,select,textarea').forEach(element => { element.disabled = value; });
    $('tenant-select').disabled = value || !state.tenants.length; $('refresh').disabled = value; $('create-tenant').disabled = value;
    $('navigation').querySelectorAll('button').forEach(button => { button.disabled = value; });
  }
  $('action-form').addEventListener('submit', async event => {
    event.preventDefault(); if (!state.dialog || state.busy) return;
    const dialog = state.dialog; let path, body, success, credentialSubmitted = false;
    $('dialog-error').hidden = true;
    try {
      if (dialog.type === 'tenant') {
        saveTenantStep();
        if (dialog.step < 2) { dialog.step += 1; renderDialog(); return; }
        if (!allowed('tenant.create')) throw new UIError('当前身份没有创建租户的权限。');
        body = parseSafeJSON(dialog.form.advancedTenant || JSON.stringify(tenantPayload()));
        if (!body.name || !list(body.config?.agents).length || !list(body.config?.models).length) throw new UIError('请完整配置租户名称、Agent 与模型。');
        if (modelCredential) {
          const models = list(body.config.models).filter(model => model?.provider === 'openai' && model.modelName === dialog.form.modelName);
          if (models.length !== 1 || models[0].apiKeyRef) throw new UIError('一次性密钥需要对应唯一的原选模型，且不能同时配置 SecretRef。');
          models[0].apiKey = modelCredential;
        }
        for (const model of body.config.models) {
          const hasKey = typeof model?.apiKey === 'string' && model.apiKey.trim() !== '';
          const hasRef = typeof model?.apiKeyRef === 'string' && model.apiKeyRef.trim() !== '';
          if (hasKey === hasRef) throw new UIError('每个模型必须且只能配置一种凭据来源。请检查高级 JSON 中的 SecretRef；一次性密钥已清除时，请返回基本配置重新输入。');
        }
        path = '/api/v1/tenants'; success = '租户已创建。可以继续创建应用与版本。';
      } else if (dialog.type === 'app') { path = '/api/v1/agent-apps'; body = { tenantId: dialog.tenantId, name: formValue('name'), description: formValue('description') }; success = '应用已创建，可以保存第一个版本。'; }
      else if (dialog.type === 'version') { path = '/api/v1/agent-versions'; body = versionPayload(); success = '版本草稿已保存，请审阅后单独发布。'; }
      else if (dialog.type === 'publish') { path = `/api/v1/agent-versions/${encodeURIComponent(dialog.context.version.id)}/publish`; body = { tenantId: dialog.tenantId }; success = '版本已发布。部署流量尚未改变。'; }
      else if (dialog.type === 'deploy') {
        if (!dialog.context.options) throw new UIError('请等待完整部署配置读取成功后再操作。');
        if (!dialog.step) {
          dialog.form = { stableVersionId: formValue('stableVersionId'), canaryVersionId: formValue('canaryVersionId'), canaryPercent: formValue('canaryPercent') };
          const percent = Number(dialog.form.canaryPercent);
          if (!Number.isFinite(percent) || percent < 0 || percent > 99.99 || Math.abs(percent * 100 - Math.round(percent * 100)) > 1e-7) throw new UIError('灰度比例必须为 0–99.99，最多两位小数。');
          if ((!dialog.form.canaryVersionId && percent !== 0) || (dialog.form.canaryVersionId && percent === 0)) throw new UIError('配置灰度版本时，比例须大于 0；不使用灰度时请清空灰度版本并设置为 0。');
          if (dialog.form.canaryVersionId === dialog.form.stableVersionId) throw new UIError('稳定版本与灰度版本应使用不同的版本 ID。');
          const versionIDs = new Set(dialog.context.options.versions.map(version => version.id));
          if (!versionIDs.has(dialog.form.stableVersionId) || (dialog.form.canaryVersionId && !versionIDs.has(dialog.form.canaryVersionId))) throw new UIError('请选择此应用已读取的发布版本。新发布版本需关闭窗口后重新读取。');
          dialog.step = 1; renderDialog(); return;
        }
        path = '/api/v1/deployments'; body = { tenantId: dialog.tenantId, appName: dialog.context.app.name, stableVersionId: dialog.form.stableVersionId, canaryVersionId: dialog.form.canaryVersionId, canaryBps: Math.round(Number(dialog.form.canaryPercent) * 100) }; success = '部署配置已更新，请在部署记录中核对版本与流量。';
      } else return;
      const current = generation; const originalToken = accessToken;
      setBusy(true); $('dialog-submit').textContent = '正在提交…';
      if (dialog.type === 'deploy') {
        await verifyDeploymentUnchanged(dialog);
        if (current !== generation || originalToken !== accessToken || state.dialog !== dialog) return;
      }
      const submission = api(path, { method: 'POST', body });
      // api serializes synchronously before its first await. Discard the
      // one-time credential as soon as that submission has started.
      credentialSubmitted = Boolean(modelCredential); modelCredential = '';
      if (dialog.type === 'tenant') list(body.config?.models).forEach(model => { delete model.apiKey; });
      const result = await submission;
      // A logout or tenant navigation invalidates every result, including a
      // successful write response. The next read is always server-authoritative.
      if (current !== generation || originalToken !== accessToken) return;
      setBusy(false);
      if (dialog.type === 'tenant' && result?.id) { state.tenants.push(cleanTenant(result)); state.tenantId = String(result.id); state.page = 'apps'; state.pages = {}; updateShell(); }
      closeDialog(); toast(success); await loadPage();
    } catch (error) {
      if (!state.dialog || state.dialog !== dialog || !accessToken || error.name === 'AbortError') return;
      if (credentialSubmitted) renderDialog();
      $('dialog-error').textContent = errorMessage(error, true) + (credentialSubmitted ? ' 本次输入的模型密钥已清除；需要重试时，请先核对租户是否已创建，再返回基本配置重新输入。' : ''); $('dialog-error').hidden = false;
    } finally {
      // A local validation failure may precede submission. Remove any injected
      // key from that temporary body; keep the page-only key for a corrected
      // retry until submission, returning to step one, cancellation or logout.
      if (dialog.type === 'tenant') list(body?.config?.models).forEach(model => { if (model && typeof model === 'object') delete model.apiKey; });
      if (state.dialog === dialog && accessToken) { setBusy(false); const labels = { tenant: '确认创建租户', app: '创建应用', version: '保存草稿', publish: '确认发布此版本', deploy: '确认应用此部署' }; $('dialog-submit').textContent = dialog.type === 'tenant' ? ['下一步：数据后端', '下一步：确认配置', '确认创建租户'][dialog.step] : dialog.type === 'deploy' && !dialog.step ? '审阅部署变更' : labels[dialog.type] || '保存'; }
    }
  });
  $('page-content').addEventListener('change', event => { if (event.target.id === 'status-filter') { state.status = event.target.value; state.pages = {}; loadPage(); } });
  $('page-content').addEventListener('click', async event => {
    const button = event.target.closest('button'); if (!button || state.busy) return;
    if (button.dataset.copy !== undefined) {
      const id = button.dataset.copy;
      try { if (!navigator.clipboard?.writeText) throw new Error(); await navigator.clipboard.writeText(id); toast('ID 已复制'); } catch { openDialog('copy', { id }); const input = $('dialog-body').querySelector('input'); input?.focus(); input?.select(); }
      return;
    }
    if (button.dataset.pageNext) { const page = pageState(button.dataset.pageNext); page.cursors[page.index + 1] = button.dataset.cursor; page.index += 1; loadPage(); return; }
    if (button.dataset.pageBack) { pageState(button.dataset.pageBack).index -= 1; loadPage(); return; }
    if (button.dataset.requestTab) { state.requestTab = button.dataset.requestTab; state.status = ''; state.pages = {}; loadPage(); return; }
    if (button.dataset.request) { state.requestId = button.dataset.request; loadPage(); return; }
    const action = button.dataset.action;
    if (action === 'retry') loadPage();
    else if (action === 'tenant') openDialog('tenant');
    else if (action === 'app') openDialog('app');
    else if (action === 'backends') navigate('backends');
    else if (action === 'requests-back') { state.requestId = ''; loadPage(); }
    else if (action === 'version' || action === 'deploy') { const app = list(state.data.apps?.items).find(item => item.id === button.dataset.app); if (app) openDialog(action, { app }); }
    else if (action === 'publish') { const version = list(state.data.versions?.items).find(item => item.id === button.dataset.id); if (version) openDialog('publish', { version }); }
  });
  // Clear credentials and tenant content when the document leaves the active
  // page, including browsers that keep it in their back/forward cache.
  window.addEventListener('pagehide', () => logout());
})();
