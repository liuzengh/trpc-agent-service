const { chromium } = require(process.env.TRPC_PLAYWRIGHT_MODULE || 'playwright');
const assert = require('node:assert/strict');
const http = require('node:http');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const upstream = process.argv[2] || 'http://127.0.0.1:18080';
const output = process.env.TRPC_WEB_SCREENSHOT_DIR || path.join(os.tmpdir(), 'trpc-web-screens');
fs.mkdirSync(output, { recursive: true });
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.TRPC_CHROME_EXECUTABLE || undefined, headless: true });
  let mode = 'fragmented';
  let errorStatus = 401;
  let cancelObserved = false;
  let nextSession = 1;
  const requests = [];
  const proxy = http.createServer(async (req, res) => {
    try {
      if (req.url !== '/v1/chat/completions') {
        const response = await fetch(upstream + req.url);
        res.writeHead(response.status, Object.fromEntries([...response.headers].filter(([key]) => !['content-length', 'content-encoding', 'transfer-encoding'].includes(key))));
        res.end(Buffer.from(await response.arrayBuffer()));
        return;
      }
      let raw = '';
      for await (const chunk of req) raw += chunk;
      const body = JSON.parse(raw);
      requests.push({ headers: req.headers, body });
      if (mode === 'error') {
        const code = { 401: 'unauthenticated', 403: 'forbidden', 409: 'session_busy' }[errorStatus];
        res.writeHead(errorStatus, { 'Content-Type': 'application/json', 'X-Request-ID': 'qa-auth-error' });
        res.end(JSON.stringify({ error: { code, message: 'qa refusal' } }));
        return;
      }
      const session = req.headers['x-session-id'] || `qa-session-${nextSession++}`;
      res.writeHead(200, { 'Content-Type': 'text/event-stream', 'X-Session-ID': session, 'X-Agent-Revision-ID': 'echo-v1', 'X-Request-ID': 'qa-request' });
      const capturedMode = mode;
      res.on('close', () => { if (capturedMode === 'slow' && !res.writableEnded) cancelObserved = true; });
      const event = text => 'data: ' + JSON.stringify({ choices: [{ delta: { content: text } }] }) + '\r\n\r\n';
      if (mode === 'slow') {
        res.write(event('正在生成片段'));
        return;
      }
      if (mode === 'echo') {
        res.end(event('qa: ' + body.messages.at(-1).content) + 'data: [DONE]\r\n\r\n');
        return;
      }
      const data = Buffer.from('data: {"choices":[{"delta":{"role":"assistant"}}]}\r\n\r\n' + event('分片输出 <img src=x onerror=alert(1)>'));
      for (let index = 0; index < data.length; index += 7) {
        res.write(data.subarray(index, index + 7));
        await delay(8);
      }
      res.end(mode === 'truncated' ? '' : 'data: [DONE]\r\n\r\n');
    } catch (error) {
      res.destroy();
    }
  });
  await new Promise(resolve => proxy.listen(0, '127.0.0.1', resolve));
  const fakeBase = `http://127.0.0.1:${proxy.address().port}`;
  const errors = [];
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  page.setDefaultTimeout(10000);
  const waitIdle = () => page.waitForFunction(() => {
    const send = document.querySelector('#send');
    return send && !send.hidden && !send.disabled;
  });
  const sendMessage = async text => {
    await page.locator('#input').fill(text);
    await page.locator('#input').press('Enter');
  };
  const openSettings = async () => {
    if (!(await page.locator('#open-settings').isVisible())) {
      await page.locator('#toggle-sidebar').click();
    }
    await page.locator('#open-settings').click();
    await page.locator('#settings').waitFor({ state: 'visible' });
  };
  const assertNoHorizontalOverflow = async label => {
    const overflowing = await page.evaluate(() => {
      const result = [];
      if (document.documentElement.scrollWidth > innerWidth + 1) result.push('document');
      for (const node of document.querySelectorAll('#message-list .bubble-text, #input, #composer')) {
        const rect = node.getBoundingClientRect();
        if (rect.left < -1 || rect.right > innerWidth + 1 || node.scrollWidth > node.clientWidth + 1) {
          result.push(node.id || node.className);
        }
      }
      return result;
    });
    assert.deepEqual(overflowing, [], `horizontal overflow: ${label}`);
  };
  page.on('pageerror', error => errors.push(error.message));
  const realRequests = [];
  const realHeaders = [];
  page.on('request', req => {
    if (req.url().endsWith('/v1/chat/completions')) {
      realRequests.push(req.postDataJSON());
      realHeaders.push(req.headers());
    }
  });
  try {
    await page.goto(upstream);
    assert.equal(await page.locator('#stop').isVisible(), false, 'Stop must be hidden when idle');
    await page.locator('textarea').fill('网页验收');
    await page.locator('textarea').press('Enter');
    await page.getByText('echo: 网页验收', { exact: true }).waitFor();
    await waitIdle();
    const realSession = await page.locator('#meta-session').textContent();
    assert(realSession && realSession !== '—');
    await page.locator('textarea').fill('继续会话');
    await page.locator('textarea').press('Enter');
    await page.getByText('echo: 继续会话', { exact: true }).waitFor();
    await waitIdle();
    assert.equal(realRequests.length, 2);
    assert.equal(realHeaders[0]['x-session-id'], undefined);
    assert.equal(realHeaders[1]['x-session-id'], realSession);
    assert(realRequests.every(body => body.messages.length === 1));
    await page.screenshot({ path: output + '/desktop.png', fullPage: true });

    await page.goto(fakeBase);
    await page.locator('textarea').fill('分片检查');
    await page.locator('textarea').press('Enter');
    await page.getByText('分片输出 <img src=x onerror=alert(1)>', { exact: true }).waitFor();
    assert.equal(await page.locator('img[src="x"]').count(), 0);
    await waitIdle();
    const firstSession = await page.locator('#meta-session').textContent();
    assert.equal(requests.at(-1).headers['x-session-id'], undefined);

    mode = 'echo';
    await page.locator('#new-chat').click();
    assert.equal(await page.locator('#message-list > li').count(), 0);
    await sendMessage('第二个会话');
    await page.getByText('qa: 第二个会话', { exact: true }).waitFor();
    await waitIdle();
    assert.equal(requests.at(-1).headers['x-session-id'], undefined, 'new conversation must not reuse a session');
    const secondSession = await page.locator('#meta-session').textContent();
    assert.notEqual(secondSession, firstSession);
    await page.locator('#conversation-list').getByRole('button', { name: '分片检查', exact: true }).click();
    await page.getByText('分片输出 <img src=x onerror=alert(1)>', { exact: true }).waitFor();
    assert.equal(await page.getByText('qa: 第二个会话', { exact: true }).count(), 0);
    await sendMessage('回到原会话');
    await page.getByText('qa: 回到原会话', { exact: true }).waitFor();
    await waitIdle();
    assert.equal(requests.at(-1).headers['x-session-id'], firstSession);

    mode = 'slow';
    const beforeSlow = requests.length;
    await page.locator('textarea').fill('停止检查');
    await page.locator('textarea').press('Enter');
    await page.getByText('正在生成片段', { exact: true }).waitFor();
    assert.equal(await page.locator('#send').isVisible(), false, 'Send must be hidden while streaming');
    await page.locator('#input').fill('重复提交检查');
    await page.locator('#input').press('Enter');
    await page.locator('#input').press('Enter');
    await delay(150);
    assert.equal(requests.length, beforeSlow + 1, 'a pending stream must prevent a second request');
    await page.getByRole('button', { name: /停止/ }).click();
    await delay(250);
    await waitIdle();
    assert(cancelObserved, 'Stop must abort the HTTP stream');
    assert.equal(requests.at(-1).headers['x-session-id'], firstSession);
    await page.screenshot({ path: output + '/cancel.png', fullPage: true });

    mode = 'truncated';
    const beforeEOF = requests.length;
    await sendMessage('流提前结束');
    const incomplete = page.locator('#message-list > li').last().locator('.message-note.error');
    await incomplete.waitFor();
    assert.match(await incomplete.innerText(), /中断|不完整|未完成|没有正常结束|EOF/);
    assert.match(await page.locator('#message-list > li').last().innerText(), /分片输出/);
    await waitIdle();
    assert.equal(requests.length, beforeEOF + 1, 'premature EOF must not silently retry');
    await page.screenshot({ path: output + '/truncated.png', fullPage: true });

    await page.locator('#input').fill('旧凭据下的未发送草稿');
    await openSettings();
    await page.locator('#app-id').fill('invalid app');
    assert.equal(await page.locator('#app-id').evaluate(field => field.checkValidity()), false, 'invalid App ID must fail browser validation');
    await page.locator('#app-id').fill('echo');
    assert.equal(await page.locator('#app-id').evaluate(field => field.checkValidity()), true);
    await page.locator('#credential').fill('qa-replacement-key-not-a-secret');
    await page.locator('#settings').getByRole('button', { name: '保存', exact: true }).click();
    await page.locator('#settings').waitFor({ state: 'hidden' });
    await page.waitForFunction(() => document.querySelector('#message-list').childElementCount === 0);
    assert.equal(await page.locator('#input').inputValue(), '', 'changing credentials must clear an unsent draft');
    assert.equal(await page.locator('#conversation-list button').count(), 1);
    assert.equal(await page.locator('#conversation-list').getByRole('button', { name: '分片检查', exact: true }).count(), 0);
    mode = 'echo';
    await sendMessage('更换凭据后的会话');
    await page.getByText('qa: 更换凭据后的会话', { exact: true }).waitFor();
    await waitIdle();
    assert.equal(requests.at(-1).headers.authorization, 'Bearer qa-replacement-key-not-a-secret');
    assert.equal(requests.at(-1).headers['x-session-id'], undefined, 'changed credentials must start without the previous session');

    const savedApp = requests.at(-1).headers['x-agent-app-id'];
    const savedSession = await page.locator('#meta-session').textContent();
    await openSettings();
    await page.locator('#app-id').fill('qa-canceled-app');
    await page.locator('#credential').fill('qa-canceled-key-not-a-secret');
    await page.locator('#credential').press('Escape');
    await page.locator('#settings').waitFor({ state: 'hidden' });
    assert.equal(await page.getByText('qa: 更换凭据后的会话', { exact: true }).count(), 1, 'Escape after a prior Save must preserve history');
    assert.equal(await page.locator('#meta-session').textContent(), savedSession);
    await sendMessage('取消设置后的会话');
    await page.getByText('qa: 取消设置后的会话', { exact: true }).waitFor();
    await waitIdle();
    assert.equal(requests.at(-1).headers.authorization, 'Bearer qa-replacement-key-not-a-secret');
    assert.equal(requests.at(-1).headers['x-agent-app-id'], savedApp);
    assert.equal(requests.at(-1).headers['x-session-id'], savedSession);

    mode = 'error';
    for (const [status, description] of [[401, /凭据|认证|鉴权/], [403, /无权/], [409, /正在处理|冲突/]]) {
      errorStatus = status;
      await sendMessage('错误检查 ' + status);
      const authError = page.locator('#message-list > li').last().locator('.message-note.error');
      await authError.waitFor();
      const note = await authError.innerText();
      assert(note.includes(String(status)));
      assert.match(note, description);
      assert.match(note, /qa-auth-error/);
      await waitIdle();
    }
    assert.equal(await page.evaluate(() => localStorage.length + sessionStorage.length), 0);
    await page.screenshot({ path: output + '/error.png', fullPage: true });

    await waitIdle();
    await page.locator('#input').fill('断开前未发送的草稿');
    await openSettings();
    await page.locator('#disconnect').click();
    await page.locator('#settings').waitFor({ state: 'hidden' });
    await page.waitForFunction(() => document.querySelector('#message-list').childElementCount === 0);
    await page.waitForFunction(() => document.querySelector('#credential').value === '');
    assert.equal(await page.locator('#credential').inputValue(), '', 'disconnect must clear the password DOM value');
    assert.equal(await page.locator('#input').inputValue(), '', 'disconnect must clear an unsent draft');
    assert.equal(await page.locator('#send').isDisabled(), true);

    for (const width of [390, 320]) {
      await page.setViewportSize({ width, height: 844 });
      await page.goto(upstream);
      await page.locator('textarea').fill('手机端检查');
      await page.locator('textarea').press('Enter');
      await page.getByText('echo: 手机端检查', { exact: true }).waitFor();
      await waitIdle();
      const overflow = await page.evaluate(() => document.documentElement.scrollWidth > innerWidth);
      assert.equal(overflow, false, `horizontal overflow at ${width}px`);
      await page.screenshot({ path: `${output}/mobile-${width}.png`, fullPage: true });
    }

    mode = 'echo';
    await page.goto(fakeBase);
    const longText = '长文本检查 '.repeat(15) + 'https://example.test/' + 'a'.repeat(240);
    await sendMessage(longText);
    await page.getByText('qa: ' + longText, { exact: true }).waitFor();
    await waitIdle();
    await assertNoHorizontalOverflow('320px long message');
    await page.screenshot({ path: output + '/mobile-320-long-text.png', fullPage: true });
    await openSettings();
    await page.locator('#app-id').fill('a'.repeat(128));
    await page.locator('#credential').fill('qa-only-'.repeat(40));
    const outsideDialog = await page.locator('#settings').evaluate(dialog => {
      const bounds = dialog.getBoundingClientRect();
      const result = [];
      if (bounds.left < 0 || bounds.right > innerWidth || bounds.top < 0 || bounds.bottom > innerHeight) result.push('dialog');
      if (dialog.scrollWidth > dialog.clientWidth + 1) result.push('dialog overflow');
      for (const node of dialog.querySelectorAll('input, button')) {
        const rect = node.getBoundingClientRect();
        if (rect.left < bounds.left || rect.right > bounds.right || rect.top < bounds.top || rect.bottom > bounds.bottom) {
          result.push(node.id || node.textContent.trim());
        }
      }
      return result;
    });
    assert.deepEqual(outsideDialog, [], 'settings controls must fit inside the 320px viewport');
    await page.screenshot({ path: output + '/mobile-320-settings.png', fullPage: true });
    await page.locator('#settings').getByRole('button', { name: '取消', exact: true }).click();
    assert(requests.every(request => request.body.messages.length === 1));
    assert.deepEqual(errors, []);
    console.log(JSON.stringify({ result: 'PASS', checks: ['real-send-and-continuation', 'latest-message-only', 'new-and-switched-conversations', 'credential-change-clears-history', 'settings-escape-after-save', 'disconnect-clears-password-and-draft', 'fragmented-utf8-sse', 'premature-eof-fails', 'text-xss', 'single-inflight-request', 'stop-aborts', 'auth-error', 'no-browser-persistence', 'desktop-mobile-layout', '320px-long-text-and-settings'], screenshots: output }));
  } finally {
    await page.close();
    await browser.close();
    proxy.closeAllConnections();
    await new Promise(resolve => proxy.close(resolve));
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
