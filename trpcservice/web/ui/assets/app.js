'use strict';

// tRPC Agent web chat.
//
// Everything here happens in this page: the conversations, their titles and the
// credential live in memory for as long as the tab is open, and are never
// written to storage, to the URL or to a log. The server keeps the real
// history — this page sends one turn per request and identifies the
// conversation with the session id the server gave it.

const CHAT_PATH = '/v1/chat/completions';
const MODEL = 'deterministic-echo';
const DEFAULT_APP_ID = 'echo';
// The placeholder chat key published in the project README. It is named after
// what it is, it unlocks a local demo tenant, and it is here so the page works
// on a fresh checkout. Any other credential is typed into the settings dialog.
const DEFAULT_CREDENTIAL = 'local-development-key-not-a-secret';
// How close to the bottom counts as "following along". Above it, new output
// must not move the reader's view.
const NEAR_BOTTOM_PX = 64;
const TITLE_MAX_CHARS = 24;

const state = {
  appId: DEFAULT_APP_ID,
  credential: DEFAULT_CREDENTIAL,
  conversations: [],
  activeId: '',
  // The one request in flight, or null. One at a time: a second run of the
  // same session is refused by the server anyway, and a second run of another
  // one would leave two streams writing into one page.
  run: null,
  nextId: 1,
};

const layout = document.getElementById('layout');
const scrim = document.getElementById('scrim');
const sidebar = document.getElementById('sidebar');
const toggleSidebar = document.getElementById('toggle-sidebar');
const closeSidebar = document.getElementById('close-sidebar');
const newChatButton = document.getElementById('new-chat');
const conversationList = document.getElementById('conversation-list');
const settingsButton = document.getElementById('open-settings');
const chatTitle = document.getElementById('chat-title');
const messagesView = document.getElementById('messages');
const messageList = document.getElementById('message-list');
const emptyState = document.getElementById('empty');
const emptyHint = document.getElementById('empty-hint');
const statusLine = document.getElementById('status');
const composer = document.getElementById('composer');
const input = document.getElementById('input');
const sendButton = document.getElementById('send');
const stopButton = document.getElementById('stop');
const metaApp = document.getElementById('meta-app');
const metaSession = document.getElementById('meta-session');
const metaRevision = document.getElementById('meta-revision');
const metaRequest = document.getElementById('meta-request');
const settingsDialog = document.getElementById('settings');
const appIdInput = document.getElementById('app-id');
const credentialInput = document.getElementById('credential');
const disconnectButton = document.getElementById('disconnect');

// The live nodes of the reply being streamed, when that reply is on screen.
let streamText = null;
let streamTyping = null;

// State -----------------------------------------------------------------

function createConversation() {
  return {
    id: 'c' + state.nextId++,
    title: '',
    sessionId: '',
    revisionId: '',
    requestId: '',
    messages: [],
  };
}

function activeConversation() {
  return state.conversations.find((conversation) => conversation.id === state.activeId);
}

function addMessage(conversation, message) {
  conversation.messages.push(message);
  return message;
}

function resetConversations() {
  const conversation = createConversation();
  state.conversations = [conversation];
  state.activeId = conversation.id;
}

// clearDraft empties the composer. An unsent draft was written under the app
// and credential that were set when it was typed, and clearing the
// conversations without clearing it would leave it to be sent under the next
// ones.
function clearDraft() {
  input.value = '';
  autoGrow();
}

function conversationTitle(text) {
  const line = text.split('\n', 1)[0].trim() || text.trim();
  const characters = Array.from(line);
  if (characters.length <= TITLE_MAX_CHARS) {
    return line;
  }
  return characters.slice(0, TITLE_MAX_CHARS).join('') + '…';
}

// Rendering -------------------------------------------------------------

function renderAll() {
  renderConversations();
  renderMessages();
  renderMeta();
  updateControls();
}

function renderConversations() {
  const busy = Boolean(state.run);
  conversationList.replaceChildren();
  for (const conversation of state.conversations) {
    const item = document.createElement('li');
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'conversation-item';
    button.textContent = conversation.title || '新对话';
    button.title = conversation.title || '新对话';
    button.disabled = busy;
    if (conversation.id === state.activeId) {
      button.setAttribute('aria-current', 'true');
    }
    button.addEventListener('click', () => selectConversation(conversation.id));
    item.append(button);
    conversationList.append(item);
  }
}

function renderMessages() {
  const conversation = activeConversation();
  streamText = null;
  streamTyping = null;
  messageList.replaceChildren();
  for (const message of conversation.messages) {
    messageList.append(messageNode(message));
  }
  emptyState.hidden = conversation.messages.length > 0;
  // Nothing is explained here while the page can send: the composer is below,
  // and it is its own instruction. The hint exists for the one state the user
  // cannot act their way out of without going to settings.
  emptyHint.textContent = state.credential ? '' : '未连接：请在设置中填写对话凭据。';
  emptyHint.hidden = !emptyHint.textContent;
  chatTitle.textContent = conversation.title || '新对话';
}

// messageNode builds one message. Every string that came from a user or from a
// model is written with textContent: this page never turns either into markup.
function messageNode(message) {
  const item = document.createElement('li');
  item.className = 'message ' + message.role;

  const role = document.createElement('span');
  role.className = 'message-role';
  role.textContent = message.role === 'user' ? '你' : 'Agent';
  item.append(role);

  const bubble = document.createElement('div');
  bubble.className = 'bubble';
  const text = document.createElement('p');
  text.className = 'bubble-text';
  text.textContent = message.text;
  bubble.append(text);
  item.append(bubble);

  if (message.status === 'streaming') {
    streamText = text;
    if (!message.text) {
      streamTyping = typingNode();
      bubble.append(streamTyping);
    }
  }
  if (message.note) {
    const note = document.createElement('p');
    note.className = 'message-note' + (message.noteKind === 'error' ? ' error' : '');
    note.textContent = message.note;
    item.append(note);
  }
  if (message.role === 'assistant' && message.status !== 'streaming' && message.text) {
    const actions = document.createElement('div');
    actions.className = 'message-actions';
    actions.append(copyButton(message));
    item.append(actions);
  }
  return item;
}

function typingNode() {
  const typing = document.createElement('span');
  typing.className = 'typing';
  typing.setAttribute('aria-hidden', 'true');
  for (let i = 0; i < 3; i++) {
    typing.append(document.createElement('span'));
  }
  return typing;
}

function copyButton(message) {
  const button = document.createElement('button');
  button.type = 'button';
  button.className = 'copy-button';
  button.title = '复制回复';
  button.setAttribute('aria-label', '复制回复');
  const icon = document.createElement('span');
  icon.className = 'icon icon-copy';
  icon.setAttribute('aria-hidden', 'true');
  const label = document.createElement('span');
  label.textContent = '复制';
  button.append(icon, label);
  button.addEventListener('click', async () => {
    try {
      await navigator.clipboard.writeText(message.text);
      icon.className = 'icon icon-check';
      label.textContent = '已复制';
      window.setTimeout(() => {
        icon.className = 'icon icon-copy';
        label.textContent = '复制';
      }, 1500);
    } catch (error) {
      announce('复制失败，请手动选择文本。', true);
    }
  });
  return button;
}

function renderMeta() {
  const conversation = activeConversation();
  metaApp.textContent = state.appId || '—';
  metaSession.textContent = conversation.sessionId || '—';
  metaRevision.textContent = conversation.revisionId || '—';
  metaRequest.textContent = conversation.requestId || '—';
}

function updateControls() {
  const busy = Boolean(state.run);
  sendButton.hidden = busy;
  sendButton.disabled = busy || !state.credential;
  stopButton.hidden = !busy;
  newChatButton.disabled = busy;
  settingsButton.disabled = busy;
}

function announce(text, isError) {
  statusLine.textContent = text;
  statusLine.classList.toggle('error', Boolean(isError));
}

function nearBottom() {
  return messagesView.scrollHeight - messagesView.scrollTop - messagesView.clientHeight
    < NEAR_BOTTOM_PX;
}

function scrollToBottom() {
  messagesView.scrollTop = messagesView.scrollHeight;
}

// Sending ---------------------------------------------------------------

function submitComposer() {
  const text = input.value.trim();
  if (!text || state.run) {
    return;
  }
  if (!state.credential) {
    announce('请先在设置中填写对话凭据。', true);
    openSettings();
    return;
  }
  input.value = '';
  autoGrow();
  send(text);
}

async function send(text) {
  const conversation = activeConversation();
  addMessage(conversation, { role: 'user', text: text, status: 'done', note: '' });
  const reply = addMessage(conversation, {
    role: 'assistant',
    text: '',
    status: 'streaming',
    note: '',
    requestId: '',
  });
  if (!conversation.title) {
    conversation.title = conversationTitle(text);
  }
  const run = { conversationId: conversation.id, controller: new AbortController(), reply: reply };
  state.run = run;
  renderAll();
  scrollToBottom();
  announce('正在生成…');

  try {
    const headers = {
      'Content-Type': 'application/json',
      'Authorization': 'Bearer ' + state.credential,
      'X-Agent-App-ID': state.appId,
    };
    // A continuing conversation names its session; a new one lets the server
    // mint the id and reads it back off the response.
    if (conversation.sessionId) {
      headers['X-Session-ID'] = conversation.sessionId;
    }
    const response = await fetch(CHAT_PATH, {
      method: 'POST',
      headers: headers,
      cache: 'no-store',
      signal: run.controller.signal,
      // Only the new turn. The server persists this session's history, so
      // replaying a local copy of it would append every earlier message again.
      body: JSON.stringify({
        model: MODEL,
        stream: true,
        messages: [{ role: 'user', content: text }],
      }),
    });
    // Before any of the body: these three are what a user can quote about this
    // run, and a stream that fails half way must not take them with it.
    captureRunHeaders(conversation, reply, response.headers);
    if (!response.ok) {
      await failFromResponse(reply, response);
      return;
    }
    const contentType = response.headers.get('Content-Type') || '';
    if (!contentType.includes('text/event-stream') || !response.body) {
      fail(reply, '服务端没有返回流式响应。');
      return;
    }
    await consumeStream(response.body, run);
  } catch (error) {
    if (error && error.name === 'AbortError') {
      reply.status = 'canceled';
      reply.note = reply.text
        ? '已停止生成，以上内容是不完整的回复。' + requestSuffix(reply)
        : '已停止生成，没有收到回复。' + requestSuffix(reply);
      reply.noteKind = 'muted';
    } else {
      // Never resent on its own: the request may well have started a run on the
      // server, and a silent retry would ask for a second one.
      fail(reply, '请求发送失败，可能是网络中断或服务未运行。' + requestSuffix(reply));
    }
  } finally {
    if (reply.status === 'streaming') {
      reply.status = 'incomplete';
      reply.note = reply.note || ('回复没有正常结束。' + requestSuffix(reply));
      reply.noteKind = 'error';
    }
    state.run = null;
    announceOutcome(reply);
    // Guarded on the conversation still being the one on screen, so a stream
    // that ends late cannot repaint a conversation the user has moved on from.
    // Its text went into the message object either way, and is there when that
    // conversation is selected again.
    if (run.conversationId === state.activeId) {
      const stick = nearBottom();
      renderMessages();
      if (stick) {
        scrollToBottom();
      }
    }
    renderConversations();
    renderMeta();
    updateControls();
  }
}

function captureRunHeaders(conversation, reply, headers) {
  const sessionId = headers.get('X-Session-ID');
  if (sessionId) {
    conversation.sessionId = sessionId;
  }
  const revisionId = headers.get('X-Agent-Revision-ID');
  if (revisionId) {
    conversation.revisionId = revisionId;
  }
  const requestId = headers.get('X-Request-ID');
  if (requestId) {
    conversation.requestId = requestId;
    reply.requestId = requestId;
  }
  renderMeta();
}

// consumeStream reads the SSE body.
//
// The reader hands back arbitrary byte chunks, so nothing here assumes that a
// chunk is a line, that a line is an event, or that a multi-byte character
// arrived in one piece: the decoder is told the input is streamed, lines are
// cut out of a buffer that survives across reads, and an event is only
// dispatched on the blank line that ends it.
async function consumeStream(body, run) {
  const reply = run.reply;
  const reader = body.getReader();
  const decoder = new TextDecoder('utf-8');
  let buffer = '';
  let dataLines = [];
  let sawDone = false;
  let malformed = false;
  let streamError = '';

  function dispatch() {
    if (dataLines.length === 0) {
      return;
    }
    const data = dataLines.join('\n');
    dataLines = [];
    if (data === '[DONE]') {
      sawDone = true;
      return;
    }
    let frame = null;
    try {
      frame = JSON.parse(data);
    } catch (error) {
      malformed = true;
      return;
    }
    if (frame && frame.error) {
      streamError = typeof frame.error.message === 'string' && frame.error.message
        ? frame.error.message
        : '服务端在流中报告了错误';
      return;
    }
    const choice = frame && Array.isArray(frame.choices) ? frame.choices[0] : null;
    const delta = choice ? choice.delta : null;
    // Role-only frames and empty deltas are ordinary and add nothing.
    if (delta && typeof delta.content === 'string' && delta.content !== '') {
      reply.text += delta.content;
      showStreamedText(run);
    }
  }

  function consumeLine(line) {
    if (line === '') {
      dispatch();
      return;
    }
    if (line.charAt(0) === ':') {
      return;
    }
    const colon = line.indexOf(':');
    const field = colon === -1 ? line : line.slice(0, colon);
    let value = colon === -1 ? '' : line.slice(colon + 1);
    if (value.charAt(0) === ' ') {
      value = value.slice(1);
    }
    if (field === 'data') {
      dataLines.push(value);
    }
  }

  function consumeBuffered() {
    let index = buffer.indexOf('\n');
    while (index !== -1) {
      let line = buffer.slice(0, index);
      buffer = buffer.slice(index + 1);
      if (line.charAt(line.length - 1) === '\r') {
        line = line.slice(0, -1);
      }
      consumeLine(line);
      index = buffer.indexOf('\n');
    }
  }

  for (;;) {
    const chunk = await reader.read();
    if (chunk.done) {
      break;
    }
    buffer += decoder.decode(chunk.value, { stream: true });
    consumeBuffered();
    if (sawDone) {
      // The marker is the end of the answer. Let go of the body rather than
      // waiting for a close that a proxy may sit on.
      try {
        await reader.cancel();
      } catch (error) {
        // Cancelling an already-finished body is not a failure.
      }
      break;
    }
  }
  buffer += decoder.decode();
  if (!sawDone && buffer !== '') {
    // End of body ends the last line and the last event with it, so a stream
    // whose final newline never arrived is still read rather than discarded.
    consumeBuffered();
    consumeLine(buffer);
    buffer = '';
    dispatch();
  }

  if (streamError) {
    fail(reply, '服务端在流中返回错误：' + streamError + requestSuffix(reply));
    return;
  }
  if (!sawDone) {
    reply.status = 'incomplete';
    reply.note = '连接在收到结束标记前中断，以上回复可能不完整。' + requestSuffix(reply);
    reply.noteKind = 'error';
    return;
  }
  if (malformed) {
    reply.status = 'incomplete';
    reply.note = '部分数据帧无法解析，以上回复可能不完整。' + requestSuffix(reply);
    reply.noteKind = 'error';
    return;
  }
  reply.status = 'done';
  if (!reply.text) {
    reply.note = '本次回复为空。' + requestSuffix(reply);
    reply.noteKind = 'muted';
  }
}

function showStreamedText(run) {
  if (run.conversationId !== state.activeId || !streamText) {
    return;
  }
  const stick = nearBottom();
  streamText.textContent = run.reply.text;
  if (streamTyping) {
    streamTyping.remove();
    streamTyping = null;
  }
  if (stick) {
    scrollToBottom();
  }
}

function fail(reply, note) {
  reply.status = 'error';
  reply.note = note;
  reply.noteKind = 'error';
}

async function failFromResponse(reply, response) {
  const body = await readErrorBody(response);
  const parts = [describeStatus(response.status, body.code)];
  if (body.message) {
    parts.push('服务端说明：' + body.message);
  }
  const retryAfter = response.headers.get('Retry-After');
  if (response.status === 409 && retryAfter) {
    parts.push('可在 ' + retryAfter + ' 秒后重试。');
  }
  fail(reply, parts.join(' ') + requestSuffix(reply));
}

async function readErrorBody(response) {
  const contentType = response.headers.get('Content-Type') || '';
  try {
    if (contentType.includes('application/json')) {
      const body = await response.json();
      const error = body && typeof body.error === 'object' ? body.error : null;
      if (!error) {
        return { code: '', message: '' };
      }
      const code = typeof error.code === 'string' ? error.code : '';
      return {
        code: code || (typeof error.type === 'string' ? error.type : ''),
        message: typeof error.message === 'string' ? error.message : '',
      };
    }
    const text = await response.text();
    return { code: '', message: text.trim().slice(0, 300) };
  } catch (error) {
    return { code: '', message: '' };
  }
}

function describeStatus(status, code) {
  if (code === 'session_busy') {
    return '这个会话正在处理上一个请求（409）。';
  }
  if (code === 'pin_conflict') {
    return '这个会话已经固定在另一个 Revision 上（409）。';
  }
  switch (status) {
    case 400:
      return '请求被拒绝（400）。';
    case 401:
      return '凭据无效或缺失（401），请在设置中检查对话凭据。';
    case 403:
      return '当前凭据无权访问这个 Agent App（403），请在设置中检查 App ID。';
    case 404:
      return '接口不存在（404）。';
    case 405:
      return '请求方法不被允许（405）。';
    case 409:
      return '请求与会话当前状态冲突（409）。';
    case 413:
      return '请求内容过大（413）。';
    case 429:
      return '请求过于频繁（429），请稍后再试。';
    case 500:
      return '服务端内部错误（500）。';
    case 503:
      // Only that it is unavailable. A 503 can also come from something in
      // front of this platform, after the request was already passed on, so
      // this is not a place to promise that nothing happened.
      return '服务暂时不可用（503）。';
    default:
      return '请求失败（' + status + '）。';
  }
}

function requestSuffix(reply) {
  return reply.requestId ? '（请求 ID：' + reply.requestId + '）' : '';
}

function announceOutcome(reply) {
  if (reply.status === 'done') {
    announce('');
    return;
  }
  if (reply.status === 'canceled') {
    announce('已停止生成。');
    return;
  }
  announce('这次回复没有正常完成，详情见对话中的提示。', true);
}

// Commands --------------------------------------------------------------

function startConversation() {
  if (state.run) {
    return;
  }
  const current = activeConversation();
  if (!current || current.messages.length > 0) {
    const conversation = createConversation();
    state.conversations.unshift(conversation);
    state.activeId = conversation.id;
    renderAll();
  }
  closeDrawer();
  input.focus();
}

function selectConversation(id) {
  if (state.run || id === state.activeId) {
    closeDrawer();
    return;
  }
  state.activeId = id;
  renderAll();
  scrollToBottom();
  announce('');
  closeDrawer();
  input.focus();
}

function stopRun() {
  if (state.run) {
    state.run.controller.abort();
  }
}

function openSettings() {
  appIdInput.value = state.appId;
  credentialInput.value = state.credential;
  settingsDialog.showModal();
}

function applySettings() {
  const appId = appIdInput.value.trim();
  const credential = credentialInput.value.trim();
  if (!appId || !credential) {
    return;
  }
  // A different credential is a different user, and a different app is a
  // different agent: neither may inherit the conversations of the one before,
  // because their sessions live on the server under the old scope.
  const changed = appId !== state.appId || credential !== state.credential;
  state.appId = appId;
  state.credential = credential;
  if (changed) {
    resetConversations();
    clearDraft();
    announce('设置已更新，本地会话已清空。');
  }
  renderAll();
}

function disconnect() {
  state.credential = '';
  resetConversations();
  clearDraft();
  settingsDialog.close('cancel');
  renderAll();
  announce('已断开：凭据和本地会话都已清除。');
}

function openDrawer() {
  layout.classList.add('drawer-open');
  toggleSidebar.setAttribute('aria-expanded', 'true');
  closeSidebar.focus();
}

function closeDrawer() {
  if (!layout.classList.contains('drawer-open')) {
    return;
  }
  layout.classList.remove('drawer-open');
  toggleSidebar.setAttribute('aria-expanded', 'false');
}

function autoGrow() {
  input.style.height = 'auto';
  input.style.height = Math.min(input.scrollHeight, 200) + 'px';
}

// Wiring ----------------------------------------------------------------

composer.addEventListener('submit', (event) => {
  event.preventDefault();
  submitComposer();
});

input.addEventListener('input', autoGrow);

input.addEventListener('keydown', (event) => {
  if (event.key !== 'Enter' || event.shiftKey || event.altKey || event.ctrlKey || event.metaKey) {
    return;
  }
  // Enter while an IME is composing commits a candidate. It is not a send.
  if (event.isComposing || event.keyCode === 229) {
    return;
  }
  event.preventDefault();
  submitComposer();
});

stopButton.addEventListener('click', stopRun);
newChatButton.addEventListener('click', startConversation);
settingsButton.addEventListener('click', openSettings);
disconnectButton.addEventListener('click', disconnect);

settingsDialog.addEventListener('close', () => {
  if (settingsDialog.returnValue === 'save') {
    applySettings();
  }
  // Whatever was typed and not saved leaves with the dialog.
  appIdInput.value = state.appId;
  credentialInput.value = state.credential;
});

toggleSidebar.addEventListener('click', () => {
  if (layout.classList.contains('drawer-open')) {
    closeDrawer();
  } else {
    openDrawer();
  }
});
closeSidebar.addEventListener('click', () => {
  closeDrawer();
  toggleSidebar.focus();
});
scrim.addEventListener('click', closeDrawer);
sidebar.addEventListener('keydown', (event) => {
  if (event.key === 'Escape') {
    closeDrawer();
    toggleSidebar.focus();
  }
});

resetConversations();
renderAll();
autoGrow();
