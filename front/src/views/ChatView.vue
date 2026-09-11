<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { openChatStream, sendChat, type ChatMessageEvent, type ChatStreamHandle } from '../api/chat'
import { listMessages, listSessions, type LedgerSession } from '../api/history'
import { useAgentStore } from '../stores/agent'
import { useAuthStore } from '../stores/auth'
import { useTenantStore } from '../stores/tenant'
import { formatBeijingTime } from '../utils/time'

const agents = useAgentStore()
const authStore = useAuthStore()
const tenantStore = useTenantStore()

// 对话同样按租户隔离：owner 可在下拉里选任意租户，admin/member 固定为登录态租户
// （后端 ScopeTenant 也会强制，前端只是不再提供可编辑入口）。
const isOwner = computed(() => authStore.userRole === 'owner')

interface ChatMsg {
  /** 账本/流里的消息 ID，用于去重（SSE 推送与历史加载可能重复同一条）。 */
  id?: string
  role: 'user' | 'assistant' | 'system'
  text: string
  at?: string
}

// 租户选择直接读写全局租户（App.vue 的切换器同源），一处改动处处一致：
// Agent 列表也按它拉取，不会出现「租户 A 的会话配租户 B 的 Agent」。
const tenantId = computed({
  get: () => tenantStore.currentTenantId || authStore.tenantId,
  set: (v: string) => tenantStore.setCurrentTenant(v),
})

const agentId = ref('')
const sessionId = ref('')
const sessions = ref<LedgerSession[]>([])
const messages = ref<ChatMsg[]>([])
const input = ref('')
const sending = ref(false)
const loadingHistory = ref(false)
const connected = ref(false)
const listEl = ref<HTMLElement | null>(null)
let stream: ChatStreamHandle | null = null

// 会话下拉：账本里的历史会话 + 当前这个还没落库的新会话。
const sessionChoices = computed(() => {
  const options = sessions.value.map((s) => ({
    value: s.session_id,
    label: `${s.agent_id || '未绑定 Agent'} · ${formatBeijingTime(s.last_message_at) || '—'}`,
  }))
  if (sessionId.value && !sessions.value.some((s) => s.session_id === sessionId.value)) {
    options.unshift({ value: sessionId.value, label: '（新会话）' })
  }
  return options
})

onMounted(async () => {
  await tenantStore.fetch()
  if (isOwner.value && !tenantStore.currentTenantId && tenantStore.tenants.length > 0) {
    tenantStore.setCurrentTenant(tenantStore.tenants[0].id)
  }
  await Promise.all([agents.fetch(tenantId.value), refreshSessions()])
  newSession()
})

// 切租户等于换上下文：会话、消息、流全部作废，Agent 列表重新拉。
watch(tenantId, async (id) => {
  if (!id) return
  newSession()
  await Promise.all([agents.fetch(id), refreshSessions()])
})

// 切换会话（含点「新会话」）时把已落库的对话读回来——这正是「数据库里有回复但
// 页面上看不到」的那一半：SSE 只负责在线推送，历史要靠账本。
watch(sessionId, (id) => {
  if (id) void loadHistory(id)
})

onBeforeUnmount(() => stream?.close())

async function refreshSessions() {
  try {
    sessions.value = await listSessions(tenantId.value)
  } catch (e) {
    // 账本不可用（例如后端未配 MySQL）不该挡住实时对话。
    ElMessage.warning(`历史会话不可用：${e}`)
  }
}

function newSession() {
  detachStream()
  sessionId.value = crypto.randomUUID()
  messages.value = []
  input.value = ''
}

async function loadHistory(id: string) {
  loadingHistory.value = true
  try {
    const rows = await listMessages(id)
    // 空结果不清屏：会话刚建时账本里还没有行，而此时可能已经有一条 SSE 回复在
    // 屏幕上（时序上先到的那条不该被一次历史加载抹掉）。
    if (rows.length === 0 && messages.value.length > 0) return
    messages.value = rows
      .slice()
      .sort((a, b) => a.turn_timestamp - b.turn_timestamp || a.id - b.id)
      .map((r) => ({
        id: r.message_id,
        role: r.role === 'USER' ? ('user' as const) : ('assistant' as const),
        text: r.content,
        at: r.created_at,
      }))
    const row = sessions.value.find((s) => s.session_id === id)
    if (row?.agent_id && !agentId.value) agentId.value = row.agent_id
    await scrollDown()
  } catch (e) {
    ElMessage.error(`加载历史消息失败：${e}`)
  } finally {
    loadingHistory.value = false
  }
}

async function scrollDown() {
  await nextTick()
  listEl.value?.scrollTo({ top: listEl.value.scrollHeight })
}

function detachStream() {
  stream?.close()
  stream = null
  connected.value = false
}

function attachStream() {
  detachStream()
  stream = openChatStream(sessionId.value, {
    onOpen: () => {
      connected.value = true
    },
    onMessage: (ev: ChatMessageEvent) => {
      if (ev.message_id && messages.value.some((m) => m.id === ev.message_id)) return
      messages.value.push({ id: ev.message_id, role: 'assistant', text: ev.text })
      void scrollDown()
    },
    onError: () => {
      connected.value = false
    },
  })
}

async function send() {
  const text = input.value.trim()
  if (!text) return
  if (!tenantId.value || !agentId.value || !sessionId.value) {
    ElMessage.warning('请先选择租户与 Agent')
    return
  }
  sending.value = true
  try {
    // 订阅先于发送：否则本轮的回复可能在流建立之前就已推送，页面上永远看不到。
    if (!stream) attachStream()
    const res = await sendChat({
      tenant_id: tenantId.value,
      agent_id: agentId.value,
      session_id: sessionId.value,
      text,
    })
    messages.value.push({ id: res.message_id, role: 'user', text })
    input.value = ''
    await scrollDown()
    void refreshSessions()
  } catch (e) {
    messages.value.push({ role: 'system', text: `发送失败：${e}` })
  } finally {
    sending.value = false
  }
}

function roleClass(r: ChatMsg['role']) {
  return r === 'user' ? 'bubble-user' : r === 'assistant' ? 'bubble-assistant' : 'bubble-system'
}
</script>

<template>
  <main class="chat-page">
    <h1>Agent 对话</h1>
    <p class="hint">
      通过 admin 通道与 Agent 对话（回复以 SSE 实时到达），走与 IM 相同的 worker 链路——审批/技能/工具/RBAC 全部生效。
      每条回复同时写入对话账本，切换会话即可回看历史。
    </p>
    <div class="chat-config">
      <el-select v-if="isOwner" v-model="tenantId" placeholder="选择租户" class="cfg-item" filterable>
        <el-option v-for="t in tenantStore.tenants" :key="t.id" :label="`${t.name || t.id} (${t.id})`" :value="t.id" />
      </el-select>
      <el-tag v-else type="info" class="cfg-item">租户：{{ authStore.tenantId }}（已固定）</el-tag>
      <el-select v-model="agentId" placeholder="选择 Agent" class="cfg-item" filterable>
        <el-option v-for="a in agents.agents" :key="a.id" :label="`${a.name} (${a.id})`" :value="a.id" />
      </el-select>
      <el-select v-model="sessionId" placeholder="会话" class="cfg-item" filterable>
        <el-option v-for="s in sessionChoices" :key="s.value" :label="s.label" :value="s.value" />
      </el-select>
      <el-button @click="newSession">新会话</el-button>
    </div>

    <div ref="listEl" v-loading="loadingHistory" class="chat-list">
      <div v-for="(m, i) in messages" :key="m.id || i" class="msg">
        <span class="who">{{ m.role === 'user' ? '你' : m.role === 'assistant' ? 'Agent' : '系统' }}</span>
        <div :class="['bubble', roleClass(m.role)]">{{ m.text }}</div>
        <span v-if="m.at" class="at">{{ formatBeijingTime(m.at) }}</span>
      </div>
      <el-empty v-if="messages.length === 0" description="发一条消息开始对话（回复将以 SSE 流式到达）" :image-size="56" />
      <div v-if="connected" class="live">● 已连接</div>
    </div>

    <div class="send-row">
      <el-input v-model="input" placeholder="输入消息，回车发送" @keyup.enter="send" />
      <el-button type="primary" :loading="sending" @click="send">发送</el-button>
    </div>
  </main>
</template>

<style scoped>
.chat-page {
  padding: 24px;
  display: flex;
  flex-direction: column;
  gap: 12px;
}
.hint {
  color: #909399;
  font-size: 13px;
  margin-top: -8px;
}
.chat-config {
  display: flex;
  gap: 8px;
  align-items: center;
}
.cfg-item {
  width: 220px;
}
.chat-list {
  border: 1px solid var(--el-border-color);
  border-radius: 8px;
  height: calc(100vh - 320px);
  min-height: 240px;
  overflow-y: auto;
  padding: 12px;
  display: flex;
  flex-direction: column;
  gap: 10px;
  background: var(--el-fill-color-light);
}
.msg {
  display: flex;
  flex-direction: column;
  gap: 4px;
  max-width: 80%;
}
.who {
  font-size: 12px;
  color: #909399;
}
.at {
  font-size: 11px;
  color: #c0c4cc;
}
.bubble {
  padding: 8px 12px;
  border-radius: 8px;
  white-space: pre-wrap;
  word-break: break-word;
  font-size: 13px;
  line-height: 1.6;
}
.bubble-user {
  align-self: flex-end;
  background: var(--el-color-primary-light-8);
}
.bubble-assistant {
  align-self: flex-start;
  background: var(--el-bg-color);
  border: 1px solid var(--el-border-color-lighter);
}
.bubble-system {
  align-self: center;
  color: #e6a23c;
  background: var(--el-color-warning-light-9);
  font-size: 12px;
}
.live {
  align-self: flex-end;
  font-size: 12px;
  color: #67c23a;
}
.send-row {
  display: flex;
  gap: 8px;
}
</style>
