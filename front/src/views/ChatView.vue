<script setup lang="ts">
import { nextTick, onBeforeUnmount, onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { sendChat, openChatStream, type ChatMessageEvent } from '../api/chat'
import { useAgentStore } from '../stores/agent'

const agents = useAgentStore()

interface ChatMsg {
  role: 'user' | 'assistant' | 'system'
  text: string
}

const tenantId = ref('')
const agentId = ref('')
const sessionId = ref('')
const messages = ref<ChatMsg[]>([])
const input = ref('')
const sending = ref(false)
const connected = ref(false)
const listEl = ref<HTMLElement | null>(null)
let stream: EventSource | null = null

onMounted(async () => {
  agents.fetch()
  newSession()
})

onBeforeUnmount(() => stream?.close())

function newSession() {
  if (stream) {
    stream.close()
    stream = null
  }
  connected.value = false
  sessionId.value = crypto.randomUUID()
  messages.value = []
  input.value = ''
}

async function scrollDown() {
  await nextTick()
  listEl.value?.scrollTo({ top: listEl.value.scrollHeight })
}

function attachStream() {
  if (stream) stream.close()
  stream = openChatStream(
    sessionId.value,
    (ev: ChatMessageEvent) => {
      connected.value = true
      messages.value.push({ role: 'assistant', text: ev.text })
      void scrollDown()
    },
    () => {
      connected.value = false
    },
  )
}

async function send() {
  const text = input.value.trim()
  if (!text) return
  if (!tenantId.value.trim() || !agentId.value || !sessionId.value) {
    ElMessage.warning('请先填写租户 ID 并选择 Agent')
    return
  }
  sending.value = true
  try {
    messages.value.push({ role: 'user', text })
    input.value = ''
    void scrollDown()
    if (!stream) attachStream()
    await sendChat({
      tenant_id: tenantId.value.trim(),
      agent_id: agentId.value,
      session_id: sessionId.value,
      text,
    })
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
    <p class="hint">通过 admin 通道与 Agent 对话（SSE 实时回复），走与 IM 相同的 worker 链路——审批/技能/工具/RBAC 全部生效。</p>
    <div class="chat-config">
      <el-input v-model="tenantId" placeholder="租户 ID" class="cfg-item" />
      <el-select v-model="agentId" placeholder="选择 Agent" class="cfg-item" filterable>
        <el-option v-for="a in agents.agents" :key="a.id" :label="`${a.name} (${a.id})`" :value="a.id" />
      </el-select>
      <el-input :model-value="sessionId" class="cfg-item" placeholder="会话 ID" disabled />
      <el-button @click="newSession">新会话</el-button>
    </div>

    <div ref="listEl" class="chat-list">
      <div v-for="(m, i) in messages" :key="i" class="msg">
        <span class="who">{{ m.role === 'user' ? '你' : m.role === 'assistant' ? 'Agent' : '系统' }}</span>
        <div :class="['bubble', roleClass(m.role)]">{{ m.text }}</div>
      </div>
      <el-empty v-if="messages.length === 0" description="发一条消息开始对话（回复将以 SSE 流式到达）" :image-size="56" />
      <div v-if="connected" class="live">● 已连接</div>
    </div>

    <div class="send-row">
      <el-input
        v-model="input"
        placeholder="输入消息，回车发送"
        @keyup.enter="send"
      />
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
