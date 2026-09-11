<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { listMessages, listSessions, type LedgerMessage, type LedgerSession } from '../api/history'
import { useAuthStore } from '../stores/auth'
import { useTenantStore } from '../stores/tenant'
import { formatBeijingTime } from '../utils/time'

const authStore = useAuthStore()
const tenantStore = useTenantStore()
// 会话历史按角色做行级隔离（后端强制，前端同步隐藏过滤框）：
// owner 全部租户 / admin 本租户全部成员 / member 仅自己产生的会话。
const isOwner = computed(() => authStore.userRole === 'owner')
const isMember = computed(() => authStore.userRole === 'member')
const scopeHint = computed(() =>
  isOwner.value
    ? ''
    : isMember.value
      ? `仅显示我参与过的会话（租户 ${authStore.tenantId}）`
      : `本租户 ${authStore.tenantId} 的全部会话`,
)

// 租户过滤与全局租户同源（owner 可选任意租户，其他角色只会拿到自己那一个）。
const tenantId = computed({
  get: () => tenantStore.currentTenantId || authStore.tenantId,
  set: (v: string) => tenantStore.setCurrentTenant(v),
})
const sessions = ref<LedgerSession[]>([])
const sessionsLoading = ref(false)
const active = ref<LedgerSession | null>(null)
const messages = ref<LedgerMessage[]>([])
const msgsLoading = ref(false)
const oldestTurn = ref(0)
const hasMore = ref(false)

onMounted(async () => {
  await tenantStore.fetch()
  await refresh()
})

async function refresh() {
  sessionsLoading.value = true
  try {
    sessions.value = await listSessions(tenantId.value)
  } catch (e) {
    ElMessage.error(String(e))
  } finally {
    sessionsLoading.value = false
  }
}

async function open(row: LedgerSession) {
  active.value = row
  messages.value = []
  await loadOlder(true)
}

async function loadOlder(first = false) {
  if (!active.value) return
  msgsLoading.value = true
  try {
    const page = await listMessages(active.value.session_id, first ? 0 : oldestTurn.value, 50)
    messages.value = first ? page : [...messages.value, ...page]
    oldestTurn.value = page.length ? Math.min(...page.map((m) => m.turn_timestamp)) : oldestTurn.value
    hasMore.value = page.length === 50
  } catch (e) {
    ElMessage.error(String(e))
  } finally {
    msgsLoading.value = false
  }
}

function roleTag(r: string) {
  return r === 'USER' ? 'warning' : 'success'
}

function fmtTime(v?: string) {
  return formatBeijingTime(v)
}
</script>

<template>
  <main class="history-page">
    <h1>会话历史</h1>
    <p class="hint">业务对话账本（chat_messages）：每轮 USER + ASSISTANT，turn 分页；工具调用细节见框架 session_events 与审计日志。</p>
    <div class="toolbar">
      <el-select v-if="isOwner" v-model="tenantId" placeholder="选择租户" class="filter" filterable @change="refresh">
        <el-option v-for="t in tenantStore.tenants" :key="t.id" :label="`${t.name || t.id} (${t.id})`" :value="t.id" />
      </el-select>
      <el-tag v-else type="info">{{ scopeHint }}</el-tag>
      <el-button type="primary" @click="refresh">查询</el-button>
    </div>

    <div class="split">
      <el-table v-loading="sessionsLoading" :data="sessions" border class="left" highlight-current-row @row-click="open">
        <el-table-column prop="session_id" label="会话" width="150" show-overflow-tooltip />
        <el-table-column prop="tenant_id" label="租户" width="110" />
        <el-table-column prop="agent_id" label="Agent" width="120" />
        <el-table-column prop="member_id" label="成员" width="100" show-overflow-tooltip />
        <el-table-column prop="channel" label="通道" width="80" />
        <el-table-column label="最近消息" width="170">
          <template #default="{ row }">{{ fmtTime(row.last_message_at) }}</template>
        </el-table-column>
      </el-table>
      <div class="right">
        <template v-if="active">
          <div class="detail-head">
            <strong>{{ active.session_id }}</strong>
            <span class="muted"> · {{ active.agent_id }} / {{ active.member_id }} / {{ active.channel }}</span>
          </div>
          <div v-loading="msgsLoading" class="detail-body">
            <div v-for="m in messages" :key="m.id" class="row">
              <el-tag :type="roleTag(m.role)" size="small">{{ m.role }}</el-tag>
              <div class="bubble">{{ m.content }}</div>
            </div>
            <el-button v-if="hasMore" class="older" size="small" @click="loadOlder()">加载更早</el-button>
            <el-empty v-if="!messages.length" description="暂无消息" :image-size="50" />
          </div>
        </template>
        <el-empty v-else description="选择左侧会话查看对话" :image-size="70" />
      </div>
    </div>
  </main>
</template>

<style scoped>
.history-page {
  padding: 24px;
}
.hint {
  color: #909399;
  font-size: 13px;
  margin-top: -8px;
}
.toolbar {
  display: flex;
  gap: 8px;
  margin-bottom: 12px;
}
.filter {
  width: 240px;
}
.split {
  display: flex;
  gap: 12px;
  align-items: stretch;
}
.left {
  flex: 1.2;
  min-height: 300px;
}
.right {
  flex: 1;
  border: 1px solid var(--el-border-color);
  border-radius: 8px;
  padding: 12px;
  background: var(--el-fill-color-light);
  max-height: calc(100vh - 300px);
  overflow-y: auto;
}
.detail-head {
  margin-bottom: 10px;
  font-size: 13px;
}
.muted {
  color: #909399;
  font-weight: 400;
}
.row {
  display: flex;
  gap: 8px;
  align-items: flex-start;
  margin-bottom: 10px;
}
.bubble {
  background: var(--el-bg-color);
  border: 1px solid var(--el-border-color-lighter);
  border-radius: 6px;
  padding: 6px 10px;
  font-size: 13px;
  white-space: pre-wrap;
  word-break: break-word;
  flex: 1;
  line-height: 1.6;
}
.older {
  margin-top: 4px;
}
</style>
