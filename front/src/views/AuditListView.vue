<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { listAudit, type AuditLog } from '../api/audit'
import { formatBeijingTime } from '../utils/time'

const logs = ref<AuditLog[]>([])
const loading = ref(false)
const tenantFilter = ref('')
const autoRefresh = ref(false)
let timer: number | undefined

async function load() {
  loading.value = true
  try {
    logs.value = await listAudit(tenantFilter.value.trim(), 200)
  } catch {
    // Audit requires MySQL (GET /audit is only registered with a DB); an
    // in-memory/dev backend leaves the list empty without throwing.
    logs.value = []
  } finally {
    loading.value = false
  }
}

onMounted(() => {
  load()
  timer = window.setInterval(() => {
    if (autoRefresh.value) void load()
  }, 10_000)
})

function fmtTime(iso: string) {
  return formatBeijingTime(iso)
}

function decisionTag(d?: string) {
  if (!d) return 'info'
  if (d === 'deny' || d === 'failed') return 'danger'
  if (d === 'allow' || d === 'approve') return 'success'
  return d === 'executed' ? 'primary' : 'info'
}
</script>

<template>
  <main class="audit-page">
    <h1>审计日志</h1>
    <p class="hint">治理决策与请求记账（决策/延迟/成本/trace_id）；写入 MySQL，最多展示最近 200 条。</p>
    <div class="toolbar">
      <el-input
        v-model="tenantFilter"
        placeholder="按租户过滤（留空全部）"
        clearable
        style="width: 240px"
        @keyup.enter="load"
        @clear="load"
      />
      <el-button type="primary" :loading="loading" @click="load">查询</el-button>
      <el-checkbox v-model="autoRefresh">每 10s 自动刷新</el-checkbox>
    </div>

    <el-table v-loading="loading" :data="logs" border size="small">
      <el-table-column label="时间" width="170">
        <template #default="{ row }">{{ fmtTime(row.created_at) }}</template>
      </el-table-column>
      <el-table-column prop="tenant_id" label="租户" width="120" />
      <el-table-column prop="channel" label="渠道" width="90" />
      <el-table-column prop="session_id" label="会话" width="140" show-overflow-tooltip />
      <el-table-column prop="agent_name" label="Agent" width="120" show-overflow-tooltip />
      <el-table-column prop="tool_name" label="工具" width="120" show-overflow-tooltip />
      <el-table-column label="决策" width="100">
        <template #default="{ row }">
          <el-tag :type="decisionTag(row.decision)" size="small">{{ row.decision || '-' }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column label="延迟" width="90">
        <template #default="{ row }">
          {{ row.latency_ms != null ? `${row.latency_ms}ms` : '-' }}
        </template>
      </el-table-column>
      <el-table-column label="成本" width="110">
        <template #default="{ row }">{{ row.cost != null ? `${row.cost} tokens` : '-' }}</template>
      </el-table-column>
      <el-table-column prop="error_type" label="错误" show-overflow-tooltip />
      <el-table-column prop="trace_id" label="Trace" width="180" show-overflow-tooltip />
    </el-table>
    <el-empty v-if="!loading && logs.length === 0" description="暂无审计记录（需接入 MySQL 并产生 Agent 运行）" />
  </main>
</template>

<style scoped>
.audit-page {
  padding: 24px;
}
.hint {
  color: #909399;
  font-size: 13px;
  margin-top: -8px;
}
.toolbar {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 16px;
}
</style>
