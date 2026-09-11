<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref } from 'vue'
import { AUDIT_SOURCE_ASSET, isAssetChange, listAudit, type AuditLog } from '../api/audit'
import { useAuthStore } from '../stores/auth'
import { formatBeijingTime } from '../utils/time'

const authStore = useAuthStore()
// 审计按租户隔离：非 owner 只看自己所属租户，后端同样强制，前端不再暴露租户过滤框。
const isOwner = computed(() => authStore.userRole === 'owner')

const logs = ref<AuditLog[]>([])
const loading = ref(false)
const autoRefresh = ref(false)
let timer: number | undefined

// 筛选：来源（资产变更 / Agent 运行）、资产种类、决策、操作者。
// 「操作可追溯」靠的就是这四维——谁、在哪个租户、改了哪类资产的哪一行、结果如何。
const filters = reactive({
  tenantId: '',
  source: '' as '' | 'asset' | 'run',
  kind: '',
  decision: '',
  userId: '',
})

const assetKinds: Array<{ value: string; label: string }> = [
  { value: 'agent', label: 'Agent' },
  { value: 'kb', label: '知识库' },
  { value: 'skill', label: 'Skill' },
  { value: 'binding', label: 'IM 绑定' },
  { value: 'endpoint', label: '模型端点' },
]

const decisions = ['executed', 'deny', 'failed', 'allow', 'approve']

async function load() {
  loading.value = true
  try {
    logs.value = await listAudit({
      tenant_id: isOwner.value ? filters.tenantId.trim() || undefined : undefined,
      // 来源：资产变更行 channel=console；Agent 运行行的 channel 是 admin/wecom/feishu，
      // 后端按等值过滤，所以「运行」一侧不传 channel（否则要枚举通道）。
      channel: filters.source === 'asset' ? AUDIT_SOURCE_ASSET : undefined,
      kind: filters.kind || undefined,
      decision: filters.decision || undefined,
      user_id: filters.userId.trim() || undefined,
    })
    if (filters.source === 'run') {
      logs.value = logs.value.filter((r) => !isAssetChange(r))
    }
  } catch {
    // Audit requires MySQL (GET /audit is only registered with a DB); an
    // in-memory/dev backend leaves the list empty without throwing.
    logs.value = []
  } finally {
    loading.value = false
  }
}

function resetFilters() {
  filters.source = ''
  filters.kind = ''
  filters.decision = ''
  filters.userId = ''
  void load()
}

onMounted(() => {
  void load()
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

/** 资产变更行以「种类 + 资产 id」呈现，运行行以「Agent + 工具」呈现。 */
function kindLabel(k: string) {
  return assetKinds.find((x) => x.value === k)?.label ?? k
}

// 定时器清理：组件卸载后不再触发请求。
onBeforeUnmount(() => {
  if (timer) window.clearInterval(timer)
})
</script>

<template>
  <main class="audit-page">
    <h1>审计日志</h1>
    <p class="hint">
      两类记录同表：<b>资产变更</b>（谁创建/共享/删除/被拒）与 <b>Agent 运行</b>（治理决策与请求记账）。
      用下方「来源」区分；最多展示最近 200 条。
    </p>

    <div class="filters">
      <el-input
        v-if="isOwner"
        v-model="filters.tenantId"
        placeholder="租户（留空全部）"
        clearable
        style="width: 170px"
        @keyup.enter="load"
      />
      <el-tag v-else type="info">当前租户：{{ authStore.tenantId }}</el-tag>

      <el-select v-model="filters.source" placeholder="来源" clearable style="width: 150px" @change="load">
        <el-option label="资产变更" value="asset" />
        <el-option label="Agent 运行" value="run" />
      </el-select>

      <el-select
        v-model="filters.kind"
        placeholder="资产种类"
        clearable
        style="width: 140px"
        @change="load"
      >
        <el-option v-for="k in assetKinds" :key="k.value" :label="k.label" :value="k.value" />
      </el-select>

      <el-select v-model="filters.decision" placeholder="决策" clearable style="width: 140px" @change="load">
        <el-option v-for="d in decisions" :key="d" :label="d" :value="d" />
      </el-select>

      <el-input
        v-model="filters.userId"
        placeholder="操作者（成员 id）"
        clearable
        style="width: 180px"
        @keyup.enter="load"
      />

      <el-button type="primary" :loading="loading" @click="load">查询</el-button>
      <el-button @click="resetFilters">重置</el-button>
      <el-checkbox v-model="autoRefresh">每 10s 刷新</el-checkbox>
    </div>

    <el-table v-loading="loading" :data="logs" border size="small">
      <el-table-column label="时间" width="165">
        <template #default="{ row }">{{ fmtTime(row.created_at) }}</template>
      </el-table-column>
      <el-table-column label="来源" width="100">
        <template #default="{ row }">
          <el-tag :type="isAssetChange(row) ? 'warning' : 'info'" size="small">
            {{ isAssetChange(row) ? '资产变更' : 'Agent 运行' }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="tenant_id" label="租户" width="110" />
      <el-table-column prop="user_id" label="操作者" width="110" show-overflow-tooltip />
      <el-table-column label="对象" min-width="170" show-overflow-tooltip>
        <template #default="{ row }">
          <template v-if="isAssetChange(row)">
            {{ kindLabel(row.agent_name || '') }} · <code>{{ row.tool_name }}</code>
          </template>
          <template v-else>
            {{ row.agent_name || '-' }}<span v-if="row.tool_name"> · {{ row.tool_name }}</span>
          </template>
        </template>
      </el-table-column>
      <el-table-column label="决策" width="100">
        <template #default="{ row }">
          <el-tag :type="decisionTag(row.decision)" size="small">{{ row.decision || '-' }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column label="延迟" width="85">
        <template #default="{ row }">
          {{ row.latency_ms != null ? `${row.latency_ms}ms` : '-' }}
        </template>
      </el-table-column>
      <el-table-column label="成本" width="100">
        <template #default="{ row }">{{ row.cost != null ? `${row.cost} tokens` : '-' }}</template>
      </el-table-column>
      <el-table-column prop="error_type" label="原因/错误" min-width="150" show-overflow-tooltip />
      <el-table-column prop="trace_id" label="Trace" width="170" show-overflow-tooltip />
    </el-table>
    <el-empty
      v-if="!loading && logs.length === 0"
      description="暂无审计记录（需接入 MySQL；资产变更与 Agent 运行都会入账）"
    />
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
.filters {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 10px;
  margin-bottom: 16px;
}
</style>
