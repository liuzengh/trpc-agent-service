<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { getUsage, type UsageResponse, type UsageRow, type UsageSummary } from '../api/usage'
import { listTenants, type Tenant } from '../api/tenant'
import { listAgents, type Agent } from '../api/agent'
import { formatBeijingTime } from '../utils/time'

const loading = ref(false)
const tenants = ref<Tenant[]>([])
const agents = ref<Agent[]>([])
const tenantId = ref('')
const agentId = ref('')
const dimension = ref('')
const data = ref<UsageResponse>({ summary: [], rows: [] })

const dimensions = ['token', 'tool', 'sandbox', 'artifact', 'skill']

const tokenTotal = computed(() => {
  const t = data.value.summary.find((s) => s.dimension === 'token')
  return t?.total ?? 0
})

onMounted(async () => {
  try {
    tenants.value = await listTenants()
    agents.value = await listAgents()
    await load()
  } catch (e) {
    ElMessage.error(String(e))
  }
})

async function load() {
  loading.value = true
  try {
    data.value = await getUsage({
      tenant_id: tenantId.value || undefined,
      agent_id: agentId.value || undefined,
      dimension: dimension.value || undefined,
    })
  } catch (e) {
    ElMessage.error(String(e))
  } finally {
    loading.value = false
  }
}

function dimLabel(d: string) {
  const map: Record<string, string> = {
    token: 'Token 用量',
    tool: '工具调用',
    sandbox: '沙箱执行',
    artifact: '制品',
    skill: 'Skill',
  }
  return map[d] ?? d
}

function fmt(n: number) {
  return n.toLocaleString('zh-CN', { maximumFractionDigits: 2 })
}

function formatAt(s: string) {
  return formatBeijingTime(s)
}

/** Renders the meta of a usage row as human-readable detail chips. */
function detailTags(row: UsageRow): string[] {
  const meta = row.meta
  if (!meta) return []
  if (meta.tools && meta.calls) {
    return meta.tools.map((name) => `${name} ×${meta.calls![name] ?? 1}`)
  }
  if (meta.skills) {
    return meta.skills.map((s) => {
      if (typeof s === 'string') return `skill:${s.slice(0, 8)}` // legacy row: bare id
      return s.name ? `${s.name} (${s.code} v${s.version})` : `${s.code} v${s.version}`
    })
  }
  return []
}
</script>

<template>
  <main class="usage-page">
    <h1>用量计量</h1>
    <p class="hint">按租户/Agent/维度统计用量（仅计量、不折算金额）；token 维度由每次 Agent 会话自动写入。</p>

    <div class="filters">
      <el-select v-model="tenantId" clearable placeholder="全部租户" style="width: 180px" @change="load">
        <el-option v-for="t in tenants" :key="t.id" :label="t.name" :value="t.id" />
      </el-select>
      <el-select v-model="agentId" clearable placeholder="全部 Agent" style="width: 200px" @change="load">
        <el-option v-for="a in agents" :key="a.id" :label="`${a.name}（${a.id}）`" :value="a.id" />
      </el-select>
      <el-select v-model="dimension" clearable placeholder="全部维度" style="width: 160px" @change="load">
        <el-option v-for="d in dimensions" :key="d" :label="dimLabel(d)" :value="d" />
      </el-select>
      <el-button type="primary" @click="load">查询</el-button>
    </div>

    <el-row :gutter="16" class="cards" v-loading="loading">
      <el-col :span="8" v-for="s in data.summary" :key="s.dimension">
        <div class="card">
          <div class="card-title">{{ dimLabel(s.dimension) }}</div>
          <div class="card-value">{{ fmt(s.total) }}</div>
          <div class="card-sub">{{ s.count }} 条记录</div>
        </div>
      </el-col>
    </el-row>
    <div v-if="!loading && data.summary.length === 0" class="muted">暂无用量数据</div>

    <el-table :data="data.rows" border v-loading="loading" style="margin-top: 16px">
      <el-table-column prop="dimension" label="维度" width="120">
        <template #default="{ row }">{{ dimLabel(row.dimension) }}</template>
      </el-table-column>
      <el-table-column prop="amount" label="用量" width="140">
        <template #default="{ row }">{{ fmt(row.amount) }}</template>
      </el-table-column>
      <el-table-column label="明细" min-width="240">
        <template #default="{ row }">
          <template v-if="detailTags(row).length">
            <el-tag v-for="tag in detailTags(row)" :key="tag" size="small" style="margin: 2px 6px 2px 0">
              {{ tag }}
            </el-tag>
          </template>
          <span v-else class="muted">—</span>
        </template>
      </el-table-column>
      <el-table-column prop="tenant_id" label="租户" width="160" />
      <el-table-column prop="agent_id" label="Agent" width="200" show-overflow-tooltip />
      <el-table-column prop="created_at" label="时间">
        <template #default="{ row }">{{ formatAt(row.created_at) }}</template>
      </el-table-column>
    </el-table>
  </main>
</template>

<style scoped>
.usage-page {
  padding: 24px;
}
.hint {
  color: #909399;
  font-size: 13px;
  margin-top: -8px;
}
.filters {
  display: flex;
  gap: 12px;
  margin: 16px 0;
  flex-wrap: wrap;
}
.cards {
  margin: 0;
}
.card {
  border: 1px solid var(--el-border-color-lighter);
  border-radius: 8px;
  padding: 16px;
  background: #fff;
}
.card-title {
  font-size: 13px;
  color: #909399;
}
.card-value {
  font-size: 26px;
  font-weight: 500;
  margin: 6px 0;
}
.card-sub {
  font-size: 12px;
  color: #909399;
}
.muted {
  color: #909399;
  font-size: 13px;
  padding: 8px 0;
}
</style>
