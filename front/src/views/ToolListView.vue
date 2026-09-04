<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { grantTool, listGrants, listTools, revokeTool, type ToolDef } from '../api/tool'
import { listAgents, type Agent } from '../api/agent'

const tools = ref<ToolDef[]>([])
const loading = ref(false)

// ---- grant dialog ----
const grantVisible = ref(false)
const activeTool = ref<ToolDef | null>(null)
const agents = ref<Agent[]>([])
const currentGrants = ref<string[]>([])
const selected = ref<string[]>([])
const saving = ref(false)

onMounted(async () => {
  loading.value = true
  try {
    tools.value = await listTools()
  } finally {
    loading.value = false
  }
})

function riskTag(r: string) {
  return r === 'high' ? 'danger' : r === 'medium' ? 'warning' : 'info'
}

async function openGrants(row: ToolDef) {
  activeTool.value = row
  grantVisible.value = true
  selected.value = []
  try {
    if (agents.value.length === 0) agents.value = await listAgents()
    currentGrants.value = await listGrants(row.id)
    selected.value = [...currentGrants.value]
  } catch (e) {
    ElMessage.error(String(e))
  }
}

async function saveGrants() {
  if (!activeTool.value) return
  saving.value = true
  try {
    const want = new Set(selected.value)
    const have = new Set(currentGrants.value)
    for (const agentID of want) {
      if (!have.has(agentID)) await grantTool(activeTool.value.id, agentID)
    }
    for (const agentID of have) {
      if (!want.has(agentID)) await revokeTool(activeTool.value.id, agentID)
    }
    currentGrants.value = [...selected.value]
    grantVisible.value = false
    ElMessage.success('已保存授权')
  } catch (e) {
    ElMessage.error(String(e))
  } finally {
    saving.value = false
  }
}
</script>

<template>
  <main class="tool-page">
    <h1>工具目录</h1>
    <p class="hint">平台内置工具 + 租户注册工具；「授权」决定哪些 Agent 运行时可用该工具（RBAC），高风险工具调用会自动触发人工审批。</p>
    <el-table v-loading="loading" :data="tools" border>
      <el-table-column prop="id" label="ID" width="240" />
      <el-table-column prop="name" label="名称" width="160" />
      <el-table-column label="风险等级" width="110">
        <template #default="{ row }">
          <el-tag :type="riskTag(row.risk_level)">{{ row.risk_level }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="description" label="描述" show-overflow-tooltip />
      <el-table-column label="操作" width="110">
        <template #default="{ row }">
          <el-button size="small" type="primary" @click="openGrants(row)">授权</el-button>
        </template>
      </el-table-column>
    </el-table>
    <el-empty v-if="!loading && tools.length === 0" description="暂无工具" />

    <el-dialog v-model="grantVisible" :title="`工具授权 · ${activeTool?.name ?? ''}`" width="520px">
      <p class="hint">勾选可使用该工具的 Agent（未授权的 Agent 即使发布时选择也会被运行时跳过）。</p>
      <div class="grant-list">
        <el-checkbox v-for="a in agents" :key="a.id" :value="a.id">{{ a.name }}（{{ a.id }}）</el-checkbox>
      </div>
      <el-empty v-if="agents.length === 0" description="暂无 Agent，请先创建" :image-size="50" />
      <template #footer>
        <el-button @click="grantVisible = false">取消</el-button>
        <el-button type="primary" :loading="saving" @click="saveGrants">保存授权</el-button>
      </template>
    </el-dialog>
  </main>
</template>

<style scoped>
.tool-page {
  padding: 24px;
}
.hint {
  color: #909399;
  font-size: 13px;
  margin-top: -8px;
}
.grant-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
  margin: 8px 0;
  max-height: 320px;
  overflow-y: auto;
}
</style>
