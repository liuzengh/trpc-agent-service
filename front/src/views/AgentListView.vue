<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { useAgentStore } from '../stores/agent'
import { useEndpointStore } from '../stores/endpoint'
import { useKBStore } from '../stores/kb'
import { useSkillStore } from '../stores/skill'
import { listTools, type ToolDef } from '../api/tool'
import { listVersions, type Agent, type VersionInfo } from '../api/agent'
import { useAuthStore } from '../stores/auth'

const store = useAgentStore()
const endpoints = useEndpointStore()
const kbs = useKBStore()
const skills = useSkillStore()
const authStore = useAuthStore()
const canCreate = computed(() => authStore.hasPermission('agent:create'))
const canUpdate = computed(() => authStore.hasPermission('agent:update'))
const canDelete = computed(() => authStore.hasPermission('agent:delete'))

const dialogVisible = ref(false)
const editing = ref(false)
const form = reactive<Agent>({ id: '', tenant_id: '', name: '', description: '', status: 'draft', current_version: 0 })

// publish dialog
const publishVisible = ref(false)
const publishing = ref('')
const pubForm = reactive({
  system_prompt: '',
  endpoint_id: '',
  tool_ids: [] as string[],
  kb_ids: [] as string[],
  skill_ids: [] as string[],
  approval_tool_ids: [] as string[],
})
const tools = ref<ToolDef[]>([])

// Chat agents run on chat endpoints; embedding endpoints exist only for KB
// vectorization and must not be selectable here.
const chatEndpoints = computed(() =>
  endpoints.endpoints.filter((e) => !e.type || e.type === 'chat'),
)

// only published skills are injectable at runtime
const publishedSkills = computed(() => skills.skills.filter((s) => s.status === 'published'))

// rollback dialog
const rollbackVisible = ref(false)
const rolling = ref('')
const versions = ref<VersionInfo[]>([])
const rollbackVersion = ref(0)

onMounted(async () => {
  store.fetch()
  endpoints.fetch()
  kbs.fetch()
  tools.value = await listTools()
})

function openCreate() {
  editing.value = false
  Object.assign(form, { id: '', tenant_id: '', name: '', description: '', status: 'draft', current_version: 0 })
  dialogVisible.value = true
}

function openEdit(row: Agent) {
  editing.value = true
  Object.assign(form, row)
  dialogVisible.value = true
}

async function submit() {
  try {
    if (editing.value) {
      await store.update({ ...form })
    } else {
      await store.create({ ...form })
    }
    dialogVisible.value = false
    ElMessage.success('已保存')
  } catch (e) {
    ElMessage.error(String(e))
  }
}

async function remove(row: Agent) {
  try {
    await store.remove(row.id)
    ElMessage.success('已删除')
  } catch (e) {
    ElMessage.error(String(e))
  }
}

async function toggle(row: Agent) {
  try {
    const next = row.status === 'disabled' ? 'published' : 'disabled'
    await store.update({ ...row, status: next })
    ElMessage.success(next === 'disabled' ? '已禁用' : '已启用')
  } catch (e) {
    ElMessage.error(String(e))
  }
}

async function openPublish(row: Agent) {
  publishing.value = row.id
  Object.assign(pubForm, {
    system_prompt: '',
    endpoint_id: '',
    tool_ids: [] as string[],
    kb_ids: [] as string[],
    skill_ids: [] as string[],
    approval_tool_ids: [] as string[],
  })
  // pre-fill from the current profile when available
  try {
    const p = await import('../api/agent').then((m) => m.getProfile(row.id))
    pubForm.system_prompt = p.system_prompt
    pubForm.endpoint_id = p.endpoint_id
    pubForm.tool_ids = p.tool_ids ?? []
    pubForm.kb_ids = p.kb_ids ?? []
    pubForm.skill_ids = p.skill_ids ?? []
    pubForm.approval_tool_ids = p.approval_tool_ids ?? []
  } catch {
    /* never published yet */
  }
  publishVisible.value = true
}

async function publish() {
  try {
    const v = await store.publish(publishing.value, { ...pubForm })
    publishVisible.value = false
    ElMessage.success(`已发布 v${v}`)
  } catch (e) {
    ElMessage.error(String(e))
  }
}

async function openRollback(row: Agent) {
  rolling.value = row.id
  rollbackVersion.value = row.current_version
  versions.value = await listVersions(row.id)
  rollbackVisible.value = true
}

async function rollback() {
  try {
    await store.rollback(rolling.value, rollbackVersion.value)
    rollbackVisible.value = false
    ElMessage.success(`已回滚到 v${rollbackVersion.value}`)
  } catch (e) {
    ElMessage.error(String(e))
  }
}

function statusTag(s: string) {
  return s === 'published' ? 'success' : s === 'disabled' ? 'info' : 'warning'
}
</script>

<template>
  <main class="page">
    <h1>Agent 配置</h1>
    <p class="hint">Agent 是团队级助手；通过发布冻结版本、可原子回滚，运行中会话不受切换影响。</p>
    <div class="toolbar">
      <el-button v-if="canCreate" type="primary" @click="openCreate">新建 Agent</el-button>
    </div>

    <el-table v-loading="store.loading" :data="store.agents" border>
      <el-table-column prop="name" label="名称" width="160" />
      <el-table-column prop="tenant_id" label="租户" width="160" />
      <el-table-column label="状态" width="110">
        <template #default="{ row }">
          <el-tag :type="statusTag(row.status)">{{ row.status }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="current_version" label="当前版本" width="100" />
      <el-table-column prop="description" label="描述" show-overflow-tooltip />
      <el-table-column label="操作" width="340">
        <template #default="{ row }">
          <el-button v-if="canUpdate" size="small" @click="openEdit(row)">编辑</el-button>
          <el-button v-if="canUpdate" size="small" @click="toggle(row)">{{ row.status === 'disabled' ? '启用' : '禁用' }}</el-button>
          <el-button v-if="canUpdate" size="small" type="primary" @click="openPublish(row)">发布</el-button>
          <el-button v-if="canUpdate" size="small" @click="openRollback(row)">回滚</el-button>
          <el-button v-if="canDelete" size="small" type="danger" @click="remove(row)">删除</el-button>
        </template>
      </el-table-column>
    </el-table>

    <!-- create / edit -->
    <el-dialog v-model="dialogVisible" :title="editing ? '编辑 Agent' : '新建 Agent'" width="520px">
      <el-form :model="form" label-width="80px">
        <el-form-item label="ID">
          <el-input v-model="form.id" :disabled="editing" placeholder="agent id" />
        </el-form-item>
        <el-form-item label="租户 ID">
          <el-input v-model="form.tenant_id" placeholder="tenant id" />
        </el-form-item>
        <el-form-item label="名称">
          <el-input v-model="form.name" placeholder="Agent 名称" />
        </el-form-item>
        <el-form-item label="描述">
          <el-input v-model="form.description" type="textarea" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dialogVisible = false">取消</el-button>
        <el-button type="primary" @click="submit">保存</el-button>
      </template>
    </el-dialog>

    <!-- publish -->
    <el-dialog v-model="publishVisible" :title="`发布 ${publishing}`" width="560px">
      <el-form label-width="100px">
        <el-form-item label="System Prompt">
          <el-input v-model="pubForm.system_prompt" type="textarea" :rows="4" placeholder="系统提示词，如：你是客服助手，引用知识库回答…" />
        </el-form-item>
        <el-form-item label="模型端点">
          <el-select v-model="pubForm.endpoint_id" placeholder="选择 chat 端点" style="width: 100%">
            <el-option v-for="e in chatEndpoints" :key="e.id" :label="`${e.name} (${e.model_name})`" :value="e.id" />
          </el-select>
        </el-form-item>
        <el-form-item label="工具">
          <el-select v-model="pubForm.tool_ids" multiple placeholder="选择工具" style="width: 100%">
            <el-option v-for="t in tools" :key="t.id" :label="t.name" :value="t.id" />
          </el-select>
        </el-form-item>
        <el-form-item label="知识库">
          <el-select v-model="pubForm.kb_ids" multiple placeholder="挂载知识库（检索工具）" style="width: 100%">
            <el-option
              v-for="kb in kbs.kbs"
              :key="kb.id"
              :label="`${kb.name} (${kb.collection_name})`"
              :value="kb.id"
            />
          </el-select>
        </el-form-item>
        <el-form-item label="Skill">
          <el-select v-model="pubForm.skill_ids" multiple placeholder="挂载已发布 Skill（SKILL.md 注入系统提示词）" style="width: 100%">
            <el-option
              v-for="s in publishedSkills"
              :key="s.skill_id"
              :label="`${s.code} · ${s.name} (v${s.current_version})`"
              :value="s.skill_id"
            />
          </el-select>
        </el-form-item>
        <el-form-item label="审批工具">
          <el-select v-model="pubForm.approval_tool_ids" multiple placeholder="这些工具调用前需人工审批" style="width: 100%">
            <el-option
              v-for="t in tools"
              :key="t.id"
              :label="`${t.name}${t.risk_level === 'high' ? '（高危·自动）' : ''}`"
              :value="t.id"
            />
          </el-select>
          <div class="form-tip">勾选的工具执行前会暂停并请求人工批准；标注「高危·自动」的工具无需勾选也会触发审批。</div>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="publishVisible = false">取消</el-button>
        <el-button type="primary" @click="publish">发布</el-button>
      </template>
    </el-dialog>

    <!-- rollback -->
    <el-dialog v-model="rollbackVisible" :title="`回滚 ${rolling}`" width="420px">
      <el-form label-width="100px">
        <el-form-item label="目标版本">
          <el-select v-model="rollbackVersion" style="width: 100%">
            <el-option v-for="v in versions" :key="v.version" :label="`v${v.version} (${v.status})`" :value="v.version" />
          </el-select>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="rollbackVisible = false">取消</el-button>
        <el-button type="primary" @click="rollback">回滚</el-button>
      </template>
    </el-dialog>
  </main>
</template>

<style scoped>
.page {
  padding: 24px;
}
.hint {
  color: #909399;
  font-size: 13px;
  margin-top: -8px;
}
.toolbar {
  margin-bottom: 16px;
}
.form-tip {
  color: #909399;
  font-size: 12px;
  line-height: 1.5;
  margin-top: 4px;
}
</style>
