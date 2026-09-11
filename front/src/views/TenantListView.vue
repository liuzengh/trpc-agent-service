<script setup lang="ts">
import { onMounted, reactive, ref, computed } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { useTenantStore, type Tenant } from '../stores/tenant'
import type { TenantAuditPolicy, TenantQuota, TenantConfigVersion } from '../api/tenant'
import { listConfigVersions, rollbackConfig } from '../api/tenant'
import { listTools, type ToolDef } from '../api/tool'
import { useAuthStore } from '../stores/auth'

const store = useTenantStore()
const authStore = useAuthStore()
const dialogVisible = ref(false)
const editing = ref(false)

// 租户本身的增删改、治理与回滚都是 owner 专属：admin 只有 tenant:read，
// 后端对其写接口一律返回 404，前端也不应把按钮暴露出来。
const canManage = computed(() => authStore.hasPermission('tenant:manage'))

const form = reactive<Tenant>({ id: '', name: '', status: 'active' })

// --- governance panel ---
const govVisible = ref(false)
const govTarget = ref<Tenant | null>(null)
const savingGov = ref(false)
const tools = ref<ToolDef[]>([])

// --- config history / rollback ---
const histVisible = ref(false)
const histTarget = ref<Tenant | null>(null)
const histLoading = ref(false)
const histRows = ref<TenantConfigVersion[]>([])

async function openHistory(row: Tenant) {
  histTarget.value = row
  histVisible.value = true
  histLoading.value = true
  try {
    histRows.value = await listConfigVersions(row.id)
  } catch (e) {
    ElMessage.error(String(e))
    histRows.value = []
  } finally {
    histLoading.value = false
  }
}

async function rollbackTo(v: TenantConfigVersion) {
  const t = histTarget.value
  if (!t) return
  try {
    await ElMessageBox.confirm(
      `将租户「${t.name}」恢复到配置版本 v${v.version}（会生成新的头部版本）？`,
      '回滚确认',
      { type: 'warning' },
    )
  } catch {
    return
  }
  try {
    await rollbackConfig(t.id, v.version)
    ElMessage.success('已回滚')
    await openHistory(t)
    store.fetch()
  } catch (e) {
    ElMessage.error(String(e))
  }
}

interface GovForm {
  budgetEnabled: boolean
  tokenQuota: number
  redact: boolean
  allowUsersText: string
  toolWhitelist: string[]
  forceApproval: string[]
}

const gov = reactive<GovForm>({
  budgetEnabled: false,
  tokenQuota: 0,
  redact: true,
  allowUsersText: '',
  toolWhitelist: [],
  forceApproval: [],
})

onMounted(async () => {
  store.fetch()
  if (!canManage.value) return
  try {
    tools.value = await listTools()
  } catch {
    tools.value = []
  }
})

function openCreate() {
  editing.value = false
  Object.assign(form, { id: '', name: '', status: 'active', quota: undefined, audit_policy: undefined })
  dialogVisible.value = true
}

function openEdit(row: Tenant) {
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

async function remove(row: Tenant) {
  try {
    await ElMessageBox.confirm(
      `删除租户「${row.name}」将同时移除其 Agent/端点/会话路由等配置，且不可恢复。确认继续？`,
      '删除租户',
      { type: 'warning', confirmButtonText: '删除', cancelButtonText: '取消' },
    )
  } catch {
    return // cancelled
  }
  try {
    await store.remove(row.id)
    ElMessage.success('已删除')
  } catch (e) {
    ElMessage.error(String(e))
  }
}

function openGovernance(row: Tenant) {
  govTarget.value = row
  const q: TenantQuota | undefined = row.quota
  const p: TenantAuditPolicy | undefined = row.audit_policy
  gov.budgetEnabled = !!q && (q.token_quota ?? 0) > 0
  gov.tokenQuota = q?.token_quota ?? 0
  gov.redact = p?.redact !== false // absent = default on
  gov.allowUsersText = (p?.im_allow_users ?? []).join('\n')
  gov.toolWhitelist = p?.tool_whitelist ?? []
  gov.forceApproval = p?.force_approval_tools ?? []
  govVisible.value = true
}

async function saveGovernance() {
  if (!govTarget.value) return
  savingGov.value = true
  try {
    const quota: TenantQuota | undefined = gov.budgetEnabled && gov.tokenQuota > 0
      ? { token_quota: gov.tokenQuota }
      : undefined
    const users = gov.allowUsersText
      .split('\n')
      .map((s) => s.trim())
      .filter(Boolean)
    const auditPolicy: TenantAuditPolicy = {
      redact: gov.redact,
      im_allow_users: users,
      tool_whitelist: gov.toolWhitelist,
      force_approval_tools: gov.forceApproval,
    }
    await store.update({ ...govTarget.value, quota, audit_policy: auditPolicy })
    govVisible.value = false
    ElMessage.success('治理策略已保存')
  } catch (e) {
    ElMessage.error(String(e))
  } finally {
    savingGov.value = false
  }
}
</script>

<template>
  <main class="tenant-page">
    <h1>租户管理</h1>
    <p v-if="!canManage" class="hint">当前租户：{{ authStore.tenantId }}（仅 owner 可管理租户）</p>
    <div v-if="canManage" class="toolbar">
      <el-button type="primary" @click="openCreate">新建租户</el-button>
    </div>

    <el-table v-loading="store.loading" :data="store.tenants" border>
      <el-table-column prop="id" label="ID" width="220" />
      <el-table-column prop="name" label="名称" />
      <el-table-column prop="status" label="状态" width="120" />
      <el-table-column v-if="canManage" label="操作" width="360">
        <template #default="{ row }">
          <el-button size="small" @click="openEdit(row)">编辑</el-button>
          <el-button size="small" @click="openGovernance(row)">治理</el-button>
          <el-button size="small" @click="openHistory(row)">历史</el-button>
          <el-button size="small" type="danger" @click="remove(row)">删除</el-button>
        </template>
      </el-table-column>
    </el-table>

    <el-dialog v-model="dialogVisible" :title="editing ? '编辑租户' : '新建租户'">
      <el-form :model="form" label-width="80px">
        <el-form-item label="ID">
          <el-input v-model="form.id" :disabled="editing" placeholder="tenant id" />
        </el-form-item>
        <el-form-item label="名称">
          <el-input v-model="form.name" placeholder="租户名称" />
        </el-form-item>
        <el-form-item label="状态">
          <el-select v-model="form.status">
            <el-option label="启用" value="active" />
            <el-option label="禁用" value="disabled" />
          </el-select>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dialogVisible = false">取消</el-button>
        <el-button type="primary" @click="submit">保存</el-button>
      </template>
    </el-dialog>

    <!-- tenant-level governance (Filter) strategies -->
    <el-dialog v-model="govVisible" :title="`治理策略 — ${govTarget?.name ?? ''}`" width="640px">
      <el-form :model="gov" label-width="130px">
        <el-divider content-position="left">预算限制</el-divider>
        <el-form-item label="启用 token 预算">
          <el-switch v-model="gov.budgetEnabled" />
        </el-form-item>
        <el-form-item v-if="gov.budgetEnabled" label="预算上限 (tokens)">
          <el-input-number v-model="gov.tokenQuota" :min="1" :step="100000" controls-position="right" />
          <span class="hint">租户累计 token 达到上限后拒绝新的对话</span>
        </el-form-item>

        <el-divider content-position="left">敏感信息脱敏</el-divider>
        <el-form-item label="工具参数/结果脱敏">
          <el-switch v-model="gov.redact" />
          <span class="hint">默认开启：手机号 / 身份证 / 银行卡 / 邮箱打码后进模型与审计</span>
        </el-form-item>

        <el-divider content-position="left">IM 用户权限</el-divider>
        <el-form-item label="允许的 IM 用户">
          <el-input
            v-model="gov.allowUsersText"
            type="textarea"
            :rows="3"
            placeholder="每行一个用户 id（企业微信 userid / 飞书 open_id）；留空 = 所有用户可用"
          />
        </el-form-item>

        <el-divider content-position="left">工具白名单</el-divider>
        <el-form-item label="允许的工具">
          <el-select v-model="gov.toolWhitelist" multiple filterable placeholder="留空 = 不限制（知识库检索不受此限制）" style="width: 100%">
            <el-option v-for="t in tools" :key="t.id" :label="`${t.name} (${t.id})`" :value="t.id" />
          </el-select>
        </el-form-item>
        <el-form-item label="强制审批工具">
          <el-select v-model="gov.forceApproval" multiple filterable placeholder="调用这些工具需人工批准（与 Agent 级审批叠加）" style="width: 100%">
            <el-option v-for="t in tools" :key="t.id" :label="`${t.name} (${t.id})`" :value="t.id" />
          </el-select>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="govVisible = false">取消</el-button>
        <el-button type="primary" :loading="savingGov" @click="saveGovernance">保存</el-button>
      </template>
    </el-dialog>

    <!-- config version history / rollback -->
    <el-dialog v-model="histVisible" :title="`配置历史 — ${histTarget?.name ?? ''}`" width="720px">
      <p class="hint">每次保存治理/数据后端配置会记录一个版本快照；回滚会应用所选版本并生成新头部版本。</p>
      <el-table v-loading="histLoading" :data="histRows" border size="small">
        <el-table-column prop="version" label="版本" width="90" />
        <el-table-column label="配置摘要">
          <template #default="{ row }">
            <span v-if="row.config">
              token 预算：{{ row.config.quota?.token_quota ?? '不限' }} ·
              脱敏：{{ row.config.audit_policy?.redact === false ? '关闭' : '开启' }} ·
              白名单用户：{{ row.config.audit_policy?.im_allow_users?.length ?? 0 }}
            </span>
          </template>
        </el-table-column>
        <el-table-column prop="created_at" label="记录时间" width="190" />
        <el-table-column label="操作" width="100">
          <template #default="{ row }">
            <el-button size="small" type="warning" :disabled="row.version === histRows[0]?.version" @click="rollbackTo(row)">回滚</el-button>
          </template>
        </el-table-column>
      </el-table>
      <div v-if="!histLoading && histRows.length === 0" class="muted">暂无配置历史（保存一次治理配置后生成）</div>
    </el-dialog>
  </main>
</template>

<style scoped>
.tenant-page {
  padding: 24px;
}
.toolbar {
  margin-bottom: 16px;
}
.hint {
  margin-left: 12px;
  color: #909399;
  font-size: 12px;
}
</style>
