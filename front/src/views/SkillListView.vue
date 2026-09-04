<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { useSkillStore } from '../stores/skill'
import type { Skill, SkillScope, SkillVersion } from '../api/skill'

const store = useSkillStore()

// ---- create / edit dialog ----
const dialogVisible = ref(false)
const editingId = ref('')
const editingStatus = ref<'draft' | 'published' | 'disabled'>('draft')
const form = reactive({ code: '', name: '', description: '', scope: 'tenant' as SkillScope, owner_tenant_id: '' })

// ---- versions dialog ----
const versionsVisible = ref(false)
const activeSkill = ref<Skill | null>(null)
const versions = ref<SkillVersion[]>([])
const versionsLoading = ref(false)
const vForm = reactive({ version: 1, content_md: '', prompt_template: '' })

onMounted(() => store.fetch())

const publishedSkills = computed(() => store.skills.filter((s) => s.status === 'published'))

function scopeTag(s: SkillScope) {
  return s === 'global' ? 'info' : 'warning'
}

function openCreate() {
  editingId.value = ''
  Object.assign(form, { code: '', name: '', description: '', scope: 'tenant' as SkillScope, owner_tenant_id: '' })
  dialogVisible.value = true
}

function openEdit(row: Skill) {
  editingId.value = row.skill_id
  Object.assign(form, {
    code: row.code,
    name: row.name,
    description: row.description ?? '',
    scope: row.scope,
    owner_tenant_id: row.owner_tenant_id ?? '',
  })
  dialogVisible.value = true
}

async function submit() {
  try {
    const body = {
      code: form.code.trim(),
      name: form.name.trim(),
      description: form.description.trim() || undefined,
      scope: form.scope,
      owner_tenant_id: form.scope === 'tenant' && form.owner_tenant_id.trim() ? form.owner_tenant_id.trim() : undefined,
    }
    if (editingId.value) {
      await store.update(editingId.value, body)
      ElMessage.success('已更新')
    } else {
      await store.create(body)
      ElMessage.success('已创建（draft），请到「版本」中新建并发布')
    }
    dialogVisible.value = false
  } catch (e) {
    ElMessage.error(String(e))
  }
}

async function remove(row: Skill) {
  try {
    await ElMessageBox.confirm(`删除 skill「${row.code}」？版本与绑定将一并解除。`, '确认删除', { type: 'warning' })
  } catch {
    return
  }
  try {
    await store.remove(row.skill_id)
    ElMessage.success('已删除')
  } catch (e) {
    ElMessage.error(String(e))
  }
}

async function openVersions(row: Skill) {
  activeSkill.value = row
  versionsVisible.value = true
  versionsLoading.value = true
  versions.value = []
  Object.assign(vForm, { version: row.current_version + 1, content_md: '', prompt_template: '' })
  try {
    versions.value = await store.listVersions(row.skill_id)
    // recommend the next free version number
    const max = versions.value.reduce((a, v) => Math.max(a, v.version), 0)
    vForm.version = max + 1
  } catch (e) {
    ElMessage.error(String(e))
  } finally {
    versionsLoading.value = false
  }
}

async function addVersion() {
  if (!activeSkill.value) return
  if (!vForm.content_md.trim()) {
    ElMessage.warning('SKILL.md 正文不能为空')
    return
  }
  try {
    await store.addVersion(activeSkill.value.skill_id, {
      version: vForm.version,
      content_md: vForm.content_md,
      prompt_template: vForm.prompt_template.trim() || undefined,
    })
    ElMessage.success('版本已创建')
    Object.assign(vForm, { content_md: '', prompt_template: '' })
    versions.value = await store.listVersions(activeSkill.value.skill_id)
  } catch (e) {
    ElMessage.error(String(e))
  }
}

async function publishVersion(v: SkillVersion) {
  if (!activeSkill.value) return
  try {
    await store.publish(activeSkill.value.skill_id, v.version)
    ElMessage.success(`v${v.version} 已发布`)
    versions.value = await store.listVersions(activeSkill.value.skill_id)
  } catch (e) {
    ElMessage.error(String(e))
  }
}

function versionStatusTag(s: string) {
  return s === 'published' ? 'success' : s === 'disabled' ? 'danger' : 'warning'
}
</script>

<template>
  <main class="skill-page">
    <h1>Skill 资产</h1>
    <p class="hint">
      Skill 是平台级/租户级能力资产（SKILL.md 指令）。发布后将可在 Agent 发布时挂载，运行时注入系统提示词。
      平台级 global 对所有租户可见；租户级 tenant 需指定 owner_tenant_id。
    </p>
    <div class="toolbar">
      <el-button type="primary" @click="openCreate">新建 Skill</el-button>
      <span class="muted">{{ publishedSkills.length }} 个已发布</span>
    </div>

    <el-table v-loading="store.loading" :data="store.skills" border>
      <el-table-column prop="code" label="Code" width="160" />
      <el-table-column prop="name" label="名称" width="160" show-overflow-tooltip />
      <el-table-column prop="description" label="描述" show-overflow-tooltip />
      <el-table-column label="Scope" width="100">
        <template #default="{ row }">
          <el-tag :type="scopeTag(row.scope)" size="small">{{ row.scope }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="owner_tenant_id" label="租户" width="140">
        <template #default="{ row }">{{ row.owner_tenant_id ?? '—' }}</template>
      </el-table-column>
      <el-table-column prop="current_version" label="当前版本" width="100" />
      <el-table-column label="状态" width="110">
        <template #default="{ row }">
          <el-tag :type="row.status === 'published' ? 'success' : row.status === 'disabled' ? 'danger' : 'warning'" size="small">
            {{ row.status }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column label="操作" width="210">
        <template #default="{ row }">
          <el-button size="small" type="primary" @click="openVersions(row)">版本</el-button>
          <el-button size="small" @click="openEdit(row)">编辑</el-button>
          <el-button size="small" type="danger" @click="remove(row)">删除</el-button>
        </template>
      </el-table-column>
    </el-table>

    <!-- create / edit -->
    <el-dialog v-model="dialogVisible" :title="editingId ? '编辑 Skill' : '新建 Skill'" width="560px">
      <el-form :model="form" label-width="120px">
        <el-form-item label="Code">
          <el-input v-model="form.code" placeholder="唯一标识，如 triage（创建后不可改）" :disabled="!!editingId" />
        </el-form-item>
        <el-form-item label="名称">
          <el-input v-model="form.name" placeholder="显示名" />
        </el-form-item>
        <el-form-item label="描述">
          <el-input v-model="form.description" placeholder="能力说明（可选）" />
        </el-form-item>
        <el-form-item label="Scope">
          <el-select v-model="form.scope" style="width: 100%">
            <el-option label="tenant（租户私有）" value="tenant" />
            <el-option label="global（平台共享）" value="global" />
          </el-select>
        </el-form-item>
        <el-form-item v-if="form.scope === 'tenant'" label="属主租户">
          <el-input v-model="form.owner_tenant_id" placeholder="tenant id" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dialogVisible = false">取消</el-button>
        <el-button type="primary" @click="submit">保存</el-button>
      </template>
    </el-dialog>

    <!-- versions -->
    <el-dialog v-model="versionsVisible" :title="`版本管理 · ${activeSkill?.code ?? ''}`" width="760px">
      <el-form label-width="110px" class="v-form">
        <el-form-item label="版本号">
          <el-input-number v-model="vForm.version" :min="activeSkill ? activeSkill.current_version + 1 : 1" />
        </el-form-item>
        <el-form-item label="SKILL.md 正文">
          <el-input v-model="vForm.content_md" type="textarea" :rows="8" placeholder="# 技能名&#10;技能说明与执行规则…（运行时注入系统提示词）" />
        </el-form-item>
        <el-form-item label="Prompt 模板">
          <el-input v-model="vForm.prompt_template" type="textarea" :rows="2" placeholder="可选：正文为空时使用的提示模板" />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" @click="addVersion">创建版本</el-button>
        </el-form-item>
      </el-form>
      <el-divider />
      <el-table v-loading="versionsLoading" :data="versions" border max-height="300">
        <el-table-column prop="version" label="版本" width="80" />
        <el-table-column label="状态" width="110">
          <template #default="{ row }">
            <el-tag :type="versionStatusTag(row.status)" size="small">{{ row.status }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="content_md" label="SKILL.md 摘要" show-overflow-tooltip />
        <el-table-column prop="checksum" label="Checksum" width="90" show-overflow-tooltip />
        <el-table-column label="操作" width="120">
          <template #default="{ row }">
            <el-button v-if="row.status === 'draft'" size="small" type="success" @click="publishVersion(row)">发布</el-button>
            <span v-else class="muted">{{ row.published_at?.slice(0, 10) ?? '' }}</span>
          </template>
        </el-table-column>
      </el-table>
    </el-dialog>
  </main>
</template>

<style scoped>
.skill-page {
  padding: 24px;
}
.hint {
  color: #909399;
  font-size: 13px;
  margin-top: -8px;
}
.toolbar {
  margin-bottom: 16px;
  display: flex;
  align-items: center;
  gap: 12px;
}
.muted {
  color: #909399;
  font-size: 13px;
}
.v-form {
  margin-bottom: -8px;
}
</style>
