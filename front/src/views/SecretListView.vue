<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { listSecrets, putSecret, deleteSecret, type Secret } from '../api/secret'

const loading = ref(false)
const secrets = ref<Secret[]>([])

const dialogVisible = ref(false)
const saving = ref(false)
const form = ref({ key: '', value: '' })

async function load() {
  loading.value = true
  try {
    secrets.value = await listSecrets()
  } catch (e) {
    ElMessage.error(String(e))
  } finally {
    loading.value = false
  }
}

function openCreate() {
  form.value = { key: '', value: '' }
  dialogVisible.value = true
}

async function submit() {
  if (!form.value.key.trim() || !form.value.value) {
    ElMessage.warning('密钥 key 与明文值均必填')
    return
  }
  saving.value = true
  try {
    // put is an upsert: re-saving the same key rotates the stored value.
    await putSecret(form.value.key.trim(), form.value.value)
    ElMessage.success(form.value.key ? '已保存（同 key 覆盖即轮换）' : '已保存')
    dialogVisible.value = false
    await load()
  } catch (e) {
    ElMessage.error(String(e))
  } finally {
    saving.value = false
  }
}

async function remove(s: Secret) {
  try {
    await ElMessageBox.confirm(`确认删除密钥「${s.key}」？引用它的模型端点 / IM 绑定将无法解析凭据。`, '删除确认', { type: 'warning' })
  } catch {
    return
  }
  try {
    await deleteSecret(s.key)
    ElMessage.success('已删除')
    await load()
  } catch (e) {
    ElMessage.error(String(e))
  }
}

onMounted(load)
</script>

<template>
  <main class="secret-page">
    <h1>密钥管理</h1>
    <p class="hint">
      统一凭据库（AES-256-GCM 加密落库）：模型 API Key 由模型端点通过
      <code>api_key_ref</code> 引用，IM 凭据由通道绑定通过 <code>credential_ref</code>
      引用。出于安全，密钥值只写不读——列表仅显示 key 与更新时间。
    </p>
    <div class="toolbar">
      <el-button type="primary" @click="openCreate">新建密钥</el-button>
    </div>

    <el-table v-loading="loading" :data="secrets" border>
      <el-table-column prop="key" label="Key" min-width="260">
        <template #default="{ row }"><code>{{ row.key }}</code></template>
      </el-table-column>
      <el-table-column prop="updated_at" label="更新时间" width="220" />
      <el-table-column label="操作" width="120">
        <template #default="{ row }">
          <el-button size="small" type="danger" @click="remove(row)">删除</el-button>
        </template>
      </el-table-column>
    </el-table>
    <div v-if="!loading && secrets.length === 0" class="muted">暂无凭据</div>

    <el-dialog v-model="dialogVisible" title="新建/轮换密钥" width="520px">
      <el-form :model="form" label-width="90px">
        <el-form-item label="Key">
          <el-input v-model="form.key" placeholder="如 llm:openai:default / im:wecom:corp-1" />
        </el-form-item>
        <el-form-item label="明文值">
          <el-input v-model="form.value" type="password" show-password placeholder="API key / Secret（仅写一次，不提供回读）" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dialogVisible = false">取消</el-button>
        <el-button type="primary" :loading="saving" @click="submit">保存</el-button>
      </template>
    </el-dialog>
  </main>
</template>

<style scoped>
.toolbar {
  margin-bottom: 12px;
}
.hint {
  color: #909399;
  font-size: 13px;
  margin: 0 0 12px;
}
.muted {
  color: #909399;
  margin-top: 12px;
}
</style>
