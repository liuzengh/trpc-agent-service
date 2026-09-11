<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { useEndpointStore } from '../stores/endpoint'
import type { Endpoint } from '../api/endpoint'
import { useAuthStore } from '../stores/auth'

const store = useEndpointStore()
const authStore = useAuthStore()
const canCreate = computed(() => authStore.hasPermission('endpoint:manage'))

// 行级权限：global 端点是平台资产（只有 owner 可改），租户端点管理员可管全部、
// member 只能改自己注册的。
const canManage = (row: Endpoint) =>
  row.scope === 'global'
    ? authStore.userRole === 'owner'
    : authStore.canManageAsset(row as { created_by?: string })
const dialogVisible = ref(false)
const editing = ref(false)

const form = reactive<Endpoint>({
  id: '',
  scope: 'tenant',
  tenant_id: '',
  name: '',
  provider: 'openai',
  type: 'chat',
  base_url: '',
  model_name: '',
  api_key: '',
})

onMounted(() => store.fetch())

function openCreate() {
  editing.value = false
  Object.assign(form, {
    id: '',
    scope: 'tenant',
    tenant_id: authStore.tenantId,
    name: '',
    provider: 'openai',
    type: 'chat',
    base_url: '',
    model_name: '',
    api_key: '',
    visibility: 'private',
  })
  dialogVisible.value = true
}

function openEdit(row: Endpoint) {
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

async function remove(row: Endpoint) {
  try {
    await store.remove(row.id)
    ElMessage.success('已删除')
  } catch (e) {
    ElMessage.error(String(e))
  }
}
</script>

<template>
  <main class="page">
    <h1>模型端点</h1>
    <p class="hint">
      租户资产，作者所有：新建默认<b>私有</b>，共享后租户内成员可用；global 端点是平台共享资源（仅 owner 可建/改）。
    </p>
    <div class="toolbar">
      <el-button v-if="canCreate" type="primary" @click="openCreate">新建端点</el-button>
    </div>

    <el-table v-loading="store.loading" :data="store.endpoints" border>
      <el-table-column prop="name" label="名称" width="150" />
      <el-table-column prop="provider" label="协议" width="110" />
      <el-table-column prop="type" label="类型" width="100">
        <template #default="{ row }">{{ row.type ?? 'chat' }}</template>
      </el-table-column>
      <el-table-column prop="model_name" label="模型" width="160" />
      <el-table-column label="可见性" width="120">
        <template #default="{ row }">
          <el-tag v-if="row.scope === 'global'" type="info" size="small">平台共享</el-tag>
          <el-tag v-else :type="row.visibility === 'shared' ? 'success' : 'info'" size="small">
            {{ row.visibility === 'shared' ? '租户共享' : '私有' }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column label="作者" width="110">
        <template #default="{ row }">
          <span v-if="row.created_by">{{ row.created_by }}</span>
          <span v-else class="muted">—</span>
        </template>
      </el-table-column>
      <el-table-column prop="tenant_id" label="租户" width="130" />
      <el-table-column prop="base_url" label="Base URL" show-overflow-tooltip />
      <el-table-column label="操作" width="150">
        <template #default="{ row }">
          <template v-if="canManage(row)">
            <el-button size="small" @click="openEdit(row)">编辑</el-button>
            <el-button size="small" type="danger" @click="remove(row)">删除</el-button>
          </template>
          <span v-else class="muted">只读</span>
        </template>
      </el-table-column>
    </el-table>

    <el-dialog v-model="dialogVisible" :title="editing ? '编辑端点' : '新建端点'" width="560px">
      <el-form :model="form" label-width="100px">
        <el-form-item label="ID">
          <el-input v-model="form.id" :disabled="editing" placeholder="endpoint id" />
        </el-form-item>
        <el-form-item label="名称">
          <el-input v-model="form.name" placeholder="管理用名称" />
        </el-form-item>
        <el-form-item label="协议">
          <el-select v-model="form.provider">
            <el-option label="OpenAI 兼容" value="openai" />
            <el-option label="Anthropic" value="anthropic" />
            <el-option label="Gemini" value="gemini" />
          </el-select>
        </el-form-item>
        <el-form-item label="类型">
          <el-select v-model="form.type">
            <el-option label="chat（对话）" value="chat" />
            <el-option label="embedding（向量化）" value="embedding" />
          </el-select>
        </el-form-item>
        <el-form-item label="Base URL">
          <el-input v-model="form.base_url" placeholder="https://api.openai.com/v1" />
        </el-form-item>
        <el-form-item label="模型名">
          <el-input v-model="form.model_name" placeholder="gpt-4o / claude-3-5 / glm-4.7" />
        </el-form-item>
        <el-form-item label="API Key">
          <el-input v-model="form.api_key" type="password" show-password placeholder="sk-..." />
        </el-form-item>
        <!-- global 是平台资产，只有 owner 能建（后端强制 403） -->
        <el-form-item v-if="authStore.userRole === 'owner'" label="作用域">
          <el-select v-model="form.scope">
            <el-option label="租户" value="tenant" />
            <el-option label="全局" value="global" />
          </el-select>
        </el-form-item>
        <el-form-item v-if="form.scope === 'tenant'" label="租户 ID">
          <el-input v-model="form.tenant_id" placeholder="tenant id" />
        </el-form-item>
        <el-form-item v-if="form.scope === 'tenant'" label="可见性">
          <el-radio-group v-model="form.visibility">
            <el-radio value="private">私有（仅我与租户管理员）</el-radio>
            <el-radio value="shared">共享给租户</el-radio>
          </el-radio-group>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dialogVisible = false">取消</el-button>
        <el-button type="primary" @click="submit">保存</el-button>
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
</style>
