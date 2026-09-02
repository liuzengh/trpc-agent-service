<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { useEndpointStore } from '../stores/endpoint'
import type { Endpoint } from '../api/endpoint'

const store = useEndpointStore()
const dialogVisible = ref(false)
const editing = ref(false)

const form = reactive<Endpoint>({
  id: '',
  scope: 'tenant',
  tenant_id: '',
  name: '',
  provider: 'openai',
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
    tenant_id: '',
    name: '',
    provider: 'openai',
    base_url: '',
    model_name: '',
    api_key: '',
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
    <p class="hint">配置 LLM 端点（OpenAI / Anthropic / Gemini / 兼容协议），Agent 发布时按 endpoint_id 绑定。</p>
    <div class="toolbar">
      <el-button type="primary" @click="openCreate">新建端点</el-button>
    </div>

    <el-table v-loading="store.loading" :data="store.endpoints" border>
      <el-table-column prop="name" label="名称" width="160" />
      <el-table-column prop="provider" label="协议" width="130" />
      <el-table-column prop="model_name" label="模型" width="180" />
      <el-table-column prop="scope" label="作用域" width="100" />
      <el-table-column prop="tenant_id" label="租户" width="160" />
      <el-table-column prop="base_url" label="Base URL" show-overflow-tooltip />
      <el-table-column label="操作" width="180">
        <template #default="{ row }">
          <el-button size="small" @click="openEdit(row)">编辑</el-button>
          <el-button size="small" type="danger" @click="remove(row)">删除</el-button>
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
        <el-form-item label="Base URL">
          <el-input v-model="form.base_url" placeholder="https://api.openai.com/v1" />
        </el-form-item>
        <el-form-item label="模型名">
          <el-input v-model="form.model_name" placeholder="gpt-4o / claude-3-5 / glm-4.7" />
        </el-form-item>
        <el-form-item label="API Key">
          <el-input v-model="form.api_key" type="password" show-password placeholder="sk-..." />
        </el-form-item>
        <el-form-item label="作用域">
          <el-select v-model="form.scope">
            <el-option label="租户" value="tenant" />
            <el-option label="全局" value="global" />
          </el-select>
        </el-form-item>
        <el-form-item v-if="form.scope === 'tenant'" label="租户 ID">
          <el-input v-model="form.tenant_id" placeholder="tenant id" />
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
