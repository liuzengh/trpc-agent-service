<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { useTenantStore, type Tenant } from '../stores/tenant'

const store = useTenantStore()
const dialogVisible = ref(false)
const editing = ref(false)

const form = reactive<Tenant>({ id: '', name: '', status: 'active' })

onMounted(() => store.fetch())

function openCreate() {
  editing.value = false
  Object.assign(form, { id: '', name: '', status: 'active' })
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
    await store.remove(row.id)
    ElMessage.success('已删除')
  } catch (e) {
    ElMessage.error(String(e))
  }
}
</script>

<template>
  <main class="tenant-page">
    <h1>租户管理</h1>
    <div class="toolbar">
      <el-button type="primary" @click="openCreate">新建租户</el-button>
    </div>

    <el-table v-loading="store.loading" :data="store.tenants" border>
      <el-table-column prop="id" label="ID" width="220" />
      <el-table-column prop="name" label="名称" />
      <el-table-column prop="status" label="状态" width="120" />
      <el-table-column label="操作" width="180">
        <template #default="{ row }">
          <el-button size="small" @click="openEdit(row)">编辑</el-button>
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
  </main>
</template>

<style scoped>
.tenant-page {
  padding: 24px;
}
.toolbar {
  margin-bottom: 16px;
}
</style>
