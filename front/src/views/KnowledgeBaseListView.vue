<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { useKBStore } from '../stores/kb'
import { useEndpointStore } from '../stores/endpoint'
import type { Document, KBInput, SearchHit } from '../api/kb'

const store = useKBStore()
const endpoints = useEndpointStore()

// Only embedding endpoints can back a knowledge base.
const embeddingEndpoints = computed(() => endpoints.endpoints.filter((e) => e.type === 'embedding'))

// ---- KB create dialog ----
const dialogVisible = ref(false)
const form = reactive<KBInput>({ id: '', tenant_id: '', name: '', embedding_endpoint_id: '' })

// ---- documents dialog ----
const docsVisible = ref(false)
const activeKB = ref('')
const activeKBName = ref('')
const docs = ref<Document[]>([])
const docsLoading = ref(false)
const docForm = reactive({ title: '', source_uri: '', text: '' })
const addingDoc = ref(false)

// ---- search dialog ----
const searchVisible = ref(false)
const searchQuery = ref('')
const searching = ref(false)
const hits = ref<SearchHit[]>([])

onMounted(() => {
  store.fetch()
  endpoints.fetch()
})

function openCreate() {
  Object.assign(form, { id: '', tenant_id: '', name: '', embedding_endpoint_id: '' })
  dialogVisible.value = true
}

async function submit() {
  try {
    await store.create({ ...form })
    dialogVisible.value = false
    ElMessage.success('已创建知识库')
  } catch (e) {
    ElMessage.error(String(e))
  }
}

async function remove(row: { id: string; name: string }) {
  try {
    await store.remove(row.id)
    ElMessage.success('已删除')
  } catch (e) {
    ElMessage.error(String(e))
  }
}

async function openDocs(row: { id: string; name: string }) {
  activeKB.value = row.id
  activeKBName.value = row.name
  docsVisible.value = true
  docsLoading.value = true
  docs.value = []
  Object.assign(docForm, { title: '', source_uri: '', text: '' })
  try {
    docs.value = await store.listDocuments(row.id)
  } catch (e) {
    ElMessage.error(String(e))
  } finally {
    docsLoading.value = false
  }
}

async function addDoc() {
  if (!docForm.source_uri.trim() && !docForm.text.trim()) {
    ElMessage.warning('填写 URL 或内联文本')
    return
  }
  addingDoc.value = true
  try {
    const body: Record<string, string> = { title: docForm.title.trim() }
    if (docForm.source_uri.trim()) {
      body.source_uri = docForm.source_uri.trim()
    } else {
      body.source_uri = 'inline'
      body.text = docForm.text
    }
    await store.addDocument(activeKB.value, body)
    ElMessage.success('已摄入，稍后状态就绪')
    Object.assign(docForm, { title: '', source_uri: '', text: '' })
    docs.value = await store.listDocuments(activeKB.value)
  } catch (e) {
    ElMessage.error(String(e))
  } finally {
    addingDoc.value = false
  }
}

function statusTag(s: string) {
  return s === 'ready' ? 'success' : s === 'failed' ? 'danger' : 'warning'
}

async function openSearch(row: { id: string; name: string }) {
  activeKB.value = row.id
  activeKBName.value = row.name
  searchQuery.value = ''
  hits.value = []
  searchVisible.value = true
}

async function doSearch() {
  if (!searchQuery.value.trim()) return
  searching.value = true
  try {
    hits.value = await store.search(activeKB.value, searchQuery.value.trim())
  } catch (e) {
    ElMessage.error(String(e))
  } finally {
    searching.value = false
  }
}
</script>

<template>
  <main class="kb-page">
    <h1>知识库管理</h1>
    <p class="hint">知识库是租户级共享资产，可挂载到多个 Agent；向量存 Milvus/内存，检索由 Agent 运行时注入。</p>
    <div class="toolbar">
      <el-button type="primary" @click="openCreate">新建知识库</el-button>
    </div>

    <el-table v-loading="store.loading" :data="store.kbs" border>
      <el-table-column prop="name" label="名称" width="160" />
      <el-table-column prop="tenant_id" label="租户" width="160" />
      <el-table-column prop="collection_name" label="向量集合" />
      <el-table-column prop="embedding_endpoint_id" label="嵌入端点" width="200" show-overflow-tooltip />
      <el-table-column prop="dimension" label="维度" width="90" />
      <el-table-column label="操作" width="260">
        <template #default="{ row }">
          <el-button size="small" @click="openDocs(row)">文档</el-button>
          <el-button size="small" type="primary" @click="openSearch(row)">检索</el-button>
          <el-button size="small" type="danger" @click="remove(row)">删除</el-button>
        </template>
      </el-table-column>
    </el-table>

    <!-- create -->
    <el-dialog v-model="dialogVisible" title="新建知识库" width="520px">
      <el-form :model="form" label-width="120px">
        <el-form-item label="ID">
          <el-input v-model="form.id" placeholder="留空自动生成" />
        </el-form-item>
        <el-form-item label="租户 ID">
          <el-input v-model="form.tenant_id" placeholder="tenant id" />
        </el-form-item>
        <el-form-item label="名称">
          <el-input v-model="form.name" placeholder="KB 名称" />
        </el-form-item>
        <el-form-item label="嵌入端点">
          <el-select v-model="form.embedding_endpoint_id" placeholder="选择 embedding 端点" style="width: 100%">
            <el-option
              v-for="e in embeddingEndpoints"
              :key="e.id"
              :label="`${e.name} (${e.model_name})`"
              :value="e.id"
            />
          </el-select>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dialogVisible = false">取消</el-button>
        <el-button type="primary" @click="submit">创建</el-button>
      </template>
    </el-dialog>

    <!-- documents -->
    <el-dialog v-model="docsVisible" :title="`文档管理 · ${activeKBName}`" width="720px">
      <el-form label-width="80px" class="doc-form">
        <el-form-item label="标题">
          <el-input v-model="docForm.title" placeholder="文档标题（可选）" />
        </el-form-item>
        <el-form-item label="来源 URL">
          <el-input v-model="docForm.source_uri" placeholder="https://...（抓取网页摄入）" />
        </el-form-item>
        <el-form-item label="或内联文本">
          <el-input v-model="docForm.text" type="textarea" :rows="3" placeholder="直接粘贴文本摄入（二选一）" />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" :loading="addingDoc" @click="addDoc">摄入文档</el-button>
        </el-form-item>
      </el-form>
      <el-divider />
      <el-table v-loading="docsLoading" :data="docs" border max-height="300">
        <el-table-column prop="title" label="标题" width="180" show-overflow-tooltip />
        <el-table-column prop="source_uri" label="来源" show-overflow-tooltip />
        <el-table-column prop="chunk_count" label="分块" width="70" />
        <el-table-column label="状态" width="100">
          <template #default="{ row }">
            <el-tag :type="statusTag(row.status)">{{ row.status }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="error" label="错误" show-overflow-tooltip />
      </el-table>
    </el-dialog>

    <!-- search -->
    <el-dialog v-model="searchVisible" :title="`检索 · ${activeKBName}`" width="680px">
      <div class="search-row">
        <el-input v-model="searchQuery" placeholder="输入查询，回车检索" @keyup.enter="doSearch" />
        <el-button type="primary" :loading="searching" @click="doSearch">检索</el-button>
      </div>
      <el-table v-if="hits.length" :data="hits" border class="hit-table">
        <el-table-column prop="source_name" label="来源" width="200" show-overflow-tooltip />
        <el-table-column prop="content" label="内容" show-overflow-tooltip />
        <el-table-column prop="score" label="得分" width="100" />
      </el-table>
      <el-empty v-else-if="!searching" description="暂无结果" :image-size="60" />
    </el-dialog>
  </main>
</template>

<style scoped>
.kb-page {
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
.doc-form {
  margin-bottom: -8px;
}
.search-row {
  display: flex;
  gap: 8px;
  margin-bottom: 12px;
}
.hit-table {
  margin-top: 4px;
}
</style>
