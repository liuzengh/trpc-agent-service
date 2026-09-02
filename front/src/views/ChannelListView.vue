<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { createChannel, deleteChannel, listChannels, type ChannelBinding } from '../api/channel'
import { useAgentStore } from '../stores/agent'

const agents = useAgentStore()

const bindings = ref<ChannelBinding[]>([])
const loading = ref(false)
const form = reactive({
  tenant_id: '',
  agent_id: '',
  channel: 'wecom' as 'wecom' | 'feishu',
  account_id: '',
  credential_ref: '',
})

onMounted(async () => {
  agents.fetch()
  await refresh()
})

async function refresh() {
  loading.value = true
  try {
    bindings.value = await listChannels()
  } catch (e) {
    ElMessage.error(String(e))
  } finally {
    loading.value = false
  }
}

async function submit() {
  if (!form.tenant_id.trim() || !form.agent_id || !form.account_id.trim()) {
    ElMessage.warning('请填写租户 ID、选择 Agent 与账号 ID')
    return
  }
  try {
    await createChannel({
      tenant_id: form.tenant_id.trim(),
      agent_id: form.agent_id,
      channel: form.channel,
      account_id: form.account_id.trim(),
      credential_ref: form.credential_ref.trim() || undefined,
    })
    ElMessage.success('已绑定 IM 账号')
    Object.assign(form, { account_id: '', credential_ref: '', channel: 'wecom' })
    await refresh()
  } catch (e) {
    ElMessage.error(String(e))
  }
}

async function remove(row: ChannelBinding) {
  try {
    await deleteChannel(row.binding_id)
    ElMessage.success('已解绑')
    await refresh()
  } catch (e) {
    ElMessage.error(String(e))
  }
}
</script>

<template>
  <main class="channel-page">
    <h1>IM 通道绑定</h1>
    <p class="hint">把企业微信 / 飞书账号绑定到租户下的 Agent；credential_ref 只存密钥引用（真实 SDK 收发需本地手测配置）。</p>

    <el-card class="create-card" shadow="never">
      <template #header>新建绑定</template>
      <div class="create-row">
        <el-select v-model="form.channel" style="width: 120px">
          <el-option label="企业微信" value="wecom" />
          <el-option label="飞书" value="feishu" />
        </el-select>
        <el-input v-model="form.tenant_id" placeholder="租户 ID" style="width: 160px" />
        <el-select v-model="form.agent_id" placeholder="Agent" style="width: 200px" filterable>
          <el-option v-for="a in agents.agents" :key="a.id" :label="`${a.name} (${a.id})`" :value="a.id" />
        </el-select>
        <el-input v-model="form.account_id" placeholder="账号 ID（企业/app）" style="width: 200px" />
        <el-input v-model="form.credential_ref" placeholder="credential 引用（可选）" style="width: 200px" />
        <el-button type="primary" @click="submit">绑定</el-button>
      </div>
    </el-card>

    <el-table v-loading="loading" :data="bindings" border class="tbl">
      <el-table-column prop="channel" label="通道" width="110" />
      <el-table-column prop="tenant_id" label="租户" width="160" />
      <el-table-column prop="agent_id" label="Agent" width="180" />
      <el-table-column prop="account_id" label="账号 ID" width="200" show-overflow-tooltip />
      <el-table-column prop="credential_ref" label="凭证引用" show-overflow-tooltip />
      <el-table-column label="操作" width="100">
        <template #default="{ row }">
          <el-button size="small" type="danger" @click="remove(row)">解绑</el-button>
        </template>
      </el-table-column>
    </el-table>
    <el-empty v-if="!loading && bindings.length === 0" description="暂无绑定" />
  </main>
</template>

<style scoped>
.channel-page {
  padding: 24px;
}
.hint {
  color: #909399;
  font-size: 13px;
  margin-top: -8px;
}
.create-card {
  margin-bottom: 16px;
}
.create-row {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}
.tbl {
  margin-top: 4px;
}
</style>
