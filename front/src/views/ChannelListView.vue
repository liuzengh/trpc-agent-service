<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { createChannel, deleteChannel, listChannels, type ChannelBinding } from '../api/channel'
import { putSecret } from '../api/secret'
import { useAgentStore } from '../stores/agent'
import { useAuthStore } from '../stores/auth'

const agents = useAgentStore()
const authStore = useAuthStore()

const bindings = ref<ChannelBinding[]>([])
const loading = ref(false)
const form = reactive({
  tenant_id: '',
  agent_id: '',
  channel: 'wecom' as 'wecom' | 'feishu',
  account_id: '',
  credential: '', // bot/app Secret — stored in the secret store, never in the binding
  verification_token: '', // Feishu only
  visibility: 'private' as 'private' | 'shared',
})

// 行级权限：admin/owner 管全租户；member 只能解绑自己创建的绑定。
const canManage = (row: ChannelBinding) => authStore.canManageAsset(row as { created_by?: string })

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
  if (!form.agent_id || !form.account_id.trim() || !form.credential.trim()) {
    ElMessage.warning('请选择 Agent，并填写账号 ID 与 Secret')
    return
  }
  // 非 owner 固定绑定到自己所属租户（后端同样会强制钉住，这里只是不再暴露输入）。
  const tenantId = authStore.userRole === 'owner' ? form.tenant_id.trim() : authStore.tenantId
  if (!tenantId) {
    ElMessage.warning('缺少租户 ID')
    return
  }
  try {
    const account = form.account_id.trim()
    // Credentials go into the secret store (encrypted) under a per-tenant key;
    // the binding keeps only the reference. The tenant is part of the key so two
    // tenants binding the same account id can never share one secret.
    const secretKey = `${tenantId}/${form.channel}:${account}:secret`
    await putSecret(secretKey, form.credential.trim())

    let tokenRef: string | undefined
    if (form.channel === 'feishu' && form.verification_token.trim()) {
      tokenRef = `${tenantId}/${form.channel}:${account}:verify_token`
      await putSecret(tokenRef, form.verification_token.trim())
    }

    await createChannel({
      tenant_id: tenantId,
      agent_id: form.agent_id,
      channel: form.channel,
      account_id: account,
      credential_ref: secretKey,
      verification_token_ref: tokenRef,
      visibility: form.visibility,
    })
    ElMessage.success('已绑定 IM 账号，网关已连接')
    Object.assign(form, { account_id: '', credential: '', verification_token: '', channel: 'wecom' })
    await refresh()
  } catch (e) {
    ElMessage.error(String(e))
  }
}

async function remove(row: ChannelBinding) {
  try {
    await deleteChannel(row.binding_id)
    ElMessage.success('已解绑，网关已断开')
    await refresh()
  } catch (e) {
    ElMessage.error(String(e))
  }
}
</script>

<template>
  <main class="channel-page">
    <h1>IM 通道绑定</h1>
    <p class="hint">把企业微信 / 飞书账号绑定到租户下的 Agent。Secret / Verification Token 会加密存入凭据库，绑定保存后网关立即建立长连接（无需改 config）。</p>

    <el-card class="create-card" shadow="never">
      <template #header>新建绑定</template>
      <div class="create-row">
        <el-select v-model="form.channel" style="width: 120px">
          <el-option label="企业微信" value="wecom" />
          <el-option label="飞书" value="feishu" />
        </el-select>
        <!-- 只有 owner 可跨租户绑定；其他人固定绑定自己的租户（后端强制） -->
        <el-input
          v-if="authStore.userRole === 'owner'"
          v-model="form.tenant_id"
          placeholder="租户 ID"
          style="width: 160px"
        />
        <el-tag v-else type="info" style="align-self: center">租户：{{ authStore.tenantId }}</el-tag>
        <el-select v-model="form.agent_id" placeholder="Agent" style="width: 200px" filterable>
          <el-option v-for="a in agents.agents" :key="a.id" :label="`${a.name} (${a.id})`" :value="a.id" />
        </el-select>
        <el-input v-model="form.account_id" :placeholder="form.channel === 'wecom' ? 'Bot ID' : 'App ID'" style="width: 200px" />
        <el-input v-model="form.credential" :placeholder="form.channel === 'wecom' ? 'Secret' : 'App Secret'" show-password style="width: 200px" />
        <el-input
          v-if="form.channel === 'feishu'"
          v-model="form.verification_token"
          placeholder="Verification Token"
          show-password
          style="width: 200px"
        />
        <el-select v-model="form.visibility" style="width: 130px">
          <el-option label="私有" value="private" />
          <el-option label="共享给租户" value="shared" />
        </el-select>
        <el-button type="primary" @click="submit">绑定</el-button>
      </div>
    </el-card>

    <el-table v-loading="loading" :data="bindings" border class="tbl">
      <el-table-column prop="channel" label="通道" width="110" />
      <el-table-column prop="tenant_id" label="租户" width="140" />
      <el-table-column prop="agent_id" label="Agent" width="180" />
      <el-table-column prop="account_id" label="账号 ID" width="180" show-overflow-tooltip />
      <el-table-column label="可见性" width="120">
        <template #default="{ row }">
          <el-tag :type="row.visibility === 'shared' ? 'success' : 'info'" size="small">
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
      <el-table-column prop="credential_ref" label="凭证引用" show-overflow-tooltip />
      <el-table-column label="操作" width="100">
        <template #default="{ row }">
          <el-button v-if="canManage(row)" size="small" type="danger" @click="remove(row)">解绑</el-button>
          <span v-else class="muted">只读</span>
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
