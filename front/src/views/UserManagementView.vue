<template>
  <div class="user-management" style="padding: 20px;">
    <h2>用户管理</h2>
    <el-button v-if="canManage" type="primary" @click="openCreate">新增成员</el-button>
    <el-tag v-if="canManage" type="info" style="margin-left: 12px">
      {{ isOwner ? '平台视角：可见全部租户成员' : `当前租户：${authStore.tenantId}` }}
    </el-tag>

    <el-tabs v-model="activeTab">
      <el-tab-pane label="成员列表" name="members">
        <el-table :data="members" v-loading="loading" style="width: 100%">
          <el-table-column prop="user_id" label="用户 ID" width="180" />
          <el-table-column prop="tenant_id" label="租户" width="160" />
          <el-table-column prop="role" label="角色" width="120">
            <template #default="scope">
              <el-tag :type="tagType(scope.row.role)">
                {{ scope.row.role }}
              </el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="created_at" label="创建时间" width="180" />
          <el-table-column label="操作" v-if="canManage">
            <template #default="scope">
              <el-select
                v-model="scope.row.role"
                size="small"
                style="width: 120px; margin-right: 10px;"
                @change="(val) => updateMemberRole(scope.row.user_id, val)"
              >
                <el-option v-if="authStore.userRole === 'owner'" label="Owner" value="owner" />
                <el-option label="Admin" value="admin" />
                <el-option label="Member" value="member" />
              </el-select>
              <el-button
                type="danger"
                size="small"
                @click="deleteMember(scope.row.user_id)"
              >
                删除
              </el-button>
            </template>
          </el-table-column>
        </el-table>
      </el-tab-pane>

      <el-tab-pane label="权限说明" name="permissions" v-if="canManage">
        <el-descriptions title="角色权限说明" :column="1" border>
          <el-descriptions-item label="Owner">
            平台最高权限（跨所有租户）：租户管理、成员管理、Agent / 工具 / 知识库 / Skill /
            通道 / 密钥 / 审计 / DLQ，并可查看全部租户的对话、审计与用量。
          </el-descriptions-item>
          <el-descriptions-item label="Admin">
            只管自己所属租户：成员管理、Agent、工具、知识库、Skill、通道、审计、DLQ，
            并可查看本租户全部成员的对话历史；不可管理租户本身与平台密钥。
          </el-descriptions-item>
          <el-descriptions-item label="Member">
            租户员工：可读共享资产（Agent / 端点）并参与对话贡献；会话历史仅可见自己产生的，
            不可见租户管理、用户管理、密钥管理、审计日志与用量。
          </el-descriptions-item>
        </el-descriptions>
      </el-tab-pane>
    </el-tabs>

    <el-dialog v-model="createVisible" title="新增成员" width="420px">
      <el-form label-width="80px">
        <el-form-item label="用户名">
          <el-input v-model="newMember.user_id" autocomplete="off" />
        </el-form-item>
        <el-form-item label="绑定租户">
          <!-- owner 可自由选择成员所属租户；admin 只能把成员加到自己所属租户。 -->
          <el-select
            v-if="isOwner"
            v-model="newMember.tenant_id"
            filterable
            placeholder="选择租户"
            style="width: 100%"
          >
            <el-option v-for="t in tenantStore.tenants" :key="t.id" :label="t.name" :value="t.id" />
          </el-select>
          <el-input v-else :model-value="authStore.tenantId" disabled />
        </el-form-item>
        <el-form-item label="密码">
          <el-input v-model="newMember.password" type="password" show-password autocomplete="new-password" />
        </el-form-item>
        <el-form-item label="角色">
          <el-select v-model="newMember.role">
            <el-option label="Admin" value="admin" />
            <el-option label="Member" value="member" />
            <el-option v-if="authStore.userRole === 'owner'" label="Owner" value="owner" />
          </el-select>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="createVisible = false">取消</el-button>
        <el-button type="primary" @click="createMember">创建</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, computed } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { useAuthStore } from '../stores/auth'
import { useTenantStore } from '../stores/tenant'
import * as memberApi from '../api/member'
import type { Member, MemberRole } from '../api/member'

const authStore = useAuthStore()
const tenantStore = useTenantStore()
const activeTab = ref('members')
const loading = ref(false)
const createVisible = ref(false)
const newMember = ref({ user_id: '', password: '', role: 'member' as MemberRole, tenant_id: '' })

const members = ref<Member[]>([])

// 成员管理权（owner + admin）；owner 另外可以把成员放到任意租户。
const isOwner = computed(() => authStore.userRole === 'owner')
const canManage = computed(() => authStore.hasPermission('member:manage'))

// owner 的成员列表是全平台的，先确保租户下拉有数据可选。
onMounted(() => {
  fetchMembers()
  if (isOwner.value) void tenantStore.fetch()
})

function openCreate() {
  newMember.value = { user_id: '', password: '', role: 'member', tenant_id: authStore.tenantId }
  createVisible.value = true
}

function tagType(role: string) {
  if (role === 'owner') return 'danger'
  if (role === 'admin') return 'warning'
  return 'info'
}

async function fetchMembers() {
  loading.value = true
  try {
    members.value = await memberApi.listMembers()
  } catch (error) {
    ElMessage.error('获取成员列表失败')
  } finally {
    loading.value = false
  }
}

async function createMember() {
  const payload = newMember.value
  if (!payload.user_id || !payload.password) {
    ElMessage.warning('请输入用户名和密码')
    return
  }
  if (isOwner.value && !payload.tenant_id) {
    ElMessage.warning('请选择成员所属租户')
    return
  }
  try {
    // owner 指定 tenant_id；admin 不传（后端强制落到自己所属租户）。
    const created = await memberApi.createMember({
      user_id: payload.user_id,
      password: payload.password,
      role: payload.role,
      tenant_id: isOwner.value ? payload.tenant_id : undefined,
    })
    createVisible.value = false
    newMember.value = { user_id: '', password: '', role: 'member', tenant_id: '' }
    await fetchMembers()
    ElMessage.success(`成员已创建并绑定租户 ${created.tenant_id}`)
  } catch (error) {
    ElMessage.error('创建成员失败')
  }
}

async function updateMemberRole(userId: string, newRole: MemberRole) {
  try {
    await memberApi.updateMemberRole(userId, newRole)
    await fetchMembers()
    ElMessage.success('角色已更新')
  } catch (error) {
    ElMessage.error('修改角色失败')
    await fetchMembers()
  }
}

async function deleteMember(userId: string) {
  try {
    await ElMessageBox.confirm(`确定要删除成员 ${userId} 吗？`, '删除成员', {
      type: 'warning',
      confirmButtonText: '删除',
      cancelButtonText: '取消',
    })
  } catch {
    return
  }
  try {
    await memberApi.deleteMember(userId)
    await fetchMembers()
    ElMessage.success('成员已删除')
  } catch (error) {
    ElMessage.error('删除成员失败')
  }
}

</script>
