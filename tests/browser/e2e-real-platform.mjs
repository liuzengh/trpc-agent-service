// 真实平台 E2E：浏览器 → Go Gateway → PostgreSQL/Redis/Kafka → Worker → mock model → Outbox/SSE。
// 不拦截任何 /api 请求；测试数据使用唯一后缀，允许在共享测试环境重复执行。
import { chromium } from 'playwright'
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const here = path.dirname(fileURLToPath(import.meta.url))
const BASE = process.env.E2E_BASE ?? 'http://127.0.0.1:18080'
const systemAdminUsername = process.env.E2E_SYSTEM_ADMIN_USERNAME ?? 'e2e-root'
const systemAdminPassword = process.env.E2E_SYSTEM_ADMIN_PASSWORD ?? 'E2e-root-password-A9!'
const systemAdminChangedPassword = `${systemAdminPassword}-changed`
const artifacts = path.join(here, 'artifacts')
fs.mkdirSync(artifacts, { recursive: true })

const suffix = `${Date.now().toString(36)}-${process.pid}`
const adminUsername = `support-admin-${suffix}`
const memberUsername = `support-member-${suffix}`
const adminDisplay = `客服管理员 ${suffix}`
const memberDisplay = `客服成员 ${suffix}`
const tenantID = `support-${suffix}`
const tenantName = `客服测试 ${suffix}`
const appCode = `assistant-${suffix}`
const documentID = `faq-${suffix}`
const backendID = `postgres-${suffix}`
const backendName = `测试 PostgreSQL ${suffix}`
const newPassword = `Support-${suffix}-A9!`
const modelName = process.env.E2E_MODEL_NAME ?? 'mock-model'
const modelReply = process.env.E2E_MODEL_REPLY ?? (modelName === 'mock-model'
  ? '你好！我是本地 mock 模型，这条回复说明对话链路（Runner / 会话 / 审计 / Outbox）已经全部打通。'
  : '')
const discoveredModelName = process.env.E2E_DISCOVERED_MODEL_NAME ?? (modelName === 'mock-model' ? 'mock-discovered' : '')

let assertions = 0
function expect(condition, message) {
  assertions += 1
  if (!condition) throw new Error(`断言失败: ${message}`)
}

async function openConsole(page) {
  await page.goto(`${BASE}/console/`, { waitUntil: 'domcontentloaded' })
}

async function loginSystemAdmin(page) {
  await openConsole(page)
  await page.getByRole('textbox', { name: '本地用户名' }).fill(systemAdminUsername)
  await page.getByLabel('本地密码').fill(systemAdminPassword)
  await page.getByRole('button', { name: '登录', exact: true }).click()

  const changePassword = page.getByRole('heading', { name: '修改临时密码' })
  try {
    await changePassword.waitFor({ timeout: 3000 })
    await page.getByLabel('当前临时密码').fill(systemAdminPassword)
    await page.getByLabel('新密码', { exact: true }).fill(systemAdminChangedPassword)
    await page.getByLabel('确认新密码').fill(systemAdminChangedPassword)
    await page.getByRole('button', { name: '修改密码并继续', exact: true }).click()
  } catch {
    if (!(await page.locator('.sidebar-user-name').isVisible().catch(() => false))) {
      await page.getByRole('textbox', { name: '本地用户名' }).fill(systemAdminUsername)
      await page.getByLabel('本地密码').fill(systemAdminChangedPassword)
      await page.getByRole('button', { name: '登录', exact: true }).click()
    }
  }
  await page.waitForSelector('.sidebar-user-name', { timeout: 15000 })
  expect(await page.getByRole('tab', { name: '用户', exact: true }).isVisible(), '部署自举的本地系统管理员应看到用户管理')
}

async function createLocalUser(page, username, displayName) {
  await page.getByRole('tab', { name: '用户', exact: true }).click()
  await page.getByRole('button', { name: '新建本地用户', exact: true }).click()
  const dialog = page.getByRole('dialog', { name: '新建本地用户' })
  await dialog.getByPlaceholder('例如 ming').fill(username)
  await dialog.getByPlaceholder('用户姓名（可选）').fill(displayName)
  await dialog.getByRole('button', { name: '创建用户', exact: true }).click()
  const credential = page.locator('.user-credential-reveal')
  await credential.waitFor({ timeout: 10000 })
  await expectRow(page, displayName)
  return (await credential.locator('code').innerText()).trim()
}

async function expectRow(page, text, timeout = 10000) {
  const marker = page.getByText(text, { exact: true }).first()
  await marker.waitFor({ timeout })
  return marker.locator('xpath=ancestor::tr[1]')
}

async function selectOption(page, trigger, optionName) {
  await trigger.click()
  const option = page.getByRole('option', { name: optionName, exact: true })
  await option.waitFor({ timeout: 10000 })
  await option.click()
}

async function createTenantAndAuthorize(page) {
  await page.getByRole('tab', { name: '租户', exact: true }).click()

  // 取消创建必须只关闭当前操作，不产生租户。
  await page.getByRole('button', { name: '新建租户', exact: true }).click()
  let createDialog = page.getByRole('dialog', { name: '新建租户' })
  await createDialog.getByPlaceholder('例如 support').fill(`cancelled-${suffix}`)
  await createDialog.getByPlaceholder('例如客户支持团队').fill('不会创建的租户')
  await createDialog.getByRole('button', { name: '取消', exact: true }).click()
  await createDialog.waitFor({ state: 'detached' })
  expect((await page.getByText(`cancelled-${suffix}`, { exact: true }).count()) === 0, '取消创建租户不应产生新租户')

  await page.getByRole('button', { name: '新建租户', exact: true }).click()
  createDialog = page.getByRole('dialog', { name: '新建租户' })
  await createDialog.getByPlaceholder('例如 support').fill(tenantID)
  await createDialog.getByPlaceholder('例如客户支持团队').fill(tenantName)
  await selectOption(page, createDialog.getByRole('combobox', { name: '首个租户管理员' }), adminDisplay)
  await createDialog.getByRole('button', { name: '创建租户', exact: true }).click()

  const tenantRow = await expectRow(page, tenantID)
  await tenantRow.getByRole('button', { name: '资源授权', exact: true }).click()
  const policy = page.getByRole('dialog', { name: '资源授权' })
  await policy.waitFor({ timeout: 10000 })

  const backendSection = policy.locator('section[aria-label="数据后端授权"]')
  for (const backend of ['平台 PostgreSQL', '平台 pgvector']) {
    const checkbox = backendSection.locator('label').filter({ hasText: backend }).getByRole('checkbox')
    await checkbox.waitFor({ timeout: 10000 })
    if (!(await checkbox.isChecked())) await checkbox.check()
  }
  // The resource policy dialog saves every section through one footer action
  // (“保存授权”) instead of per-section buttons.
  await policy.getByRole('button', { name: '保存授权', exact: true }).click()
  await page.getByText(`已保存 ${tenantName} 的资源授权`, { exact: true }).waitFor({ timeout: 10000 })

  // The dialog renders one policy section at a time, so switch to the model tab
  // before touching the model authorization checkboxes.
  await policy.getByRole('tab', { name: '模型', exact: true }).click()
  const modelSection = policy.locator('section[aria-label="模型授权"]')
  const modelCheckbox = modelSection.locator('label').filter({ hasText: modelName }).getByRole('checkbox')
  await modelCheckbox.waitFor({ timeout: 10000 })
  if (!(await modelCheckbox.isChecked())) await modelCheckbox.check()
  await policy.getByRole('button', { name: '保存授权', exact: true }).click()
  await page.getByText(`已保存 ${tenantName} 的资源授权`, { exact: true }).waitFor({ timeout: 10000 })

  // “关闭”不能隐式保存未提交的授权变更；重新打开应回到服务端已保存状态。
  await modelCheckbox.uncheck()
  // With unsaved changes the footer dismiss button reads “取消”, not “关闭”.
  await policy.getByRole('button', { name: '取消', exact: true }).click()
  await policy.waitFor({ state: 'detached' })
  await tenantRow.getByRole('button', { name: '资源授权', exact: true }).click()
  const reopenedPolicy = page.getByRole('dialog', { name: '资源授权' })
  await reopenedPolicy.getByRole('tab', { name: '模型', exact: true }).click()
  const reopenedModel = reopenedPolicy.locator('section[aria-label="模型授权"] label').filter({ hasText: modelName }).getByRole('checkbox')
  await reopenedModel.waitFor({ timeout: 10000 })
  expect(await reopenedModel.isChecked(), '关闭资源授权弹窗后重新打开应恢复已保存的模型授权')
  await reopenedPolicy.getByRole('button', { name: '关闭', exact: true }).click()
}

async function loginLocalAndChangePassword(page, temporaryPassword) {
  await openConsole(page)
  await page.getByRole('textbox', { name: '本地用户名' }).fill(adminUsername)
  await page.getByLabel('本地密码').fill(temporaryPassword)
  await page.getByRole('button', { name: '登录', exact: true }).click()

  await page.getByRole('heading', { name: '修改临时密码' }).waitFor({ timeout: 10000 })
  await page.getByLabel('当前临时密码').fill(temporaryPassword)
  await page.getByLabel('新密码', { exact: true }).fill(newPassword)
  await page.getByLabel('确认新密码').fill(newPassword)
  await page.getByRole('button', { name: '修改密码并继续', exact: true }).click()
  await page.waitForSelector('.sidebar-user-name', { timeout: 15000 })

  expect((await page.locator('.sidebar-user-name').innerText()).trim() === adminDisplay, '本地管理员首次改密后应进入平台')
  expect((await page.getByRole('tab', { name: '机器人', exact: true }).count()) === 0, '机器人管理不应占用一级导航')
  expect(await page.getByRole('button', { name: '机器人设置', exact: true }).isVisible(), '租户管理员应能从应用级设置进入机器人管理')
  expect((await page.getByRole('tab', { name: '用户', exact: true }).count()) === 0, '租户管理员不应看到系统用户管理')
}

async function openBotManagement(page) {
  if (await page.getByRole('button', { name: '创建机器人', exact: true }).isVisible().catch(() => false)) return
  let settings = page.getByRole('button', { name: '机器人设置', exact: true })
  if (!(await settings.isVisible().catch(() => false))) {
    await page.getByRole('tab', { name: '对话', exact: true }).click()
    settings = page.getByRole('button', { name: '机器人设置', exact: true })
    await settings.waitFor({ timeout: 10000 })
  }
  await settings.click()
  await page.getByRole('button', { name: '创建机器人', exact: true }).waitFor({ timeout: 10000 })
}

async function addTenantMember(page) {
  await page.getByRole('tab', { name: '成员', exact: true }).click()
  await page.getByRole('button', { name: '添加成员', exact: true }).click()
  const dialog = page.getByRole('dialog', { name: '添加租户成员' })
  const search = dialog.getByRole('searchbox', { name: '搜索可添加用户' })
  await search.fill(memberDisplay)
  await search.press('Enter')
  await selectOption(page, dialog.getByRole('combobox', { name: '选择用户' }), memberDisplay)
  await dialog.getByRole('button', { name: '添加成员', exact: true }).click()
  const row = await expectRow(page, memberDisplay)
  expect((await row.innerText()).includes('租户成员'), '新增用户应以租户成员身份出现')
}

async function exerciseMemberControls(page) {
  await page.getByRole('tab', { name: '成员', exact: true }).click()
  await page.getByRole('button', { name: '刷新成员', exact: true }).click()

  let row = await expectRow(page, memberDisplay)
  await selectOption(page, row.getByRole('combobox', { name: `${memberDisplay}租户角色` }), '租户管理员')
  await page.getByText('成员权限已更新', { exact: true }).waitFor({ timeout: 10000 })
  row = await expectRow(page, memberDisplay)
  expect((await row.innerText()).includes('租户管理员'), '成员角色切换应持久化')

  await row.getByRole('button', { name: '未授权', exact: true }).click()
  row = await expectRow(page, memberDisplay)
  await row.getByRole('button', { name: '已授权', exact: true }).waitFor({ timeout: 10000 })
  await row.getByRole('button', { name: '已授权', exact: true }).click()
  row = await expectRow(page, memberDisplay)
  await row.getByRole('button', { name: '未授权', exact: true }).waitFor({ timeout: 10000 })

  await row.getByRole('button', { name: '正常', exact: true }).click()
  row = await expectRow(page, memberDisplay)
  await row.getByRole('button', { name: '已停用', exact: true }).waitFor({ timeout: 10000 })
  await row.getByRole('button', { name: '已停用', exact: true }).click()
  row = await expectRow(page, memberDisplay)
  await row.getByRole('button', { name: '正常', exact: true }).waitFor({ timeout: 10000 })

  await selectOption(page, row.getByRole('combobox', { name: `${memberDisplay}租户角色` }), '租户成员')
  row = await expectRow(page, memberDisplay)
  await row.getByText('租户成员', { exact: true }).waitFor({ timeout: 10000 })
  expect((await row.innerText()).includes('租户成员'), '成员角色应能恢复为普通成员')
}

async function exerciseSystemUserControls(page) {
  await page.getByRole('tab', { name: '用户', exact: true }).click()
  await page.getByRole('button', { name: '刷新用户', exact: true }).click()

  const search = page.getByRole('searchbox', { name: '搜索平台用户' })
  await search.fill(memberDisplay)
  await search.press('Enter')
  let row = await expectRow(page, memberDisplay)
  expect((await page.locator('.members-table tbody tr').count()) === 1, '平台用户搜索应缩小结果集')
  await page.getByRole('button', { name: '清空搜索平台用户', exact: true }).click()
  row = await expectRow(page, memberDisplay)

  await row.getByRole('button', { name: '普通用户', exact: true }).click()
  row = await expectRow(page, memberDisplay)
  await row.getByRole('button', { name: '系统管理员', exact: true }).waitFor({ timeout: 10000 })
  await row.getByRole('button', { name: '系统管理员', exact: true }).click()
  row = await expectRow(page, memberDisplay)
  await row.getByRole('button', { name: '普通用户', exact: true }).waitFor({ timeout: 10000 })

  await row.getByRole('button', { name: '正常', exact: true }).click()
  row = await expectRow(page, memberDisplay)
  await row.getByRole('button', { name: '已停用', exact: true }).waitFor({ timeout: 10000 })
  await row.getByRole('button', { name: '已停用', exact: true }).click()
  row = await expectRow(page, memberDisplay)
  await row.getByRole('button', { name: '正常', exact: true }).waitFor({ timeout: 10000 })

  await row.getByRole('button', { name: '重置密码', exact: true }).click()
  const credential = page.locator('.user-credential-reveal')
  await credential.waitFor({ timeout: 10000 })
  expect((await credential.locator('code').innerText()).trim().length >= 12, '重置本地密码应产生一次性临时密码')
  await credential.getByRole('button', { name: '复制密码', exact: true }).click()
  await credential.getByRole('button', { name: '已复制', exact: true }).waitFor({ timeout: 3000 })
  await credential.getByRole('button', { name: '关闭', exact: true }).click()
  await credential.waitFor({ state: 'detached' })
}

async function exerciseBackendControls(page) {
  await page.getByRole('tab', { name: '数据后端', exact: true }).click()
  await page.getByRole('button', { name: '刷新数据后端', exact: true }).click()
  await page.getByRole('button', { name: '新建数据后端', exact: true }).click()
  let dialog = page.getByRole('dialog', { name: '新建数据后端' })
  await dialog.getByPlaceholder('例如 session-redis-a').fill(backendID)
  await dialog.getByPlaceholder('例如会话 Redis A').fill(backendName)
  const createBackendButton = dialog.getByRole('button', { name: '创建数据后端', exact: true })
  expect(await createBackendButton.isDisabled(), '多数据域后端未选择用途时不应允许创建')
  await dialog.getByRole('button', { name: '会话', exact: true }).click()
  await createBackendButton.click()
  let row = await expectRow(page, backendID)
  expect((await row.innerText()).includes('PostgreSQL'), '新建 PostgreSQL 后端应显示正确类型')

  await row.getByRole('button', { name: '编辑', exact: true }).click()
  dialog = page.getByRole('dialog', { name: '编辑数据后端' })
  const editedName = `${backendName} 已编辑`
  await dialog.getByPlaceholder('例如会话 Redis A').fill(editedName)
  const statusSelect = dialog.locator('label').filter({ hasText: '状态' }).getByRole('combobox')
  await selectOption(page, statusSelect, '已停用')
  await dialog.getByRole('button', { name: '保存修改', exact: true }).click()
  row = await expectRow(page, editedName)
  expect((await row.innerText()).includes('已停用'), '编辑数据后端应持久化状态')

  await row.getByRole('button', { name: `删除 ${editedName}`, exact: true }).click()
  const confirm = page.getByRole('dialog', { name: '删除数据后端？' })
  await confirm.getByRole('button', { name: '取消', exact: true }).click()
  await confirm.waitFor({ state: 'detached' })
  row = await expectRow(page, editedName)
  await row.getByRole('button', { name: `删除 ${editedName}`, exact: true }).click()
  const confirmAgain = page.getByRole('dialog', { name: '删除数据后端？' })
  await confirmAgain.getByRole('button', { name: '删除', exact: true }).click()
  await page.getByText(editedName, { exact: true }).waitFor({ state: 'detached', timeout: 10000 })
}

async function exerciseModelControls(page) {
  await page.getByRole('tab', { name: '模型资产', exact: true }).click()
  await page.getByRole('button', { name: '同步模型', exact: true }).click()
  await page.getByText('模型目录已同步', { exact: true }).waitFor({ timeout: 15000 })
  await page.getByRole('button', { name: '关闭通知' }).click()

  await page.getByText(modelName, { exact: true }).first().waitFor({ timeout: 10000 })
  if (!discoveredModelName) return

  const search = page.getByRole('searchbox', { name: '搜索模型' })
  await search.fill(discoveredModelName)
  await page.getByText(discoveredModelName, { exact: true }).waitFor({ timeout: 10000 })
  await page.getByRole('button', { name: '清空搜索模型', exact: true }).click()

  await page.getByRole('button', { name: `删除模型 ${discoveredModelName}`, exact: true }).click()
  let confirm = page.getByRole('dialog', { name: '删除模型？' })
  await confirm.getByRole('button', { name: '取消', exact: true }).click()
  await confirm.waitFor({ state: 'detached' })
  await page.getByRole('button', { name: `删除模型 ${discoveredModelName}`, exact: true }).click()
  confirm = page.getByRole('dialog', { name: '删除模型？' })
  await confirm.getByRole('button', { name: '删除模型', exact: true }).click()
  await page.getByText(`已移除 ${discoveredModelName}`, { exact: true }).waitFor({ timeout: 10000 })
}

async function exerciseAccountAndSystemControls(page) {
  await page.getByLabel('打开账号菜单').click()
  await page.getByRole('button', { name: '账号设置', exact: true }).click()
  const nameInput = page.getByLabel('账号名称')
  const originalName = (await nameInput.inputValue()).trim()
  expect(await page.getByRole('button', { name: '保存', exact: true }).isDisabled(), '账号名称未变化时保存按钮应禁用')
  expect((await page.getByText('普通账号', { exact: true }).count()) > 0, '账号页应显示本地登录身份')
  const changedName = `系统管理员 ${suffix}`
  await nameInput.fill(changedName)
  await page.getByRole('button', { name: '保存', exact: true }).click()
  await page.getByText('账号名称已更新', { exact: true }).waitFor({ timeout: 10000 })
  expect((await page.locator('.sidebar-user-name').innerText()).trim() === changedName, '账号名称保存后侧栏应同步刷新')

  await nameInput.fill(originalName)
  await page.getByRole('button', { name: '保存', exact: true }).click()
  await page.getByText('账号名称已更新', { exact: true }).waitFor({ timeout: 10000 })

  await page.getByRole('tab', { name: '登录设置', exact: true }).click()
  await page.getByText('统一登录回调地址', { exact: true }).waitFor({ timeout: 10000 })
  const copyCallback = page.getByRole('button', { name: '复制地址', exact: true })
  if (await copyCallback.count()) {
    await copyCallback.click()
    await page.getByRole('button', { name: '已复制', exact: true }).waitFor({ timeout: 3000 })
  }
  const localProvider = page.locator('.login-provider-setting-card').filter({ hasText: '本地账号' })
  expect((await localProvider.count()) === 1, '登录设置应展示当前启用的本地登录方式')

  await page.getByRole('tab', { name: '系统状态', exact: true }).click()
  await page.getByRole('button', { name: '重新探测系统状态', exact: true }).click()
  await page.getByText('PostgreSQL', { exact: true }).first().waitFor({ timeout: 10000 })
  const copyListen = page.getByRole('button', { name: '复制监听地址', exact: true })
  await copyListen.click()
  await page.getByRole('button', { name: '已复制监听地址', exact: true }).waitFor({ timeout: 3000 })
}

async function exerciseTenantStatusControl(page) {
  await page.getByRole('tab', { name: '租户', exact: true }).click()
  await page.getByRole('button', { name: '刷新租户', exact: true }).click()
  let row = await expectRow(page, tenantID)
  await row.getByRole('button', { name: '正常', exact: true }).click()
  row = await expectRow(page, tenantID)
  await row.getByRole('button', { name: '已停用', exact: true }).waitFor({ timeout: 10000 })
  await row.getByRole('button', { name: '已停用', exact: true }).click()
  row = await expectRow(page, tenantID)
  await row.getByRole('button', { name: '正常', exact: true }).waitFor({ timeout: 10000 })
}

async function createAndActivateBot(page) {
  await openBotManagement(page)
  await page.getByRole('button', { name: '创建机器人', exact: true }).click()
  const dialog = page.getByRole('dialog', { name: '创建机器人' })
  await dialog.getByPlaceholder('例如 support').fill(appCode)
  await dialog.getByPlaceholder('描述机器人的角色、业务范围和回答规则。').fill('负责回答客服常见问题，回答简洁准确。')

  await dialog.getByRole('button', { name: '创建机器人', exact: true }).waitFor({ state: 'visible' })
  await page.waitForFunction(() => {
    const button = [...document.querySelectorAll('button')].find((entry) => entry.textContent?.trim() === '创建机器人')
    return Boolean(button && !button.disabled)
  }, undefined, { timeout: 15000 })
  await dialog.getByRole('button', { name: '创建机器人', exact: true }).click()

  const row = await expectRow(page, appCode, 15000)
  expect((await row.innerText()).includes('草稿'), '新机器人应先保存为草稿')
  await row.getByRole('button', { name: '启用', exact: true }).click()
  await page.getByRole('row').filter({ hasText: appCode }).filter({ hasText: '运行中' }).waitFor({ timeout: 15000 })
}

async function openBotEdit(page) {
  const row = await expectRow(page, appCode)
  await row.getByRole('button', { name: '配置', exact: true }).click()
  const dialog = page.getByRole('dialog', { name: `编辑机器人 · ${appCode}` })
  await dialog.waitFor({ timeout: 10000 })
  return dialog
}

async function stageBotCandidate(page, instruction) {
  const dialog = await openBotEdit(page)
  await dialog.getByPlaceholder('描述机器人的角色、业务范围和回答规则。').fill(instruction)
  await dialog.getByRole('button', { name: '保存并配置灰度', exact: true }).click()
  const release = page.getByRole('dialog', { name: `版本发布 · ${appCode}` })
  await release.waitFor({ timeout: 15000 })
  return release
}

async function exerciseBotControls(page) {
  await openBotManagement(page)
  await page.getByRole('searchbox', { name: '搜索机器人' }).fill(appCode)
  expect((await page.locator('.application-table tbody tr').count()) === 1, '机器人搜索应只保留匹配项')
  await page.getByRole('button', { name: '清空搜索机器人', exact: true }).click()
  await page.locator('.application-filters').getByRole('button', { name: /运行中/ }).click()
  await expectRow(page, appCode)
  await page.locator('.application-filters').getByRole('button', { name: /全部/ }).click()

  // 编辑器中的“添加/删除”按钮先验证本地表单状态，取消后不应污染真实配置。
  let dialog = await openBotEdit(page)
  await dialog.getByRole('button', { name: '添加渠道', exact: true }).click()
  await dialog.getByRole('button', { name: '删除渠道 1', exact: true }).waitFor()
  await dialog.getByRole('button', { name: '删除渠道 1', exact: true }).click()

  const httpSection = dialog.locator('.bot-tool-policy').filter({ hasText: '自定义 HTTP 工具' })
  await httpSection.getByRole('button', { name: '添加', exact: true }).click()
  await httpSection.getByRole('button', { name: '删除工具', exact: true }).waitFor()
  await httpSection.getByRole('button', { name: '删除工具', exact: true }).click()

  const mcpSection = dialog.locator('.bot-tool-policy').filter({ hasText: 'MCP 服务' })
  await mcpSection.getByRole('button', { name: '添加', exact: true }).click()
  await mcpSection.getByRole('button', { name: '删除服务', exact: true }).waitFor()
  await mcpSection.getByRole('button', { name: '删除服务', exact: true }).click()
  await dialog.getByRole('button', { name: '取消', exact: true }).click()
  await dialog.waitFor({ state: 'detached' })

  // 直接发布一版，验证稳定版本保存路径。
  let row = await expectRow(page, appCode)
  const versionBefore = (await row.locator('.time-cell').innerText()).trim()
  dialog = await openBotEdit(page)
  await dialog.getByPlaceholder('描述机器人的角色、业务范围和回答规则。').fill(`客服机器人直接发布 ${suffix}`)
  await dialog.getByRole('button', { name: '直接发布', exact: true }).click()
  await dialog.waitFor({ state: 'detached', timeout: 15000 })
  row = await expectRow(page, appCode)
  const versionAfter = (await row.locator('.time-cell').innerText()).trim()
  expect(versionAfter !== versionBefore, '直接发布应生成新的稳定版本')

  // 候选 → 灰度 → 调整 → 停止 → 全量发布。
  let release = await stageBotCandidate(page, `客服机器人候选发布 ${suffix}`)
  await release.getByRole('button', { name: '收起设置', exact: true }).click()
  await release.getByRole('button', { name: '展开设置', exact: true }).click()
  await release.getByRole('button', { name: '仅测试成员', exact: true }).click()
  await release.getByRole('button', { name: '开始灰度发布', exact: true }).click()
  await release.getByText('请选择至少一名测试成员，或设置灰度比例。', { exact: true }).waitFor({ timeout: 10000 })

  const memberPicker = release.locator('.release-member-picker .select-control')
  await selectOption(page, memberPicker, memberDisplay)
  await release.locator('.release-member-picker').getByRole('button', { name: '添加', exact: true }).click()
  await release.getByRole('button', { name: `移除 ${memberDisplay}`, exact: true }).waitFor()
  await release.getByRole('button', { name: `移除 ${memberDisplay}`, exact: true }).click()
  await selectOption(page, memberPicker, memberDisplay)
  await release.locator('.release-member-picker').getByRole('button', { name: '添加', exact: true }).click()

  const webIngress = release.locator('.release-ingress-grid label').filter({ hasText: '网页' }).getByRole('checkbox')
  await webIngress.uncheck()
  expect(await release.getByRole('button', { name: '开始灰度发布', exact: true }).isDisabled(), '没有发布入口时应禁止开始灰度')
  await webIngress.check()
  await release.getByRole('button', { name: '10%', exact: true }).click()
  await release.getByRole('button', { name: '开始灰度发布', exact: true }).click()
  await release.getByRole('button', { name: '停止灰度', exact: true }).waitFor({ timeout: 15000 })

  await release.getByRole('button', { name: '25%', exact: true }).click()
  await release.getByRole('button', { name: '保存灰度设置', exact: true }).click()
  await release.getByText('灰度 25%', { exact: true }).waitFor({ timeout: 10000 })

  await release.getByRole('button', { name: '停止灰度', exact: true }).click()
  let confirm = page.getByRole('dialog', { name: '停止灰度？' })
  await confirm.getByRole('button', { name: '取消', exact: true }).click()
  await confirm.waitFor({ state: 'detached' })
  await release.getByRole('button', { name: '停止灰度', exact: true }).click()
  confirm = page.getByRole('dialog', { name: '停止灰度？' })
  await confirm.getByRole('button', { name: '停止灰度', exact: true }).click()
  await release.getByRole('button', { name: '放弃候选', exact: true }).waitFor({ timeout: 10000 })

  await release.getByRole('button', { name: '直接全量发布', exact: true }).click()
  confirm = page.getByRole('dialog', { name: '全量发布候选版本？' })
  await confirm.getByRole('button', { name: '取消', exact: true }).click()
  await confirm.waitFor({ state: 'detached' })
  await release.getByRole('button', { name: '直接全量发布', exact: true }).click()
  confirm = page.getByRole('dialog', { name: '全量发布候选版本？' })
  await confirm.getByRole('button', { name: '全量发布', exact: true }).click()
  await release.waitFor({ state: 'detached', timeout: 15000 })

  // 新候选可以放弃，且历史版本可以恢复为新的稳定版本。
  release = await stageBotCandidate(page, `客服机器人待放弃候选 ${suffix}`)
  await release.getByRole('button', { name: '放弃候选', exact: true }).click()
  confirm = page.getByRole('dialog', { name: '放弃候选版本？' })
  await confirm.getByRole('button', { name: '放弃候选', exact: true }).click()
  await release.getByText('当前没有待发布版本', { exact: true }).waitFor({ timeout: 10000 })
  await release.getByRole('button', { name: '关闭版本发布', exact: true }).click()

  row = await expectRow(page, appCode)
  await row.getByRole('button', { name: '版本发布', exact: true }).click()
  release = page.getByRole('dialog', { name: `版本发布 · ${appCode}` })
  const restore = release.getByRole('button', { name: '恢复此版本', exact: true }).first()
  await restore.waitFor({ timeout: 10000 })
  await restore.click()
  confirm = page.getByRole('dialog', { name: '恢复历史版本？' })
  await confirm.getByRole('button', { name: '取消', exact: true }).click()
  await confirm.waitFor({ state: 'detached' })
  await restore.click()
  confirm = page.getByRole('dialog', { name: '恢复历史版本？' })
  await confirm.getByRole('button', { name: '确认恢复', exact: true }).click()
  await release.waitFor({ state: 'detached', timeout: 15000 })

  row = await expectRow(page, appCode)
  await row.getByRole('button', { name: '停用', exact: true }).click()
  row = await expectRow(page, appCode)
  await row.getByText('已停用', { exact: true }).waitFor({ timeout: 10000 })
  await row.getByRole('button', { name: '重新启用', exact: true }).click()
  row = await expectRow(page, appCode)
  await row.getByText('运行中', { exact: true }).waitFor({ timeout: 10000 })
}

async function ingestKnowledge(page) {
  await page.getByRole('tab', { name: '知识库', exact: true }).click()

  await page.getByRole('button', { name: '上传文档', exact: true }).click()
  let dialog = page.getByRole('dialog', { name: '添加知识文档' })
  await dialog.getByRole('button', { name: '取消', exact: true }).click()
  await dialog.waitFor({ state: 'detached' })

  await page.getByRole('button', { name: '上传文档', exact: true }).click()
  dialog = page.getByRole('dialog', { name: '添加知识文档' })
  await dialog.getByRole('tab', { name: '直接文本', exact: true }).click()

  // 依赖可见字段名称，而不是内部 CSS 或字段顺序。
  const labels = dialog.locator('label')
  const idLabel = labels.filter({ hasText: '文档 ID' })
  const nameLabel = labels.filter({ hasText: '文档名称' })
  await idLabel.locator('input').fill(documentID)
  await nameLabel.locator('input').fill(`客服 FAQ ${suffix}`)
  await dialog.getByPlaceholder('在此直接输入或粘贴知识文档正文…').fill('退款申请请提供订单号；普通问题将在一个工作日内处理。')
  await dialog.getByRole('button', { name: '发布', exact: true }).click()

  const row = await expectRow(page, documentID, 15000)
  await row.getByText('可用', { exact: true }).waitFor({ timeout: 30000 })
  expect((await row.innerText()).includes('文本'), '知识文档应以真实文本来源完成索引')

  await page.getByRole('button', { name: '刷新知识文档', exact: true }).click()
  await expectRow(page, documentID)

  const fileDocumentID = `guide-${suffix}`
  const fileName = `guide-${suffix}.md`
  await page.getByRole('button', { name: '上传文档', exact: true }).click()
  dialog = page.getByRole('dialog', { name: '添加知识文档' })
  await dialog.getByRole('tab', { name: '本地文件', exact: true }).click()
  const firstFile = {
    name: fileName,
    mimeType: 'text/markdown',
    buffer: Buffer.from('# 客服说明\n\n订单查询请提供订单号。'),
  }
  const firstChooserPromise = page.waitForEvent('filechooser')
  await dialog.getByRole('button', { name: '上传文件', exact: true }).click()
  const firstChooser = await firstChooserPromise
  await firstChooser.setFiles(firstFile)
  await dialog.getByRole('button', { name: '更换文件', exact: true }).waitFor()

  const changedFile = {
    name: fileName,
    mimeType: 'text/markdown',
    buffer: Buffer.from('# 客服说明\n\n订单查询请提供订单号；退款需提供原订单。'),
  }
  const changeChooserPromise = page.waitForEvent('filechooser')
  await dialog.getByRole('button', { name: '更换文件', exact: true }).click()
  const changeChooser = await changeChooserPromise
  await changeChooser.setFiles(changedFile)
  await dialog.locator('label').filter({ hasText: '文档 ID' }).locator('input').fill(fileDocumentID)
  await dialog.getByRole('button', { name: '发布', exact: true }).click()
  let fileRow = await expectRow(page, fileDocumentID, 15000)
  await fileRow.getByText('可用', { exact: true }).waitFor({ timeout: 30000 })
  expect((await fileRow.innerText()).includes('文件'), 'Markdown 文件应通过真实上传链路完成索引')

  await fileRow.getByRole('button', { name: '删除', exact: true }).click()
  let confirm = page.getByRole('dialog', { name: '删除知识文档？' })
  await confirm.getByRole('button', { name: '取消', exact: true }).click()
  await confirm.waitFor({ state: 'detached' })
  fileRow = await expectRow(page, fileDocumentID)
  await fileRow.getByRole('button', { name: '删除', exact: true }).click()
  confirm = page.getByRole('dialog', { name: '删除知识文档？' })
  await confirm.getByRole('button', { name: '删除文档', exact: true }).click()
  await page.getByText(fileDocumentID, { exact: true }).waitFor({ state: 'detached', timeout: 15000 })

  await page.getByRole('button', { name: '上传文档', exact: true }).click()
  dialog = page.getByRole('dialog', { name: '添加知识文档' })
  await dialog.getByRole('tab', { name: '网页抓取 (URL)', exact: true }).click()
  await dialog.getByPlaceholder('例如：https://docs.example.com/guide').fill('invalid.example/path')
  await dialog.getByRole('button', { name: '开始抓取并索引', exact: true }).click()
  await dialog.getByRole('alert').filter({ hasText: '网页 URL 必须以 http:// 或 https:// 开头' }).waitFor()
  await dialog.getByRole('button', { name: '取消', exact: true }).click()
}

async function chatAndVerifyPersistence(page) {
  await page.getByRole('tab', { name: '对话', exact: true }).click()
  const composer = page.getByPlaceholder('输入消息…')
  await composer.waitFor({ timeout: 10000 })
  const waitForAssistantReply = async (expectedText, timeout = 60000) => {
    const assistant = page.locator('.msg.assistant').filter({ hasText: expectedText }).last()
    const deadline = Date.now() + timeout
    while (Date.now() < deadline) {
      const error = page.locator('.msg.error').last()
      if (await error.count() > 0 && await error.isVisible()) {
        throw new Error(`chat execution failed: ${await error.innerText()}`)
      }
      if (await assistant.count() > 0 && await assistant.isVisible() && await composer.isEnabled()) return
      await page.waitForTimeout(100)
    }
    throw new Error(`assistant reply timeout: ${expectedText}`)
  }
  const firstMarker = `SESSION_A_${suffix.replaceAll('-', '_')}`
  const firstQuestion = modelReply ? `请确认客服链路 ${suffix}` : `只回复这一段标记，不要添加其他内容：${firstMarker}`
  await composer.fill(firstQuestion)
  await page.getByRole('button', { name: '发送', exact: true }).click()
  await waitForAssistantReply(modelReply || firstMarker)
  expect(await page.locator('.msg.user').filter({ hasText: firstQuestion }).isVisible(), '真实聊天应显示第一会话的用户消息')

  await page.getByRole('button', { name: '新建会话', exact: true }).click()
  const secondMarker = `SESSION_B_${suffix.replaceAll('-', '_')}`
  const secondQuestion = modelReply ? `第二个独立会话 ${suffix}` : `只回复这一段标记，不要添加其他内容：${secondMarker}`
  await composer.fill(secondQuestion)
  await page.getByRole('button', { name: '发送', exact: true }).click()
  await waitForAssistantReply(modelReply || secondMarker)
  expect(await page.locator('.msg.user').filter({ hasText: secondQuestion }).isVisible(), '第二个会话应能独立发送并收到回复')

  const sessionSwitcher = page.locator('.session-switch-btn')
  await sessionSwitcher.click()
  const sessionDialog = page.getByRole('dialog', { name: '当前机器人会话' })
  await sessionDialog.waitFor({ timeout: 10000 })
  expect((await sessionDialog.locator('.chat-thread-row').count()) >= 2, '同一机器人应保留至少两个独立 Web 会话')
  await page.keyboard.press('Escape')

  await page.reload({ waitUntil: 'domcontentloaded' })
  const restoredSecond = page.locator('.msg.user').filter({ hasText: secondQuestion })
  await restoredSecond.waitFor({ timeout: 20000 })
  expect(await restoredSecond.isVisible(), '刷新后应从持久会话恢复当前会话')

  await page.locator('.session-switch-btn').click()
  const restoredDialog = page.getByRole('dialog', { name: '当前机器人会话' })
  const firstThread = restoredDialog.locator('.chat-thread-option').filter({ hasText: firstQuestion.slice(0, 18) }).first()
  await firstThread.waitFor({ timeout: 10000 })
  await firstThread.click()
  await page.locator('.msg.user').filter({ hasText: firstQuestion }).waitFor({ timeout: 10000 })
  expect((await page.locator('.msg.user').filter({ hasText: secondQuestion }).count()) === 0, '切回第一会话时不应混入第二会话消息')

  const chatFile = {
    name: `chat-${suffix}.txt`,
    mimeType: 'text/plain',
    buffer: Buffer.from('订单号 TEST-1001'),
  }
  const chooserPromise = page.waitForEvent('filechooser')
  await page.getByRole('button', { name: '添加文件', exact: true }).click()
  const chooser = await chooserPromise
  await chooser.setFiles(chatFile)
  await page.getByRole('button', { name: `移除 ${chatFile.name}`, exact: true }).waitFor()
  await page.getByRole('button', { name: `移除 ${chatFile.name}`, exact: true }).click()
  expect((await page.locator('.composer-files').count()) === 0, '聊天附件移除按钮应清空待发送文件')

  // 新会话仅发送附件，验证真实 multipart 输入也能经过 Worker/Runner。
  await page.getByRole('button', { name: '新建会话', exact: true }).click()
  await page.locator('.composer-box input[type="file"]').setInputFiles(chatFile)
  await page.getByRole('button', { name: '发送', exact: true }).click()
  if (modelReply) await waitForAssistantReply(modelReply)
  else {
    const assistant = page.locator('.msg.assistant .msg-text').last()
    await assistant.waitFor({ timeout: 60000 })
    while (!(await composer.isEnabled())) await page.waitForTimeout(100)
  }
  await page.locator('.message-attachment').getByText(chatFile.name, { exact: true }).waitFor({ timeout: 10000 })

  await page.locator('.session-switch-btn').click()
  let threadRows = page.locator('.chat-thread-row')
  await threadRows.nth(2).waitFor({ timeout: 10000 })
  const beforeDelete = await threadRows.count()
  expect(beforeDelete >= 3, '附件会话应作为独立持久会话出现在切换器中')
  let deleteRow = threadRows.first()
  await deleteRow.hover()
  let deleteButton = deleteRow.getByRole('button', { name: /^删除会话 / })
  await deleteButton.waitFor({ timeout: 10000 })
  expect(!(await deleteButton.isDisabled()), '已完成会话的删除按钮应可用')
  await deleteButton.click()
  let confirm = page.getByRole('dialog', { name: '删除会话？' })
  await confirm.getByRole('button', { name: '取消', exact: true }).click()
  await confirm.waitFor({ state: 'detached' })
  if ((await page.getByRole('dialog', { name: '当前机器人会话' }).count()) === 0) {
    await page.locator('.session-switch-btn').click()
  }
  threadRows = page.locator('.chat-thread-row')
  deleteRow = threadRows.first()
  await deleteRow.hover()
  deleteButton = deleteRow.getByRole('button', { name: /^删除会话 / })
  await deleteButton.click()
  confirm = page.getByRole('dialog', { name: '删除会话？' })
  await confirm.getByRole('button', { name: '删除会话', exact: true }).click()
  await confirm.waitFor({ state: 'detached' })
  await page.locator('.session-switch-btn').click()
  threadRows = page.locator('.chat-thread-row')
  await page.waitForFunction((expected) => document.querySelectorAll('.chat-thread-row').length === expected, beforeDelete - 1, { timeout: 10000 })
  expect(await threadRows.count() === beforeDelete - 1, '删除会话应从真实会话列表移除')
  await page.keyboard.press('Escape')
}

async function verifyExecution(page) {
  await page.getByRole('tab', { name: '执行记录', exact: true }).click()
  await page.getByRole('button', { name: '刷新执行记录' }).click()
  const filter = page.locator('.execution-filter')
  await filter.getByRole('tab', { name: '已完成', exact: true }).click()
  const row = page.locator('section[aria-label="执行记录列表"] tbody tr').filter({ hasText: '网页' }).first()
  await row.waitFor({ timeout: 15000 })
  expect((await row.innerText()).includes('已完成'), '真实 Kafka 执行应记录为已完成')

  const search = page.getByRole('searchbox', { name: '搜索执行记录' })
  await search.fill('不存在的执行记录')
  await page.getByText('没有符合筛选的记录。', { exact: true }).waitFor()
  await page.getByRole('button', { name: '清空搜索执行记录', exact: true }).click()
  await row.waitFor({ timeout: 10000 })
  await row.click()
  await page.getByText('已送达', { exact: true }).first().waitFor({ timeout: 15000 })
}

async function run() {
  const browser = await chromium.launch()
  const systemContext = await browser.newContext({ viewport: { width: 1440, height: 900 }, permissions: ['clipboard-read', 'clipboard-write'] })
  const tenantContext = await browser.newContext({ viewport: { width: 1440, height: 900 } })
  const systemPage = await systemContext.newPage()
  const tenantPage = await tenantContext.newPage()
  const diagnostics = []
  for (const [scope, page] of [['system', systemPage], ['tenant', tenantPage]]) {
    page.on('pageerror', (error) => diagnostics.push({ scope, type: 'pageerror', message: error.message }))
    page.on('requestfailed', (request) => {
      if (!request.url().includes('/console/')) return
      diagnostics.push({
        scope,
        type: 'requestfailed',
        url: request.url(),
        failure: request.failure()?.errorText ?? 'unknown',
      })
    })
    page.on('console', (message) => {
      if (message.type() !== 'error') return
      diagnostics.push({ scope, type: 'console', message: message.text() })
    })
  }
  try {
    await loginSystemAdmin(systemPage)
    const temporaryPassword = await createLocalUser(systemPage, adminUsername, adminDisplay)
    await createLocalUser(systemPage, memberUsername, memberDisplay)
    await createTenantAndAuthorize(systemPage)

    await loginLocalAndChangePassword(tenantPage, temporaryPassword)
    await addTenantMember(tenantPage)
    await exerciseMemberControls(tenantPage)
    await createAndActivateBot(tenantPage)
    await exerciseBotControls(tenantPage)
    await ingestKnowledge(tenantPage)
    await chatAndVerifyPersistence(tenantPage)
    await verifyExecution(tenantPage)

    await exerciseSystemUserControls(systemPage)
    await exerciseBackendControls(systemPage)
    await exerciseModelControls(systemPage)
    await exerciseAccountAndSystemControls(systemPage)
    await exerciseTenantStatusControl(systemPage)

    console.log(`[PASS] real-platform（${assertions} 项断言，tenant=${tenantID} app=${appCode} model=${modelName}）`)
  } catch (error) {
    try { fs.writeFileSync(path.join(artifacts, 'real-platform-browser-diagnostics.json'), `${JSON.stringify(diagnostics, null, 2)}\n`) } catch { /* ignore */ }
    try { fs.writeFileSync(path.join(artifacts, 'real-platform-system-fail.html'), await systemPage.content()) } catch { /* ignore */ }
    try { fs.writeFileSync(path.join(artifacts, 'real-platform-tenant-fail.html'), await tenantPage.content()) } catch { /* ignore */ }
    try { await systemPage.screenshot({ path: path.join(artifacts, 'real-platform-system-fail.png'), fullPage: true }) } catch { /* ignore */ }
    try { await tenantPage.screenshot({ path: path.join(artifacts, 'real-platform-tenant-fail.png'), fullPage: true }) } catch { /* ignore */ }
    throw error
  } finally {
    await systemContext.close()
    await tenantContext.close()
    await browser.close()
  }
}

await run().catch((error) => {
  console.error(`[FAIL] real-platform: ${error.stack || error.message}`)
  process.exit(1)
})
