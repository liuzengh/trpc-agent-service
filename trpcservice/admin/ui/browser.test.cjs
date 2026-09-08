// Optional real-browser integration test against the Go test's isolated server.
const {chromium}=require(process.env.TEST_PLAYWRIGHT_MODULE);
const assert=require('node:assert/strict');
(async()=>{
 const browser=await chromium.launch({headless:true,executablePath:process.env.TEST_BROWSER_EXECUTABLE||undefined});
 try{
  const page=await browser.newPage({viewport:{width:1280,height:960}});
  const errors=[];page.on('pageerror',error=>errors.push(error.message));
  page.on('dialog',dialog=>dialog.accept());
  await page.goto(process.env.TEST_ADMIN_UI_URL+'/admin/ui/');
  await page.locator('#token').fill(process.env.TEST_ADMIN_UI_TOKEN);await page.locator('#login-form button').click();
  await page.locator('#console').waitFor({state:'visible'});await page.getByRole('cell',{name:'tutorial-tenant',exact:true}).waitFor();
  assert.equal(await page.evaluate(()=>localStorage.length+sessionStorage.length),0);
  const idle=()=>page.waitForFunction(()=>document.querySelector('#console').getAttribute('aria-busy')!=='true');
  const click=async locator=>{await idle();await locator.click();await idle()};
  const field=key=>page.locator('[data-field="'+key+'"]');
  const save=async()=>{await click(page.locator('#save'));await page.waitForFunction(()=>document.querySelector('#console').getAttribute('aria-busy')!=='true');assert.equal(await page.locator('#notice').textContent(),'已保存')};
  console.log('creating tenant');await click(page.locator('#new-resource'));await field('tenant_id').fill('browser-tenant');await field('display_name').fill('Browser Tenant');await field('secret_namespace').fill('browser/tenant');await save();
  await page.locator('#tenant').selectOption('browser-tenant');await idle();
  await click(page.locator('[data-kind="apps"]'));await page.locator('#title').filter({hasText:'Agent 应用'}).waitFor();
  console.log('creating app');await click(page.locator('#new-resource'));await field('app_id').fill('browser-app');await field('name').fill('Browser Agent');await save();
  await click(page.locator('[data-kind="revisions"]'));await page.locator('#title').filter({hasText:'版本与发布'}).waitFor();await click(page.locator('#new-resource'));
  console.log('creating skill revision');await field('app_id').fill('browser-app');await field('revision_id').fill('browser-revision-1');await page.locator('#skill-options input').check();await save();
  assert.match(await field('agent_config').inputValue(),/sample/);
  await click(page.locator('#publish'));await page.waitForFunction(()=>document.querySelector('#notice').textContent==='已发布稳定版本');
  await click(page.getByRole('row').filter({hasText:'browser-revision-1'}).getByRole('button',{name:'查看'}));await field('revision_id').fill('browser-revision-2');await save();
  await click(page.locator('#publish'));await page.waitForFunction(()=>document.querySelector('#notice').textContent==='已发布稳定版本');
  await click(page.getByRole('row').filter({hasText:'browser-revision-1'}).getByRole('button',{name:'查看'}));await click(page.locator('#publish'));await page.waitForFunction(()=>document.querySelector('#notice').textContent==='已发布稳定版本');
  const appResponse=await page.request.post(process.env.TEST_ADMIN_UI_URL+'/admin/apps/get',{headers:{Authorization:'Bearer '+process.env.TEST_ADMIN_UI_TOKEN},data:{tenant_id:'browser-tenant',app_id:'browser-app'}});
  assert.equal((await appResponse.json()).stable_revision_id,'browser-revision-1');
  await click(page.locator('[data-kind="channels"]'));await page.locator('#title').filter({hasText:'IM 通道绑定'}).waitFor();await click(page.locator('#new-resource'));
  await field('channel_binding_id').fill('browser-binding');await field('app_id').fill('browser-app');await field('channel_type').fill('http');await field('account_id').fill('browser');await field('secret_ref').fill('env://BROWSER_HTTP_TOKEN');await field('config').fill('{}');await field('callback_key').fill('browser-http');await save();
  await field('status').fill('active');await save();
  await click(page.locator('[data-kind="backends"]'));await page.locator('#title').filter({hasText:'数据后端'}).waitFor();await click(page.locator('#new-resource'));
  await field('binding_id').fill('browser-backend');await field('app_id').fill('browser-app');await save();
  await click(page.locator('[data-kind="audit"]'));await page.locator('#title').filter({hasText:'审计日志'}).waitFor();await page.locator('#rows tr').first().waitFor();
  await click(page.locator('#logout'));await page.locator('#token').fill('browser-fixture-auditor-token-123456789');await click(page.locator('#login-form button'));await page.locator('#console').waitFor({state:'visible'});
  await click(page.locator('[data-kind="revisions"]'));await page.getByRole('cell',{name:'browser-revision-1',exact:true}).waitFor();assert.equal(await page.locator('#new-resource').isVisible(),false);
  await click(page.getByRole('row').filter({hasText:'browser-revision-1'}).getByRole('button',{name:'查看'}));assert.equal(await page.locator('#publish').isVisible(),false);
  assert.equal(await field('revision_id').inputValue(),'browser-revision-1');assert.equal(await field('revision_no').inputValue(),'1');
  assert.equal(await page.evaluate(()=>localStorage.length+sessionStorage.length),0);
  if(process.env.TEST_ADMIN_UI_SCREENSHOT)await page.screenshot({path:process.env.TEST_ADMIN_UI_SCREENSHOT,fullPage:true});
  assert.deepEqual(errors,[]);
  console.log('browser passed: tenant/app/revision/skill/channel/backend writes, publication, audit and read-only role');
 }finally{await browser.close()}
})().catch(error=>{console.error(error.message);process.exit(1)});
