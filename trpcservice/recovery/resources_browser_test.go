package recovery_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"testing"
)

// Optional real-browser coverage uses an explicitly supplied local Playwright
// installation. It never downloads a browser or connects to a running platform.
func exerciseResourceBrowser(t *testing.T, ctx context.Context, handler http.Handler, token string) {
	t.Helper()
	if os.Getenv("TEST_RESOURCES_BROWSER") != "1" {
		return
	}
	module, browser := os.Getenv("TEST_PLAYWRIGHT_MODULE"), os.Getenv("TEST_CHROMIUM_EXECUTABLE")
	if module == "" || browser == "" {
		t.Fatal("browser checks require explicit local Playwright and Chromium paths")
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	cmd := exec.CommandContext(ctx, "node", "-e", resourceBrowserScript)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "TEST_BROWSER_URL=" + server.URL, "TEST_BROWSER_TOKEN=" + token, "TEST_PLAYWRIGHT_MODULE=" + module, "TEST_CHROMIUM_EXECUTABLE=" + browser}
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("resource browser checks failed: %v\n%s", err, output)
	} else {
		t.Log(string(output))
	}
}

const resourceBrowserScript = `
const { chromium } = require(process.env.TEST_PLAYWRIGHT_MODULE);
(async () => {
  const browser = await chromium.launch({executablePath:process.env.TEST_CHROMIUM_EXECUTABLE,headless:true,args:['--no-sandbox']});
  let page;
  try {
    page = await browser.newPage({viewport:{width:1440,height:1100}});
    page.setDefaultTimeout(10000);
    const errors=[];page.on('pageerror',e=>errors.push(e.message));
    await page.goto(process.env.TEST_BROWSER_URL+'/admin/ui/');
    await page.getByPlaceholder('输入你的管理凭据').fill(process.env.TEST_BROWSER_TOKEN);
    await page.getByRole('button',{name:'进入工作空间'}).click();
    await page.locator('.top-actions .ant-select').click();
    await page.locator('.ant-select-dropdown:visible .ant-select-item-option-content').filter({hasText:'Tutorial Tenant'}).click();
    await page.getByRole('button',{name:'资源中心',exact:true}).click();
    await page.getByRole('button',{name:'上传 Skill',exact:true}).waitFor();
    await page.getByRole('tab',{name:'数据后端'}).click();
    await page.getByRole('button',{name:'新建连接',exact:true}).click();
    const modal=page.getByRole('dialog');
    await modal.getByLabel('连接名称',{exact:true}).fill('Browser Redis');
    await modal.getByLabel('主机地址',{exact:true}).fill('redis.browser.invalid');
    await modal.getByLabel('密码',{exact:true}).fill('browser-secret-canary');
    const saved=page.waitForResponse(r=>r.url().endsWith('/admin/backend-connections/create'));
    await modal.getByRole('button',{name:'保存连接',exact:true}).click();
    const response=await saved;
    if(response.status()!==201)throw new Error('connection create failed: '+await response.text());
    if((await response.text()).includes('browser-secret-canary'))throw new Error('credential reflected by API');
    await page.getByRole('row').filter({hasText:'Browser Redis'}).waitFor();
    await page.getByRole('row').filter({hasText:'Browser Redis'}).getByRole('button',{name:'绑定到 Agent'}).click();
    await page.getByRole('dialog').getByRole('combobox').click();
    await page.locator('.ant-select-dropdown:visible .ant-select-item-option-content').filter({hasText:'Browser App'}).click();
    const bound=page.waitForResponse(r=>r.url().endsWith('/admin/backend-connections/bind'));
    await page.getByRole('button',{name:'创建绑定',exact:true}).click();
    if((await bound).status()!==201)throw new Error('connection binding failed');
    if(await page.locator('input[placeholder="env://DEPLOYMENT_SECRET"]').count())throw new Error('manual credential reference still visible');
    await page.getByRole('tab',{name:'Skill 目录'}).click();
    const name='browser-upload';
    for(const version of ['1','2','3']) {
      await page.getByRole('button',{name:'上传 Skill',exact:true}).click();
      const dialog=page.getByRole('dialog');
      await dialog.getByLabel('Skill 名称',{exact:true}).fill(name);
      await dialog.getByLabel('版本',{exact:true}).fill(version);
      const files=[{name:'SKILL.md',mimeType:'text/markdown',buffer:Buffer.from('---\nname: '+name+'\ndescription: Browser skill '+version+'\n---\nUse approved instructions.\n')}];
      if(version!=='3')files.push({name:'run.sh',mimeType:'text/plain',buffer:Buffer.from('#!/bin/sh\nprintf "version '+version+'\\n"\n')});
      await dialog.locator('input[type=file]').setInputFiles(files);
      const uploaded=page.waitForResponse(r=>r.url().endsWith('/admin/skills/upload'));
      await dialog.getByRole('button',{name:'校验并提交审核',exact:true}).click();
      const uploadResponse=await uploaded;
      if(uploadResponse.status()!==200)throw new Error('skill upload failed: '+await uploadResponse.text());
      const row=page.getByRole('row').filter({hasText:'Browser skill '+version});
      await row.getByText('待审核',{exact:true}).waitFor();
      await row.getByRole('button',{name:'查看 / 审核'}).click();
      await page.getByRole('checkbox',{name:'我已审核此版本内容，同意仅对当前工作空间授权'}).check();
      await page.getByRole('button',{name:'批准此版本',exact:true}).click();
      await row.getByText('已授权',{exact:true}).waitFor();
    }
    await page.evaluate(()=>{location.hash='/agents/tutorial-app';});
    const v2=page.locator('.skill-choice').filter({hasText:'Browser skill 2'});
    await v2.locator('input[type=checkbox]').check();
    const request=page.waitForRequest(r=>r.url().endsWith('/admin/drafts/save'));
    const draftSaved=page.waitForResponse(r=>r.url().endsWith('/admin/drafts/save'));
    await page.getByRole('button',{name:/保存草稿/}).click();
    const body=(await request).postDataJSON();
    if(!body.config.agent_config.skills.some(s=>s.name===name&&s.version==='2'))throw new Error('selected Skill version was not preserved');
    const draftResponse=await draftSaved;
    if(draftResponse.status()!==200)throw new Error('draft save failed: '+await draftResponse.text());
    await page.locator('.skill-choice').filter({hasText:'Browser skill 3'}).locator('input[type=checkbox]').check();
    const [instructionResponse]=await Promise.all([
      page.waitForResponse(r=>r.url().endsWith('/admin/drafts/save')),
      page.getByRole('button',{name:/保存草稿/}).click(),
    ]);
    if(instructionResponse.status()!==200)throw new Error('instruction Skill save failed: '+await instructionResponse.text());
    const instruction=instructionResponse.request().postDataJSON().config;
    if(instruction.tool_policy.allowed_tools.includes('skill_run')||!instruction.agent_config.skills.some(s=>s.name===name&&s.version==='3'))throw new Error('instruction-only Skill enabled script execution or selected wrong version');
    if(errors.length)throw new Error(errors.join('\n'));
    console.log('Browser passed: typed storage credentials, file upload, explicit approval, multiple versions, Agent selection.');
  } catch (e) { if(page) console.error('Browser state: '+(await page.locator('body').innerText()).slice(-12000)); throw e;
  } finally { await browser.close(); }
})().catch(e=>{console.error(e);process.exitCode=1;});
`
