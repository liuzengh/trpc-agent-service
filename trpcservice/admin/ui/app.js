"use strict";
(() => {
  const $ = id => document.getElementById(id);
  let token = "", principal = null, kind = "tenants", cursor = "", selected = null, busy = false, sequence = 0;
  const titles = {tenants:"租户",apps:"Agent 应用",revisions:"版本与发布",channels:"IM 通道绑定",backends:"数据后端",skills:"Skill 目录",audit:"审计日志",operations:"工具执行记录"};
  const help = {tenants:"租户是配置、数据和费用的隔离边界。",apps:"应用通过稳定版本与灰度策略决定新会话行为。",revisions:"版本不可变。修改配置请创建新版本，再显式发布；旧版本也可重新发布用于回滚。",channels:"绑定外部账号、租户和应用；密钥使用引用，不填写真实 Token。",backends:"注册物理后端与 SecretRef。已有绑定切换应走迁移流程，不能直接覆盖数据。",skills:"这里只显示部署者授权给当前租户的不可变 Skill。选择版本时使用完整 checksum；执行须经 Runner 审批和沙箱。",audit:"日志查询不改变业务状态；不显示密钥原文。",operations:"查询工具业务操作。unknown 不能视为失败或自动回滚，不提供盲目重发。"};
  titles.executions='工具调用记录'; titles.operations='业务操作'; help.executions='输入 request_id 查询 Skill、MCP 和其他工具的授权执行状态；原始输出不在审计列表中展示。';
  const schemas = {
    tenants:[['tenant_id','租户 ID'],['display_name','显示名称'],['region','区域'],['secret_namespace','密钥命名空间'],['quota_config','配额','json'],['audit_policy','审计策略','json']],
    apps:[['app_id','应用 ID'],['name','名称'],['description','说明'],['status','状态'],['rollout_policy','灰度策略','json']],
    revisions:[['revision_id','版本 ID（可留空自动生成）'],['app_id','应用 ID'],['revision_no','版本序号','number'],['agent_type','Agent 类型'],['agent_config','Agent 配置 / 指令 / Skills','json'],['model_config','模型配置（使用密钥引用）','json'],['tool_policy','工具权限','json'],['knowledge_config','知识库配置','json'],['memory_config','Memory 配置','json'],['guardrail_config','Guardrail 配置','json']],
    channels:[['channel_binding_id','绑定 ID（可留空）'],['app_id','应用 ID'],['channel_type','通道类型'],['account_id','账号 ID'],['callback_key','Callback Key（可留空）'],['secret_ref','通道密钥引用'],['status','状态'],['config','通道配置','json']],
    backends:[['binding_id','绑定 ID（可留空）'],['app_id','应用 ID（可留空使用租户范围）'],['resource_type','资源类型'],['backend_type','后端类型'],['secret_ref','后端密钥引用'],['isolation_level','隔离方式'],['config','后端配置','json']]
  };
  function message(text, error=false) { $('notice').textContent = text; $('notice').className = error ? 'error' : ''; }
  const canWrite = () => principal && ['superadmin','tenant_admin'].includes(principal.role);
  const tenant = () => $('tenant').value;
  function node(tag, text, className) { const n=document.createElement(tag); if(text!==undefined)n.textContent=text; if(className)n.className=className; return n; }
  async function api(path, value) {
    if(!token)throw new Error('请先连接管理平台');
    const requestToken=token;
    const controller = new AbortController(), timer = setTimeout(()=>controller.abort(),20000);
    try {
      const response = await fetch(path,{method:'POST',headers:{'Authorization':'Bearer '+token,'Content-Type':'application/json'},body:JSON.stringify(value),credentials:'omit',cache:'no-store',redirect:'error',signal:controller.signal});
      const raw=await response.text(); if(raw.length>8*1024*1024)throw new Error('响应过大，请缩小查询范围');
      if(token!==requestToken)throw new Error('登录身份已变化，已忽略旧请求结果');
      let data; try{data=JSON.parse(raw)}catch{throw new Error('服务器返回了非 JSON 数据')}
      if(!response.ok) { if(response.status===401)disconnect(); throw new Error(data.error||('HTTP '+response.status)); }
      return data;
    } finally { clearTimeout(timer); }
  }
  async function perform(fn) { if(busy)return; busy=true; $('console').inert=true;$('console').setAttribute('aria-busy','true');message(''); try{await fn()}catch(e){message(e.name==='AbortError'?'请求超时，写操作请先查询状态再决定是否重试':e.message,true)}finally{busy=false;$('console').inert=false;$('console').setAttribute('aria-busy','false')} }
  function disconnect() { token=''; principal=null; sequence++; selected=null; $('token').value=''; $('login').hidden=false; $('console').hidden=true; $('logout').hidden=true; $('identity').textContent='未连接'; $('rows').replaceChildren(); $('detail').textContent=''; $('fields').replaceChildren(); }
  async function loadTenants() {
    const previous=tenant(); let after='', values=[];
    // Bound initial enumeration; all other collections use explicit pagination.
    for(let i=0;i<10;i++){const p=await api('/admin/catalog/list',{kind:'tenants',limit:100,after});values.push(...p.items);after=p.next||'';if(!after)break}
    $('tenant').replaceChildren(node('option','请选择'));
    $('tenant').firstChild.value='';
    for(const v of values){const opt=node('option',v.display_name+' · '+v.tenant_id);opt.value=v.tenant_id;$('tenant').append(opt)}
    $('tenant').value=values.some(v=>v.tenant_id===previous)?previous:(values[0]?.tenant_id||'');
    if(after)message('租户列表超过初始加载上限，请使用租户列表分页查询');
  }
  function idOf(v){return v.tenant_id&&kind==='tenants'?v.tenant_id:v.app_id&&kind==='apps'?v.app_id:v.revision_id||v.channel_binding_id||v.binding_id||v.operation_id||v.execution_id||v.audit_id||v.name||''}
  async function refresh(more=false) {
    const serial=++sequence, requested=kind, requestedTenant=tenant();
    if(!more){cursor='';$('rows').replaceChildren();$('editor').hidden=true;selected=null}
    $('filter-label').textContent=kind==='executions'?'请求编号':'应用筛选';$('app-filter').placeholder=kind==='executions'?'必填 request_id':'可选 app_id';
    $('title').textContent=titles[kind];$('description').textContent=help[kind];
    document.querySelectorAll('[data-kind]').forEach(b=>b.classList.toggle('active',b.dataset.kind===kind));
    $('new-resource').hidden=!schemas[kind]||!canWrite()||(kind==='tenants'&&principal.role!=='superadmin');
    if(kind!=='tenants'&&!tenant()){$('empty').hidden=false;$('more').hidden=true;return}
    if(kind==='executions'&&!$('app-filter').value.trim()){$('empty').hidden=false;$('more').hidden=true;return}
    let result;
    if(kind==='skills')result=await api('/admin/skills/list',{tenant_id:tenant()});
    else if(kind==='audit')result=await api('/admin/audit/query',{tenant_id:tenant(),limit:100});
    else if(kind==='executions')result=await api('/admin/tool-executions/list',{tenant_id:tenant(),request_id:$('app-filter').value.trim()});
    else if(kind==='operations')result=await api('/admin/tool-operations/list',{tenant_id:tenant(),after_id:cursor,limit:50});
    else result=await api('/admin/catalog/list',{kind,tenant_id:kind==='tenants'?'':tenant(),app_id:kind==='tenants'?'':$('app-filter').value.trim(),after:cursor,limit:50});
    if(serial!==sequence||requested!==kind||requestedTenant!==tenant()||!principal)return;
    const rows=Array.isArray(result)?result:(result.items||[]);
    cursor=kind==='operations'&&rows.length===50?rows[rows.length-1].operation_id:(result.next||'');
    for(const value of rows){
      const tr=node('tr');tr.append(node('td',idOf(value)),node('td',value.display_name||value.name||value.agent_config?.name||value.channel_type||value.backend_type||value.tool_name||value.decision||''),node('td',String(value.status||value.version||value.revision_no||value.occurred_at||'')));
      const cell=node('td'), button=node('button','查看');button.type='button';button.addEventListener('click',()=>perform(()=>edit(value)));cell.append(button);tr.append(cell);$('rows').append(tr);
    }
    $('empty').hidden=$('rows').children.length!==0;$('more').hidden=!cursor;
  }
  function defaults(){return {
    tenants:{tenant_id:'',display_name:'',region:'local',secret_namespace:'',quota_config:{},audit_policy:{level:'basic',retention_days:0,failure_mode:'fail_closed'}},
    apps:{app_id:'',name:'',description:'',status:'active',rollout_policy:{}},
    revisions:{revision_id:'',app_id:$('app-filter').value,revision_no:1,agent_type:'llm',agent_config:{name:'assistant',instruction:'准确回答用户的问题，调用工具前遵守权限和审批。'},model_config:{source:'startup_env'},tool_policy:{allowed_tools:['current_time']},knowledge_config:{},memory_config:{},guardrail_config:{}},
    channels:{channel_binding_id:'',app_id:$('app-filter').value,channel_type:'telegram',account_id:'',callback_key:'',secret_ref:'',status:'disabled',config:{bot_token_ref:'env://TENANT_BOT_TOKEN',webhook_secret_ref:'env://TENANT_WEBHOOK_SECRET',attachments_enabled:false}},
    backends:{binding_id:'',app_id:$('app-filter').value,resource_type:'session',backend_type:'inmemory',secret_ref:'',isolation_level:'logical',config:{}}
  }[kind]}
  function field(key){return document.querySelector('[data-field="'+key+'"]')}
  function values(){const v={};for(const [key,,type] of schemas[kind]||[]){const text=field(key).value;try{v[key]=type==='json'?JSON.parse(text||'{}'):type==='number'?Number(text):text.trim()}catch{throw new Error(key+' 必须是合法 JSON')}}return v}
  async function edit(value=null) {
    selected=value?structuredClone(value):null;
    const editable=!!schemas[kind], model=value||defaults()||{};
    $('fields').replaceChildren();$('detail').textContent=JSON.stringify(model,null,2);$('editor').hidden=false;
    $('editor-title').textContent=(value?'查看 / 编辑':'新建')+' '+titles[kind];
    $('editor-note').textContent=value?'版本不可变；Secret 只显示引用，脱敏占位值不能直接保存回配置。':'保存会立即调用真实 Admin API。创建版本后需显式发布，才影响新会话。';
    for(const [key,label,type] of schemas[kind]||[]){
      const row=node('label',label,type==='json'?'wide':'');const input=node(type==='json'?'textarea':'input');input.dataset.field=key;
      if(type==='number')input.type='number';input.value=type==='json'?JSON.stringify(model[key]??{},null,2):(model[key]??'');
      const identity=['tenant_id','app_id','channel_binding_id','binding_id','callback_key','channel_type','account_id','secret_ref'].includes(key);
      const mutable={tenants:['quota_config','audit_policy'],apps:['rollout_policy'],channels:['config','status'],backends:[]};
      input.disabled=!canWrite()||!!(value&&kind!=='revisions'&&(identity||!mutable[kind]?.includes(key)));row.append(input);$('fields').append(row);
    }
    if(kind==='revisions'&&value&&canWrite()){field('revision_id').value='';field('revision_no').value=Number(value.revision_no)+1;$('editor-note').textContent='保存创建新版本；“发布此版本”发布正在查看的旧版本，可用于回滚。'}
    $('save').hidden=!editable||!canWrite()||!!(value&&['apps','tenants','backends'].includes(kind));
    $('publish').hidden=!(value&&kind==='revisions'&&canWrite());$('rollout').hidden=!(value&&kind==='apps'&&canWrite());$('tenant-policy').hidden=!(value&&kind==='tenants'&&canWrite());
    $('skill-picker').hidden=kind!=='revisions'||!canWrite();$('skill-options').replaceChildren();
    if(kind==='revisions'&&canWrite()){
      const current=JSON.parse(field('agent_config').value).skills||[];
      const list=await api('/admin/skills/list',{tenant_id:tenant()});
      for(const item of list.items){const label=node('label'),check=node('input');check.type='checkbox';check.dataset.skillName=item.name;check.checked=current.some(x=>x.name===item.name&&x.checksum===item.checksum);
        check.addEventListener('change',()=>{if(check.checked)for(const other of $('skill-options').querySelectorAll('input'))if(other!==check&&other.dataset.skillName===item.name)other.checked=false});
        check.addEventListener('change',()=>{try{const agent=JSON.parse(field('agent_config').value),policy=JSON.parse(field('tool_policy').value);agent.skills=(agent.skills||[]).filter(x=>x.name!==item.name);if(check.checked)agent.skills.push({name:item.name,version:item.version,checksum:item.checksum});policy.allowed_tools=Array.from(new Set([...(policy.allowed_tools||[]),'skill_load','skill_run']));if(!agent.skills.length)policy.allowed_tools=policy.allowed_tools.filter(n=>!['skill_load','skill_run'].includes(n));field('agent_config').value=JSON.stringify(agent,null,2);field('tool_policy').value=JSON.stringify(policy,null,2)}catch{message('先修正 Agent/工具配置的 JSON',true);check.checked=!check.checked}});
        label.append(check,document.createTextNode(item.name+' @ '+item.version+' — '+item.description));$('skill-options').append(label)}
      if(!list.items.length)$('skill-options').textContent='当前租户没有部署者授予的 Skill。';
    }
    $('editor').scrollIntoView({block:'nearest',behavior:'smooth'});
  }
  $('login-form').addEventListener('submit',event=>{event.preventDefault();perform(async()=>{token=$('token').value.trim();$('token').value='';principal=await api('/admin/me',{});$('identity').textContent=principal.name+' · '+principal.role;$('login').hidden=true;$('console').hidden=false;$('logout').hidden=false;await loadTenants();await refresh()})});
  $('logout').addEventListener('click',()=>{disconnect();message('已清除本页凭据')});
  $('navigation').addEventListener('click',event=>{const button=event.target.closest('[data-kind]');if(button)perform(async()=>{kind=button.dataset.kind;$('app-filter').value='';await refresh()})});
  $('tenant').addEventListener('change',()=>{sequence++;selected=null;$('editor').hidden=true;$('rows').replaceChildren();$('app-filter').value='';perform(()=>refresh())});$('refresh').addEventListener('click',()=>perform(()=>refresh()));$('more').addEventListener('click',()=>perform(()=>refresh(true)));
  $('new-resource').addEventListener('click',()=>perform(()=>edit()));$('close-editor').addEventListener('click',()=>{$('editor').hidden=true;selected=null});
  $('resource-form').addEventListener('submit',event=>{event.preventDefault();perform(async()=>{
    if(!canWrite())throw new Error('当前身份只读');if(selected&&kind!=='tenants'&&selected.tenant_id!==tenant())throw new Error('租户已切换，请重新打开配置');const value=values();
    if(JSON.stringify(value).includes('[REDACTED'))throw new Error('配置包含脱敏占位值，请改用有效 SecretRef，不要直接覆盖');
    let path='/admin/'+({tenants:'tenants',apps:'apps',revisions:'revisions',channels:'channel-bindings',backends:'backend-bindings'}[kind]);
    if(kind!=='tenants')value.tenant_id=tenant();if(kind==='revisions')value.created_by=principal.name;
    let body=value;
    if(kind==='channels'&&selected){path='/admin/channel-bindings/update';body={tenant_id:tenant(),binding_id:selected.channel_binding_id,config:value.config,status:value.status,expected_version:selected.version}}
    const result=await api(path,body);message('已保存');if(kind==='tenants')await loadTenants();await refresh();await edit(result);
  })});
  $('publish').addEventListener('click',()=>perform(async()=>{
    if(!canWrite()||!selected)return;if(selected.tenant_id!==tenant())throw new Error('租户已切换，请重新打开版本');const app=await api('/admin/apps/get',{tenant_id:tenant(),app_id:selected.app_id});
    if(!confirm('将应用 '+app.app_id+' 的稳定版本从 '+(app.stable_revision_id||'未发布')+' 切换为 '+selected.revision_id+'？已固定版本的会话不会自动迁移。'))return;
    await api('/admin/revisions/publish',{tenant_id:tenant(),app_id:selected.app_id,revision_id:selected.revision_id,expected_version:app.version});message('已发布稳定版本');await refresh();
  }));
  $('rollout').addEventListener('click',()=>perform(async()=>{if(!canWrite()||!selected)return;await api('/admin/apps/rollout',{tenant_id:tenant(),app_id:selected.app_id,rollout_policy:values().rollout_policy,expected_version:selected.version});message('灰度策略已保存');await refresh()}));
  $('tenant-policy').addEventListener('click',()=>perform(async()=>{if(!canWrite()||!selected)return;const v=values();await api('/admin/tenants/policies',{tenant_id:selected.tenant_id,quota_config:v.quota_config,audit_policy:v.audit_policy,expected_version:selected.version});message('租户策略已保存');await loadTenants();await refresh()}));
  window.addEventListener('pagehide',()=>{token=''});
})();
