# Requires PowerShell 7 on Windows and Docker Compose v2.24.4+.
# Credentials are read from JSON only. Never put real credentials in the repo.
[CmdletBinding()]
param(
    [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$ConfigPath,
    [ValidatePattern('^trpc-telegram-[a-z0-9][a-z0-9-]{0,45}$')]
    [string]$ProjectName = 'trpc-telegram-local',
    [ValidateRange(1024,65535)][int]$AdminPort = 48081,
    [ValidateRange(1024,65535)][int]$GatewayPort = 48080,
    [ValidateRange(1024,65535)][int]$PostgresPort = 64532,
    [ValidateRange(1024,65535)][int]$OtelGrpcPort = 64327,
    [ValidateRange(1024,65535)][int]$OtelHttpPort = 64328,
    [switch]$ValidateOnly,
    [switch]$SkipBuild
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$inputPath = [IO.Path]::GetFullPath($ConfigPath)
if ($inputPath.StartsWith($repoRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Credential JSON must be stored outside the source repository.'
}
if (-not (Test-Path -LiteralPath $inputPath -PathType Leaf)) {
    throw 'Blocked: local credential JSON is missing. Copy deploy/telegram.local.example.json outside the repository and provide real credentials and allowed Telegram user IDs.'
}
try { $config = Get-Content -LiteralPath $inputPath -Raw | ConvertFrom-Json -AsHashtable }
catch { throw 'Credential JSON could not be parsed.' }
if ($config -isnot [Collections.IDictionary]) { throw 'Credential JSON must be an object.' }
$botToken = [string]$config['TRPC_SECRET_TELEGRAM_BOT_TOKEN']
if ($botToken -cnotmatch '^[1-9][0-9]{5,19}:[A-Za-z0-9_-]{30,128}\z') { throw 'Blocked: a real Telegram bot token is required.' }
$modelKey = [string]$config['TRPC_SECRET_OPENAI_API_KEY']
. (Join-Path $PSScriptRoot 'im_local_config.ps1')
Assert-IMModelKey $modelKey
$modelBaseURL = [string]$config['TRPC_OPENAI_BASE_URL']
Assert-IMModelBaseURL $modelBaseURL
$allowedUsers = @($config['TELEGRAM_ALLOWED_USER_ID'] | ForEach-Object { [string]$_ } | Sort-Object -Unique)
if ($allowedUsers.Count -eq 0 -or @($allowedUsers | Where-Object { $_ -cnotmatch '^[1-9][0-9]{0,15}$' }).Count -gt 0) {
    throw 'Blocked: TELEGRAM_ALLOWED_USER_ID must contain one or more positive numeric Telegram user IDs.'
}
$modelName = [string]$config['TELEGRAM_MODEL_NAME']
if (-not $modelName) { $modelName = 'gpt-4o-mini' }
if ($modelName -cnotmatch '^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$') { throw 'Invalid OpenAI model name.' }
$systemPrompt = [string]$config['SYSTEM_PROMPT']
if (-not $systemPrompt) { $systemPrompt = '请用简洁、准确的中文回答；无法确认的信息应说明，不要声称执行过未执行的操作。' }
if ([Text.Encoding]::UTF8.GetByteCount($systemPrompt) -gt 16384 -or $systemPrompt.Contains([char]0)) { throw 'SYSTEM_PROMPT is invalid or exceeds 16 KiB.' }
foreach ($key in @('LOCAL_HTTPS_PROXY','CONTAINER_HTTPS_PROXY')) {
    if ($config[$key]) {
        $proxy = $null
        if (-not [Uri]::TryCreate([string]$config[$key], [UriKind]::Absolute, [ref]$proxy) -or
            $proxy.Scheme -notin @('http','https') -or $proxy.UserInfo -or $proxy.Query -or $proxy.Fragment -or $proxy.AbsolutePath -ne '/') {
            throw 'Proxy must be an HTTP(S) origin without credentials, a path or query.'
        }
    }
}

function Invoke-TelegramPreflight([ValidateSet('getMe','getWebhookInfo')][string]$Method) {
    $arguments = @{ Uri="https://api.telegram.org/bot$botToken/$Method"; Method='Get'; TimeoutSec=25; MaximumRedirection=0 }
    if ($config['LOCAL_HTTPS_PROXY']) { $arguments.Proxy = [string]$config['LOCAL_HTTPS_PROXY'] }
    try { $response = Invoke-RestMethod @arguments }
    catch { throw "Telegram $Method preflight failed. Check the credential and local network; provider URLs and errors are suppressed to protect the bot token." }
    if (-not $response.ok) { throw "Telegram $Method preflight was rejected." }
    return $response.result
}

$bot = Invoke-TelegramPreflight 'getMe'
if (-not $bot.is_bot -or [string]$bot.id -cnotmatch '^[1-9][0-9]*$') { throw 'Telegram getMe did not return a valid bot identity.' }
$botId = [string]$bot.id
$webhook = Invoke-TelegramPreflight 'getWebhookInfo'
if ($webhook.url) { throw 'Blocked: this bot already has a webhook. Bootstrap will not delete or replace it; select a dedicated bot.' }
Write-Output "Telegram account preflight passed: bot_id=$botId. Private messages are restricted to the configured user allowlist."
if ($ValidateOnly) {
    Write-Output 'Validation only: no deployment, getUpdates, message send or model invocation was performed. OpenAI credentials were checked for format only.'
    return
}
if ($PSVersionTable.PSVersion.Major -lt 7 -or -not $IsWindows) { throw 'Deployment requires PowerShell 7 on Windows for protected local credential storage.' }
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { throw 'Docker CLI is required.' }
$ports = @($AdminPort,$GatewayPort,$PostgresPort,$OtelGrpcPort,$OtelHttpPort)
if (@($ports | Select-Object -Unique).Count -ne $ports.Count) { throw 'Published ports must be distinct.' }

# Only this dedicated child directory is hardened. The input JSON is read-only.
$privateDir = Join-Path ([IO.Path]::GetDirectoryName($inputPath)) $ProjectName
[IO.Directory]::CreateDirectory($privateDir) | Out-Null
$identity = [Security.Principal.WindowsIdentity]::GetCurrent().User
$acl = [Security.AccessControl.DirectorySecurity]::new()
$acl.SetAccessRuleProtection($true, $false)
foreach ($sid in @($identity, [Security.Principal.SecurityIdentifier]::new('S-1-5-18'))) {
    $rule = [Security.AccessControl.FileSystemAccessRule]::new($sid, 'FullControl', 'ContainerInherit,ObjectInherit', 'None', 'Allow')
    $acl.AddAccessRule($rule)
}
[IO.FileSystemAclExtensions]::SetAccessControl([IO.DirectoryInfo]::new($privateDir), $acl)
$statePath = Join-Path $privateDir 'telegram.bootstrap.state.json'
$scopePath = Join-Path $privateDir 'secret-bindings.compose.json'
try { $lock = [IO.File]::Open((Join-Path $privateDir 'bootstrap.lock'), 'OpenOrCreate', 'ReadWrite', 'None') }
catch { throw 'Another bootstrap is using this private state directory.' }
$oldEnv = @{}
try {
    function Save-BootstrapState {
        $temporary = Join-Path $privateDir ('state-' + [Guid]::NewGuid().ToString('N') + '.tmp')
        try {
            [IO.File]::WriteAllText($temporary, ($state | ConvertTo-Json -Depth 12), [Text.UTF8Encoding]::new($false))
            Move-Item -LiteralPath $temporary -Destination $statePath -Force
        } finally { if (Test-Path -LiteralPath $temporary) { Remove-Item -LiteralPath $temporary -Force } }
    }
    $hasState = Test-Path -LiteralPath $statePath
    if ($hasState) {
        try { $state = Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json -AsHashtable }
        catch { throw 'Bootstrap state is unreadable; restore its protected backup rather than regenerating infrastructure secrets.' }
        if ($state.projectName -ne $ProjectName -or $state.botId -ne $botId -or $state.modelName -ne $modelName -or
            [string]$state['modelBaseURL'] -cne $modelBaseURL -or
            ($state.allowedUsers -join ',') -cne ($allowedUsers -join ',') -or ($state.ports -join ',') -ne ($ports -join ',')) {
            throw 'Bootstrap identity, model, allowlist or ports differ from persisted state. Review the existing deployment before changing its configuration.'
        }
    } else {
        # Never adopt another project merely because its name happens to match.
        $containers = @(& docker ps -aq --filter "label=com.docker.compose.project=$ProjectName" 2>$null)
        if ($LASTEXITCODE -ne 0) { throw 'Docker engine is unavailable.' }
        $volumes = @(& docker volume ls -q --filter "label=com.docker.compose.project=$ProjectName" 2>$null)
        if ($LASTEXITCODE -ne 0) { throw 'Docker volume inventory failed.' }
        if ($containers.Count -gt 0 -or $volumes.Count -gt 0) { throw 'A Compose project with this name already exists without matching bootstrap state; use a different project name.' }
        $secrets = [ordered]@{}
        foreach ($key in @('POSTGRES_PASSWORD','MASTER_KEY','SERVICE_AUTH_SECRET','ADMIN_API_TOKEN','AUDIT_IDENTITY_HMAC_KEY','METRICS_AUTH_TOKEN','GRAFANA_PASSWORD','TRPC_SECRET_TELEGRAM_WEBHOOK','TELEGRAM_WEBHOOK_ROUTE_KEY')) {
            $secrets[$key] = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32)).ToLowerInvariant()
        }
        $state = [ordered]@{projectName=$ProjectName;botId=$botId;modelName=$modelName;modelBaseURL=$modelBaseURL;allowedUsers=$allowedUsers;ports=$ports;secrets=$secrets;tenantId='';appId='';versionId='';deploymentId=''}
        Save-BootstrapState
    }
    $environment = @{}
    foreach ($key in @('POSTGRES_PASSWORD','MASTER_KEY','SERVICE_AUTH_SECRET','ADMIN_API_TOKEN','AUDIT_IDENTITY_HMAC_KEY','METRICS_AUTH_TOKEN','GRAFANA_PASSWORD','TRPC_SECRET_TELEGRAM_WEBHOOK','TELEGRAM_WEBHOOK_ROUTE_KEY')) {
        if ([string]$state.secrets[$key] -cnotmatch '^[a-f0-9]{64}$') { throw 'Persisted infrastructure secret is invalid; do not regenerate it for an existing database.' }
        $environment[$key] = [string]$state.secrets[$key]
    }
    $environment += @{
        TRPC_SECRET_TELEGRAM_BOT_TOKEN=$botToken;TRPC_SECRET_OPENAI_API_KEY=$modelKey;TELEGRAM_EXPECTED_BOT_ID=$botId
        TRPC_OPENAI_BASE_URL=$modelBaseURL
        TELEGRAM_ADMIN_PORT=[string]$AdminPort;TELEGRAM_GATEWAY_PORT=[string]$GatewayPort;TELEGRAM_POSTGRES_PORT=[string]$PostgresPort
        TELEGRAM_OTLP_GRPC_PORT=[string]$OtelGrpcPort;TELEGRAM_OTLP_HTTP_PORT=[string]$OtelHttpPort;CONTAINER_HTTPS_PROXY=[string]$config['CONTAINER_HTTPS_PROXY']
        DATA_PLANE_PROFILES='[]';MCP_PROFILES='[]';ADMIN_PRINCIPALS_JSON='';FAIR_QUEUE_ENABLED='false'
        TRPC_SECRET_WECOM_TOKEN='';TRPC_SECRET_WECOM_CORP_SECRET='';TRPC_SECRET_WECOM_AES=''
        CONSUMER_CONCURRENCY='1';DELIVERY_CONCURRENCY='1';SUMMARY_CONCURRENCY='1'
    }
    foreach ($entry in $environment.GetEnumerator()) {
        $oldEnv[$entry.Key] = [Environment]::GetEnvironmentVariable($entry.Key,'Process')
        [Environment]::SetEnvironmentVariable($entry.Key,[string]$entry.Value,'Process')
    }
    # An empty explicit env file prevents accidentally loading deploy/.env.
    $emptyEnv = Join-Path $privateDir 'compose.empty.env'
    [IO.File]::WriteAllText($emptyEnv, '', [Text.UTF8Encoding]::new($false))
    $compose = @('--project-name',$ProjectName,'--env-file',$emptyEnv,'-f',(Join-Path $repoRoot 'deploy/docker-compose.yml'),'-f',(Join-Path $repoRoot 'deploy/docker-compose.telegram.yml'))
    function Invoke-BootstrapCompose([string[]]$Arguments) {
        # Compose config/logs/inspect may expose environment values: suppress raw
        # subprocess output and report only the stage and exit status.
        & docker compose @compose @Arguments *> $null
        if ($LASTEXITCODE -ne 0) { throw "Docker Compose stage failed: $($Arguments[0]) (exit $LASTEXITCODE). Inspect only this project's logs locally, with credential redaction." }
    }
    Invoke-BootstrapCompose @('config','--quiet')
    $services = @('migrate','admin','gateway','worker','summary-worker','consumer','delivery','telegram-poller')
    if (-not $SkipBuild) {
        foreach ($service in $services) {
            Write-Output "Building $service..."
            Invoke-BootstrapCompose @('--profile','telegram','build',$service)
        }
    }
    if ($hasState) { Invoke-BootstrapCompose @('--profile','telegram','stop','telegram-poller') }
    Invoke-BootstrapCompose @('up','-d','--no-build','--wait','--wait-timeout','180','postgres','redis','otel-collector','admin')
    $adminBase = "http://127.0.0.1:$AdminPort"
    $headers = @{Authorization="Bearer $($state.secrets.ADMIN_API_TOKEN)"}
    function Invoke-BootstrapAdmin([string]$Method,[string]$Path,[object]$Body=$null) {
        $arguments = @{Uri="$adminBase$Path";Method=$Method;Headers=$headers;TimeoutSec=30;MaximumRedirection=0;NoProxy=$true}
        if ($null -ne $Body) { $arguments.ContentType='application/json; charset=utf-8';$arguments.Body=[Text.Encoding]::UTF8.GetBytes(($Body | ConvertTo-Json -Depth 20 -Compress)) }
        try { return Invoke-RestMethod @arguments }
        catch { throw "Admin API $Method $Path failed. State is retained for a safe rerun; raw responses are suppressed." }
    }
    function Assert-BootstrapIdentity([string]$Kind,[string]$Actual,[string]$Expected) {
        $parsed = [Guid]::Empty
        if (-not [Guid]::TryParse($Actual,[ref]$parsed) -or $parsed -eq [Guid]::Empty -or ($Expected -and $Actual -ne $Expected)) {
            throw "$Kind identity is missing or differs from persisted bootstrap state."
        }
    }
    $tenantName = "telegram-bot-$botId"
    $agent = @{name='support';type='llm';defaultModel=$modelName;systemPrompt=$systemPrompt;maxLLMCalls=1;tools=@()}
    $model = @{provider='openai';modelName=$modelName;apiKeyRef='env://TRPC_SECRET_OPENAI_API_KEY';maxTokens=1024}
    $tenantConfig = @{
        agents=@($agent);models=@($model);toolPolicy=@{mode='whitelist';allowed=@()}
        channels=@(@{type='telegram';accountId=$botId;agentApp='support';webhookKey=$state.secrets.TELEGRAM_WEBHOOK_ROUTE_KEY
            tokenRef='env://TRPC_SECRET_TELEGRAM_BOT_TOKEN';secretRef='env://TRPC_SECRET_TELEGRAM_WEBHOOK'
            accessPolicy=@{allowDirectMessages=$true;allowGroupMessages=$false;allowedUsers=$allowedUsers;allowedGroups=@()}})
        storage=@{sessionBackend='redis';sessionProfile='local-redis';memoryBackend='postgres';memoryProfile='local-postgres'}
        governance=@{auditLevel='detailed'};budget=@{maxTokensPerDay=512000;maxTokensPerRequest=256000;maxConcurrentSessions=2}
    }
    # Invoke-RestMethod can emit a JSON array as one pipeline object. Explicit
    # enumeration preserves the empty-list case under StrictMode.
    $tenants = @(foreach ($item in (Invoke-BootstrapAdmin 'Get' '/api/v1/tenants')) { if ($null -ne $item) { $item } })
    $matches = @($tenants | Where-Object { $_.name -eq $tenantName })
    if ($matches.Count -gt 1) { throw 'Multiple matching tenants exist; refusing ambiguous adoption.' }
    if ($matches.Count -eq 0) {
        if ($state.tenantId) { throw 'Persisted tenant is missing; refusing to recreate it silently.' }
        $tenant = Invoke-BootstrapAdmin 'Post' '/api/v1/tenants' @{name=$tenantName;config=$tenantConfig}
    } else { $tenant = $matches[0] }
    if (($state.tenantId -and $state.tenantId -ne $tenant.id) -or $tenant.channels.Count -ne 1 -or $tenant.channels[0].accountId -ne $botId -or
        $tenant.channels[0].webhookKey -ne $state.secrets.TELEGRAM_WEBHOOK_ROUTE_KEY -or $tenant.models[0].modelName -ne $modelName -or
        $tenant.channels[0].accessPolicy.allowGroupMessages -or -not $tenant.channels[0].accessPolicy.allowDirectMessages -or
        (($tenant.channels[0].accessPolicy.allowedUsers | Sort-Object -Unique) -join ',') -cne ($allowedUsers -join ',')) {
        throw 'Existing tenant configuration does not match this bootstrap; refusing to change it.'
    }
    Assert-BootstrapIdentity 'Tenant' ([string]$tenant.id) ([string]$state.tenantId)
    $state.tenantId = [string]$tenant.id
    Save-BootstrapState
    . (Join-Path $PSScriptRoot 'im_secret_bindings.ps1')
    $bindings = New-IMSecretBindingsOverride -TenantId $state.tenantId -ModelProvider openai -ModelName $modelName -ModelSecretRef 'env://TRPC_SECRET_OPENAI_API_KEY' -ChannelType telegram -ChannelAccountId $botId -ChannelTokenRef 'env://TRPC_SECRET_TELEGRAM_BOT_TOKEN' -ChannelSecretRef 'env://TRPC_SECRET_TELEGRAM_WEBHOOK'
    Write-IMSecretBindingsOverride -Path $scopePath -Definition $bindings | Out-Null
    $compose += @('-f',$scopePath)
    Invoke-BootstrapCompose @('up','-d','--no-build','--wait','--wait-timeout','180','admin','gateway','worker','summary-worker','consumer','delivery')
    function Get-BootstrapItems([string]$View) {
        $page = Invoke-BootstrapAdmin 'Get' "/api/v1/operations/${View}?tenantId=$($state.tenantId)&limit=100"
        if ($page.PSObject.Properties['nextCursor'] -and $page.nextCursor) { throw 'Bootstrap tenant contains more than 100 records; review it explicitly before resuming.' }
        return @($page.items)
    }
    $apps = @(Get-BootstrapItems 'apps' | Where-Object { $_.name -eq 'support' })
    if ($apps.Count -gt 1) { throw 'Ambiguous support app.' }
    if ($apps.Count -eq 0) {
        if ($state.appId) { throw 'Persisted app is missing; refusing to recreate it silently.' }
        $app = Invoke-BootstrapAdmin 'Post' '/api/v1/agent-apps' @{tenantId=$state.tenantId;name='support';description='Private Telegram assistant'}
    }
    else { $app = $apps[0] }
    Assert-BootstrapIdentity 'App' ([string]$app.id) ([string]$state.appId)
    if ($app.status -ne 'active') { throw 'Bootstrap app is not active.' }
    $state.appId = [string]$app.id
    Save-BootstrapState
    $versions = @(Get-BootstrapItems 'versions' | Where-Object { $_.appId -eq $state.appId })
    if ($versions.Count -gt 1) { throw 'Multiple versions exist; bootstrap will not choose or publish one implicitly.' }
    if ($versions.Count -eq 0) {
        if ($state.versionId) { throw 'Persisted version is missing; refusing to recreate it silently.' }
        $snapshotModel = @{provider='openai';modelName=$modelName;apiKeyRef='env://TRPC_SECRET_OPENAI_API_KEY';maxTokens=1024}
        $version = Invoke-BootstrapAdmin 'Post' '/api/v1/agent-versions' @{tenantId=$state.tenantId;appName='support';snapshot=@{agent=$agent;model=$snapshotModel}}
    } else { $version = $versions[0] }
    Assert-BootstrapIdentity 'Version' ([string]$version.id) ([string]$state.versionId)
    $state.versionId = [string]$version.id
    Save-BootstrapState
    if ($version.status -eq 'draft') { Invoke-BootstrapAdmin 'Post' "/api/v1/agent-versions/$($state.versionId)/publish" @{tenantId=$state.tenantId} | Out-Null }
    elseif ($version.status -ne 'published') { throw 'Version is not eligible for a stable deployment.' }
    $deployments = @(Get-BootstrapItems 'deployments' | Where-Object { $_.appId -eq $state.appId -and $_.status -eq 'active' })
    if ($deployments.Count -gt 1 -or ($deployments.Count -eq 1 -and ($deployments[0].kind -ne 'stable' -or $deployments[0].versionId -ne $state.versionId))) {
        throw 'Existing active deployment differs from the bootstrap version; refusing to replace it.'
    }
    if ($deployments.Count -eq 0) {
        if ($state.deploymentId) { throw 'Persisted deployment is no longer active; refusing to reactivate it silently.' }
        $deployment = Invoke-BootstrapAdmin 'Post' '/api/v1/deployments' @{tenantId=$state.tenantId;appName='support';stableVersionId=$state.versionId;canaryBps=0}
        Assert-BootstrapIdentity 'Deployment' ([string]$deployment.stableId) ''
        $state.deploymentId = [string]$deployment.stableId
    } else {
        Assert-BootstrapIdentity 'Deployment' ([string]$deployments[0].id) ([string]$state.deploymentId)
        $state.deploymentId = [string]$deployments[0].id
    }
    Save-BootstrapState
    # Polling begins only after admission, authorization, publication and routing.
    Invoke-BootstrapCompose @('--profile','telegram','up','-d','--no-build','telegram-poller')
    Write-Output "Local Telegram stack started: project=$ProjectName tenant=$($state.tenantId) bot_id=$botId"
    Write-Output "Console: $adminBase/console/ . Admin login credential is in the protected bootstrap state file; its value is not printed."
    Write-Output 'A configured user can now send a private message to the bot. Startup alone does not verify model availability or successful IM delivery.'
} finally {
    foreach ($entry in $oldEnv.GetEnumerator()) { [Environment]::SetEnvironmentVariable($entry.Key,$entry.Value,'Process') }
    $lock.Dispose()
}
