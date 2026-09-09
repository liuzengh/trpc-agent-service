# Shared deployment transaction for the two local IM entry scripts.
# Provider checks and input validation are owned by those entry scripts.
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$InputPath,
    [Parameter(Mandatory)][string]$ProjectName,
    [Parameter(Mandatory)][ValidateSet('telegram','wecom_bot')][string]$ChannelType,
    [Parameter(Mandatory)][string]$BotId,
    [Parameter(Mandatory)][string]$ModelKey,
    [Parameter(Mandatory)][string]$ModelName,
    [string]$ModelBaseURL = '',
    [Parameter(Mandatory)][string[]]$AllowedUsers,
    [Parameter(Mandatory)][string]$SystemPrompt,
    [Parameter(Mandatory)][int]$MaxLLMCalls,
    [string[]]$AgentTools = @(),
    [Parameter(Mandatory)][int64]$Reservation,
    [Parameter(Mandatory)][hashtable]$ProviderEnvironment,
    [Parameter(Mandatory)][Collections.IDictionary]$Config,
    [int]$AdminPort,[int]$GatewayPort,[int]$PostgresPort,[int]$OtelGrpcPort,[int]$OtelHttpPort,
    [switch]$SkipBuild
)
Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'
$repoRoot=[IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$agent=@{name='support';type='llm';defaultModel=$modelName;systemPrompt=$systemPrompt;maxLLMCalls=$maxLLMCalls;tools=$agentTools}
$agentMaterial=@($modelName,$systemPrompt,[string]$maxLLMCalls,($agentTools -join ',')) | ConvertTo-Json -Compress
$agentFingerprint=[Convert]::ToHexString([Security.Cryptography.SHA256]::HashData([Text.Encoding]::UTF8.GetBytes($agentMaterial)))
if ($channelType -eq 'telegram') {
    $connector='telegram-poller';$connectorProfile='telegram';$overlayName='deploy/docker-compose.telegram.yml'
    $bridgeVariable='TRPC_SECRET_TELEGRAM_WEBHOOK';$routeVariable='TELEGRAM_WEBHOOK_ROUTE_KEY'
    $channelTokenRef='env://TRPC_SECRET_TELEGRAM_BOT_TOKEN';$channelSecretRef='env://TRPC_SECRET_TELEGRAM_WEBHOOK'
    $stateName='telegram.bootstrap.state.json';$tenantPrefix='telegram-bot-';$appDescription='Private Telegram assistant'
} else {
    $connector='wecom-bot';$connectorProfile='wecom-bot';$overlayName='deploy/docker-compose.wecom-bot.yml'
    $bridgeVariable='TRPC_SECRET_WECOM_BOT_BRIDGE_TOKEN';$routeVariable='WECOM_BOT_WEBHOOK_ROUTE_KEY'
    $channelTokenRef='env://TRPC_SECRET_WECOM_BOT_BRIDGE_TOKEN';$channelSecretRef=''
    $stateName='wecom-bot.bootstrap.state.json';$tenantPrefix='wecom-bot-';$appDescription='企业微信个人智能助手'
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
$statePath = Join-Path $privateDir $stateName
$scopePath = Join-Path $privateDir 'secret-bindings.compose.json'
try { $lock = [IO.File]::Open((Join-Path $privateDir 'bootstrap.lock'), 'OpenOrCreate', 'ReadWrite', 'None') }
catch { throw 'Another bootstrap is using this private state directory.' }
$oldEnv = @{}
try {
    function Save-BootstrapState {
        $temporary = Join-Path $privateDir ('state-' + [Guid]::NewGuid().ToString('N') + '.tmp')
        try {
            $bytes = [Text.UTF8Encoding]::new($false).GetBytes(($state | ConvertTo-Json -Depth 12))
            $stream = [IO.File]::Open($temporary, 'CreateNew', 'Write', 'None')
            try { $stream.Write($bytes,0,$bytes.Length); $stream.Flush($true) } finally { $stream.Dispose() }
            [IO.File]::Move($temporary,$statePath,$true)
        } finally { if (Test-Path -LiteralPath $temporary) { Remove-Item -LiteralPath $temporary -Force } }
    }
    $hasState = Test-Path -LiteralPath $statePath
    if ($hasState) {
        try { $state = Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json -AsHashtable }
        catch { throw 'Bootstrap state is unreadable; restore its protected backup rather than regenerating infrastructure secrets.' }
        if ($state.projectName -ne $ProjectName -or $state.botId -cne $botId -or $state.modelName -cne $modelName -or
            [string]$state['modelBaseURL'] -cne $modelBaseURL -or
            ($state.Contains('agentFingerprint') -and $state.agentFingerprint -cne $agentFingerprint) -or
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
        foreach ($key in @('POSTGRES_PASSWORD','MASTER_KEY','SERVICE_AUTH_SECRET','ADMIN_API_TOKEN','AUDIT_IDENTITY_HMAC_KEY','METRICS_AUTH_TOKEN','GRAFANA_PASSWORD',$bridgeVariable,$routeVariable)) {
            $secrets[$key] = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32)).ToLowerInvariant()
        }
        $state = [ordered]@{projectName=$ProjectName;botId=$botId;modelName=$modelName;modelBaseURL=$modelBaseURL;agentFingerprint=$agentFingerprint;allowedUsers=$allowedUsers;ports=$ports;secrets=$secrets;tenantId='';appId='';versionId='';deploymentId=''}
        Save-BootstrapState
    }
    $environment = @{}
    foreach ($key in @('POSTGRES_PASSWORD','MASTER_KEY','SERVICE_AUTH_SECRET','ADMIN_API_TOKEN','AUDIT_IDENTITY_HMAC_KEY','METRICS_AUTH_TOKEN','GRAFANA_PASSWORD',$bridgeVariable,$routeVariable)) {
        if ([string]$state.secrets[$key] -cnotmatch '^[a-f0-9]{64}$') { throw 'Persisted infrastructure secret is invalid; do not regenerate it for an existing database.' }
        $environment[$key] = [string]$state.secrets[$key]
    }
    $environment += @{
        TRPC_SECRET_OPENAI_API_KEY=$modelKey;TRPC_OPENAI_BASE_URL=$modelBaseURL
        POSTGRES_HOST_PORT=[string]$PostgresPort;ADMIN_HOST_PORT=[string]$AdminPort;GATEWAY_HOST_PORT=[string]$GatewayPort
        OTEL_GRPC_HOST_PORT=[string]$OtelGrpcPort;OTEL_HTTP_HOST_PORT=[string]$OtelHttpPort
        TELEGRAM_ADMIN_PORT=[string]$AdminPort;TELEGRAM_GATEWAY_PORT=[string]$GatewayPort;TELEGRAM_POSTGRES_PORT=[string]$PostgresPort
        TELEGRAM_OTLP_GRPC_PORT=[string]$OtelGrpcPort;TELEGRAM_OTLP_HTTP_PORT=[string]$OtelHttpPort;CONTAINER_HTTPS_PROXY=[string]$config['CONTAINER_HTTPS_PROXY']
        DATA_PLANE_PROFILES='[]';MCP_PROFILES='[]';ADMIN_PRINCIPALS_JSON='';FAIR_QUEUE_ENABLED='false'
        TRPC_SECRET_WECOM_TOKEN='';TRPC_SECRET_WECOM_CORP_SECRET='';TRPC_SECRET_WECOM_AES=''
        CONSUMER_CONCURRENCY='1';DELIVERY_CONCURRENCY='1';SUMMARY_CONCURRENCY='1'
    }
    if ($channelType -eq 'wecom_bot') { $environment['TRPC_SECRET_TELEGRAM_BOT_TOKEN']='';$environment['TRPC_SECRET_TELEGRAM_WEBHOOK']='' }
    foreach ($entry in $providerEnvironment.GetEnumerator()) { $environment[$entry.Key] = $entry.Value }
    foreach ($entry in $environment.GetEnumerator()) {
        $oldEnv[$entry.Key] = [Environment]::GetEnvironmentVariable($entry.Key,'Process')
        [Environment]::SetEnvironmentVariable($entry.Key,[string]$entry.Value,'Process')
    }
    # An empty explicit env file prevents accidentally loading deploy/.env.
    $emptyEnv = Join-Path $privateDir 'compose.empty.env'
    [IO.File]::WriteAllText($emptyEnv, '', [Text.UTF8Encoding]::new($false))
    $compose = @('--project-name',$ProjectName,'--env-file',$emptyEnv,'-f',(Join-Path $repoRoot 'deploy/docker-compose.yml'),'-f',(Join-Path $repoRoot $overlayName))
    function Invoke-BootstrapCompose([string[]]$Arguments) {
        # Compose config/logs/inspect may expose environment values: suppress raw
        # subprocess output and report only the stage and exit status.
        & docker compose @compose @Arguments *> $null
        if ($LASTEXITCODE -ne 0) { throw "Docker Compose stage failed: $($Arguments[0]) (exit $LASTEXITCODE). Inspect only this project's logs locally, with credential redaction." }
    }
    Invoke-BootstrapCompose @('config','--quiet')
    $services = @('migrate','admin','gateway','worker','summary-worker','consumer','delivery',$connector)
    if (-not $SkipBuild) {
        foreach ($service in $services) {
            Write-Output "Building $service..."
            Invoke-BootstrapCompose @('--profile',$connectorProfile,'build',$service)
        }
    }
    if ($hasState) { Invoke-BootstrapCompose @('--profile',$connectorProfile,'stop',$connector) }
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
    $tenantName = "$tenantPrefix$botId"
    $model = @{provider='openai';modelName=$modelName;apiKeyRef='env://TRPC_SECRET_OPENAI_API_KEY';maxTokens=1024}
    $tenantConfig = @{
        agents=@($agent);models=@($model);toolPolicy=@{mode='whitelist';allowed=$agentTools}
        channels=@(@{type=$channelType;accountId=$botId;agentApp='support';webhookKey=$state.secrets[$routeVariable]
            tokenRef=$channelTokenRef;secretRef=$channelSecretRef
            accessPolicy=@{allowDirectMessages=$true;allowGroupMessages=$false;allowedUsers=$allowedUsers;allowedGroups=@()}})
        storage=@{sessionBackend='redis';sessionProfile='local-redis';memoryBackend='postgres';memoryProfile='local-postgres'}
        governance=@{auditLevel='detailed'};budget=@{maxTokensPerDay=([int64]$reservation*2);maxTokensPerRequest=$reservation;maxConcurrentSessions=2}
    }
    $tenants = @(foreach ($item in (Invoke-BootstrapAdmin 'Get' '/api/v1/tenants')) { if ($null -ne $item) { $item } })
    $matches = @($tenants | Where-Object { $_.name -eq $tenantName })
    if ($matches.Count -gt 1) { throw 'Multiple matching tenants exist; refusing ambiguous adoption.' }
    if ($matches.Count -eq 0) {
        if ($state.tenantId) { throw 'Persisted tenant is missing; refusing to recreate it silently.' }
        $tenant = Invoke-BootstrapAdmin 'Post' '/api/v1/tenants' @{name=$tenantName;config=$tenantConfig}
    } else { $tenant = $matches[0] }
    if (($state.tenantId -and $state.tenantId -ne $tenant.id) -or $tenant.channels.Count -ne 1 -or $tenant.channels[0].type -cne $channelType -or $tenant.channels[0].accountId -cne $botId -or
        $tenant.channels[0].webhookKey -cne $state.secrets[$routeVariable] -or $tenant.models.Count -ne 1 -or $tenant.models[0].modelName -cne $modelName -or
        $tenant.agents.Count -ne 1 -or $tenant.agents[0].systemPrompt -cne $systemPrompt -or
        $tenant.agents[0].maxLLMCalls -ne $maxLLMCalls -or
        (@($tenant.agents[0].tools | Sort-Object) -join ',') -cne (@($agentTools | Sort-Object) -join ',') -or
        $tenant.channels[0].accessPolicy.allowGroupMessages -or -not $tenant.channels[0].accessPolicy.allowDirectMessages -or
        (($tenant.channels[0].accessPolicy.allowedUsers | Sort-Object -Unique) -join ',') -cne ($allowedUsers -join ',')) {
        throw 'Existing tenant configuration does not match this bootstrap; refusing to change it.'
    }
    Assert-BootstrapIdentity 'Tenant' ([string]$tenant.id) ([string]$state.tenantId)
    $state.tenantId = [string]$tenant.id
    Save-BootstrapState
    . (Join-Path $PSScriptRoot 'im_secret_bindings.ps1')
    $bindings = New-IMSecretBindingsOverride -TenantId $state.tenantId -ModelProvider openai -ModelName $modelName -ModelSecretRef 'env://TRPC_SECRET_OPENAI_API_KEY' -ChannelType $channelType -ChannelAccountId $botId -ChannelTokenRef $channelTokenRef -ChannelSecretRef $channelSecretRef
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
        $app = Invoke-BootstrapAdmin 'Post' '/api/v1/agent-apps' @{tenantId=$state.tenantId;name='support';description=$appDescription}
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
    Invoke-BootstrapCompose @('--profile',$connectorProfile,'up','-d','--no-build',$connector)
    Write-Output "Local IM stack started: project=$ProjectName tenant=$($state.tenantId) channel=$channelType"
    Write-Output "Console: $adminBase/console/ . Admin login credential is in the protected bootstrap state file; its value is not printed."
    Write-Output 'A configured user can now send a private message to the bot. Startup alone does not verify model availability or successful IM delivery.'
} finally {
    foreach ($entry in $oldEnv.GetEnumerator()) { [Environment]::SetEnvironmentVariable($entry.Key,$entry.Value,'Process') }
    $lock.Dispose()
}
