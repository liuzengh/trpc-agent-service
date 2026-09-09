[CmdletBinding()]
param(
    [string]$EnvFile = (Join-Path $PSScriptRoot '..\deploy\.env.wecom.local'),
    [ValidatePattern('^[a-z0-9][a-z0-9_-]{0,62}$')]
    [string]$ProjectName = 'trpc-platform-wecom',
    [switch]$SkipBuild
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'im_secret_bindings.ps1')

function Read-DotEnv {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        throw "local env file not found; run scripts/wecom_sandbox_setup.ps1 first"
    }
    $values = [ordered]@{}
    foreach ($line in [IO.File]::ReadAllLines($Path, [Text.Encoding]::UTF8)) {
        if ([string]::IsNullOrWhiteSpace($line) -or $line.TrimStart().StartsWith('#')) {
            continue
        }
        $separator = $line.IndexOf('=')
        if ($separator -le 0) {
            throw 'local env file contains a malformed line'
        }
        $key = $line.Substring(0, $separator)
        $value = $line.Substring($separator + 1)
        if ($key -notmatch '^[A-Z][A-Z0-9_]*$' -or $value -match "[`r`n]" -or $values.Contains($key)) {
            throw 'local env file contains an unsafe or duplicate entry'
        }
        $values[$key] = $value
    }
    return $values
}

function Require-Value {
    param([System.Collections.IDictionary]$Values, [string]$Key)
    if (-not $Values.Contains($Key) -or [string]::IsNullOrWhiteSpace([string]$Values[$Key])) {
        throw "required local value $Key is missing"
    }
    return [string]$Values[$Key]
}

function Protect-LocalEnvFile {
    param([string]$Path)
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent().Name
    & icacls.exe $Path /inheritance:r /grant:r "${identity}:(R,W)" | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw 'could not restrict a local credential file ACL'
    }
}

function Save-DotEnv {
    param([System.Collections.IDictionary]$Values, [string]$Path)
    $fullPath = [IO.Path]::GetFullPath($Path)
    $parent = Split-Path -Parent $fullPath
    $temporary = Join-Path $parent ('.wecom-bootstrap-' + [Guid]::NewGuid().ToString('N') + '.tmp')
    try {
        $lines = foreach ($entry in $Values.GetEnumerator()) {
            '{0}={1}' -f $entry.Key, [string]$entry.Value
        }
        [IO.File]::WriteAllLines($temporary, $lines, [Text.UTF8Encoding]::new($false))
        Protect-LocalEnvFile $temporary
        Move-Item -LiteralPath $temporary -Destination $fullPath -Force
        Protect-LocalEnvFile $fullPath
    } finally {
        if (Test-Path -LiteralPath $temporary) {
            Remove-Item -LiteralPath $temporary -Force
        }
    }
}

function Wait-Healthy {
    param([uri]$URI, [string]$Name)
    $deadline = [DateTimeOffset]::UtcNow.AddMinutes(4)
    do {
        try {
            $response = Invoke-WebRequest -Uri $URI -Method Get -TimeoutSec 5
            if ([int]$response.StatusCode -eq 200) {
                Write-Host "$Name health check passed." -ForegroundColor Green
                return
            }
        } catch {
            # Startup races are expected; only the bounded final failure is surfaced.
        }
        Start-Sleep -Seconds 2
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    throw "$Name did not become healthy within four minutes"
}

function Invoke-AdminJSON {
    param(
        [ValidateSet('GET', 'POST')]
        [string]$Method,
        [string]$Path,
        [object]$Body = $null
    )
    $parameters = @{
        Uri = "$script:AdminBase$Path"
        Method = $Method
        Headers = @{ Authorization = "Bearer $script:AdminToken" }
        TimeoutSec = 20
    }
    if ($null -ne $Body) {
        $parameters.ContentType = 'application/json'
        $parameters.Body = $Body | ConvertTo-Json -Depth 12 -Compress
    }
    return Invoke-RestMethod @parameters
}

$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$composeFile = Join-Path $repoRoot 'deploy\docker-compose.yml'
$fullEnvFile = [IO.Path]::GetFullPath($EnvFile)
$bindingOverride = Join-Path $repoRoot "tmp\$ProjectName-im-bindings.json"
$values = Read-DotEnv $fullEnvFile

$required = @(
    'POSTGRES_PASSWORD', 'MASTER_KEY', 'SERVICE_AUTH_SECRET', 'ADMIN_API_TOKEN',
    'GRAFANA_PASSWORD', 'TRPC_WEBHOOK_ROUTE_KEY', 'TRPC_SECRET_WECOM_TOKEN',
    'TRPC_SECRET_WECOM_CORP_SECRET', 'TRPC_SECRET_WECOM_AES',
    'WECOM_CORP_ID', 'WECOM_AGENT_ID',
    'WECOM_ALLOWED_USER_ID', 'WECOM_CALLBACK_BASE_URL', 'GATEWAY_HOST_PORT',
    'ADMIN_HOST_PORT'
)
foreach ($key in $required) {
    $null = Require-Value $values $key
}
$modelProvider = 'openai'
$modelName = 'gpt-4o-mini'
foreach ($entry in $values.GetEnumerator()) {
    [Environment]::SetEnvironmentVariable($entry.Key, [string]$entry.Value, 'Process')
}

$composeArgs = @('compose', '--project-name', $ProjectName, '--env-file', $fullEnvFile, '-f', $composeFile)
if ($values.Contains('WECOM_TENANT_ID') -and (Test-Path -LiteralPath $bindingOverride)) {
    # Preserve scoped authorizations while rebuilding an existing sandbox.
    $composeArgs += @('-f', $bindingOverride)
}
$composeArgs += @('up', '-d')
if (-not $SkipBuild) {
    $composeArgs += '--build'
}
Write-Host 'Starting the isolated WeCom sandbox stack. Secret values are not printed.' -ForegroundColor Cyan
& docker @composeArgs
if ($LASTEXITCODE -ne 0) {
    throw 'Docker Compose sandbox startup failed'
}

$gatewayPort = [int](Require-Value $values 'GATEWAY_HOST_PORT')
$adminPort = [int](Require-Value $values 'ADMIN_HOST_PORT')
Wait-Healthy ([uri]"http://127.0.0.1:$gatewayPort/health") 'Gateway'
Wait-Healthy ([uri]"http://127.0.0.1:$adminPort/health") 'Admin'

$script:AdminBase = "http://127.0.0.1:$adminPort"
$script:AdminToken = Require-Value $values 'ADMIN_API_TOKEN'
$tenantName = 'wangzilong-wecom-sandbox'
$tenantID = if ($values.Contains('WECOM_TENANT_ID')) { [string]$values['WECOM_TENANT_ID'] } else { '' }
if (-not $tenantID) {
    $existing = @(Invoke-AdminJSON -Method GET -Path '/api/v1/tenants' | Where-Object { $_.name -eq $tenantName })
    if ($existing.Count -gt 1) {
        throw 'multiple sandbox tenants share the reserved name'
    }
    if ($existing.Count -eq 1) {
        $tenantID = [string]$existing[0].id
    }
}

if (-not $tenantID) {
    $agent = [ordered]@{
        name = 'support'
        type = 'llm'
        systemPrompt = 'You are the enterprise WeCom sandbox assistant. Be concise and never expose secrets.'
        defaultModel = $modelName
        maxLLMCalls = 1
        tools = @()
    }
    $model = [ordered]@{
        provider = $modelProvider
        modelName = $modelName
        apiKeyRef = 'env://TRPC_SECRET_OPENAI_API_KEY'
        maxTokens = 512
        temperature = 0.2
    }
    $binding = [ordered]@{
        accountId = "wecom-$($values['WECOM_AGENT_ID'])"
        webhookKey = [string]$values['TRPC_WEBHOOK_ROUTE_KEY']
        agentApp = 'support'
        type = 'wework'
        webhookUrl = [string]$values['WECOM_CALLBACK_BASE_URL']
        tokenRef = 'env://TRPC_SECRET_WECOM_TOKEN'
        secretRef = 'env://TRPC_SECRET_WECOM_CORP_SECRET'
        encodingAESKeyRef = 'env://TRPC_SECRET_WECOM_AES'
        appId = [string]$values['WECOM_AGENT_ID']
        config = [ordered]@{ corp_id = [string]$values['WECOM_CORP_ID'] }
        accessPolicy = [ordered]@{
            allowDirectMessages = $true
            allowGroupMessages = $false
            allowedUsers = @([string]$values['WECOM_ALLOWED_USER_ID'])
        }
    }
    $created = Invoke-AdminJSON -Method POST -Path '/api/v1/tenants' -Body ([ordered]@{
        name = $tenantName
        config = [ordered]@{
            agents = @($agent)
            models = @($model)
            toolPolicy = [ordered]@{ mode = 'whitelist'; allowed = @() }
            channels = @($binding)
            storage = [ordered]@{
                sessionBackend = 'postgres'; sessionProfile = 'local-postgres'
                memoryBackend = 'postgres'; memoryProfile = 'local-postgres'
            }
            governance = [ordered]@{ auditLevel = 'detailed' }
            budget = [ordered]@{
                maxTokensPerDay = 1280000
                maxTokensPerRequest = 128000
                maxConcurrentSessions = 4
            }
        }
    })
    $tenantID = [string]$created.id
    if (-not $tenantID) {
        throw 'Admin created a tenant without an ID'
    }
    $values['WECOM_TENANT_ID'] = $tenantID
    Save-DotEnv $values $fullEnvFile
    Write-Host 'Created the isolated WeCom tenant.' -ForegroundColor Green
}

# Creating a tenant assigns its scope ID. Install references for that exact
# scope before version admission or any Gateway/Delivery credential read.
# Compose --env-file provides interpolation only; it does not inject dynamic
# TRPC_SECRET_BINDING_* names into containers without this explicit override.
$bindingDefinition = New-IMSecretBindingsOverride -TenantId $tenantID `
    -ModelProvider $modelProvider -ModelName $modelName `
    -ModelSecretRef 'env://TRPC_SECRET_OPENAI_API_KEY' `
    -ChannelType 'wework' -ChannelAccountId "wecom-$($values['WECOM_AGENT_ID'])" `
    -ChannelTokenRef 'env://TRPC_SECRET_WECOM_TOKEN' `
    -ChannelSecretRef 'env://TRPC_SECRET_WECOM_CORP_SECRET' `
    -ChannelEncodingAESKeyRef 'env://TRPC_SECRET_WECOM_AES'
$bindingOverride = Write-IMSecretBindingsOverride -Path $bindingOverride -Definition $bindingDefinition
& docker compose --project-name $ProjectName --env-file $fullEnvFile -f $composeFile -f $bindingOverride config --quiet
if ($LASTEXITCODE -ne 0) { throw 'Scoped credential binding Compose configuration is invalid.' }
$bindingArgs = @('compose', '--project-name', $ProjectName, '--env-file', $fullEnvFile,
    '-f', $composeFile, '-f', $bindingOverride, 'up', '-d', '--no-deps',
    'admin', 'worker', 'summary-worker', 'gateway', 'delivery')
& docker @bindingArgs
if ($LASTEXITCODE -ne 0) { throw 'Could not load tenant credential authorization into the sandbox services.' }
Wait-Healthy ([uri]"http://127.0.0.1:$adminPort/health") 'Admin with scoped credential bindings'
Wait-Healthy ([uri]"http://127.0.0.1:$gatewayPort/health") 'Gateway with scoped credential bindings'
$values['WECOM_TENANT_ID'] = $tenantID
Save-DotEnv $values $fullEnvFile

$appID = if ($values.Contains('WECOM_AGENT_APP_ID')) { [string]$values['WECOM_AGENT_APP_ID'] } else { '' }
if (-not $appID) {
    $app = Invoke-AdminJSON -Method POST -Path '/api/v1/agent-apps' -Body ([ordered]@{
        tenantId = $tenantID; name = 'support'; description = 'WeCom sandbox application'
    })
    $appID = [string]$app.id
    if (-not $appID) {
        throw 'Admin created an Agent app without an ID'
    }
    $values['WECOM_AGENT_APP_ID'] = $appID
    Save-DotEnv $values $fullEnvFile
}

$versionID = if ($values.Contains('WECOM_AGENT_VERSION_ID')) { [string]$values['WECOM_AGENT_VERSION_ID'] } else { '' }
if (-not $versionID) {
    $versionSnapshot = [ordered]@{
        agent = [ordered]@{
            name = 'support'; type = 'llm'
            systemPrompt = 'You are the enterprise WeCom sandbox assistant. Be concise and never expose secrets.'
            defaultModel = $modelName; maxLLMCalls = 1; tools = @()
        }
        model = [ordered]@{
            provider = $modelProvider; modelName = $modelName
            apiKeyRef = 'env://TRPC_SECRET_OPENAI_API_KEY'
            maxTokens = 512; temperature = 0.2
        }
    }
    $version = Invoke-AdminJSON -Method POST -Path '/api/v1/agent-versions' -Body ([ordered]@{
        tenantId = $tenantID
        appName = 'support'
        snapshot = $versionSnapshot
    })
    $versionID = [string]$version.id
    if (-not $versionID) {
        throw 'Admin created an Agent version without an ID'
    }
    $values['WECOM_AGENT_VERSION_ID'] = $versionID
    Save-DotEnv $values $fullEnvFile
}

# A failed publish leaves a durable draft. Keep its ID before publishing and
# inspect its current state on every run so the same draft can be resumed.
$versionState = $null
$versionCursor = ''
do {
    $path = '/api/v1/operations/versions?tenantId=' + [uri]::EscapeDataString($tenantID) + '&limit=100'
    if ($versionCursor) { $path += '&cursor=' + [uri]::EscapeDataString($versionCursor) }
    $page = Invoke-AdminJSON -Method GET -Path $path
    $matchingVersions = @($page.items | Where-Object { $_.id -eq $versionID })
    if ($matchingVersions.Count -eq 1) { $versionState = $matchingVersions[0]; break }
    $versionCursor = if ($page.PSObject.Properties['nextCursor']) { [string]$page.nextCursor } else { '' }
} while ($versionCursor)
if ($null -eq $versionState -or [string]$versionState.appId -ne $appID) { throw 'Saved sandbox version does not belong to the expected tenant application.' }
if ($versionState.status -eq 'draft') {
    Invoke-AdminJSON -Method POST -Path "/api/v1/agent-versions/$versionID/publish" -Body @{ tenantId = $tenantID } | Out-Null
} elseif ($versionState.status -ne 'published') {
    throw 'Saved sandbox version is not a draft or published version.'
}

if ((-not $values.Contains('WECOM_DEPLOYMENT_READY')) -or [string]$values['WECOM_DEPLOYMENT_READY'] -ne 'true') {
    Invoke-AdminJSON -Method POST -Path '/api/v1/deployments' -Body ([ordered]@{
        tenantId = $tenantID; appName = 'support'; stableVersionId = $versionID
    }) | Out-Null
    $values['WECOM_DEPLOYMENT_READY'] = 'true'
    Save-DotEnv $values $fullEnvFile
}

& (Join-Path $PSScriptRoot 'external_acceptance_preflight.ps1') `
    -Channel WeCom -CallbackBaseUrl ([uri]$values['WECOM_CALLBACK_BASE_URL']) -ProbeEndpoint
if ($LASTEXITCODE -ne 0) {
    throw 'public callback preflight failed after sandbox startup'
}

Write-Output 'WECOM_SANDBOX_BOOTSTRAP_PASS provider_calls=0'
Write-Output "Gateway=http://127.0.0.1:$gatewayPort Admin=http://127.0.0.1:$adminPort"
Write-Output 'The callback URL route key and all provider credentials remain only in the local ignored env file.'
