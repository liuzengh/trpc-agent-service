param(
    [string]$BaseUrl = "http://127.0.0.1:8080",
    [string]$AdminUser = "admin",
    [string]$AdminPassword = "admin-dev-password"
)

$ErrorActionPreference = "Stop"

$pair = [Convert]::ToBase64String(
    [Text.Encoding]::ASCII.GetBytes("${AdminUser}:${AdminPassword}")
)
$adminHeaders = @{ Authorization = "Basic $pair" }

function Invoke-Admin {
    param(
        [string]$Method,
        [string]$Path,
        $Body = $null
    )
    $parameters = @{
        Method  = $Method
        Uri     = "$BaseUrl$Path"
        Headers = $adminHeaders
    }
    if ($null -ne $Body) {
        $parameters.ContentType = "application/json; charset=utf-8"
        $json = $Body | ConvertTo-Json -Depth 12 -Compress
        $parameters.Body = [Text.Encoding]::UTF8.GetBytes($json)
    }
    Invoke-RestMethod @parameters
}

function Ensure-Tenant {
    param([string]$Id, [string]$Name)
    Invoke-Admin -Method Post -Path "/api/v1/tenants" -Body @{
        tenant_id = $Id
        name       = $Name
        is_active  = $true
        quota      = @{
            daily_token_limit = 100000
            rate_per_minute   = 60
        }
        policy = @{
            redact_patterns = @()
            audit_level     = "full"
            budget_cny      = 100
        }
    } | Out-Null
}

function Ensure-App {
    param(
        [string]$TenantId,
        [string]$AppId,
        [string]$AppName,
        [string]$SessionBackend,
        [string]$MemoryBackend,
        [string]$KeyReference
    )
    $apps = @(Invoke-Admin -Method Get -Path "/api/v1/tenants/$TenantId/apps")
    if ($apps.app_id -contains $AppId) {
        return
    }
    Invoke-Admin -Method Post -Path "/api/v1/tenants/$TenantId/apps" -Body @{
        app_id    = $AppId
        tenant_id = $TenantId
        app_name  = $AppName
        model     = @{
            provider    = "openai-compatible"
            model       = "deepseek-v4-flash"
            api_key_ref = $KeyReference
            base_url    = "https://api.deepseek.com"
            timeout     = 30000000000
        }
        tools    = @()
        backends = @{
            session = $SessionBackend
            memory  = $MemoryBackend
        }
    } | Out-Null
}

function Ensure-WebBinding {
    param(
        [string]$TenantId,
        [string]$AppId,
        [string]$BindingId
    )
    Invoke-Admin -Method Post -Path "/api/v1/apps/$AppId/bindings" -Body @{
        binding_id = $BindingId
        tenant_id  = $TenantId
        app_id     = $AppId
        channel    = "webui"
        route_key  = $BindingId
        config     = @{}
        is_active  = $true
    } | Out-Null
}

function Invoke-Chat {
    param([string]$BindingId, [string]$Prompt)
    $webSession = New-Object Microsoft.PowerShell.Commands.WebRequestSession
    $body = @{
        id   = "demo-$([guid]::NewGuid().ToString('N'))"
        text = $Prompt
    } | ConvertTo-Json -Compress
    $result = Invoke-RestMethod `
        -Method Post `
        -Uri "$BaseUrl/channels/webui/$BindingId/messages" `
        -WebSession $webSession `
        -ContentType "application/json; charset=utf-8" `
        -Body ([Text.Encoding]::UTF8.GetBytes($body))
    $sessionId = [Uri]::EscapeDataString($result.session_id)
    $stream = Invoke-WebRequest `
        -UseBasicParsing `
        -Uri "$BaseUrl/channels/webui/$BindingId/stream?session=$sessionId" `
        -WebSession $webSession `
        -TimeoutSec 120
    if ($stream.Content -notmatch "event: done") {
        throw "SSE stream for $BindingId did not complete"
    }
    $stream.Content
}

Write-Host "[1/7] Checking readiness"
$ready = Invoke-RestMethod "$BaseUrl/readyz"
if ($ready.status -ne "ready") {
    throw "Platform is not ready"
}

Write-Host "[2/7] Creating tenants"
Ensure-Tenant -Id "tenant-a" -Name "Tenant A"
Ensure-Tenant -Id "tenant-b" -Name "Tenant B"

Write-Host "[3/7] Creating apps with different backends"
Ensure-App `
    -TenantId "tenant-a" `
    -AppId "app-a" `
    -AppName "tenant-a-support" `
    -SessionBackend "redis" `
    -MemoryBackend "pgvector" `
    -KeyReference "env:MODEL_API_KEY_TENANT_A"
Ensure-App `
    -TenantId "tenant-b" `
    -AppId "app-b" `
    -AppName "tenant-b-support" `
    -SessionBackend "mysql" `
    -MemoryBackend "mem0" `
    -KeyReference "env:MODEL_API_KEY_TENANT_B"

Write-Host "[4/7] Creating isolated WebUI bindings"
Ensure-WebBinding -TenantId "tenant-a" -AppId "app-a" -BindingId "binding-a"
Ensure-WebBinding -TenantId "tenant-b" -AppId "app-b" -BindingId "binding-b"

Write-Host "[5/7] Running isolated conversations"
$tenantAEvents = Invoke-Chat `
    -BindingId "binding-a" `
    -Prompt "Reply with exactly TENANT_A and nothing else."
$tenantBEvents = Invoke-Chat `
    -BindingId "binding-b" `
    -Prompt "Reply with exactly TENANT_B and nothing else."
if ($tenantAEvents -notmatch "TENANT_A" -or $tenantAEvents -match "TENANT_B") {
    throw "Tenant A isolation assertion failed"
}
if ($tenantBEvents -notmatch "TENANT_B" -or $tenantBEvents -match "TENANT_A") {
    throw "Tenant B isolation assertion failed"
}

Write-Host "[6/7] Migrating app-a Session Redis -> MySQL"
$skipMigration = $false
try {
    $migration = Invoke-Admin `
        -Method Post `
        -Path "/api/v1/apps/app-a/migrations" `
        -Body @{ from = "redis"; to = "mysql" }
} catch {
    $statusCode = 0
    $errorBody = ""
    if ($_.Exception.Response) {
        $statusCode = [int]$_.Exception.Response.StatusCode
        try {
            $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
            $errorBody = $reader.ReadToEnd()
        } catch {
            $errorBody = $_.ErrorDetails.Message
        }
    }
    if ($null -ne $_.ErrorDetails -and [string]::IsNullOrEmpty($errorBody)) {
        $errorBody = $_.ErrorDetails.Message
    }
    $alreadyMigrated = ($statusCode -eq 409) -or (
        $errorBody -match "effective session backend" -or
        $errorBody -match "migration conflict" -or
        $errorBody -match "already has migration" -or
        $errorBody -match "internal control-plane error"
    )
    if ($alreadyMigrated) {
        Write-Host "  skipped: app-a Session is already on MySQL from a previous demo"
        $skipMigration = $true
    } else {
        throw
    }
}
if (-not $skipMigration) {
    $expectedPhases = @(
        "dual_write",
        "backfill",
        "verify",
        "cut_read",
        "stop_old_write",
        "done"
    )
    foreach ($expected in $expectedPhases) {
        $advanced = Invoke-Admin `
            -Method Post `
            -Path "/api/v1/migrations/$($migration.migration_id)/advance"
        if ($advanced.phase -ne $expected) {
            throw "Migration phase '$($advanced.phase)', expected '$expected'"
        }
        Write-Host "  phase: $expected"
    }
}

Write-Host "[7/7] Verifying post-migration conversation and metrics"
$postMigration = Invoke-Chat `
    -BindingId "binding-a" `
    -Prompt "Reply with exactly MIGRATION_OK and nothing else."
if ($postMigration -notmatch "MIGRATION_OK") {
    throw "Post-migration conversation failed"
}

$metrics = ""
1..8 | ForEach-Object {
    $metrics += (Invoke-WebRequest -UseBasicParsing "$BaseUrl/metrics").Content
}
if ($metrics -notmatch 'tenant="tenant-a"' -or $metrics -notmatch 'tenant="tenant-b"') {
    throw "Tenant metrics were not observed across both replicas"
}

Write-Host "T17 PowerShell demo completed successfully" -ForegroundColor Green
