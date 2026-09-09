[CmdletBinding()]
param(
    [uri]$BaseUrl = 'http://127.0.0.1:18081',
    [string]$AdminToken = 'validation-only-admin-token-32-bytes'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$base = $BaseUrl.AbsoluteUri.TrimEnd('/')
$modelSecret = 'validation-only-model-key'
$telegramToken = '123456:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef'
$telegramSecret = 'validation-only-telegram-secret-32'
$runID = "c-local-$([DateTime]::UtcNow.ToString('yyyyMMddHHmmssfff'))-$([System.Diagnostics.Process]::GetCurrentProcess().Id)"
$tenantID = $null

function Invoke-AdminApi {
    param([string]$Method, [string]$Path, [object]$Body = $null, [switch]$Anonymous)
    $params = @{
        Uri = "$script:base$Path"
        Method = $Method
        ContentType = 'application/json'
        TimeoutSec = 20
        UseBasicParsing = $true
    }
    if (-not $Anonymous) {
        $params.Headers = @{ Authorization = "Bearer $script:AdminToken" }
    }
    if ($null -ne $Body) {
        $params.Body = $Body | ConvertTo-Json -Depth 20 -Compress
    }
    return Invoke-WebRequest @params
}

try {
    try {
        Invoke-AdminApi 'Get' '/api/v1/tenants' -Anonymous | Out-Null
        throw 'unauthenticated admin request unexpectedly succeeded'
    }
    catch {
        if ($null -eq $_.Exception.Response -or [int]$_.Exception.Response.StatusCode -ne 401) {
            throw
        }
        Write-Output 'unauthenticated admin request: HTTP 401 (PASS)'
    }

    $tenantBody = @{
        name = $runID
        config = @{
            agents = @(@{ name = 'support'; type = 'llm'; defaultModel = 'gpt-4o-mini'; maxLLMCalls = 1 })
            models = @(@{ provider = 'openai'; modelName = 'gpt-4o-mini'; apiKey = $modelSecret; maxTokens = 1024 })
            toolPolicy = @{ mode = 'whitelist'; allowed = @() }
            channels = @(@{
                type = 'telegram'; agentApp = 'support'; accountId = "$runID-bot"; webhookKey = "$runID-route"
                token = $telegramToken; secret = $telegramSecret
                accessPolicy = @{ allowDirectMessages = $true; allowedUsers = @('user-1') }
            })
            storage = @{ sessionBackend = 'postgres'; sessionProfile = 'local-postgres'; memoryBackend = 'postgres'; memoryProfile = 'local-postgres' }
            governance = @{ auditLevel = 'detailed' }
        }
    }
    $response = Invoke-AdminApi 'Post' '/api/v1/tenants' $tenantBody
    $json = $response.Content | ConvertFrom-Json
    $tenantID = [string]$json.id
    if ([string]::IsNullOrWhiteSpace($tenantID)) { throw 'tenant response did not include an id' }
    if ($response.Content.Contains($modelSecret) -or $response.Content.Contains($telegramToken) -or $response.Content.Contains($telegramSecret) -or -not $response.Content.Contains('***REDACTED***')) {
        throw 'tenant response did not redact credentials'
    }
    Write-Output 'tenant create and redaction: HTTP 201 (PASS)'

    $read = Invoke-AdminApi 'Get' "/api/v1/tenants/$tenantID"
    if ($read.Content.Contains($modelSecret) -or $read.Content.Contains($telegramToken) -or $read.Content.Contains($telegramSecret) -or $read.Headers['Cache-Control'] -ne 'no-store') {
        throw 'tenant GET failed redaction or cache policy'
    }
    Write-Output 'tenant read/redaction: HTTP 200 (PASS)'

    $app = Invoke-AdminApi 'Post' '/api/v1/agent-apps' @{ tenantId = $tenantID; name = 'support'; description = 'C-local vertical slice' }
    $appID = [string](($app.Content | ConvertFrom-Json).id)
    if ([string]::IsNullOrWhiteSpace($appID)) { throw 'agent app response did not include an id' }
    Write-Output 'agent app create: HTTP 201 (PASS)'

    $snapshot = @{ agent = @{ name = 'support'; type = 'llm'; defaultModel = 'gpt-4o-mini'; maxLLMCalls = 1; tools = @() }; model = @{ provider = 'openai'; modelName = 'gpt-4o-mini'; maxTokens = 1024 } }
    $version = Invoke-AdminApi 'Post' '/api/v1/agent-versions' @{ tenantId = $tenantID; appName = 'support'; snapshot = $snapshot }
    $versionID = [string](($version.Content | ConvertFrom-Json).id)
    if ([string]::IsNullOrWhiteSpace($versionID)) { throw 'version response did not include an id' }
    Write-Output 'agent version draft: HTTP 201 (PASS)'

    Invoke-AdminApi 'Post' "/api/v1/agent-versions/$versionID/publish" @{ tenantId = $tenantID } | Out-Null
    Write-Output 'agent version publish: HTTP 200 (PASS)'

    $deployment = Invoke-AdminApi 'Post' '/api/v1/deployments' @{ tenantId = $tenantID; appName = 'support'; stableVersionId = $versionID }
    if ([string]::IsNullOrWhiteSpace([string](($deployment.Content | ConvertFrom-Json).stableId))) { throw 'deployment response did not include stableId' }
    Write-Output 'stable deployment: HTTP 201 (PASS)'

    $list = (Invoke-AdminApi 'Get' '/api/v1/tenants').Content | ConvertFrom-Json
    if (-not (@($list) | Where-Object { $_.id -eq $tenantID })) { throw 'created tenant missing from list' }
    Write-Output 'tenant list visibility: PASS'
}
finally {
    if ($tenantID) {
        try {
            Invoke-AdminApi 'Delete' "/api/v1/tenants/$tenantID" | Out-Null
            Write-Output 'tenant cleanup: HTTP 204 (PASS)'
        }
        catch {
            Write-Output 'tenant cleanup failed; discard the named validation volume before reuse'
            if ($ErrorActionPreference -eq 'Stop') { throw }
        }
    }
}
