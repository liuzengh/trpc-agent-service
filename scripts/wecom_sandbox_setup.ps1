[CmdletBinding()]
param(
    [string]$EnvFile = (Join-Path $PSScriptRoot '..\deploy\.env.wecom.local'),
    [switch]$ProbeEndpoint
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Fail-Setup {
    param([string]$Message)
    throw "WeCom setup failed: $Message"
}

function Read-DotEnv {
    param([string]$Path)
    $values = [ordered]@{}
    if (-not (Test-Path -LiteralPath $Path)) {
        return $values
    }
    foreach ($line in [IO.File]::ReadAllLines($Path, [Text.Encoding]::UTF8)) {
        if ([string]::IsNullOrWhiteSpace($line) -or $line.TrimStart().StartsWith('#')) {
            continue
        }
        $separator = $line.IndexOf('=')
        if ($separator -le 0) {
            Fail-Setup "the local env file contains a malformed line"
        }
        $key = $line.Substring(0, $separator)
        if ($values.Contains($key)) {
            Fail-Setup "the local env file repeats key $key"
        }
        $values[$key] = $line.Substring($separator + 1)
    }
    return $values
}

function Read-VisibleValue {
    param([string]$Prompt, [string]$Current = '')
    $suffix = if ($Current) { ' [Enter keeps current]' } else { '' }
    $value = Read-Host "$Prompt$suffix"
    if ([string]::IsNullOrEmpty($value) -and $Current) {
        return $Current
    }
    return $value
}

function Read-HiddenValue {
    param([string]$Prompt, [string]$Current = '')
    $suffix = if ($Current) { ' [Enter keeps current]' } else { '' }
    $secure = Read-Host "$Prompt$suffix" -AsSecureString
    $pointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
    try {
        $value = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($pointer)
    } finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($pointer)
    }
    if ([string]::IsNullOrEmpty($value) -and $Current) {
        return $Current
    }
    return $value
}

function New-UrlSafeSecret {
    $bytes = [byte[]]::new(48)
    [Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
    try {
        return [Convert]::ToBase64String($bytes).TrimEnd('=').Replace('+', '_').Replace('/', '-').Substring(0, 48)
    } finally {
        [Array]::Clear($bytes, 0, $bytes.Length)
    }
}

function Assert-Match {
    param([string]$Value, [string]$Pattern, [string]$Description)
    if ($Value -notmatch $Pattern) {
        Fail-Setup $Description
    }
}

function Assert-WeComAgentID {
    param([string]$Value)
    $parsed = [uint32]0
    if (-not [uint32]::TryParse($Value, [ref]$parsed) -or $parsed -eq 0) {
        Fail-Setup 'AgentId must be a positive 32-bit decimal value'
    }
}

function Assert-WeComAESKey {
    param([string]$Value)
    if ($Value -notmatch '^[A-Za-z0-9+/]{43}$') {
        Fail-Setup 'EncodingAESKey must be 43 characters of unpadded standard base64'
    }
    try {
        $decoded = [Convert]::FromBase64String($Value + '=')
    } catch {
        Fail-Setup 'EncodingAESKey is not valid base64'
    }
    try {
        if ($decoded.Length -ne 32) {
            Fail-Setup 'EncodingAESKey must decode to 32 bytes'
        }
    } finally {
        [Array]::Clear($decoded, 0, $decoded.Length)
    }
}

function Resolve-WeComUserID {
    param([string]$CorpId, [string]$CorpSecret, [string]$Mobile)
    try {
        $tokenUri = 'https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid={0}&corpsecret={1}' -f `
            [uri]::EscapeDataString($CorpId), [uri]::EscapeDataString($CorpSecret)
        $tokenResponse = Invoke-RestMethod -Method Get -Uri $tokenUri -ErrorAction Stop
        if ($null -eq $tokenResponse -or $tokenResponse.errcode -ne 0 -or [string]::IsNullOrWhiteSpace([string]$tokenResponse.access_token)) {
            Fail-Setup 'WeCom token lookup failed; check CorpID/App Secret and contact-read permission'
        }
        $body = @{ mobile = $Mobile } | ConvertTo-Json -Compress
        $lookupUri = 'https://qyapi.weixin.qq.com/cgi-bin/user/getuserid?access_token={0}' -f `
            [uri]::EscapeDataString([string]$tokenResponse.access_token)
        $lookupResponse = Invoke-RestMethod -Method Post -Uri $lookupUri -ContentType 'application/json; charset=utf-8' `
            -Body ([Text.Encoding]::UTF8.GetBytes($body)) -ErrorAction Stop
        if ($null -eq $lookupResponse -or $lookupResponse.errcode -ne 0 -or [string]::IsNullOrWhiteSpace([string]$lookupResponse.userid)) {
            Fail-Setup 'WeCom UserID lookup failed; verify the member mobile and contact-read permission'
        }
        return [string]$lookupResponse.userid
    } catch {
        if ($_.Exception.Message -like 'WeCom *lookup failed*') {
            throw
        }
        Fail-Setup 'WeCom UserID lookup could not reach the API'
    }
}

function Existing-OrRandom {
    param([System.Collections.IDictionary]$Values, [string]$Key)
    if ($Values.Contains($Key) -and -not [string]::IsNullOrWhiteSpace([string]$Values[$Key])) {
        return [string]$Values[$Key]
    }
    return New-UrlSafeSecret
}

function Protect-LocalEnvFile {
    param([string]$Path)
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent().Name
    & icacls.exe $Path /inheritance:r /grant:r "${identity}:(R,W)" | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Fail-Setup 'could not restrict a local credential file ACL'
    }
}

function Confirm-Action {
    param([string]$Prompt)
    $answer = Read-Host "$Prompt [y/N]"
    return $answer -match '^(?i:y|yes)$'
}

Write-Host ''
Write-Host 'WeCom sandbox setup (Windows)' -ForegroundColor Cyan
Write-Host 'Values stay in the ignored local env file; secrets are never echoed.'
Write-Host ''

$values = Read-DotEnv $EnvFile
Start-Process 'https://work.weixin.qq.com/wework_admin/frame'
Write-Host '1/4  Log in, open My Enterprise and the dedicated self-built test app.' -ForegroundColor Cyan
$corpID = Read-VisibleValue 'Corp ID (ww + 16 lowercase hex)' ([string]$values['WECOM_CORP_ID'])
$agentID = Read-VisibleValue 'AgentId (positive decimal)' ([string]$values['WECOM_AGENT_ID'])
$allowedUser = Read-VisibleValue 'Allowed test member UserID (leave blank to resolve by mobile)' ([string]$values['WECOM_ALLOWED_USER_ID'])
Assert-Match $corpID '^ww[0-9a-f]{16}$' 'Corp ID has an invalid shape'
Assert-WeComAgentID $agentID
if ($allowedUser) {
    Assert-Match $allowedUser '^[A-Za-z0-9_.@-]{1,128}$' 'test member UserID has an invalid shape'
}

Write-Host '2/4  Copy the app Secret and callback Token/EncodingAESKey.' -ForegroundColor Cyan
$corpSecret = Read-HiddenValue 'App Secret (copy exactly)' ([string]$values['TRPC_SECRET_WECOM_CORP_SECRET'])
$callbackToken = Read-HiddenValue 'Callback Token (1-64 URL-safe characters)' ([string]$values['TRPC_SECRET_WECOM_TOKEN'])
$aesKey = Read-HiddenValue 'EncodingAESKey (43 characters)' ([string]$values['TRPC_SECRET_WECOM_AES'])
Assert-Match $corpSecret '^[A-Za-z0-9_-]{16,128}$' 'WeCom app Secret has an invalid shape'
Assert-Match $callbackToken '^[A-Za-z0-9_-]{1,64}$' 'WeCom callback Token has an invalid shape'
Assert-WeComAESKey $aesKey

if (-not $allowedUser) {
    $memberMobile = Read-VisibleValue 'Test member mobile (used only for one-time UserID lookup)'
    Assert-Match $memberMobile '^\+?[0-9]{6,20}$' 'test member mobile has an invalid shape'
    $allowedUser = Resolve-WeComUserID -CorpId $corpID -CorpSecret $corpSecret -Mobile $memberMobile
    Write-Host 'Resolved the test member UserID through the WeCom API; the value will not be displayed.' -ForegroundColor Green
}

Write-Host '3/4  Enter the public HTTPS endpoint that forwards to Gateway :8080.' -ForegroundColor Cyan
$callbackBase = Read-VisibleValue 'Callback base URL (https://host/webhook)' ([string]$values['WECOM_CALLBACK_BASE_URL'])
Assert-Match $callbackBase '^https://[^/?#]+(:[0-9]{1,5})?/webhook$' 'callback base URL must use HTTPS and the exact /webhook path'
$routeKey = if ($values.Contains('TRPC_WEBHOOK_ROUTE_KEY')) { [string]$values['TRPC_WEBHOOK_ROUTE_KEY'] } else { '' }
if (-not $routeKey) {
    $routeKey = New-UrlSafeSecret
}
Assert-Match $routeKey '^[A-Za-z0-9_-]{32,128}$' 'route key must be 32-128 URL-safe characters'
$providerKey = Read-HiddenValue 'OpenAI-compatible provider key (optional for URL verification)' ([string]$values['TRPC_SECRET_OPENAI_API_KEY'])

$updates = [ordered]@{
    POSTGRES_PASSWORD                 = Existing-OrRandom $values 'POSTGRES_PASSWORD'
    MASTER_KEY                       = Existing-OrRandom $values 'MASTER_KEY'
    SERVICE_AUTH_SECRET              = Existing-OrRandom $values 'SERVICE_AUTH_SECRET'
    ADMIN_API_TOKEN                  = Existing-OrRandom $values 'ADMIN_API_TOKEN'
    AUDIT_IDENTITY_HMAC_KEY          = Existing-OrRandom $values 'AUDIT_IDENTITY_HMAC_KEY'
    GRAFANA_PASSWORD                 = Existing-OrRandom $values 'GRAFANA_PASSWORD'
    POSTGRES_HOST_PORT               = '15432'
    OTEL_GRPC_HOST_PORT              = '14317'
    OTEL_HTTP_HOST_PORT              = '14318'
    GATEWAY_HOST_PORT                = '18080'
    ADMIN_HOST_PORT                  = '18081'
    PROMETHEUS_HOST_PORT             = '19095'
    GRAFANA_HOST_PORT                = '13000'
    TRPC_WEBHOOK_ROUTE_KEY           = $routeKey
    TRPC_SECRET_WECOM_TOKEN          = $callbackToken
    TRPC_SECRET_WECOM_CORP_SECRET    = $corpSecret
    TRPC_SECRET_WECOM_AES            = $aesKey
    TRPC_SECRET_OPENAI_API_KEY       = $providerKey
    WECOM_CORP_ID                    = $corpID
    WECOM_AGENT_ID                   = $agentID
    WECOM_ALLOWED_USER_ID            = $allowedUser
    WECOM_CALLBACK_BASE_URL          = $callbackBase
}
foreach ($entry in $updates.GetEnumerator()) {
    $values[$entry.Key] = $entry.Value
}

$fullPath = [IO.Path]::GetFullPath($EnvFile)
$parent = Split-Path -Parent $fullPath
[IO.Directory]::CreateDirectory($parent) | Out-Null
$temporary = Join-Path $parent ('.wecom-env-' + [Guid]::NewGuid().ToString('N') + '.tmp')
try {
    $lines = foreach ($entry in $values.GetEnumerator()) {
        if ($entry.Key -notmatch '^[A-Z][A-Z0-9_]*$' -or [string]$entry.Value -match "[`r`n]") {
            Fail-Setup 'the local env file contains an unsafe key or multiline value'
        }
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
Write-Host "Saved $($updates.Count) named values to the ignored local env file." -ForegroundColor Green

Write-Host '4/4  Running the zero-provider-call callback preflight.' -ForegroundColor Cyan
foreach ($entry in $updates.GetEnumerator()) {
    [Environment]::SetEnvironmentVariable($entry.Key, [string]$entry.Value, 'Process')
}
$preflight = @{
    Channel = 'WeCom'
    CallbackBaseUrl = [uri]$callbackBase
}
if ($ProbeEndpoint) {
    $preflight.ProbeEndpoint = $true
}
& (Join-Path $PSScriptRoot 'external_acceptance_preflight.ps1') @preflight

$callbackURL = "$callbackBase`?token=$routeKey"
if (Confirm-Action 'Copy the full secret-bearing callback URL to the Windows clipboard now?') {
    Set-Clipboard -Value $callbackURL
    Write-Host 'The full callback URL is now on the clipboard; paste it only into the WeCom console.' -ForegroundColor Yellow
} else {
    Write-Host 'Callback URL was not copied. Re-run this setup when you are ready to paste it.' -ForegroundColor Yellow
}
Write-Host 'Do not click Save until Gateway is healthy. Clear the clipboard after pasting.' -ForegroundColor Yellow
