[CmdletBinding()]
param(
    [ValidateSet('Telegram', 'WeCom', 'All')]
    [string]$Channel = 'All',

    [Parameter(Mandatory = $true)]
    [uri]$CallbackBaseUrl,

    [string]$WeComCorpId = $env:WECOM_CORP_ID,
    [string]$WeComAgentId = $env:WECOM_AGENT_ID,

    [switch]$ProbeEndpoint
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Assert-Preflight {
    param(
        [bool]$Condition,
        [string]$Message
    )
    if (-not $Condition) {
        throw "preflight failed: $Message"
    }
}

function Get-RequiredSecret {
    param([string]$Name)
    $value = [Environment]::GetEnvironmentVariable($Name, 'Process')
    Assert-Preflight (-not [string]::IsNullOrWhiteSpace($value)) "$Name is not set in the current process"
    return $value
}

function Test-PrivateAddress {
    param([System.Net.IPAddress]$Address)

    if ([System.Net.IPAddress]::IsLoopback($Address)) {
        return $true
    }
    $bytes = $Address.GetAddressBytes()
    if ($Address.AddressFamily -eq [System.Net.Sockets.AddressFamily]::InterNetwork) {
        return $bytes[0] -eq 0 -or
            $bytes[0] -eq 10 -or
            $bytes[0] -eq 127 -or
            ($bytes[0] -eq 169 -and $bytes[1] -eq 254) -or
            ($bytes[0] -eq 172 -and $bytes[1] -ge 16 -and $bytes[1] -le 31) -or
            ($bytes[0] -eq 192 -and $bytes[1] -eq 168) -or
            ($bytes[0] -eq 100 -and $bytes[1] -ge 64 -and $bytes[1] -le 127) -or
            $bytes[0] -ge 224
    }

    # IPv6 unspecified, unique-local, link-local and multicast ranges.
    return ($bytes | Where-Object { $_ -ne 0 }).Count -eq 0 -or
        (($bytes[0] -band 0xfe) -eq 0xfc) -or
        ($bytes[0] -eq 0xfe -and ($bytes[1] -band 0xc0) -eq 0x80) -or
        $bytes[0] -eq 0xff
}

Assert-Preflight $CallbackBaseUrl.IsAbsoluteUri 'callback URL must be absolute'
Assert-Preflight ($CallbackBaseUrl.Scheme -eq 'https') 'callback URL must use HTTPS'
Assert-Preflight ([string]::IsNullOrEmpty($CallbackBaseUrl.UserInfo)) 'callback URL must not contain userinfo'
Assert-Preflight ([string]::IsNullOrEmpty($CallbackBaseUrl.Query)) 'pass only the base URL; keep the route key out of shell history'
Assert-Preflight ([string]::IsNullOrEmpty($CallbackBaseUrl.Fragment)) 'callback URL must not contain a fragment'
Assert-Preflight ($CallbackBaseUrl.AbsolutePath -eq '/webhook') 'callback path must be exactly /webhook'

$literalAddress = $null
if ([System.Net.IPAddress]::TryParse($CallbackBaseUrl.DnsSafeHost, [ref]$literalAddress)) {
    Assert-Preflight (-not (Test-PrivateAddress $literalAddress)) 'callback URL must not use a private, loopback, link-local or multicast IP'
} else {
    Assert-Preflight ($CallbackBaseUrl.DnsSafeHost -notin @('localhost', 'localhost.localdomain')) 'callback URL must use a public hostname'
}

$routeKey = Get-RequiredSecret 'TRPC_WEBHOOK_ROUTE_KEY'
Assert-Preflight ($routeKey -match '^[A-Za-z0-9_-]{32,128}$') 'TRPC_WEBHOOK_ROUTE_KEY must be a 32-128 character URL-safe opaque key'

$checkedSecrets = 1
if ($Channel -in @('Telegram', 'All')) {
    $botToken = Get-RequiredSecret 'TRPC_SECRET_TELEGRAM_BOT_TOKEN'
    $webhookSecret = Get-RequiredSecret 'TRPC_SECRET_TELEGRAM_WEBHOOK'
    Assert-Preflight ($botToken -match '^[1-9][0-9]{5,19}:[A-Za-z0-9_-]{30,128}$') 'Telegram bot token has an invalid shape'
    Assert-Preflight ($webhookSecret -match '^[A-Za-z0-9_-]{32,256}$') 'Telegram webhook secret has an invalid shape'
    $checkedSecrets += 2
}

if ($Channel -in @('WeCom', 'All')) {
    $callbackToken = Get-RequiredSecret 'TRPC_SECRET_WECOM_TOKEN'
    $corpSecret = Get-RequiredSecret 'TRPC_SECRET_WECOM_CORP_SECRET'
    $encodingAesKey = Get-RequiredSecret 'TRPC_SECRET_WECOM_AES'
    Assert-Preflight ($callbackToken -match '^[A-Za-z0-9_-]{1,64}$') 'WeCom callback token has an invalid shape'
    Assert-Preflight ($corpSecret -match '^[A-Za-z0-9_-]{16,128}$') 'WeCom corp secret has an invalid shape'
    Assert-Preflight ($WeComCorpId -match '^ww[0-9a-f]{16}$') 'WECOM_CORP_ID has an invalid shape'
    $parsedAgentID = [uint32]0
    Assert-Preflight ([uint32]::TryParse($WeComAgentId, [ref]$parsedAgentID) -and $parsedAgentID -gt 0) 'WECOM_AGENT_ID must be a positive 32-bit decimal ID'
    Assert-Preflight ($encodingAesKey -match '^[A-Za-z0-9+/]{43}$') 'WeCom encoding AES key must be unpadded standard base64'
    try {
        $decodedKey = [Convert]::FromBase64String($encodingAesKey + '=')
    } catch {
        throw 'preflight failed: WeCom encoding AES key is not valid base64'
    }
    Assert-Preflight ($encodingAesKey.Length -eq 43 -and $decodedKey.Length -eq 32) 'WeCom encoding AES key must decode to 32 bytes'
    $checkedSecrets += 3
}

if ($ProbeEndpoint) {
    $resolved = [System.Net.Dns]::GetHostAddresses($CallbackBaseUrl.DnsSafeHost)
    Assert-Preflight ($resolved.Count -gt 0) 'callback hostname did not resolve'
    foreach ($address in $resolved) {
        Assert-Preflight (-not (Test-PrivateAddress $address)) 'callback hostname resolved to a non-public address'
    }

    # Probe the service-owned health path first. An edge-generated 5xx (for
    # example a tunnel with no local origin) is an HTTP response but is not
    # evidence that the Gateway is reachable.
    $healthBuilder = [UriBuilder]$CallbackBaseUrl
    $healthBuilder.Path = '/health'
    $healthBuilder.Query = ''
    $healthBuilder.Fragment = ''
    try {
        $healthResponse = Invoke-WebRequest -Uri $healthBuilder.Uri -Method Get -MaximumRedirection 0 -TimeoutSec 10
        $healthStatusCode = [int]$healthResponse.StatusCode
    } catch {
        if ($null -eq $_.Exception.Response) {
            throw "preflight failed: public Gateway health probe did not reach an HTTP response ($($_.Exception.GetType().Name))"
        }
        $healthStatusCode = [int]$_.Exception.Response.StatusCode
    }
    Assert-Preflight ($healthStatusCode -eq 200) "public Gateway health returned HTTP $healthStatusCode instead of 200"

    # No route key or provider credential is sent. This implementation's exact
    # /webhook contract rejects a missing route key with 400; requiring that
    # response proves the callback path reached this Gateway rather than only
    # the tunnel or an unrelated origin.
    try {
        $response = Invoke-WebRequest -Uri $CallbackBaseUrl -Method Get -MaximumRedirection 0 -TimeoutSec 10
        $statusCode = [int]$response.StatusCode
    } catch {
        if ($null -eq $_.Exception.Response) {
            throw "preflight failed: public callback probe did not reach an HTTP response ($($_.Exception.GetType().Name))"
        }
        $statusCode = [int]$_.Exception.Response.StatusCode
    }
    Assert-Preflight ($statusCode -eq 400) "public callback returned HTTP $statusCode instead of the expected missing-route-key 400"
    Write-Output "GATEWAY_HEALTH_HTTP_STATUS=$healthStatusCode"
    Write-Output "CALLBACK_PROBE_HTTP_STATUS=$statusCode"
}

Write-Output "EXTERNAL_PREFLIGHT_PASS channel=$Channel checked_secret_shapes=$checkedSecrets provider_calls=0"
