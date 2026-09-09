# Dot-source this helper. It only builds operator authorization references;
# credential values remain in the existing per-process secret configuration.
function Get-IMSecretBindingName {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$TenantId,
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$Purpose,
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$Provider,
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$Model
    )
    $utf8 = [Text.UTF8Encoding]::new($false, $true)
    $parts = foreach ($value in @($TenantId, $Purpose, $Provider, $Model)) {
        if ($value -match '[\x00\r\n]') { throw 'Secret binding scope contains invalid control characters.' }
        $length = $utf8.GetByteCount($value).ToString([Globalization.CultureInfo]::InvariantCulture)
        $length + ':' + $value
    }
    $sha = [Security.Cryptography.SHA256]::Create()
    try { $digest = $sha.ComputeHash($utf8.GetBytes(($parts -join '|'))) }
    finally { $sha.Dispose() }
    return 'TRPC_SECRET_BINDING_' + [BitConverter]::ToString($digest).Replace('-', '')
}

function New-IMSecretBindingsOverride {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$TenantId,
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$ModelProvider,
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$ModelName,
        [Parameter(Mandatory)][ValidatePattern('^env://TRPC_SECRET_[A-Za-z0-9_]+\z')][string]$ModelSecretRef,
        [Parameter(Mandatory)][ValidateSet('wework', 'telegram', 'wecom_bot')][string]$ChannelType,
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$ChannelAccountId,
        [Parameter(Mandatory)][ValidatePattern('^env://TRPC_SECRET_[A-Za-z0-9_]+\z')][string]$ChannelTokenRef,
        [ValidatePattern('^(env://TRPC_SECRET_[A-Za-z0-9_]+)?\z')][string]$ChannelSecretRef = '',
        [ValidatePattern('^(env://TRPC_SECRET_[A-Za-z0-9_]+)?\z')][string]$ChannelEncodingAESKeyRef = ''
    )
    if (($ChannelType -eq 'wework') -ne [bool]$ChannelEncodingAESKeyRef -or
        ($ChannelType -eq 'wecom_bot') -eq [bool]$ChannelSecretRef) {
        throw 'WeCom application requires secret and AES references; Telegram requires a secret reference; smart bot accepts its bridge token only.'
    }
    foreach ($reference in @($ModelSecretRef, $ChannelTokenRef)) {
        if ($reference -cnotmatch '^env://TRPC_SECRET_[A-Za-z0-9_]+\z') { throw 'Secret references must use the exact env://TRPC_SECRET_ namespace.' }
    }
    if ($ChannelSecretRef -and $ChannelSecretRef -cnotmatch '^env://TRPC_SECRET_[A-Za-z0-9_]+\z') { throw 'Channel secret reference must use the exact env://TRPC_SECRET_ namespace.' }
    if ($ChannelEncodingAESKeyRef -and $ChannelEncodingAESKeyRef -cnotmatch '^env://TRPC_SECRET_[A-Za-z0-9_]+\z') { throw 'AES key reference must use the exact env://TRPC_SECRET_ namespace.' }
    $modelBinding = Get-IMSecretBindingName -TenantId $TenantId -Purpose 'model' -Provider $ModelProvider -Model $ModelName
    $channelBindings = [ordered]@{}
    $references = [ordered]@{ channel_token=$ChannelTokenRef }
    if ($ChannelSecretRef) { $references['channel_secret'] = $ChannelSecretRef }
    if ($ChannelEncodingAESKeyRef) { $references['channel_encoding_aes_key'] = $ChannelEncodingAESKeyRef }
    foreach ($entry in $references.GetEnumerator()) {
        $name = Get-IMSecretBindingName -TenantId $TenantId -Purpose $entry.Key -Provider $ChannelType -Model $ChannelAccountId
        $channelBindings[$name] = $entry.Value
    }
    $services = [ordered]@{}
    foreach ($service in @('admin', 'worker', 'summary-worker')) {
        $services[$service] = [ordered]@{ environment=[ordered]@{ $modelBinding=$ModelSecretRef } }
    }
    foreach ($service in @('gateway', 'delivery')) {
        $environment = [ordered]@{}
        foreach ($entry in $channelBindings.GetEnumerator()) { $environment[$entry.Key] = $entry.Value }
        $services[$service] = [ordered]@{ environment=$environment }
    }
    return [ordered]@{ services=$services }
}

function Write-IMSecretBindingsOverride {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$Path,
        [Parameter(Mandatory)][System.Collections.IDictionary]$Definition
    )
    # Rebuild the bounded shape instead of serializing arbitrary caller fields.
    if ($Definition.Count -ne 1 -or -not $Definition.Contains('services') -or
        $Definition.services -isnot [System.Collections.IDictionary] -or $Definition.services.Count -ne 5) {
        throw 'Secret binding override must contain exactly the five allowed services.'
    }
    $services = [ordered]@{}
    foreach ($service in @('admin', 'worker', 'summary-worker', 'gateway', 'delivery')) {
        $entry = $Definition.services[$service]
        if ($entry -isnot [System.Collections.IDictionary] -or $entry.Count -ne 1 -or
            -not $entry.Contains('environment') -or $entry.environment -isnot [System.Collections.IDictionary]) {
            throw 'Secret binding override has an invalid service definition.'
        }
        $environment = [ordered]@{}
        foreach ($binding in $entry.environment.GetEnumerator()) {
            if ([string]$binding.Key -cnotmatch '^TRPC_SECRET_BINDING_[A-F0-9]{64}\z' -or
                [string]$binding.Value -cnotmatch '^env://TRPC_SECRET_[A-Za-z0-9_]+\z') {
                throw 'Secret binding override accepts authorization references only.'
            }
            $environment[[string]$binding.Key] = [string]$binding.Value
        }
        $services[$service] = [ordered]@{ environment=$environment }
    }
    $fullPath = [IO.Path]::GetFullPath($Path)
    $parent = [IO.Path]::GetDirectoryName($fullPath)
    [IO.Directory]::CreateDirectory($parent) | Out-Null
    $temporary = Join-Path $parent ('.im-bindings-' + [Guid]::NewGuid().ToString('N') + '.tmp')
    try {
        $json = [ordered]@{ services=$services } | ConvertTo-Json -Depth 8
        [IO.File]::WriteAllText($temporary, $json + "`n", [Text.UTF8Encoding]::new($false))
        Move-Item -LiteralPath $temporary -Destination $fullPath -Force
    } finally {
        if (Test-Path -LiteralPath $temporary) { Remove-Item -LiteralPath $temporary -Force }
    }
    return $fullPath
}
