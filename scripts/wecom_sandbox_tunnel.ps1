[CmdletBinding()]
param(
    [ValidateRange(1024, 65535)]
    [int]$LocalGatewayPort = 18080,
    [string]$EnvFile = (Join-Path $PSScriptRoot '..\deploy\.env.wecom.local'),
    [switch]$Restart
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$containerName = 'trpc-platform-wecom-tunnel'
$image = 'cloudflare/cloudflared@sha256:e39ee8da81ad5e05d77f38d2f51c60ca51bf2a8450ac3abab50c17fdb91d91bf'

function Protect-LocalEnvFile {
    param([string]$Path)
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent().Name
    & icacls.exe $Path /inheritance:r /grant:r "${identity}:(R,W)" | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw 'could not restrict a local credential file ACL'
    }
}

& docker version --format '{{.Server.Version}}' | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw 'Docker Engine is unavailable'
}

$existingID = [string]::Join('', @(& docker ps -a --filter "name=^/$containerName`$" --format '{{.ID}}')).Trim()
if ($Restart -and $existingID) {
    & docker rm -f $containerName | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "could not replace $containerName"
    }
    $existingID = ''
}

if (-not $existingID) {
    & docker run -d --name $containerName `
        --restart unless-stopped `
        --label 'trpc-agent-purpose=wecom-sandbox-tunnel' `
        $image tunnel --no-autoupdate --url "http://host.docker.internal:$LocalGatewayPort" | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw 'could not start the pinned Cloudflare Quick Tunnel container'
    }
} else {
    $running = [string]::Join('', @(& docker inspect $containerName --format '{{.State.Running}}')).Trim()
    if ($running -ne 'true') {
        & docker start $containerName | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "could not start existing $containerName"
        }
    }
}

$baseURL = ''
$startedAt = [string]::Join('', @(& docker inspect $containerName --format '{{.State.StartedAt}}')).Trim()
if ($LASTEXITCODE -ne 0 -or $startedAt -notmatch '^\d{4}-\d{2}-\d{2}T') {
    throw 'could not identify the current tunnel start time'
}
$deadline = [DateTimeOffset]::UtcNow.AddSeconds(45)
while ([DateTimeOffset]::UtcNow -lt $deadline) {
    $logs = (& docker logs --since $startedAt $containerName 2>&1 | Out-String)
    $matches = [regex]::Matches($logs, 'https://[a-z0-9-]+\.trycloudflare\.com')
    if ($matches.Count -gt 0) {
        # Restrict to this start so a not-yet-ready restart cannot accidentally
        # return even the final URL from a previous process lifetime.
        $baseURL = $matches[$matches.Count - 1].Value.TrimEnd('/') + '/webhook'
        break
    }
    Start-Sleep -Milliseconds 750
}
if (-not $baseURL) {
    throw 'the tunnel did not publish a URL within 45 seconds; inspect the container logs'
}

$fullPath = [IO.Path]::GetFullPath($EnvFile)
$parent = Split-Path -Parent $fullPath
[IO.Directory]::CreateDirectory($parent) | Out-Null
$lines = if (Test-Path -LiteralPath $fullPath) {
    [IO.File]::ReadAllLines($fullPath, [Text.Encoding]::UTF8) | Where-Object { $_ -notmatch '^WECOM_CALLBACK_BASE_URL=' }
} else {
    @()
}
$lines += "WECOM_CALLBACK_BASE_URL=$baseURL"
$temporary = Join-Path $parent ('.wecom-url-' + [Guid]::NewGuid().ToString('N') + '.tmp')
try {
    [IO.File]::WriteAllLines($temporary, $lines, [Text.UTF8Encoding]::new($false))
    Protect-LocalEnvFile $temporary
    Move-Item -LiteralPath $temporary -Destination $fullPath -Force
    Protect-LocalEnvFile $fullPath
} finally {
    if (Test-Path -LiteralPath $temporary) {
        Remove-Item -LiteralPath $temporary -Force
    }
}

Write-Output "WECOM_CALLBACK_BASE_URL=$baseURL"
Write-Output "TUNNEL_CONTAINER=$containerName"
Write-Output 'No route key or provider credential was sent to the tunnel service.'
