[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$DatabaseUrl,
    [string]$Output = 'work/queuebench-results.json',
    [int]$MessagesPerTenant = 64,
    [timespan]$Work = ([timespan]::FromMilliseconds(1)),
    [timespan]$Timeout = ([timespan]::FromMinutes(5))
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$repo = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$uri = [Uri]$DatabaseUrl
if ([string]::IsNullOrWhiteSpace($uri.AbsolutePath) -or $uri.AbsolutePath.Trim('/') -notmatch '^queuebench_[A-Za-z0-9_-]+$') {
    throw 'DatabaseUrl must target an isolated queuebench_* database'
}
$outputPath = [IO.Path]::GetFullPath((Join-Path $repo $Output))
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $outputPath) | Out-Null
$env:GOTOOLCHAIN = 'go1.26.7'
$env:CAPACITY_DATABASE_URL = $DatabaseUrl
Push-Location $repo
try {
    go run -buildvcs=false ./cmd/queue-bench -output $outputPath -messages-per-tenant $MessagesPerTenant -work ("$($Work.TotalMilliseconds)ms") -timeout ("$($Timeout.TotalSeconds)s")
    if ($LASTEXITCODE -ne 0) { throw 'queue benchmark failed' }
    Get-Content -LiteralPath $outputPath -Raw
} finally {
    Pop-Location
}
