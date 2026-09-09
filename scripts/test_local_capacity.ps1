[CmdletBinding()]
param([Parameter(Mandatory)][string]$HarnessPath)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$harness = (Resolve-Path -LiteralPath $HarnessPath).Path
$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$name = "trpc-platform-capacity-$([DateTime]::UtcNow.ToString('yyyyMMddHHmmss'))-$PID"
if ($name -notmatch '^trpc-platform-capacity-[0-9]{14}-[0-9]+$') { throw 'invalid fixture container name' }
$keys = @('GOTOOLCHAIN','GOMAXPROCS','DATABASE_URL','DATABASE_ALLOW_INSECURE','CAPACITY_DATABASE_URL')
$saved = @{}
foreach ($key in $keys) { $saved[$key] = [Environment]::GetEnvironmentVariable($key,'Process') }
$started = $false
try {
    docker run --rm -d --name $name -e POSTGRES_DB=trpc_capacity -e POSTGRES_USER=agent -e POSTGRES_PASSWORD=validation-capacity-only -p 127.0.0.1:0:5432 postgres:15.8-alpine | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'could not create isolated capacity database' }
    $started = $true
    $portText = [string]::Join('', @(docker port $name 5432/tcp)).Trim()
    if ($portText -notmatch '^127\.0\.0\.1:([0-9]+)$') { throw 'capacity database is not bound exclusively to loopback' }
    $port = $Matches[1]
    for ($attempt=0; $attempt -lt 30; $attempt++) {
        docker exec $name pg_isready -U agent -d trpc_capacity *> $null
        if ($LASTEXITCODE -eq 0) { break }
        Start-Sleep -Seconds 1
    }
    if ($LASTEXITCODE -ne 0) { throw 'capacity database did not become ready' }
    $env:GOTOOLCHAIN='go1.26.7'
    $env:GOMAXPROCS='1'
    $env:DATABASE_ALLOW_INSECURE='true'
    $env:DATABASE_URL="postgres://agent:validation-capacity-only@127.0.0.1:$port/trpc_capacity?sslmode=disable"
    $env:CAPACITY_DATABASE_URL=$env:DATABASE_URL
    Push-Location $repoRoot
    try {
        go run -buildvcs=false ./cmd/migrate
        if ($LASTEXITCODE -ne 0) { throw 'capacity schema migration failed' }
        go run -buildvcs=false $harness
        if ($LASTEXITCODE -ne 0) { throw 'capacity harness failed' }
    } finally { Pop-Location }
    Write-Output 'Scope: real isolated PostgreSQL queue operations, not model throughput, end-to-end IM, HA or production sizing.'
} finally {
    foreach ($key in $keys) { [Environment]::SetEnvironmentVariable($key,$saved[$key],'Process') }
    if ($started) {
        # The exact container was created by this invocation with --rm and no
        # named volume; only its disposable test database is removed.
        docker rm -f $name | Out-Null
        if ($LASTEXITCODE -ne 0) { Write-Warning "fixture cleanup needs attention: $name" }
    }
}
