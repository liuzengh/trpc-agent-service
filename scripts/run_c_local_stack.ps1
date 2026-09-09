[CmdletBinding()]
param(
    [ValidatePattern('^[a-z0-9][a-z0-9_-]{0,62}$')]
    [string]$ProjectName = "trpc-platform-c-local-$([System.Diagnostics.Process]::GetCurrentProcess().Id)",
    [ValidateRange(1, 65535)]
    [int]$GatewayPort = 18080,
    [ValidateRange(1, 65535)]
    [int]$AdminPort = 18081,
    [ValidateRange(1, 65535)]
    [int]$PrometheusPort = 19095,
    [ValidateRange(1, 65535)]
    [int]$GrafanaPort = 13000,
    [ValidateRange(1, 65535)]
    [int]$PostgresPort = 65432,
    [ValidateRange(1, 65535)]
    [int]$OtelGrpcPort = 64317,
    [ValidateRange(1, 65535)]
    [int]$OtelHttpPort = 64318,
    [ValidateRange(1, 1024)]
    [int]$MinFreeSpaceGB = 8,
    [switch]$Build,
    [switch]$Down
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$composeFile = Join-Path $repoRoot 'deploy\docker-compose.yml'
$overlayFile = Join-Path $repoRoot 'deploy\docker-compose.isolated.yml'
if (-not (Test-Path -LiteralPath (Join-Path $repoRoot 'go.mod'))) {
    throw "application source root not found: $repoRoot"
}
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw 'Docker CLI is required'
}

$composeBase = @('--project-name', $ProjectName, '-f', $composeFile, '-f', $overlayFile)
# These values are intentionally disposable and are never written to disk.
# Use a real operator-managed .env file for any non-validation deployment.
$validationEnv = [ordered]@{
    POSTGRES_PASSWORD = 'validation-only-password-not-for-deployment'
    MASTER_KEY = 'validation-only-master-key-32-bytes-minimum'
    SERVICE_AUTH_SECRET = 'validation-only-service-secret-32-bytes'
    ADMIN_API_TOKEN = 'validation-only-admin-token-32-bytes'
    GRAFANA_PASSWORD = 'validation-only-grafana-password'
    DATA_PLANE_PROFILES = '[]'
    MCP_PROFILES = '[]'
    PLATFORM_POSTGRES_PORT = [string]$PostgresPort
    PLATFORM_OTLP_GRPC_PORT = [string]$OtelGrpcPort
    PLATFORM_OTLP_HTTP_PORT = [string]$OtelHttpPort
    PLATFORM_GATEWAY_PORT = [string]$GatewayPort
    PLATFORM_ADMIN_PORT = [string]$AdminPort
    PLATFORM_PROMETHEUS_PORT = [string]$PrometheusPort
    PLATFORM_GRAFANA_PORT = [string]$GrafanaPort
}

$oldEnv = @{}
foreach ($entry in $validationEnv.GetEnumerator()) {
    $oldEnv[$entry.Key] = [Environment]::GetEnvironmentVariable($entry.Key, 'Process')
    [Environment]::SetEnvironmentVariable($entry.Key, [string]$entry.Value, 'Process')
}

try {
    if ($Down) {
        & docker compose @composeBase down
        if ($LASTEXITCODE -ne 0) { throw 'Docker Compose down failed' }
        exit 0
    }

    & docker compose @composeBase config --quiet
    if ($LASTEXITCODE -ne 0) { throw 'Docker Compose configuration is invalid' }

    if ($Build) {
        $repoDriveName = ([IO.Path]::GetPathRoot($repoRoot)).TrimEnd('\\').TrimEnd(':')
        $repoDrive = Get-PSDrive -Name $repoDriveName -ErrorAction Stop
        $freeSpaceGB = [math]::Round($repoDrive.Free / 1GB, 2)
        if ($freeSpaceGB -lt $MinFreeSpaceGB) {
            throw "Insufficient free space on $repoDriveName`: drive: ${freeSpaceGB}GB available, ${MinFreeSpaceGB}GB required"
        }
        Write-Output "Build preflight: drive=$repoDriveName free=${freeSpaceGB}GB minimum=${MinFreeSpaceGB}GB"

        # Compose Bake builds independent targets concurrently. Building one target at a
        # time keeps temporary BuildKit usage bounded on a C-drive-only workstation.
        $buildServices = @('migrate', 'gateway', 'worker', 'summary-worker', 'consumer', 'delivery', 'admin')
        foreach ($service in $buildServices) {
            $freeSpaceGB = [math]::Round((Get-PSDrive -Name $repoDriveName -ErrorAction Stop).Free / 1GB, 2)
            if ($freeSpaceGB -lt $MinFreeSpaceGB) {
                throw "Insufficient free space before $service build: ${freeSpaceGB}GB available, ${MinFreeSpaceGB}GB required"
            }
            Write-Output "Building service=$service"
            & docker compose @composeBase build $service
            if ($LASTEXITCODE -ne 0) { throw "Docker Compose build failed: service=$service" }
        }
    }

    $upArgs = @('compose') + $composeBase + @('up', '-d')
    if ($Build) { $upArgs += '--no-build' }
    & docker @upArgs
    if ($LASTEXITCODE -ne 0) { throw 'Docker Compose startup failed' }

    Write-Output "C-local stack started: project=$ProjectName source=$repoRoot"
    Write-Output "Gateway=http://127.0.0.1:$GatewayPort Admin=http://127.0.0.1:$AdminPort"
    Write-Output "Prometheus=http://127.0.0.1:$PrometheusPort Grafana=http://127.0.0.1:$GrafanaPort"
    & docker compose @composeBase ps --all
    if ($LASTEXITCODE -ne 0) { throw 'Docker Compose status query failed' }
}
finally {
    foreach ($entry in $oldEnv.GetEnumerator()) {
        [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, 'Process')
    }
}
