param([ValidateSet('full','light','obs','ha')][string]$Mode='full')
$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$envFile = Join-Path $root 'compose\.env'
$example = Join-Path $root 'compose\.env.example'
if (-not (Test-Path $envFile)) {
  Copy-Item -LiteralPath $example -Destination $envFile
  Write-Host "Created compose/.env from .env.example. Fill required values, then rerun."
  Get-Content $envFile | Where-Object { $_ -match '=$' -or $_ -match 'change-me' }
  exit 2
}
$required = @('IDENTITY_SECRET','PHASE7_MODEL_KEY')
if ($Mode -ne 'light') { $required += @('PHASE7_TELEGRAM_TOKEN','PHASE7_ADMIN_TOKEN') }
$missing = @()
foreach ($name in $required) {
  $line = Get-Content -LiteralPath $envFile | Where-Object { $_ -match ('^' + [regex]::Escape($name) + '=') } | Select-Object -Last 1
  $value = if ($line) { ($line -split '=',2)[1].Trim() } else { '' }
  if ([string]::IsNullOrWhiteSpace($value) -or $value -match 'change-me') { $missing += $name }
}
if ($missing.Count -gt 0) { Write-Host ('Fill required variables in compose/.env: ' + ($missing -join ', ')); exit 2 }
Push-Location $root
try {
  docker build --tag trpc-phase7:local --file Dockerfile .
  if ($LASTEXITCODE -ne 0) { throw 'docker build failed' }
  $compose = @('--project-name','trpc-phase7','--env-file','compose/.env','--profile',$Mode,'-f','compose/docker-compose.yml')
  if ($Mode -eq 'obs') { $compose += @('-f','compose/docker-compose.obs.yml') }
  if ($Mode -eq 'ha') { $compose += @('-f','compose/docker-compose.ha.yml') }
  docker compose @compose up -d
  if ($LASTEXITCODE -ne 0) { throw 'docker compose up failed' }

  $ports = if ($Mode -eq 'light') { @(18080,18083) } else { @(18080,18081,18082) }
  foreach ($port in $ports) {
    $deadline=(Get-Date).AddSeconds(90); $ready=$false
    do { try { $response=Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:$port/readyz"; $ready=([int]$response.StatusCode -eq 200) } catch {}; if (-not $ready) { Start-Sleep -Seconds 1 } } while (-not $ready -and (Get-Date) -lt $deadline)
    if (-not $ready) { throw "service on port $port did not become ready" }
  }
} finally { Pop-Location }
