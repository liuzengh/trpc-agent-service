param(
  [ValidateSet('Start','Status','Stop')][string]$Action='Start',
  [string]$GatewayUrl='http://127.0.0.1:18080',
  [string]$WorkerUrl='http://127.0.0.1:18081'
)

$ErrorActionPreference='Stop'
$root=(Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$composeFile=Join-Path $root 'compose\docker-compose.real-wecom.yml'
$projectName='trpc-phase7-real-wecom'

function Set-Phase7Value([string]$Target,[string[]]$Sources) {
  $current=[Environment]::GetEnvironmentVariable($Target,'Process')
  if (-not [string]::IsNullOrWhiteSpace($current)) { return }
  foreach ($source in $Sources) {
    $value=[Environment]::GetEnvironmentVariable($source,'Process')
    if (-not [string]::IsNullOrWhiteSpace($value)) {
      [Environment]::SetEnvironmentVariable($Target,$value,'Process')
      return
    }
  }
}

function Assert-Phase7Value([string]$Name) {
  if ([string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($Name,'Process'))) {
    throw "Required environment variable $Name is missing"
  }
}

function Invoke-RealWeComCompose([string[]]$CommandArgs) {
  Push-Location $root
  try {
    & docker compose --project-name $projectName -f $composeFile @CommandArgs
    if ($LASTEXITCODE -ne 0) { throw "docker compose failed: $($CommandArgs -join ' ')" }
  } finally { Pop-Location }
}

function Wait-Ready([string]$Url,[int]$Seconds=90) {
  $deadline=(Get-Date).AddSeconds($Seconds)
  do {
    try {
      $response=Invoke-WebRequest -UseBasicParsing -Uri ($Url + '/readyz') -Method Get
      if ([int]$response.StatusCode -eq 200) { return }
    } catch {}
    Start-Sleep -Seconds 1
  } while ((Get-Date) -lt $deadline)
  throw "Service did not become ready: $Url"
}

Set-Phase7Value 'PHASE7_WECOM_BOT_ID' @('PHASE5_WECOM_BOT_ID')
Set-Phase7Value 'PHASE7_WECOM_BOT_SECRET' @('PHASE5_WECOM_BOT_SECRET')
Set-Phase7Value 'PHASE7_REAL_MODEL_KEY' @('PHASE5_MODEL_KEY','DEEPSEEK_API_KEY')

switch ($Action) {
  'Start' {
    foreach ($name in @('IDENTITY_SECRET','PHASE7_WECOM_BOT_ID','PHASE7_WECOM_BOT_SECRET','PHASE7_REAL_MODEL_KEY')) {
      Assert-Phase7Value $name
    }
    Invoke-RealWeComCompose @('up','-d','--build')
    Wait-Ready $GatewayUrl
    Wait-Ready $WorkerUrl
    '{"status":"ready","channel":"wecom_aibot","model":"deepseek-v4-flash"}'
  }
  'Status' {
    Invoke-RealWeComCompose @('ps')
    $gatewayReady=$false
    $workerReady=$false
    try { $gatewayReady=([int](Invoke-WebRequest -UseBasicParsing -Uri ($GatewayUrl + '/readyz')).StatusCode -eq 200) } catch {}
    try { $workerReady=([int](Invoke-WebRequest -UseBasicParsing -Uri ($WorkerUrl + '/readyz')).StatusCode -eq 200) } catch {}
    @{gateway_ready=$gatewayReady;worker_ready=$workerReady} | ConvertTo-Json -Compress
  }
  'Stop' {
    Invoke-RealWeComCompose @('down','--remove-orphans')
    '{"status":"stopped","volume_retained":true}'
  }
}
