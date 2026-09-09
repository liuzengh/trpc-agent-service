param(
  [ValidateSet('Start','Status','Stop')][string]$Action='Start',
  [string]$GatewayUrl='http://127.0.0.1:19080',
  [string]$WorkerUrl='http://127.0.0.1:19081'
)

$ErrorActionPreference='Stop'
$root=(Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$composeFile=Join-Path $root 'compose\docker-compose.real-feishu.yml'
$projectName='trpc-phase7-real-feishu'

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

function Invoke-RealFeishuCompose([string[]]$CommandArgs) {
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

Set-Phase7Value 'PHASE7_FEISHU_APP_ID' @('PHASE5_FEISHU_APP_ID','FEISHU_APP_ID')
Set-Phase7Value 'PHASE7_FEISHU_APP_SECRET' @('PHASE5_FEISHU_APP_SECRET','FEISHU_APP_SECRET')
Set-Phase7Value 'PHASE7_REAL_MODEL_KEY' @('PHASE5_MODEL_KEY','DEEPSEEK_API_KEY')

switch ($Action) {
  'Start' {
    foreach ($name in @('IDENTITY_SECRET','PHASE7_FEISHU_APP_ID','PHASE7_FEISHU_APP_SECRET','PHASE7_REAL_MODEL_KEY')) {
      Assert-Phase7Value $name
    }
    Write-Host ('Starting Feishu with Process App ID: {0}' -f $env:PHASE7_FEISHU_APP_ID)
    Write-Host 'If User credentials were changed, reopen PowerShell or reload them into Process before starting.'
    Invoke-RealFeishuCompose @('up','-d','--build')
    Wait-Ready $GatewayUrl
    Wait-Ready $WorkerUrl
    '{"status":"ready","channel":"feishu","model":"deepseek-v4-flash"}'
  }
  'Status' {
    Invoke-RealFeishuCompose @('ps')
    $gatewayReady=$false
    $workerReady=$false
    try { $gatewayReady=([int](Invoke-WebRequest -UseBasicParsing -Uri ($GatewayUrl + '/readyz')).StatusCode -eq 200) } catch {}
    try { $workerReady=([int](Invoke-WebRequest -UseBasicParsing -Uri ($WorkerUrl + '/readyz')).StatusCode -eq 200) } catch {}
    @{gateway_ready=$gatewayReady;worker_ready=$workerReady} | ConvertTo-Json -Compress
  }
  'Stop' {
    Invoke-RealFeishuCompose @('down','--remove-orphans')
    '{"status":"stopped","volume_retained":true}'
  }
}
