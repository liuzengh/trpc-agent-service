param([ValidateSet('full','light','obs')][string]$Mode='full',[ValidateSet('none','takeover','sql-recovery')][string]$Scenario='none')
. (Join-Path $PSScriptRoot 'assert.ps1')
$ErrorActionPreference='Stop'
Wait-P7Http 'http://127.0.0.1:18080/healthz' 200 60
Wait-P7Http 'http://127.0.0.1:18080/readyz' 200 90
$bindings = if ($Mode -eq 'light') { @('demo-redis') } else { @('demo-redis','demo-postgres','demo-mysql') }
foreach ($binding in $bindings) {
  $message=Submit-P7Message $binding "smoke $binding"
  $duplicate=Submit-P7Message $binding "smoke $binding" -MessageID $message
  if ($duplicate -ne $message) { throw "duplicate submit returned an unexpected message ID for $binding" }
  if ($Mode -eq 'light') {
    $deadline=(Get-Date).AddSeconds(90); do { $snapshot=Get-P7WebSnapshot $binding $message; if ($snapshot.status -in @('succeeded','failed')) { break }; Start-Sleep -Milliseconds 500 } while ((Get-Date) -lt $deadline)
    if ($snapshot.status -ne 'succeeded') { throw "light smoke failed for ${binding}: $($snapshot | ConvertTo-Json -Compress)" }
  } else { Wait-P7Task $binding $message @('succeeded') 90 | Out-Null }
}
if ($Scenario -eq 'takeover') { & (Join-Path $PSScriptRoot 'faults\F02-worker-crash.ps1') }
if ($Scenario -eq 'sql-recovery') { & (Join-Path $PSScriptRoot 'faults\F06-postgres.ps1'); & (Join-Path $PSScriptRoot 'faults\F07-mysql.ps1') }
Write-Host "Smoke passed mode=$Mode scenario=$Scenario"
