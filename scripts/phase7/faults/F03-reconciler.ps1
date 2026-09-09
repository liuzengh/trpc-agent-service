. (Join-Path $PSScriptRoot '..\assert.ps1')
$stopped=''
try {
  Invoke-P7Compose -HA -CommandArgs @('stop','gateway-standby')
  Wait-P7Http 'http://127.0.0.1:18080/readyz' 200 60
  $deadline=(Get-Date).AddSeconds(30)
  do { $primary=Invoke-P7Admin '/api/v1/admin/reconciler'; if ($primary.is_leader) { break }; Start-Sleep -Seconds 1 } while ((Get-Date) -lt $deadline)
  if (-not $primary.is_leader) { throw 'primary reconciler did not establish the clean baseline leader' }

  Invoke-P7Compose -HA -CommandArgs @('--profile','ha','up','-d','gateway-standby')
  Wait-P7Http 'http://127.0.0.1:18084/readyz' 200 60
  $deadline=(Get-Date).AddSeconds(20)
  do { $primary=Invoke-P7Admin '/api/v1/admin/reconciler'; $standby=Invoke-P7Admin '/api/v1/admin/reconciler' -BaseUrl 'http://127.0.0.1:18084'; if ($primary.is_leader -ne $standby.is_leader) { break }; Start-Sleep -Seconds 1 } while ((Get-Date) -lt $deadline)
  if ($primary.is_leader -eq $standby.is_leader) { throw 'exactly one reconciler leader was not observed' }
  if (-not $primary.is_leader -or $standby.is_leader) { throw 'standby acquired leadership before the primary was stopped' }
  $stopped='gateway'
  Invoke-P7Compose -HA -CommandArgs @('kill','gateway')
  # The 5s leader lease and 2s acquisition loop are short, but allow a wider
  # bounded window for Windows/Docker scheduling after a hard container kill.
  $takenOver=$false
  $takeoverSamples=@()
  $deadline=(Get-Date).AddSeconds(30)
  do {
    try {
      $leaderState=Invoke-P7Admin '/api/v1/admin/reconciler' -BaseUrl 'http://127.0.0.1:18084'
      $takenOver=($leaderState.is_leader -eq $true)
      $takeoverSamples += ((Get-Date).ToString('o') + ' ' + ($leaderState | ConvertTo-Json -Compress))
    } catch {
      $takenOver=$false
      $takeoverSamples += ((Get-Date).ToString('o') + ' error=' + $_.Exception.Message)
    }
    if ($takenOver) { break }
    Start-Sleep -Seconds 1
  } while ((Get-Date) -lt $deadline)
  if (-not $takenOver) {
    New-Item -ItemType Directory -Force -Path $script:Phase7Evidence | Out-Null
    $takeoverSamples | Set-Content -LiteralPath (Join-Path $script:Phase7Evidence 'F03-takeover-samples.log') -Encoding utf8
    throw 'standby reconciler did not become leader'
  }
} finally {
  if ($stopped -eq 'gateway') { Invoke-P7Compose -HA -CommandArgs @('--profile','ha','up','-d','gateway') }
  Save-P7Evidence 'F03-reconciler'
  Invoke-P7Compose -HA -CommandArgs @('stop','gateway-standby')
  Wait-P7Http 'http://127.0.0.1:18080/readyz' 200 60
  $primaryRecovered=$false
  $deadline=(Get-Date).AddSeconds(30)
  do { try { $primary=Invoke-P7Admin '/api/v1/admin/reconciler'; $primaryRecovered=[bool]$primary.is_leader } catch { $primaryRecovered=$false }; if ($primaryRecovered) { break }; Start-Sleep -Seconds 1 } while ((Get-Date) -lt $deadline)
  if (-not $primaryRecovered) { throw 'primary reconciler did not recover after F03 cleanup' }
}
